use async_trait::async_trait;
use rocksdb::{DB, Options, WriteBatch as RocksWriteBatch};
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::RwLock;
use lru::LruCache;
use std::path::PathBuf;

use crate::config::Config;
use crate::storage::{Storage, StorageReader, Modify};
use crate::engine_util::{DBIterator, DBItem, RocksDBIterator};

#[derive(Copy, Clone, Eq, PartialEq)]
enum Tier { Hot, Warm, Cold }

pub struct TieredStorage {
    hot: Arc<DB>,
    warm: Arc<DB>,
    cold: Arc<DB>,

    // 访问计数：滚动窗口内 key 的访问次数
    access: Arc<RwLock<LruCache<Vec<u8>, u32>>>,

    // 阈值与滞后
    promote_threshold: u32,
    demote_threshold: u32,
    hysteresis: u32,
}

impl TieredStorage {
    pub fn new(conf: &Config) -> anyhow::Result<Self> {
        let mut open = |path_opt: &Option<String>, fallback: &str| -> anyhow::Result<DB> {
            let p = path_opt.as_ref().map(|s| s.as_str()).unwrap_or(fallback);
            let mut opts = Options::default();
            opts.create_if_missing(true);
            std::fs::create_dir_all(p)?;
            Ok(DB::open(&opts, p)?)
        };

        // 默认情况下，将三层数据分别放在 db_path/hot, db_path/warm, db_path/cold，避免对同一路径进行多次打开导致 RocksDB 锁冲突
        let default_hot = PathBuf::from(&conf.db_path).join("hot").to_string_lossy().to_string();
        let default_warm = PathBuf::from(&conf.db_path).join("warm").to_string_lossy().to_string();
        let default_cold = PathBuf::from(&conf.db_path).join("cold").to_string_lossy().to_string();

        let hot = open(&conf.hot_path, &default_hot)?;
        let warm = open(&conf.warm_path, &default_warm)?;
        let cold = open(&conf.cold_path, &default_cold)?;

        Ok(Self {
            hot: Arc::new(hot),
            warm: Arc::new(warm),
            cold: Arc::new(cold),
            access: Arc::new(RwLock::new(LruCache::new(NonZeroUsize::new(50_000).unwrap()))),
            promote_threshold: conf.promote_threshold,
            demote_threshold: conf.demote_threshold,
            hysteresis: conf.hysteresis,
        })
    }

    fn db_of(&self, t: Tier) -> &DB {
        match t {
            Tier::Hot => &self.hot,
            Tier::Warm => &self.warm,
            Tier::Cold => &self.cold,
        }
    }

    fn key_with_cf(cf: &str, key: &[u8]) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(cf.len() + 1 + key.len());
        encoded.extend_from_slice(cf.as_bytes());
        encoded.push(b'_');
        encoded.extend_from_slice(key);
        encoded
    }

    async fn bump_access(&self, encoded_key: &[u8]) {
        let mut acc = self.access.write().await;
        let k = encoded_key.to_vec();
        if let Some(c) = acc.get_mut(&k) {
            *c = c.saturating_add(1);
        } else {
            acc.put(k, 1);
        }
    }

    fn decide_target(&self, current: Tier, cnt: u32) -> Tier {
        // 滞后：避免热层轻易降级、冷层轻易升级
        match current {
            Tier::Hot => {
                if cnt + self.hysteresis <= self.demote_threshold { Tier::Warm } else { Tier::Hot }
            }
            Tier::Cold => {
                if cnt >= self.promote_threshold + self.hysteresis { Tier::Warm } else { Tier::Cold }
            }
            Tier::Warm => {
                if cnt >= self.promote_threshold { Tier::Hot }
                else if cnt <= self.demote_threshold { Tier::Cold }
                else { Tier::Warm }
            }
        }
    }

    pub async fn rebalance_once(&self) -> anyhow::Result<()> {
        // 简化：对三层各扫描一段，按访问计数决定去向；每轮限制最多搬迁若干条，避免阻塞
        const MAX_MOVE_PER_ROUND: usize = 1000;
        let mut moved = 0usize;

        for (tier, db) in [(Tier::Hot, &self.hot), (Tier::Warm, &self.warm), (Tier::Cold, &self.cold)] {
            // 从头扫描，真实生产可记忆上次位置并分页
            let mut it = db.iterator(rocksdb::IteratorMode::Start);
            while moved < MAX_MOVE_PER_ROUND {
                if let Some(Ok((k, v))) = it.next() {
                    let cnt = {
                        let acc = self.access.read().await;
                        acc.peek(&k.to_vec()).cloned().unwrap_or(0)
                    };
                    let target = self.decide_target(tier, cnt);
                    if target != tier {
                        self.db_of(target).put(&k, &v)?;
                        db.delete(&k)?;
                        moved += 1;
                    }
                } else {
                    break;
                }
            }
            if moved >= MAX_MOVE_PER_ROUND { break; }
        }

        // 窗口滚动：衰减计数，避免一直累积（简化实现：清零）
        {
            let mut acc = self.access.write().await;
            acc.clear();
        }
        Ok(())
    }
}

/********** Reader 与合并迭代器 **********/

pub struct TieredReader {
    hot: Arc<DB>,
    warm: Arc<DB>,
    cold: Arc<DB>,
    access: Arc<RwLock<LruCache<Vec<u8>, u32>>>,
}

impl TieredReader {
    fn new(h: Arc<DB>, w: Arc<DB>, c: Arc<DB>, access: Arc<RwLock<LruCache<Vec<u8>, u32>>>) -> Self {
        Self { hot: h, warm: w, cold: c, access }
    }
}

#[async_trait]
impl StorageReader for TieredReader {
    async fn get_cf(&self, cf: &str, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let k = TieredStorage::key_with_cf(cf, key);
        if let Some(v) = self.hot.get(&k)? {
            // 命中热
            {
                let mut acc = self.access.write().await;
                let cur = acc.peek(&k).cloned().unwrap_or(0);
                acc.put(k.clone(), cur.saturating_add(1));
            }
            return Ok(Some(v.to_vec()));
        }
        if let Some(v) = self.warm.get(&k)? {
            {
                let mut acc = self.access.write().await;
                let cur = acc.peek(&k).cloned().unwrap_or(0);
                acc.put(k.clone(), cur.saturating_add(1));
            }
            return Ok(Some(v.to_vec()));
        }
        if let Some(v) = self.cold.get(&k)? {
            {
                let mut acc = self.access.write().await;
                let cur = acc.peek(&k).cloned().unwrap_or(0);
                acc.put(k.clone(), cur.saturating_add(1));
            }
            return Ok(Some(v.to_vec()));
        }
        Ok(None)
    }

    fn iter_cf(&self, cf: &str) -> Box<dyn DBIterator> {
        // 三路有序合并：hot + warm + cold
        let prefix = format!("{}_", cf);
        let i1 = RocksDBIterator::new(self.hot.clone(), prefix.clone());
        let i2 = RocksDBIterator::new(self.warm.clone(), prefix.clone());
        let i3 = RocksDBIterator::new(self.cold.clone(), prefix);
        Box::new(MergedIterator::new(i1, i2, i3))
    }

    fn close(&self) {}
}

/********** MergedIterator：合并三个按前缀有序的迭代器 **********/

struct MergedIterator {
    iters: Vec<Box<dyn DBIterator>>,
    current: Option<(Vec<u8>, Vec<u8>, usize)>, // key,value,prefix_len
}

impl MergedIterator {
    fn new(i1: RocksDBIterator, i2: RocksDBIterator, i3: RocksDBIterator) -> Self {
        let mut this = Self { iters: vec![Box::new(i1), Box::new(i2), Box::new(i3)], current: None };
        this.update_current();
        this
    }

    fn update_current(&mut self) {
        // 选择最小 key 的项
        let mut best: Option<(Vec<u8>, Vec<u8>, usize, usize)> = None; // key,value,prefix_len,iter_idx
        for (idx, it) in self.iters.iter_mut().enumerate() {
            if it.valid() {
                let item = it.item();
                let mut k = Vec::new();
                let key = item.key_copy(&mut k);
                let v = item.value().unwrap_or_default();
                let prefix_len = 0; // RocksDBIterator 内部已切 cf_，其 DBItem.key() 返回去掉前缀后的 key
                if let Some((bk, _, _, _)) = &best {
                    if key < *bk {
                        best = Some((key, v, prefix_len, idx));
                    }
                } else {
                    best = Some((key, v, prefix_len, idx));
                }
            }
        }
        if let Some((k, v, p, idx)) = best {
            self.current = Some((k, v, p));
            // 将被选择的迭代器前进到下一个
            self.iters[idx].next();
        } else {
            self.current = None;
        }
    }
}

impl DBIterator for MergedIterator {
    fn item(&self) -> Box<dyn DBItem> {
        if let Some((ref k, ref v, p)) = self.current {
            Box::new(crate::engine_util::CFItem::new(k.clone(), v.clone(), p))
        } else {
            Box::new(crate::engine_util::CFItem::new(vec![], vec![], 0))
        }
    }
    fn valid(&self) -> bool { self.current.is_some() }
    fn next(&mut self) { self.update_current(); }
    fn seek(&mut self, _key: &[u8]) {
        // 简化：暂不支持跨三迭代器的精准 seek，可按需扩展
        self.update_current();
    }
    fn close(&mut self) {}
}

/********** 将 TieredStorage 适配为 Storage trait **********/

#[async_trait]
impl Storage for TieredStorage {
    async fn start(&self) -> anyhow::Result<()> {
        log::info!("TieredStorage started");
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        log::info!("TieredStorage stopped");
        Ok(())
    }

    async fn write(&self, batch: Vec<Modify>) -> anyhow::Result<()> {
        // 策略：所有新写入默认落到温层
        let mut wb = RocksWriteBatch::default();
        for m in batch {
            match m {
                Modify::Put(put) => {
                    let ek = Self::key_with_cf(&put.cf, &put.key);
                    wb.put(&ek, &put.value);
                    // 新写入也可以适当增加访问热度，避免立即被降级
                    self.bump_access(&ek).await;
                }
                Modify::Delete(del) => {
                    let ek = Self::key_with_cf(&del.cf, &del.key);
                    // 三层都删，确保不存在多份
                    self.hot.delete(&ek)?;
                    self.warm.delete(&ek)?;
                    self.cold.delete(&ek)?;
                }
            }
        }
        // 批量写温层
        self.warm.write(wb)?;
        Ok(())
    }

    async fn reader(&self) -> anyhow::Result<Box<dyn StorageReader>> {
        Ok(Box::new(TieredReader::new(
            self.hot.clone(),
            self.warm.clone(),
            self.cold.clone(),
            self.access.clone(),
        )))
    }
}