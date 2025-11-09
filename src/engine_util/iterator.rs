use rocksdb::{DBRawIterator, DB};
use std::sync::{Arc, Mutex};

pub trait DBItem: Send + Sync {
    fn key(&self) -> &[u8];
    fn key_copy(&self, dst: &mut Vec<u8>) -> Vec<u8>;
    fn value(&self) -> anyhow::Result<Vec<u8>>;
    fn value_size(&self) -> usize;
    fn value_copy(&self, dst: &mut Vec<u8>) -> anyhow::Result<Vec<u8>>;
}

pub trait DBIterator: Send + Sync {
    fn item(&self) -> Box<dyn DBItem>;
    fn valid(&self) -> bool;
    fn next(&mut self);
    fn seek(&mut self, key: &[u8]);
    fn close(&mut self);
}

pub struct CFItem {
    key: Vec<u8>,
    value: Vec<u8>,
    prefix_len: usize,
}

impl CFItem {
    pub fn new(key: Vec<u8>, value: Vec<u8>, prefix_len: usize) -> Self {
        Self {
            key,
            value,
            prefix_len,
        }
    }
}

impl DBItem for CFItem {
    fn key(&self) -> &[u8] {
        &self.key[self.prefix_len..]
    }
    
    fn key_copy(&self, dst: &mut Vec<u8>) -> Vec<u8> {
        dst.clear();
        dst.extend_from_slice(&self.key[self.prefix_len..]);
        dst.clone()
    }
    
    fn value(&self) -> anyhow::Result<Vec<u8>> {
        Ok(self.value.clone())
    }
    
    fn value_size(&self) -> usize {
        self.value.len()
    }
    
    fn value_copy(&self, dst: &mut Vec<u8>) -> anyhow::Result<Vec<u8>> {
        dst.clear();
        dst.extend_from_slice(&self.value);
        Ok(dst.clone())
    }
}

pub struct RocksDBIterator {
    #[allow(dead_code)]
    db: Arc<DB>,
    prefix: String,
    current_key: Option<Vec<u8>>,
    current_value: Option<Vec<u8>>,
    prefix_bytes: Vec<u8>,
    iter: Mutex<Option<Box<DBRawIterator<'static>>>>, // 使用 Mutex 保护迭代器
}

impl RocksDBIterator {
    pub fn new(db: Arc<DB>, prefix: String) -> Self {
        let prefix_bytes = prefix.as_bytes().to_vec();
        let iter = db.raw_iterator();
        // 使用 unsafe 将生命周期转换为 'static
        // 这是安全的，因为 Arc<DB> 拥有 DB，生命周期实际上是 'static
        let iter_static: DBRawIterator<'static> = unsafe { std::mem::transmute(iter) };
        let iter_box = Box::new(iter_static);
        
        let mut this = Self {
            db,
            prefix: prefix.clone(),
            current_key: None,
            current_value: None,
            prefix_bytes,
            iter: Mutex::new(Some(iter_box)),
        };
        this.seek_to_first();
        this
    }
    
    fn with_iter<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut DBRawIterator<'static>) -> R,
    {
        let mut iter_guard = self.iter.lock().unwrap();
        if let Some(ref mut iter) = *iter_guard {
            f(iter)
        } else {
            panic!("Iterator has been closed");
        }
    }
    
    fn seek_to_first(&mut self) {
        self.with_iter(|iter| {
            iter.seek_to_first();
        });
        self.update_current();
    }
    
    fn update_current(&mut self) {
        let prefix = self.prefix.clone();
        let (key, value) = self.with_iter(|iter| {
            if iter.valid() {
                if let Some(key) = iter.key() {
                    let key_str = String::from_utf8_lossy(key);
                    if key_str.starts_with(&prefix) {
                        let key_vec = key.to_vec();
                        let value_vec = iter.value().map(|v| v.to_vec());
                        (Some(key_vec), value_vec)
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            }
        });
        self.current_key = key;
        self.current_value = value;
    }
}

impl DBIterator for RocksDBIterator {
    fn item(&self) -> Box<dyn DBItem> {
        if let (Some(ref key), Some(ref value)) = (&self.current_key, &self.current_value) {
            Box::new(CFItem::new(key.clone(), value.clone(), self.prefix.len()))
        } else {
            Box::new(CFItem::new(vec![], vec![], 0))
        }
    }
    
    fn valid(&self) -> bool {
        self.current_key.is_some() && self.current_value.is_some()
    }
    
    fn next(&mut self) {
        self.with_iter(|iter| {
            iter.next();
        });
        self.update_current();
    }
    
    fn seek(&mut self, key: &[u8]) {
        let mut seek_key = self.prefix_bytes.clone();
        seek_key.extend_from_slice(key);
        self.with_iter(|iter| {
            iter.seek(&seek_key);
        });
        self.update_current();
    }
    
    fn close(&mut self) {
        let mut iter_guard = self.iter.lock().unwrap();
        *iter_guard = None;
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use rocksdb::{DB, Options};
    use tempfile::TempDir;

    fn create_test_db() -> (DB, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, temp_dir.path()).unwrap();
        (db, temp_dir)
    }

    #[test]
    fn test_iterator() {
        let (db, _temp_dir) = create_test_db();
        let db = Arc::new(db); // 包装为 Arc

        // 写入测试数据
        use crate::engine_util::put_cf;
        put_cf(&db, "default", b"key1", b"value1").unwrap();
        put_cf(&db, "default", b"key2", b"value2").unwrap();
        put_cf(&db, "default", b"key3", b"value3").unwrap();
        put_cf(&db, "write", b"key1", b"value1").unwrap(); // 不同 CF

        // 创建迭代器
        let mut rocksdb_iter = RocksDBIterator::new(db.clone(), "default_".to_string());

        let mut keys = Vec::new();
        while rocksdb_iter.valid() {
            let item = rocksdb_iter.item();
            keys.push(item.key().to_vec());
            rocksdb_iter.next();
        }

        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], b"key1");
        assert_eq!(keys[1], b"key2");
        assert_eq!(keys[2], b"key3");
    }

    #[test]
    fn test_iterator_seek() {
        let (db, _temp_dir) = create_test_db();
        let db = Arc::new(db); // 包装为 Arc

        use crate::engine_util::put_cf;
        put_cf(&db, "default", b"key1", b"value1").unwrap();
        put_cf(&db, "default", b"key2", b"value2").unwrap();
        put_cf(&db, "default", b"key3", b"value3").unwrap();

        let mut rocksdb_iter = RocksDBIterator::new(db.clone(), "default_".to_string());

        // Seek 到 key2
        rocksdb_iter.seek(b"key2");
        assert!(rocksdb_iter.valid());

        let item = rocksdb_iter.item();
        assert_eq!(item.key(), b"key2");
    }
}