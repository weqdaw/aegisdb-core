use rocksdb::{DB, WriteBatch as RocksDBWriteBatch, WriteOptions};
use crate::engine_util::key_with_cf;

pub struct WriteBatch {
    entries: Vec<BatchEntry>,
    size: usize,
    safe_point: usize,
    safe_point_size: usize,
}

enum BatchEntry {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl WriteBatch {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            size: 0,
            safe_point: 0,
            safe_point_size: 0,
        }
    }
    
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    
    pub fn set_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) {
        let encoded_key = key_with_cf(cf, key);
        self.entries.push(BatchEntry::Put {
            key: encoded_key,
            value: value.to_vec(),
        });
        self.size += key.len() + value.len();
    }
    
    pub fn delete_cf(&mut self, cf: &str, key: &[u8]) {
        let encoded_key = key_with_cf(cf, key);
        self.entries.push(BatchEntry::Delete { key: encoded_key });
        self.size += key.len();
    }
    
    pub fn set_safe_point(&mut self) {
        self.safe_point = self.entries.len();
        self.safe_point_size = self.size;
    }
    
    pub fn rollback_to_safe_point(&mut self) {
        self.entries.truncate(self.safe_point);
        self.size = self.safe_point_size;
    }
    
    /// 写入数据库（使用默认写选项）
    pub fn write_to_db(&self, db: &DB) -> anyhow::Result<()> {
        if self.entries.is_empty() {
            return Ok(());
        }
        
        let mut wb = RocksDBWriteBatch::default();
        
        for entry in &self.entries {
            match entry {
                BatchEntry::Put { key, value } => {
                    wb.put(key, value);
                }
                BatchEntry::Delete { key } => {
                    wb.delete(key);
                }
            }
        }
        
        db.write(wb)?;
        Ok(())
    }
    
    /// 写入数据库（使用指定的写选项，可以控制 WAL 行为）
    pub fn write_to_db_with_options(&self, db: &DB, write_opts: &WriteOptions) -> anyhow::Result<()> {
        if self.entries.is_empty() {
            return Ok(());
        }
        
        let mut wb = RocksDBWriteBatch::default();
        
        for entry in &self.entries {
            match entry {
                BatchEntry::Put { key, value } => {
                    wb.put(key, value);
                }
                BatchEntry::Delete { key } => {
                    wb.delete(key);
                }
            }
        }
        
        db.write_opt(wb, write_opts)?;
        Ok(())
    }
    
    pub fn reset(&mut self) {
        self.entries.clear();
        self.size = 0;
        self.safe_point = 0;
        self.safe_point_size = 0;
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
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
    fn test_write_batch_put_and_delete() {
        let (db, _temp_dir) = create_test_db();

        let mut wb = WriteBatch::new();
        wb.set_cf("default", b"key1", b"value1");
        wb.set_cf("default", b"key2", b"value2");
        wb.write_to_db(&db).unwrap();

        // 验证写入
        assert_eq!(db.get(b"default_key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db.get(b"default_key2").unwrap(), Some(b"value2".to_vec()));

        // 测试删除
        let mut wb2 = WriteBatch::new();
        wb2.delete_cf("default", b"key1");
        wb2.write_to_db(&db).unwrap();

        assert_eq!(db.get(b"default_key1").unwrap(), None);
        assert_eq!(db.get(b"default_key2").unwrap(), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_write_batch_safe_point() {
        let (db, _temp_dir) = create_test_db();

        let mut wb = WriteBatch::new();
        wb.set_cf("default", b"key1", b"value1");
        wb.set_safe_point();
        wb.set_cf("default", b"key2", b"value2");
        wb.set_cf("default", b"key3", b"value3");

        // 回滚到安全点
        wb.rollback_to_safe_point();
        assert_eq!(wb.len(), 1);

        wb.write_to_db(&db).unwrap();
        assert_eq!(db.get(b"default_key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db.get(b"default_key2").unwrap(), None);
        assert_eq!(db.get(b"default_key3").unwrap(), None);
    }

    #[test]
    fn test_write_batch_reset() {
        let mut wb = WriteBatch::new();
        wb.set_cf("default", b"key1", b"value1");
        assert_eq!(wb.len(), 1);

        wb.reset();
        assert_eq!(wb.len(), 0);
    }
}