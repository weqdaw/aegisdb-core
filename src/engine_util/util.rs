use rocksdb::DB;

pub fn key_with_cf(cf: &str, key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(cf.len() + 1 + key.len());
    encoded.extend_from_slice(cf.as_bytes());
    encoded.push(b'_');
    encoded.extend_from_slice(key);
    encoded
}

pub fn get_cf(db: &DB, cf: &str, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
    let encoded_key = key_with_cf(cf, key);
    match db.get(&encoded_key)? {
        Some(value) => Ok(Some(value.to_vec())),
        None => Ok(None),
    }
}

pub fn put_cf(db: &DB, cf: &str, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
    use rocksdb::WriteBatch;
    let mut wb = WriteBatch::default();
    let encoded_key = key_with_cf(cf, key);
    wb.put(&encoded_key, value);
    db.write(wb)?;
    Ok(())
}

pub fn delete_cf(db: &DB, cf: &str, key: &[u8]) -> anyhow::Result<()> {
    use rocksdb::WriteBatch;
    let mut wb = WriteBatch::default();
    let encoded_key = key_with_cf(cf, key);
    wb.delete(&encoded_key);
    db.write(wb)?;
    Ok(())
}

pub fn exceed_end_key(current: &[u8], end_key: &[u8]) -> bool {
    if end_key.is_empty() {
        return false;
    }
    current >= end_key
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
    fn test_key_with_cf() {
        let key = key_with_cf("default", b"key1");
        assert_eq!(key, b"default_key1");

        let key2 = key_with_cf("write", b"test");
        assert_eq!(key2, b"write_test");
    }

    #[test]
    fn test_get_cf() {
        let (db, _temp_dir) = create_test_db();

        // 先写入
        put_cf(&db, "default", b"key1", b"value1").unwrap();

        // 读取
        let value = get_cf(&db, "default", b"key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // 不存在的键
        let value2 = get_cf(&db, "default", b"key2").unwrap();
        assert_eq!(value2, None);
    }

    #[test]
    fn test_put_cf() {
        let (db, _temp_dir) = create_test_db();

        put_cf(&db, "default", b"key1", b"value1").unwrap();
        assert_eq!(db.get(b"default_key1").unwrap(), Some(b"value1".to_vec()));
    }

    #[test]
    fn test_delete_cf() {
        let (db, _temp_dir) = create_test_db();

        put_cf(&db, "default", b"key1", b"value1").unwrap();
        delete_cf(&db, "default", b"key1").unwrap();
        assert_eq!(db.get(b"default_key1").unwrap(), None);
    }

    #[test]
    fn test_exceed_end_key() {
        assert!(!exceed_end_key(b"a", b"b"));
        assert!(exceed_end_key(b"b", b"a"));
        assert!(exceed_end_key(b"a", b"a"));
        assert!(!exceed_end_key(b"a", b""));
    }
}