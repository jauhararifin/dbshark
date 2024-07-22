use dbest::{Db, Setting};
use std::path::Path;

use std::sync::Once;
static INIT: Once = Once::new();
fn setup() {
    INIT.call_once(|| {
        env_logger::init();
    });
}

#[test]
fn test_db_happy_path() {
    setup();

    _ = std::fs::remove_file("test.db");
    _ = std::fs::remove_file("test.wal");

    let db = Db::open(Path::new("test.db"), Setting::default()).unwrap();
    let mut tx = db.update().unwrap();

    let mut bucket = tx.bucket("table1").unwrap();
    bucket.put(b"key00001", b"val00001").unwrap();
    let result = bucket.get(b"key00001").unwrap();
    assert_eq!(Some(b"val00001".to_vec()), result);

    tx.commit().expect("commit must succeed");
    drop(db);

    let db = Db::open(Path::new("test.db"), Setting::default()).unwrap();
    drop(db);
}

#[test]
fn test_db_rollback() {
    setup();

    _ = std::fs::remove_file("test.db");
    _ = std::fs::remove_file("test.wal");

    let db = Db::open(Path::new("test.db"), Setting::default()).unwrap();

    {
        let mut tx = db.update().unwrap();
        let mut bucket = tx.bucket("table1").unwrap();
        bucket.put(b"key00001", b"val00001").unwrap();
        let result = bucket.get(b"key00001").unwrap();
        assert_eq!(Some(b"val00001".to_vec()), result);
        tx.rollback().expect("rollback must succeed");
    }

    {
        let mut tx = db.update().unwrap();
        let bucket = tx.bucket("table1").unwrap();
        let result = bucket.get(b"key00001").unwrap();
        assert_eq!(None, result);
        tx.rollback().unwrap();
    }

    let db = Db::open(Path::new("test.db"), Setting::default()).unwrap();
    drop(db);
}
