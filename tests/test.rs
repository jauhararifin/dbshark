use dbest::{Db, Setting};
use std::path::Path;

#[test]
fn test_db_happy_path() {
    env_logger::init();

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
