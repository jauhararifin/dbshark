use dbest::Db;
use std::path::Path;

#[test]
fn test_db_happy_path() {
    _ = std::fs::remove_file("test.db");
    _ = std::fs::remove_file("test.wal");

    let db = Db::open(Path::new("test.db")).unwrap();
    let mut tx = db.update().unwrap();
    tx.put(b"key00001", b"val00001").unwrap();
    tx.commit();
    drop(db);

    let db = Db::open(Path::new("test.db")).unwrap();
    drop(db);
}
