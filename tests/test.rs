use dbshark::{Db, Setting};
use rand::seq::SliceRandom;
use rand::SeedableRng;
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

    _ = std::fs::remove_dir_all("test1");

    let db = Db::open(Path::new("test1"), Setting::default()).unwrap();
    let mut tx = db.update().unwrap();

    let mut bucket = tx.bucket("table1").unwrap();
    bucket.put(b"key00001", b"val00001").unwrap();
    db.force_checkpoint().unwrap();
    let result = bucket.get(b"key00001").unwrap();
    assert_eq!(Some(b"val00001".to_vec()), result);

    bucket.put(b"key00001", b"val00001_updated").unwrap();
    let result = bucket.get(b"key00001").unwrap();
    assert_eq!(Some(b"val00001_updated".to_vec()), result);

    let long_content = (0..16000).map(|i| (i % 100) as u8).collect::<Vec<_>>();
    bucket.put(b"key00001", &long_content).unwrap();
    let result = bucket.get(b"key00001").unwrap();
    assert_eq!(Some(long_content), result);

    bucket.put(b"key00001", b"val00001").unwrap();
    let result = bucket.get(b"key00001").unwrap();
    assert_eq!(Some(b"val00001".to_vec()), result);

    tx.commit().expect("commit must succeed");
    drop(db);

    let db = Db::open(Path::new("test1"), Setting::default()).unwrap();
    drop(db);
}

#[test]
fn test_db_btree() {
    setup();

    _ = std::fs::remove_dir_all("test_btree");

    let mut items = Vec::new();
    for i in 0..1000 {
        let key = format!("key{i:05}");
        let val = format!("val{i:0900}");
        items.push((key, val));
    }
    let mut rng = rand::rngs::StdRng::seed_from_u64(0);
    items.shuffle(&mut rng);

    let db = Db::open(Path::new("test_btree"), Setting::default()).unwrap();
    let mut tx = db.update().unwrap();

    let mut bucket = tx.bucket("table1").unwrap();
    for (key, val) in &items {
        bucket.put(key.as_bytes(), val.as_bytes()).unwrap();
    }

    for (key, val) in &items {
        let val_get = bucket.get(key.as_bytes()).unwrap();
        assert_eq!(
            val,
            &String::from_utf8(val_get.unwrap()).unwrap(),
            "failed at {key}-{val}"
        );
    }

    tx.commit().unwrap();
    db.shutdown().unwrap();
}

#[test]
fn test_db_rollback() {
    setup();

    _ = std::fs::remove_dir_all("test2");

    let db = Db::open(Path::new("test2"), Setting::default()).unwrap();

    // When a transaction is rollback, all the changes made in that
    // transaction will be undone and the next txn won't see them
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

    // creating transaction without rollback/commit previous txn
    // will automatically rollback the previous transaction.
    {
        let mut tx = db.update().unwrap();
        let mut bucket = tx.bucket("table1").unwrap();
        bucket.put(b"key00001", b"val00001").unwrap();
        let result = bucket.get(b"key00001").unwrap();
        assert_eq!(Some(b"val00001".to_vec()), result);
    }
    {
        let mut tx = db.update().unwrap();
        let bucket = tx.bucket("table1").unwrap();
        let result = bucket.get(b"key00001").unwrap();
        assert_eq!(None, result);
        tx.rollback().unwrap();
    }

    let db = Db::open(Path::new("test2"), Setting::default()).unwrap();
    drop(db);
}

#[test]
fn test_db_recovery1() {
    setup();

    let dir = tempfile::tempdir().unwrap();

    let db = Db::open(Path::new(dir.path()), Setting::default()).unwrap();
    let mut tx = db.update().unwrap();
    let mut bucket = tx.bucket("table1").unwrap();
    bucket.put(b"key00001", b"val00001").unwrap();
    db.force_checkpoint().unwrap();
    let result = bucket.get(b"key00001").unwrap();
    assert_eq!(Some(b"val00001".to_vec()), result);
    drop(tx);
    drop(db);

    let db = Db::open(dir.path(), Setting::default()).unwrap();
    let mut tx = db.update().unwrap();
    let bucket = tx.bucket("table1").unwrap();
    let result = bucket.get(b"key00001").unwrap();
    assert_eq!(None, result);
    drop(tx);
    drop(db);
}
