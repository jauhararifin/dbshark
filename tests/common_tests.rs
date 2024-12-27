use dbshark::{Db, JoinHandle, Runtime, Setting, SimulatedRuntime};
use rand::{Rng, SeedableRng};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;

static INIT: Once = Once::new();
pub fn setup() {
    INIT.call_once(|| {
        env_logger::init();
    });
}

pub fn simulate_db_crashing(seed: u64) {
    setup();

    let n = 100_000_000usize;
    let p = 100usize;
    let mut runtime = SimulatedRuntime::new(seed, 0.1);

    let rng = Arc::new(parking_lot::Mutex::new(rand::rngs::StdRng::seed_from_u64(
        seed,
    )));

    for i in 0..100 {
        log::info!(i; "running program");

        let rng = rng.clone();
        runtime.run(move || {
            let path = PathBuf::from("/");
            let db = Db::<SimulatedRuntime>::open(
                &path,
                Setting {
                    checkpoint_period: Duration::from_secs(5),
                    buffer_size: 15,
                },
            )
            .unwrap();
            let db = Arc::new(db);

            let mut handles = vec![];
            for _ in 0..20 {
                let db = db.clone();
                let rng = rng.clone();
                let handle = SimulatedRuntime::spawn("worker", move || loop {
                    let mut tx = db.update().expect("cannot create write tx");
                    let mut bucket = tx.bucket("table1").unwrap();

                    let x = rng.lock().gen_range(0..n);
                    for i in 0..p {
                        let x = x + i * n;
                        let key = format!("key{x:05}");
                        let val = format!("val{x:05}");
                        bucket.put(key.as_bytes(), val.as_bytes()).unwrap();
                    }

                    if rng.lock().gen_bool(0.5) {
                        tx.commit().unwrap()
                    } else {
                        tx.rollback().unwrap()
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join();
            }
        });
    }
}

pub fn simulate_db_overflow_crashing(seed: u64) {
    setup();

    let n = 20usize;
    let p = 100usize;
    let mut runtime = SimulatedRuntime::new(seed, 0.01);

    let rng = Arc::new(parking_lot::Mutex::new(rand::rngs::StdRng::seed_from_u64(
        seed,
    )));

    let success_read = Arc::new(parking_lot::Mutex::new(0));

    for i in 0..50 {
        log::info!(i; "running program");

        let rng = rng.clone();
        let success_read = success_read.clone();
        runtime.run(move || {
            let path = PathBuf::from("/");
            let db = Db::<SimulatedRuntime>::open(
                &path,
                Setting {
                    checkpoint_period: Duration::from_secs(5),
                    buffer_size: 15,
                },
            )
            .unwrap();
            let db = Arc::new(db);

            let mut handles = vec![];
            for _ in 0..20 {
                let db = db.clone();
                let rng = rng.clone();
                let handle = SimulatedRuntime::spawn("write_worker", move || loop {
                    let mut tx = db.update().expect("cannot create write tx");
                    let mut bucket = tx.bucket("table1").unwrap();

                    let x = rng.lock().gen_range(0..n);
                    let s: u64 = rng.lock().gen();
                    let mut r = rand::rngs::StdRng::seed_from_u64(s);

                    for i in 0..p {
                        let x = x + i * n;
                        let key = format!("key{x:05}");

                        let mut val = s.to_be_bytes().to_vec();
                        let value_len = r.gen_range(0..40960);
                        for _ in 0..value_len {
                            val.push(r.gen());
                        }

                        bucket.put(key.as_bytes(), &val).unwrap();
                    }

                    if rng.lock().gen_bool(0.5) {
                        tx.commit().unwrap()
                    } else {
                        tx.rollback().unwrap()
                    }
                });
                handles.push(handle);
            }

            for _ in 0..100 {
                let db = db.clone();
                let rng = rng.clone();
                let success_read = success_read.clone();

                let handle = SimulatedRuntime::spawn("read_worker", move || loop {
                    let tx = db.read().expect("cannot create write tx");
                    let bucket = tx.bucket("table1").unwrap().unwrap();

                    let x = rng.lock().gen_range(0..n);

                    let key = format!("key{x:05}");
                    let result = bucket.get(key.as_bytes()).unwrap();
                    let Some(result) = result else {
                        return;
                    };
                    *success_read.lock() += 1;
                    let s = u64::from_be_bytes(result[..8].try_into().unwrap());
                    let mut r = rand::rngs::StdRng::seed_from_u64(s);

                    for i in 0..p {
                        let x = x + i * n;
                        let key = format!("key{x:05}");
                        let result = bucket.get(key.as_bytes()).unwrap().unwrap();

                        let mut expected = s.to_be_bytes().to_vec();
                        let value_len = r.gen_range(0..40960);
                        for _ in 0..value_len {
                            expected.push(r.gen());
                        }

                        assert_eq!(expected, result);
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join();
            }
        });
    }

    assert!(*success_read.lock() > 0);
}

pub fn simulate_concurrent_checkpoint_and_rollback(seed: u64) {
    setup();

    let mut runtime = SimulatedRuntime::new(seed, 0.001);
    for iteration in 0..100 {
        log::trace!("start iteration {iteration}");
        let result = runtime.run(move || {
            let db = Db::<SimulatedRuntime>::open(Path::new("/"), Setting::default()).unwrap();
            let db = Arc::new(db);

            let h1 = {
                let db = db.clone();
                SimulatedRuntime::spawn("rollback_worker", move || {
                    for i in 0..1000 {
                        log::trace!("rollback transaction round#{i}");
                        let mut tx = db.update().unwrap();
                        let mut bucket = tx.bucket("table1").unwrap();
                        for i in 0..3 {
                            let key = format!("key{i:05}");
                            let val = format!("val{i:05}");
                            bucket.put(key.as_bytes(), val.as_bytes()).unwrap();
                        }
                        tx.rollback().unwrap();
                    }
                })
            };

            let h2 = {
                let db = db.clone();
                SimulatedRuntime::spawn("checkpoint_worker", move || {
                    for i in 0..100 {
                        log::trace!("force checkpoint round#{i}");
                        db.force_checkpoint().unwrap();
                    }
                })
            };

            h1.join();
            h2.join();

            let db = Arc::into_inner(db).unwrap();
            db.shutdown().unwrap();
        });

        if result.is_failed() {
            panic!("iteration {iteration} is failing");
        }
    }
}
