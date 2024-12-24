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
    let mut runtime = SimulatedRuntime::new(seed);

    for i in 0..100 {
        log::info!(i; "running program");

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
                let handle = SimulatedRuntime::spawn("worker", move || loop {
                    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

                    let mut tx = db.update().expect("cannot create write tx");
                    let mut bucket = tx.bucket("table1").unwrap();

                    let x = rng.gen_range(0..n);
                    for i in 0..p {
                        let x = x + i * n;
                        let key = format!("key{x:05}");
                        let val = format!("val{x:05}");
                        bucket.put(key.as_bytes(), val.as_bytes()).unwrap();
                    }

                    if rng.gen_bool(0.5) {
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

pub fn simulate_concurrent_checkpoint_and_rollback(seed: u64) {
    setup();

    let mut runtime = SimulatedRuntime::new(seed);
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
