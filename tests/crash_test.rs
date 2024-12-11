use dbshark::experiment::{Db, JoinHandle, Runtime, Setting, SimulatedRuntime};
use rand::{Rng, SeedableRng};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use std::sync::Once;
static INIT: Once = Once::new();
fn setup() {
    INIT.call_once(|| {
        env_logger::init();
    });
}

#[test]
fn test_db_crashing() {
    setup();

    let n = 100_000_000usize;
    let p = 100usize;
    // TODO: use random seed
    let seed = 0u64;
    let mut runtime = SimulatedRuntime::new(seed);

    for i in 0..10 {
        log::info!(i; "running program");

        runtime.run(move || {
            let path = PathBuf::from("/");
            let db = Db::<SimulatedRuntime>::open(
                &path,
                Setting {
                    checkpoint_period: Duration::from_secs(5),
                },
            )
            .unwrap();
            let db = Arc::new(db);

            let mut handles = vec![];
            for _ in 0..20 {
                let db = db.clone();
                let handle = SimulatedRuntime::spawn(move || loop {
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
