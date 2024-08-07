use dbshark::{Db, Setting};
use rand::rngs::StdRng;
use rand::{thread_rng, Rng, SeedableRng};
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

fn main() {
    let is_worker = std::env::var("DBSHARK_TEST_WORKER").unwrap_or_default();
    if is_worker == "1" {
        return worker();
    }

    let dir = tempfile::tempdir().unwrap();
    println!("test started on {:?}", dir.path());

    let myself = std::env::args().next().expect("missing first arg");

    let t = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let rng = Arc::new(Mutex::new(StdRng::seed_from_u64(t)));

    loop {
        println!("restarting db test");

        let child = Arc::new(Mutex::new(
            Command::new(&myself)
                .arg(dir.path())
                .env("DBSHARK_TEST_WORKER", "1")
                .spawn()
                .expect("command failed to start"),
        ));

        std::thread::scope(|s| {
            let (send, recv) = std::sync::mpsc::channel::<()>();

            let handle = {
                let child = child.clone();
                let rng = rng.clone();
                s.spawn(move || {
                    let ms = rng.lock().unwrap().gen_range(5..10 * 1000);
                    let res = recv.recv_timeout(Duration::from_millis(ms));
                    if matches!(res, Err(std::sync::mpsc::RecvTimeoutError::Timeout)) {
                        child.lock().unwrap().kill().expect("cannot kill child");
                    }
                })
            };

            let status = loop {
                let Some(status) = child.lock().unwrap().try_wait().unwrap() else {
                    sleep(Duration::from_millis(100));
                    continue;
                };
                break status;
            };
            drop(send);
            assert_eq!(0, status.code().unwrap_or_default());

            handle.join().unwrap();
        });
    }
}

fn worker() {
    env_logger::init();

    let path = std::env::args().skip(1).next().unwrap();
    let db = Db::open(
        &PathBuf::from(path),
        Setting {
            checkpoint_period: Duration::from_secs(5),
        },
    )
    .unwrap();
    let db = Arc::new(db);

    let mut handles = vec![];

    let n = 100_000_000usize;
    let p = 100usize;

    for _ in 0..20 {
        let db = db.clone();
        handles.push(std::thread::spawn(move || {
            let mut rng = thread_rng();
            loop {
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
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
