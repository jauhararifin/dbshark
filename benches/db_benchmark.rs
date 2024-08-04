use criterion::{criterion_group, criterion_main, Criterion};
use dbshark::{Db, Setting};
use rand::seq::SliceRandom;
use rand::SeedableRng;

criterion_group!(benches, tx_insert_benchmark);
criterion_main!(benches);

pub fn tx_insert_benchmark(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();

    let mut items = Vec::new();
    for i in 0..100000 {
        let key = format!("key{i:05}");
        let val = format!("val{i:05}");
        items.push((key, val));
    }
    let mut rng = rand::rngs::StdRng::seed_from_u64(0);

    let db = Db::open(path, Setting::default()).unwrap();
    let mut tx = db.update().unwrap();
    let mut bucket = tx.bucket("sample").unwrap();

    c.bench_function("insert", |b| {
        b.iter(|| {
            let (key, val) = items.choose(&mut rng).unwrap();
            bucket.put(key.as_bytes(), val.as_bytes()).unwrap();
        })
    });
}
