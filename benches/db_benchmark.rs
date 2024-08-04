use criterion::profiler::Profiler;
use criterion::{criterion_group, criterion_main, Criterion};
use dbshark::{Db, Setting};
use pprof::ProfilerGuard;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use std::{fs::File, os::raw::c_int, path::Path};

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(FlamegraphProfiler::new(100));
    targets = tx_single_operation
);
criterion_main!(benches);

pub fn tx_single_operation(c: &mut Criterion) {
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

    let mut group = c.benchmark_group("single_tx");
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function("insert", |b| {
        b.iter(|| {
            let (key, val) = items.choose(&mut rng).unwrap();
            bucket.put(key.as_bytes(), val.as_bytes()).unwrap();
        })
    });
    group.finish();

    let mut group = c.benchmark_group("single_tx");
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function("read", |b| {
        b.iter(|| {
            let (key, _) = items.choose(&mut rng).unwrap();
            bucket.get(key.as_bytes()).unwrap();
        })
    });
    group.finish();
}

pub struct FlamegraphProfiler<'a> {
    frequency: c_int,
    active_profiler: Option<ProfilerGuard<'a>>,
}

impl<'a> FlamegraphProfiler<'a> {
    #[allow(dead_code)]
    pub fn new(frequency: c_int) -> Self {
        FlamegraphProfiler {
            frequency,
            active_profiler: None,
        }
    }
}

impl<'a> Profiler for FlamegraphProfiler<'a> {
    fn start_profiling(&mut self, _benchmark_id: &str, _benchmark_dir: &Path) {
        self.active_profiler = Some(ProfilerGuard::new(self.frequency).unwrap());
    }

    fn stop_profiling(&mut self, _benchmark_id: &str, benchmark_dir: &Path) {
        std::fs::create_dir_all(benchmark_dir).unwrap();
        let flamegraph_path = benchmark_dir.join("flamegraph.svg");
        let flamegraph_file = File::create(&flamegraph_path)
            .expect("File system error while creating flamegraph.svg");
        if let Some(profiler) = self.active_profiler.take() {
            profiler
                .report()
                .build()
                .unwrap()
                .flamegraph(flamegraph_file)
                .expect("Error writing flamegraph");
        }
    }
}
