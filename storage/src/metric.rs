use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) struct Histogram {
    lock: RwLock<()>,
    buckets: Vec<f64>,
    counter: Vec<AtomicU64>,
    total: AtomicU64,
}

#[derive(Debug)]
pub struct HistogramPercentile {
    pub p50: f64,
    pub p80: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
}

impl Histogram {
    pub(crate) fn new_exponential(start: f64, factor: f64, count: usize) -> Self {
        assert!(count > 0);
        assert!(start > 0.0);
        assert!(factor > 1.0);
        let mut buckets = Vec::with_capacity(count);
        let mut counter = Vec::with_capacity(count + 1);

        let mut last = start;
        for _ in 0..count {
            buckets.push(last);
            counter.push(AtomicU64::new(0));
            last *= factor;
        }
        counter.push(AtomicU64::new(0));

        Self {
            lock: RwLock::default(),
            buckets,
            counter,
            total: AtomicU64::new(0),
        }
    }

    pub(crate) fn observe(&self, value: f64) {
        let _guard = self.lock.read();

        let i = self
            .buckets
            .partition_point(|upper_bound| value >= *upper_bound);
        self.counter[i].fetch_add(1, Ordering::SeqCst);
        self.total.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn quantile(&self, p: f64) -> f64 {
        assert!((0.0..=1.0).contains(&p));
        let _guard = self.lock.write();

        let total = self.total.load(Ordering::SeqCst);
        let pos = p * total as f64;
        let mut cum = 0.0;
        let mut lb = 0.0;
        for (i, f) in self.counter.iter().enumerate() {
            if i == self.buckets.len() {
                // Special case: if the rank lies in the last bucket where
                // the upper bound is +inf, just return the bucket's lower bound
                return self.buckets[i - 1];
            }

            let f = f.load(Ordering::SeqCst) as f64;
            if cum + f >= pos {
                let w = self.buckets[i] - lb;
                // It is assumed that the first bucket has 0 as its lower bound
                return lb + (pos - cum) / f * w;
            }

            lb = self.buckets[i];
            cum += f;
        }

        unreachable!();
    }

    pub(crate) fn percentile(&self) -> HistogramPercentile {
        HistogramPercentile {
            p50: self.quantile(0.5),
            p80: self.quantile(0.8),
            p90: self.quantile(0.9),
            p95: self.quantile(0.95),
            p99: self.quantile(0.99),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_close_to {
        ($left:expr, $right:expr) => {
            let left = $left;
            let right = $right;
            assert!(
                (left - right).abs() < 0.001,
                "left = {left}, right = {right}"
            );
        };
    }

    #[test]
    fn test_histogram() {
        let histogram = Histogram::new_exponential(1.0, 1.3, 20);
        histogram.observe(0.5);
        histogram.observe(0.7);
        histogram.observe(1.2);
        assert_close_to!(0.0, histogram.quantile(0.0));
        assert_close_to!(0.15, histogram.quantile(0.1));
        assert_close_to!(1.3, histogram.quantile(1.0));

        histogram.observe(200.0);
        assert_close_to!(146.192, histogram.quantile(1.0));
    }
}
