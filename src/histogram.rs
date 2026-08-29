use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const LATENCY_HISTOGRAM_SHARDS: usize = 32;

const fn latency_bounds_ns() -> [u64; 14] {
    [
        Duration::from_millis(1).as_nanos() as u64,
        Duration::from_millis(5).as_nanos() as u64,
        Duration::from_millis(10).as_nanos() as u64,
        Duration::from_millis(25).as_nanos() as u64,
        Duration::from_millis(50).as_nanos() as u64,
        Duration::from_millis(100).as_nanos() as u64,
        Duration::from_millis(250).as_nanos() as u64,
        Duration::from_millis(500).as_nanos() as u64,
        Duration::from_secs(1).as_nanos() as u64,
        Duration::from_millis(2500).as_nanos() as u64,
        Duration::from_secs(5).as_nanos() as u64,
        Duration::from_secs(10).as_nanos() as u64,
        Duration::from_secs(30).as_nanos() as u64,
        Duration::from_secs(60).as_nanos() as u64,
    ]
}

const LATENCY_BOUNDS_NS: [u64; 14] = latency_bounds_ns();
const LATENCY_BUCKETS: usize = LATENCY_BOUNDS_NS.len() + 1;

#[derive(Default)]
pub(crate) struct LatencyHistogram {
    buckets: [[AtomicU64; LATENCY_BUCKETS]; LATENCY_HISTOGRAM_SHARDS],
}

#[derive(Clone, Debug, Default)]
pub(crate) struct LatencyWindow {
    buckets: [u64; LATENCY_BUCKETS],
    count: u64,
}

impl LatencyHistogram {
    pub(crate) fn observe_sharded(&self, ns: u64, shard: u64) {
        let mut bucket = LATENCY_BOUNDS_NS.len();
        for (index, bound) in LATENCY_BOUNDS_NS.iter().enumerate() {
            if ns <= *bound {
                bucket = index;
                break;
            }
        }
        self.buckets[shard as usize & (LATENCY_HISTOGRAM_SHARDS - 1)][bucket]
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot_and_reset(&self) -> LatencyWindow {
        let mut window = LatencyWindow::default();
        for shard in &self.buckets {
            for (bucket, counter) in shard.iter().enumerate() {
                let count = counter.swap(0, Ordering::Relaxed);
                window.buckets[bucket] += count;
                window.count += count;
            }
        }
        window
    }
}

impl LatencyWindow {
    pub(crate) fn percentile(&self, percent: u64) -> Option<u64> {
        if self.count == 0 || percent == 0 {
            return None;
        }
        let rank = (self.count * percent).div_ceil(100);
        let mut cumulative = 0;
        for (index, count) in self.buckets.iter().enumerate() {
            cumulative += count;
            if cumulative < rank {
                continue;
            }
            if index < LATENCY_BOUNDS_NS.len() {
                return Some(LATENCY_BOUNDS_NS[index]);
            }
            return Some(LATENCY_BOUNDS_NS[LATENCY_BOUNDS_NS.len() - 1]);
        }
        Some(LATENCY_BOUNDS_NS[LATENCY_BOUNDS_NS.len() - 1])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_uses_bucket_upper_bound() {
        let histogram = LatencyHistogram::default();
        histogram.observe_sharded(Duration::from_millis(3).as_nanos() as u64, 1);
        histogram.observe_sharded(Duration::from_millis(8).as_nanos() as u64, 2);
        histogram.observe_sharded(Duration::from_millis(40).as_nanos() as u64, 3);
        let window = histogram.snapshot_and_reset();
        assert_eq!(
            window.percentile(50),
            Some(Duration::from_millis(10).as_nanos() as u64)
        );
        assert_eq!(
            window.percentile(99),
            Some(Duration::from_millis(50).as_nanos() as u64)
        );
        assert!(histogram.snapshot_and_reset().percentile(50).is_none());
    }
}
