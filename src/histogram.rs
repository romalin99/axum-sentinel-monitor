use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub(crate) const WINDOW_SECS: u64 = 60;
pub(crate) const WINDOW_30_SECS: u64 = 30;

const SUB_BITS: u32 = 3;
const SUB: u64 = 1 << SUB_BITS;
const MAX_LATENCY_NS: u64 = 60_000_000_000;
const MAX_LOG: u32 = 36;
const BUCKETS: usize = SUB as usize + ((MAX_LOG - SUB_BITS) as usize) * SUB as usize;

#[derive(Clone)]
struct LatencyHist {
    buckets: [u64; BUCKETS],
    count: u64,
}

impl Default for LatencyHist {
    fn default() -> Self {
        Self {
            buckets: [0; BUCKETS],
            count: 0,
        }
    }
}

impl LatencyHist {
    fn add_bucket(&mut self, index: usize, count: u64) {
        if count == 0 || index >= BUCKETS {
            return;
        }
        self.buckets[index] += count;
        self.count += count;
    }

    fn percentile(&self, permille: u32) -> Option<u64> {
        if self.count == 0 || permille == 0 {
            return None;
        }
        let rank = (u128::from(self.count) * u128::from(permille)).div_ceil(1000) as u64;
        let rank = rank.max(1);
        let mut cumulative = 0;
        for (index, count) in self.buckets.iter().enumerate() {
            cumulative += count;
            if cumulative >= rank {
                return Some(bucket_upper_ns(index));
            }
        }
        Some(MAX_LATENCY_NS)
    }
}

struct Slot {
    tick: AtomicU64,
    requests: AtomicU64,
    status: [AtomicU64; 5],
    buckets: [AtomicU64; BUCKETS],
}

struct SlotView {
    requests: u64,
    status: [u64; 5],
    hist: LatencyHist,
}

impl Slot {
    fn new() -> Self {
        Self {
            tick: AtomicU64::new(u64::MAX),
            requests: AtomicU64::new(0),
            status: std::array::from_fn(|_| AtomicU64::new(0)),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    fn clear(&self) {
        self.requests.store(0, Ordering::Relaxed);
        for count in &self.status {
            count.store(0, Ordering::Relaxed);
        }
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
    }

    fn load_if_tick(&self, tick: u64) -> Option<SlotView> {
        if self.tick.load(Ordering::Acquire) != tick {
            return None;
        }
        let mut hist = LatencyHist::default();
        for (index, bucket) in self.buckets.iter().enumerate() {
            hist.add_bucket(index, bucket.load(Ordering::Relaxed));
        }
        if self.tick.load(Ordering::Acquire) != tick {
            return None;
        }
        Some(SlotView {
            requests: self.requests.load(Ordering::Relaxed),
            status: std::array::from_fn(|index| self.status[index].load(Ordering::Relaxed)),
            hist,
        })
    }
}

pub(crate) struct SlidingWindow {
    origin: Instant,
    extra_secs: Arc<AtomicU64>,
    slots: [Slot; WINDOW_SECS as usize],
}

#[derive(Clone, Debug)]
pub(crate) struct WindowAgg {
    pub seconds: u32,
    pub covered_seconds: u32,
    pub requests: u64,
    pub rps: f64,
    pub status: [u64; 5],
    pub p50_ns: Option<u64>,
    pub p95_ns: Option<u64>,
    pub p99_ns: Option<u64>,
    pub p999_ns: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct SecondSample {
    pub unix_secs: i64,
    pub requests: u64,
    pub status: [u64; 5],
    pub p50_ns: Option<u64>,
    pub p95_ns: Option<u64>,
    pub p99_ns: Option<u64>,
    pub p999_ns: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct TrafficSnapshot {
    pub window_30: WindowAgg,
    pub window_60: WindowAgg,
    pub series: Vec<SecondSample>,
}

impl SlidingWindow {
    pub(crate) fn new() -> Self {
        Self::with_clock(Instant::now(), Arc::new(AtomicU64::new(0)))
    }

    pub(crate) fn with_clock(origin: Instant, extra_secs: Arc<AtomicU64>) -> Self {
        Self {
            origin,
            extra_secs,
            slots: std::array::from_fn(|_| Slot::new()),
        }
    }

    pub(crate) fn origin(&self) -> Instant {
        self.origin
    }

    pub(crate) fn extra_secs(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.extra_secs)
    }

    pub(crate) fn observe(&self, ns: u64, status_class: u8) {
        let tick = self.tick();
        let slot = self.slot_for(tick);
        slot.requests.fetch_add(1, Ordering::Relaxed);
        if (1..=5).contains(&status_class) {
            slot.status[(status_class - 1) as usize].fetch_add(1, Ordering::Relaxed);
        }
        let index = bucket_of(ns);
        slot.buckets[index].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> TrafficSnapshot {
        let tick = self.tick();
        let unix = unix_now();
        let mut loaded = Vec::with_capacity(WINDOW_SECS as usize);
        let mut series = Vec::with_capacity(WINDOW_SECS as usize);
        for age in (0..WINDOW_SECS).rev() {
            let unix_secs = unix.saturating_sub(age as i64);
            let slot = if tick < age {
                None
            } else {
                let slot_tick = tick - age;
                self.slots[(slot_tick % WINDOW_SECS) as usize].load_if_tick(slot_tick)
            };
            series.push(sample_from(unix_secs, slot.as_ref()));
            loaded.push(slot);
        }
        TrafficSnapshot {
            window_30: aggregate_loaded(&loaded, WINDOW_30_SECS, tick),
            window_60: aggregate_loaded(&loaded, WINDOW_SECS, tick),
            series,
        }
    }

    /// Requests recorded in the trailing [`WINDOW_SECS`] window. Reads only the
    /// per-second counters — no latency histograms, no allocation — so eviction
    /// scans can afford to call it once per tracked route.
    pub(crate) fn recent_requests(&self) -> u64 {
        let tick = self.tick();
        (0..WINDOW_SECS)
            .take_while(|age| tick >= *age)
            .map(|age| {
                let slot_tick = tick - age;
                let slot = &self.slots[(slot_tick % WINDOW_SECS) as usize];
                if slot.tick.load(Ordering::Acquire) == slot_tick {
                    slot.requests.load(Ordering::Relaxed)
                } else {
                    0
                }
            })
            .sum()
    }

    fn slot_for(&self, tick: u64) -> &Slot {
        let slot = &self.slots[(tick % WINDOW_SECS) as usize];
        loop {
            let current = slot.tick.load(Ordering::Acquire);
            if current == tick {
                return slot;
            }
            if current < tick || current == u64::MAX {
                if slot
                    .tick
                    .compare_exchange(current, tick, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    slot.clear();
                    return slot;
                }
                continue;
            }
            return slot;
        }
    }

    fn tick(&self) -> u64 {
        self.origin.elapsed().as_secs() + self.extra_secs.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn advance_secs(&self, secs: u64) {
        self.extra_secs.fetch_add(secs, Ordering::Relaxed);
    }
}

fn sample_from(unix_secs: i64, slot: Option<&SlotView>) -> SecondSample {
    let Some(slot) = slot else {
        return empty_sample(unix_secs);
    };
    SecondSample {
        unix_secs,
        requests: slot.requests,
        status: slot.status,
        p50_ns: slot.hist.percentile(500),
        p95_ns: slot.hist.percentile(950),
        p99_ns: slot.hist.percentile(990),
        p999_ns: slot.hist.percentile(999),
    }
}

fn aggregate_loaded(loaded: &[Option<SlotView>], window: u64, tick: u64) -> WindowAgg {
    let covered = tick.saturating_add(1).min(window).max(1);
    let start = loaded.len().saturating_sub(window as usize);
    let mut requests = 0;
    let mut status = [0u64; 5];
    let mut hist = LatencyHist::default();
    for slot in &loaded[start..] {
        let Some(slot) = slot else {
            continue;
        };
        requests += slot.requests;
        for (dst, src) in status.iter_mut().zip(slot.status) {
            *dst += src;
        }
        hist.buckets
            .iter_mut()
            .zip(slot.hist.buckets)
            .for_each(|(dst, src)| *dst += src);
        hist.count += slot.hist.count;
    }
    WindowAgg {
        seconds: window as u32,
        covered_seconds: covered as u32,
        requests,
        rps: requests as f64 / covered as f64,
        status,
        p50_ns: hist.percentile(500),
        p95_ns: hist.percentile(950),
        p99_ns: hist.percentile(990),
        p999_ns: hist.percentile(999),
    }
}

fn empty_sample(unix_secs: i64) -> SecondSample {
    SecondSample {
        unix_secs,
        requests: 0,
        status: [0; 5],
        p50_ns: None,
        p95_ns: None,
        p99_ns: None,
        p999_ns: None,
    }
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

fn bucket_of(ns: u64) -> usize {
    let value = ns.clamp(1, MAX_LATENCY_NS);
    if value < SUB {
        return value as usize;
    }
    let log = 63 - value.leading_zeros();
    let shift = log.saturating_sub(SUB_BITS);
    let significant = (value >> shift) as usize;
    let index = SUB as usize
        + (log.saturating_sub(SUB_BITS) as usize) * SUB as usize
        + significant.saturating_sub(SUB as usize);
    index.min(BUCKETS - 1)
}

fn bucket_upper_ns(index: usize) -> u64 {
    if index < SUB as usize {
        return index as u64;
    }
    let shifted = index - SUB as usize;
    let log = (shifted / SUB as usize) as u32 + SUB_BITS;
    let significant = (shifted % SUB as usize) as u64 + SUB;
    let shift = log.saturating_sub(SUB_BITS);
    let high = significant
        .saturating_add(1)
        .checked_shl(shift)
        .map(|value| value.saturating_sub(1))
        .unwrap_or(u64::MAX);
    high.min(MAX_LATENCY_NS)
}

pub(crate) fn window_rates(status: &[u64; 5], requests: u64) -> (Option<f64>, Option<f64>) {
    if requests == 0 {
        return (None, None);
    }
    (
        Some(status[3] as f64 / requests as f64),
        Some(status[4] as f64 / requests as f64),
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn buckets_cover_latency_range() {
        for ns in [1, 500, 1_000, 1_000_000, 40_000_000, MAX_LATENCY_NS] {
            let index = bucket_of(ns);
            assert!(index < BUCKETS, "index {index} for {ns} ns");
            assert!(bucket_upper_ns(index) >= ns.min(MAX_LATENCY_NS));
        }
        assert_eq!(bucket_of(0), bucket_of(1));
    }

    #[test]
    fn percentiles_use_window_not_lifetime() {
        let window = SlidingWindow::new();
        for _ in 0..9 {
            window.observe(Duration::from_millis(1).as_nanos() as u64, 2);
        }
        window.observe(Duration::from_millis(40).as_nanos() as u64, 2);
        let snap = window.snapshot();
        assert_eq!(snap.window_60.requests, 10);
        assert!(snap.window_60.p50_ns.unwrap() <= Duration::from_millis(5).as_nanos() as u64);
        assert!(snap.window_60.p99_ns.unwrap() >= Duration::from_millis(40).as_nanos() as u64);

        window.advance_secs(61);
        let expired = window.snapshot();
        assert_eq!(expired.window_60.requests, 0);
        assert!(expired.window_60.p50_ns.is_none());
        assert!(expired.series.iter().all(|sample| sample.requests == 0));

        window.observe(Duration::from_millis(8).as_nanos() as u64, 5);
        let fresh = window.snapshot();
        assert_eq!(fresh.window_60.requests, 1);
        assert_eq!(fresh.window_60.status[4], 1);
        assert_eq!(
            fresh
                .series
                .iter()
                .map(|sample| sample.requests)
                .sum::<u64>(),
            1
        );
    }

    #[test]
    fn p999_tracks_tail_and_series_is_sixty_seconds() {
        let window = SlidingWindow::new();
        for _ in 0..998 {
            window.observe(Duration::from_millis(2).as_nanos() as u64, 2);
        }
        window.observe(Duration::from_millis(80).as_nanos() as u64, 2);
        let snap = window.snapshot();
        assert_eq!(snap.series.len(), 60);
        assert!(snap.window_60.p50_ns.unwrap() < Duration::from_millis(10).as_nanos() as u64);
        assert!(snap.window_60.p999_ns.unwrap() >= Duration::from_millis(80).as_nanos() as u64);
        assert!((snap.window_60.rps - 999.0).abs() < f64::EPSILON);
        assert_eq!(snap.window_30.requests, snap.window_60.requests);
    }

    #[test]
    fn thirty_second_view_drops_older_slots() {
        let window = SlidingWindow::new();
        window.observe(1_000_000, 2);
        window.advance_secs(31);
        window.observe(2_000_000, 4);
        let snap = window.snapshot();
        assert_eq!(snap.window_30.requests, 1);
        assert_eq!(snap.window_30.status[3], 1);
        assert_eq!(snap.window_60.requests, 2);
        assert_eq!(snap.window_60.status[1], 1);
        assert_eq!(snap.window_60.status[3], 1);
    }
}
