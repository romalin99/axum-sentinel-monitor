use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub(crate) const WINDOW_SECS: u64 = 90;
pub(crate) const WINDOW_30_SECS: u64 = 30;
pub(crate) const WINDOW_60_SECS: u64 = 60;

const SUB_BITS: u32 = 3;
const SUB: u64 = 1 << SUB_BITS;
const MAX_LATENCY_NS: u64 = 60_000_000_000;
const MAX_LOG: u32 = 36;
const BUCKETS: usize = SUB as usize + ((MAX_LOG - SUB_BITS) as usize) * SUB as usize;
const TICK_EMPTY: u64 = u64::MAX;
const TICK_RESETTING: u64 = u64::MAX - 1;
const PERCENTILES: [u32; 4] = [500, 950, 990, 999];

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

    fn add_from(&mut self, other: &Self) {
        if other.count == 0 {
            return;
        }
        self.count += other.count;
        for (dst, src) in self.buckets.iter_mut().zip(&other.buckets) {
            if *src != 0 {
                *dst += *src;
            }
        }
    }

    fn reset(&mut self) {
        if self.count == 0 {
            return;
        }
        self.buckets.fill(0);
        self.count = 0;
    }

    fn percentiles(&self) -> [Option<u64>; PERCENTILES.len()] {
        if self.count == 0 {
            return [None; PERCENTILES.len()];
        }
        let ranks = PERCENTILES.map(|permille| {
            ((u128::from(self.count) * u128::from(permille)).div_ceil(1000) as u64).max(1)
        });
        let mut values = [None; PERCENTILES.len()];
        let mut next = 0;
        let mut cumulative = 0;
        for (index, count) in self.buckets.iter().enumerate() {
            cumulative += count;
            while next < ranks.len() && cumulative >= ranks[next] {
                values[next] = Some(bucket_upper_ns(index));
                next += 1;
            }
            if next == ranks.len() {
                break;
            }
        }
        for value in &mut values[next..] {
            *value = Some(MAX_LATENCY_NS);
        }
        values
    }
}

struct Slot {
    tick: AtomicU64,
    requests: AtomicU32,
    status: [AtomicU32; 5],
    buckets: [AtomicU32; BUCKETS],
}

impl Slot {
    fn new() -> Self {
        Self {
            tick: AtomicU64::new(TICK_EMPTY),
            requests: AtomicU32::new(0),
            status: std::array::from_fn(|_| AtomicU32::new(0)),
            buckets: std::array::from_fn(|_| AtomicU32::new(0)),
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

    /// Loads this slot when it still belongs to `expected_tick`. Empty seconds
    /// skip the 272-bucket walk. `hist` is always reset before returning.
    fn read(&self, expected_tick: u64, hist: &mut LatencyHist) -> Option<(u64, [u64; 5])> {
        hist.reset();
        if self.tick.load(Ordering::Acquire) != expected_tick {
            return None;
        }
        let requests = u64::from(self.requests.load(Ordering::Relaxed));
        if requests == 0 {
            return (self.tick.load(Ordering::Acquire) == expected_tick).then_some((0, [0; 5]));
        }
        let status =
            std::array::from_fn(|index| u64::from(self.status[index].load(Ordering::Relaxed)));
        for (index, bucket) in self.buckets.iter().enumerate() {
            hist.add_bucket(index, u64::from(bucket.load(Ordering::Relaxed)));
        }
        if self.tick.load(Ordering::Acquire) != expected_tick {
            hist.reset();
            return None;
        }
        Some((requests, status))
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
    pub window_90: WindowAgg,
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

    #[cfg(test)]
    pub(crate) fn observe(&self, ns: u64, status_class: u8) {
        self.observe_at(self.current_tick(), ns, status_class);
    }

    pub(crate) fn observe_at(&self, tick: u64, ns: u64, status_class: u8) {
        let index = bucket_of(ns);
        let status = (1..=5)
            .contains(&status_class)
            .then_some((status_class - 1) as usize);
        let Some(slot) = self.slot_for(tick) else {
            return;
        };
        if slot.tick.load(Ordering::Acquire) != tick {
            return;
        }
        slot.requests.fetch_add(1, Ordering::Relaxed);
        if let Some(class) = status {
            slot.status[class].fetch_add(1, Ordering::Relaxed);
        }
        slot.buckets[index].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> TrafficSnapshot {
        self.fold(true, self.current_tick())
    }

    /// 30s/60s/90s aggregates without the 90-point series or per-second percentiles.
    #[cfg(test)]
    pub(crate) fn snapshot_windows(&self) -> (WindowAgg, WindowAgg, WindowAgg) {
        self.snapshot_windows_at(self.current_tick())
    }

    pub(crate) fn snapshot_windows_at(&self, tick: u64) -> (WindowAgg, WindowAgg, WindowAgg) {
        let folded = self.fold(false, tick);
        (folded.window_30, folded.window_60, folded.window_90)
    }

    pub(crate) fn empty_windows(tick: u64) -> (WindowAgg, WindowAgg, WindowAgg) {
        let hist = LatencyHist::default();
        (
            finish_agg(WINDOW_30_SECS, tick, 0, [0; 5], &hist),
            finish_agg(WINDOW_60_SECS, tick, 0, [0; 5], &hist),
            finish_agg(WINDOW_SECS, tick, 0, [0; 5], &hist),
        )
    }

    fn fold(&self, with_series: bool, tick: u64) -> TrafficSnapshot {
        let unix = if with_series { unix_now() } else { 0 };
        let mut hist_30 = LatencyHist::default();
        let mut hist_60 = LatencyHist::default();
        let mut hist_90 = LatencyHist::default();
        let mut slot_hist = LatencyHist::default();
        let mut status_30 = [0u64; 5];
        let mut status_60 = [0u64; 5];
        let mut status_90 = [0u64; 5];
        let mut requests_30 = 0u64;
        let mut requests_60 = 0u64;
        let mut requests_90 = 0u64;
        let mut series = Vec::with_capacity(if with_series { WINDOW_SECS as usize } else { 0 });

        for age in (0..WINDOW_SECS).rev() {
            let loaded = if tick < age {
                None
            } else {
                let slot_tick = tick - age;
                self.slots[(slot_tick % WINDOW_SECS) as usize].read(slot_tick, &mut slot_hist)
            };

            if with_series {
                let unix_secs = unix.saturating_sub(age as i64);
                series.push(match &loaded {
                    Some((requests, status)) => {
                        let [p50_ns, p95_ns, p99_ns, p999_ns] = slot_hist.percentiles();
                        SecondSample {
                            unix_secs,
                            requests: *requests,
                            status: *status,
                            p50_ns,
                            p95_ns,
                            p99_ns,
                            p999_ns,
                        }
                    }
                    None => empty_sample(unix_secs),
                });
            }

            let Some((requests, status)) = loaded else {
                continue;
            };
            if requests == 0 {
                continue;
            }
            requests_90 += requests;
            add_status(&mut status_90, status);
            hist_90.add_from(&slot_hist);
            if age < WINDOW_60_SECS {
                requests_60 += requests;
                add_status(&mut status_60, status);
                hist_60.add_from(&slot_hist);
            }
            if age < WINDOW_30_SECS {
                requests_30 += requests;
                add_status(&mut status_30, status);
                hist_30.add_from(&slot_hist);
            }
        }

        TrafficSnapshot {
            window_30: finish_agg(WINDOW_30_SECS, tick, requests_30, status_30, &hist_30),
            window_60: finish_agg(
                WINDOW_60_SECS,
                tick,
                requests_60,
                status_60,
                &hist_60,
            ),
            window_90: finish_agg(WINDOW_SECS, tick, requests_90, status_90, &hist_90),
            series,
        }
    }

    fn slot_for(&self, tick: u64) -> Option<&Slot> {
        let slot = &self.slots[(tick % WINDOW_SECS) as usize];
        loop {
            let current = slot.tick.load(Ordering::Acquire);
            if current == tick {
                return Some(slot);
            }
            if current == TICK_RESETTING {
                std::hint::spin_loop();
                continue;
            }
            if current < tick || current == TICK_EMPTY {
                if slot
                    .tick
                    .compare_exchange(current, TICK_RESETTING, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    slot.clear();
                    slot.tick.store(tick, Ordering::Release);
                    return Some(slot);
                }
                continue;
            }
            return None;
        }
    }

    pub(crate) fn current_tick(&self) -> u64 {
        self.origin.elapsed().as_secs() + self.extra_secs.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn advance_secs(&self, secs: u64) {
        self.extra_secs.fetch_add(secs, Ordering::Relaxed);
    }
}

fn finish_agg(
    window: u64,
    tick: u64,
    requests: u64,
    status: [u64; 5],
    hist: &LatencyHist,
) -> WindowAgg {
    let covered = tick.saturating_add(1).min(window).max(1);
    let [p50_ns, p95_ns, p99_ns, p999_ns] = hist.percentiles();
    WindowAgg {
        seconds: window as u32,
        covered_seconds: covered as u32,
        requests,
        rps: requests as f64 / covered as f64,
        status,
        p50_ns,
        p95_ns,
        p99_ns,
        p999_ns,
    }
}

fn add_status(dst: &mut [u64; 5], src: [u64; 5]) {
    for (dst, src) in dst.iter_mut().zip(src) {
        *dst += src;
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
    use std::sync::Arc;
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

        window.advance_secs(91);
        let expired = window.snapshot();
        assert_eq!(expired.window_60.requests, 0);
        assert_eq!(expired.window_90.requests, 0);
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
    fn p999_tracks_tail_and_series_is_ninety_seconds() {
        let window = SlidingWindow::new();
        for _ in 0..998 {
            window.observe(Duration::from_millis(2).as_nanos() as u64, 2);
        }
        window.observe(Duration::from_millis(80).as_nanos() as u64, 2);
        let snap = window.snapshot();
        assert_eq!(snap.series.len(), 90);
        assert!(snap.window_60.p50_ns.unwrap() < Duration::from_millis(10).as_nanos() as u64);
        assert!(snap.window_60.p999_ns.unwrap() >= Duration::from_millis(80).as_nanos() as u64);
        assert!((snap.window_60.rps - 999.0).abs() < f64::EPSILON);
        assert_eq!(snap.window_30.requests, snap.window_60.requests);
        assert_eq!(snap.window_60.requests, snap.window_90.requests);
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

    #[test]
    fn ninety_second_view_keeps_slots_dropped_from_sixty_seconds() {
        let window = SlidingWindow::new();
        window.observe(1_000_000, 2);
        window.advance_secs(61);
        window.observe(2_000_000, 4);
        let snap = window.snapshot();
        assert_eq!(snap.window_30.requests, 1);
        assert_eq!(snap.window_60.requests, 1);
        assert_eq!(snap.window_90.requests, 2);
        assert_eq!(snap.window_90.status[1], 1);
        assert_eq!(snap.window_90.status[3], 1);
    }

    #[test]
    fn windows_match_full_snapshot_without_building_series() {
        let window = SlidingWindow::new();
        window.observe(1_000_000, 2);
        window.advance_secs(31);
        window.observe(2_000_000, 4);
        let snap = window.snapshot();
        let (window_30, window_60, window_90) = window.snapshot_windows();
        assert_eq!(window_30.requests, snap.window_30.requests);
        assert_eq!(window_30.status, snap.window_30.status);
        assert_eq!(window_30.p50_ns, snap.window_30.p50_ns);
        assert_eq!(window_60.requests, snap.window_60.requests);
        assert_eq!(window_60.status, snap.window_60.status);
        assert_eq!(window_60.p999_ns, snap.window_60.p999_ns);
        assert_eq!(window_90.requests, snap.window_90.requests);
        assert_eq!(window_90.status, snap.window_90.status);
        assert!(window.snapshot_windows().0.seconds == 30);
    }

    #[test]
    fn concurrent_observers_do_not_lose_samples() {
        const THREADS: usize = 8;
        const SAMPLES: usize = 10_000;
        let window = Arc::new(SlidingWindow::new());
        let threads: Vec<_> = (0..THREADS)
            .map(|_| {
                let window = Arc::clone(&window);
                std::thread::spawn(move || {
                    for _ in 0..SAMPLES {
                        window.observe(1_000_000, 2);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let snapshot = window.snapshot();
        assert_eq!(snapshot.window_60.requests, (THREADS * SAMPLES) as u64);
        assert_eq!(snapshot.window_60.status[1], (THREADS * SAMPLES) as u64);
        assert_eq!(snapshot.window_90.requests, (THREADS * SAMPLES) as u64);
    }

    #[test]
    fn slot_layout_stays_compact() {
        assert!(std::mem::size_of::<Slot>() <= 1_152);
    }
}
