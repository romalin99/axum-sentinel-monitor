//! Lock-free sliding-window latency histogram over one-second slots.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Length in seconds of the ring, and of its longest window.
pub(crate) const WINDOW_SECS: u64 = 90;
/// Length in seconds of the short window.
pub(crate) const WINDOW_30_SECS: u64 = 30;
/// Length in seconds of the medium window.
pub(crate) const WINDOW_60_SECS: u64 = 60;

/// Log2 of the number of sub-buckets each power of two is split into.
const SUB_BITS: u32 = 3;
/// Sub-buckets per power of two, which bounds the relative error at 12.5%.
const SUB: u64 = 1 << SUB_BITS;
/// Largest latency recorded; longer requests are clamped to it.
const MAX_LATENCY_NS: u64 = 60_000_000_000;
/// Exponent of the first power of two, in nanoseconds, above [`MAX_LATENCY_NS`];
/// the buckets cover every latency below `2^MAX_LOG` ns.
const MAX_LOG: u32 = 36;
/// Bucket count: [`SUB`] linear buckets below `SUB` ns, then [`SUB`] per power of
/// two.
const BUCKETS: usize = SUB as usize + ((MAX_LOG - SUB_BITS) as usize) * SUB as usize;
/// Slot tick meaning the slot has never been written.
const TICK_EMPTY: u64 = u64::MAX;
/// Slot tick meaning a writer is clearing the slot for a new second.
const TICK_RESETTING: u64 = u64::MAX - 1;
/// Reported percentiles in permille: P50, P95, P99, and P99.9.
const PERCENTILES: [u32; 4] = [500, 950, 990, 999];

/// Plain (non-atomic) latency histogram used to merge slots while folding.
struct LatencyHist {
    /// Samples per latency bucket.
    buckets: [u64; BUCKETS],
    /// Total samples across all buckets.
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
    /// Merges every bucket of `other` into this histogram.
    fn add_from(&mut self, other: &Self) {
        if other.count == 0 {
            return;
        }
        self.count += other.count;
        // A branch-free loop over the fixed-size arrays vectorizes.
        for (dst, src) in self.buckets.iter_mut().zip(&other.buckets) {
            *dst += *src;
        }
    }

    /// Empties the histogram, skipping the walk when it is already empty.
    fn reset(&mut self) {
        if self.count == 0 {
            return;
        }
        self.buckets.fill(0);
        self.count = 0;
    }

    /// Returns the [`PERCENTILES`] as bucket upper bounds in nanoseconds, or all
    /// `None` when the histogram is empty.
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

/// Lock-free counters of one second of the ring.
struct Slot {
    /// Second this slot currently holds, or [`TICK_EMPTY`] / [`TICK_RESETTING`].
    tick: AtomicU64,
    /// Requests completed in this second.
    requests: AtomicU32,
    /// Responses per status class, indexed `1xx` to `5xx`.
    status: [AtomicU32; 5],
    /// Samples per latency bucket.
    buckets: [AtomicU32; BUCKETS],
}

impl Slot {
    /// Creates a slot that holds no second yet.
    fn new() -> Self {
        Self {
            tick: AtomicU64::new(TICK_EMPTY),
            requests: AtomicU32::new(0),
            status: std::array::from_fn(|_| AtomicU32::new(0)),
            buckets: std::array::from_fn(|_| AtomicU32::new(0)),
        }
    }

    /// Zeroes every counter; the caller must hold the slot in [`TICK_RESETTING`].
    fn clear(&self) {
        self.requests.store(0, Ordering::Relaxed);
        for count in &self.status {
            count.store(0, Ordering::Relaxed);
        }
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
    }

    /// Loads this slot into `hist` when it still belongs to `expected_tick`.
    ///
    /// Returns the request and per-class status counts, or `None` when the slot
    /// holds, or was recycled for, another second. Empty seconds skip the
    /// 272-bucket walk. `hist` is always reset before it is filled.
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
        // `hist` was reset above, so the buckets are assigned rather than added.
        let mut total = 0;
        for (dst, bucket) in hist.buckets.iter_mut().zip(&self.buckets) {
            let count = u64::from(bucket.load(Ordering::Relaxed));
            *dst = count;
            total += count;
        }
        hist.count = total;
        if self.tick.load(Ordering::Acquire) != expected_tick {
            hist.reset();
            return None;
        }
        Some((requests, status))
    }
}

/// Ring of one-second [`Slot`]s covering the trailing [`WINDOW_SECS`] seconds.
///
/// A tick is the number of whole seconds since `origin`; slot `tick % WINDOW_SECS`
/// is recycled by the first writer that reaches a newer second.
pub(crate) struct SlidingWindow {
    /// Instant that tick zero is measured from.
    origin: Instant,
    /// Seconds added to the tick; non-zero only when tests advance time.
    extra_secs: Arc<AtomicU64>,
    /// One slot per second of the ring.
    slots: [Slot; WINDOW_SECS as usize],
}

/// Traffic aggregated over one trailing window.
#[derive(Clone, Debug)]
pub(crate) struct WindowAgg {
    /// Nominal window length in seconds.
    pub seconds: u32,
    /// Seconds of the window elapsed since tick zero.
    pub covered_seconds: u32,
    /// Requests completed inside the window.
    pub requests: u64,
    /// Requests per second over `covered_seconds`.
    pub rps: f64,
    /// Responses per status class, indexed `1xx` to `5xx`.
    pub status: [u64; 5],
    /// Median latency in nanoseconds, or `None` without requests.
    pub p50_ns: Option<u64>,
    /// 95th-percentile latency in nanoseconds, or `None` without requests.
    pub p95_ns: Option<u64>,
    /// 99th-percentile latency in nanoseconds, or `None` without requests.
    pub p99_ns: Option<u64>,
    /// 99.9th-percentile latency in nanoseconds, or `None` without requests.
    pub p999_ns: Option<u64>,
}

/// Traffic of a single second of the ring.
#[derive(Clone, Debug)]
pub(crate) struct SecondSample {
    /// Unix timestamp of the second.
    pub unix_secs: i64,
    /// Requests completed in the second.
    pub requests: u64,
    /// Responses per status class, indexed `1xx` to `5xx`.
    pub status: [u64; 5],
    /// Median latency in nanoseconds, or `None` without requests.
    pub p50_ns: Option<u64>,
    /// 95th-percentile latency in nanoseconds, or `None` without requests.
    pub p95_ns: Option<u64>,
    /// 99th-percentile latency in nanoseconds, or `None` without requests.
    pub p99_ns: Option<u64>,
    /// 99.9th-percentile latency in nanoseconds, or `None` without requests.
    pub p999_ns: Option<u64>,
}

/// The three window aggregates plus the per-second series, oldest first.
#[derive(Clone, Debug)]
pub(crate) struct TrafficSnapshot {
    /// Aggregate of the trailing 30 seconds.
    pub window_30: WindowAgg,
    /// Aggregate of the trailing 60 seconds.
    pub window_60: WindowAgg,
    /// Aggregate of the trailing 90 seconds.
    pub window_90: WindowAgg,
    /// One sample per second, oldest first; empty when the series was not requested.
    pub series: Vec<SecondSample>,
}

impl SlidingWindow {
    /// Creates an empty window whose tick zero is now.
    pub(crate) fn new() -> Self {
        Self::with_clock(Instant::now(), Arc::new(AtomicU64::new(0)))
    }

    /// Creates an empty window on an existing time base.
    pub(crate) fn with_clock(origin: Instant, extra_secs: Arc<AtomicU64>) -> Self {
        Self {
            origin,
            extra_secs,
            slots: std::array::from_fn(|_| Slot::new()),
        }
    }

    /// Returns the instant tick zero is measured from.
    pub(crate) fn origin(&self) -> Instant {
        self.origin
    }

    /// Returns the shared tick offset, non-zero only when tests advance time.
    pub(crate) fn extra_secs(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.extra_secs)
    }

    /// Records one request of `ns` nanoseconds at the current tick.
    #[cfg(test)]
    pub(crate) fn observe(&self, ns: u64, status_class: u8) {
        let (bucket, class) = sample_of(ns, status_class);
        self.record(self.current_tick(), bucket, class);
    }

    /// Records one request in the slot of `tick`.
    ///
    /// `bucket` and `class` come from [`sample_of`], computed once by the caller
    /// and shared by the global and the per-route window. The sample is dropped
    /// when the slot has already moved on to a newer second.
    pub(crate) fn record(&self, tick: u64, bucket: usize, class: Option<usize>) {
        let Some(slot) = self.slot_for(tick) else {
            return;
        };
        if slot.tick.load(Ordering::Acquire) != tick {
            return;
        }
        slot.requests.fetch_add(1, Ordering::Relaxed);
        if let Some(class) = class {
            slot.status[class].fetch_add(1, Ordering::Relaxed);
        }
        slot.buckets[bucket].fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the window aggregates and the per-second series as of now.
    pub(crate) fn snapshot(&self) -> TrafficSnapshot {
        self.fold(true, self.current_tick())
    }

    /// Returns the 30s/60s/90s aggregates without the 90-point series or
    /// per-second percentiles.
    #[cfg(test)]
    pub(crate) fn snapshot_windows(&self) -> (WindowAgg, WindowAgg, WindowAgg) {
        self.snapshot_windows_at(self.current_tick())
    }

    /// Returns the 30s/60s/90s aggregates of the windows ending at `tick`.
    pub(crate) fn snapshot_windows_at(&self, tick: u64) -> (WindowAgg, WindowAgg, WindowAgg) {
        let folded = self.fold(false, tick);
        (folded.window_30, folded.window_60, folded.window_90)
    }

    /// Returns the 30s/60s/90s aggregates of a window without any request.
    pub(crate) fn empty_windows(tick: u64) -> (WindowAgg, WindowAgg, WindowAgg) {
        let hist = LatencyHist::default();
        (
            finish_agg(WINDOW_30_SECS, tick, 0, [0; 5], &hist),
            finish_agg(WINDOW_60_SECS, tick, 0, [0; 5], &hist),
            finish_agg(WINDOW_SECS, tick, 0, [0; 5], &hist),
        )
    }

    /// Walks the ring once, newest second first, accumulating all three windows
    /// and, when `with_series` is set, the per-second series.
    ///
    /// The windows nest, so one running total serves all three: it is the 30s
    /// window after 30 seconds, the 60s window after 60, and the 90s window at
    /// the end. Each slot is therefore merged once instead of up to three times.
    fn fold(&self, with_series: bool, tick: u64) -> TrafficSnapshot {
        let mut fold = Fold {
            window: self,
            tick,
            unix: if with_series { Some(unix_now()) } else { None },
            slot_hist: LatencyHist::default(),
            hist: LatencyHist::default(),
            status: [0; 5],
            requests: 0,
            series: Vec::with_capacity(if with_series { WINDOW_SECS as usize } else { 0 }),
        };
        fold.walk(0..WINDOW_30_SECS);
        let window_30 = fold.aggregate(WINDOW_30_SECS);
        fold.walk(WINDOW_30_SECS..WINDOW_60_SECS);
        let window_60 = fold.aggregate(WINDOW_60_SECS);
        fold.walk(WINDOW_60_SECS..WINDOW_SECS);
        let window_90 = fold.aggregate(WINDOW_SECS);
        // The series was pushed newest first; the snapshot lists oldest first.
        fold.series.reverse();
        TrafficSnapshot {
            window_30,
            window_60,
            window_90,
            series: fold.series,
        }
    }

    /// Returns the slot of `tick`, recycling it when it holds an older second.
    ///
    /// Returns `None` when the slot already belongs to a newer second.
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

    /// Returns the current tick: whole seconds since `origin`.
    pub(crate) fn current_tick(&self) -> u64 {
        self.tick_at(Instant::now())
    }

    /// Returns the tick of `now`: whole seconds since `origin`.
    pub(crate) fn tick_at(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.origin).as_secs()
            + self.extra_secs.load(Ordering::Relaxed)
    }

    /// Moves the clock forward by `secs` seconds.
    #[cfg(test)]
    pub(crate) fn advance_secs(&self, secs: u64) {
        self.extra_secs.fetch_add(secs, Ordering::Relaxed);
    }
}

/// Running state of one [`SlidingWindow::fold`].
struct Fold<'a> {
    /// Ring being folded.
    window: &'a SlidingWindow,
    /// Tick the windows end at.
    tick: u64,
    /// Unix time of `tick`, or `None` when no series is built.
    unix: Option<i64>,
    /// Scratch histogram the current slot is loaded into.
    slot_hist: LatencyHist,
    /// Running total of every slot walked so far.
    hist: LatencyHist,
    /// Running per-class status counts.
    status: [u64; 5],
    /// Running request count.
    requests: u64,
    /// Per-second samples, newest first while walking.
    series: Vec<SecondSample>,
}

impl Fold<'_> {
    /// Merges the slots of the given ages, youngest first, into the running totals.
    fn walk(&mut self, ages: std::ops::Range<u64>) {
        for age in ages {
            let loaded = if self.tick < age {
                None
            } else {
                let slot_tick = self.tick - age;
                self.window.slots[(slot_tick % WINDOW_SECS) as usize]
                    .read(slot_tick, &mut self.slot_hist)
            };

            if let Some(unix) = self.unix {
                let unix_secs = unix.saturating_sub(age as i64);
                self.series.push(match &loaded {
                    Some((requests, status)) => {
                        let [p50_ns, p95_ns, p99_ns, p999_ns] = self.slot_hist.percentiles();
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

            if let Some((requests, status)) = loaded
                && requests != 0
            {
                self.requests += requests;
                add_status(&mut self.status, status);
                self.hist.add_from(&self.slot_hist);
            }
        }
    }

    /// Returns the running totals as the aggregate of a `window`-second window.
    fn aggregate(&self, window: u64) -> WindowAgg {
        finish_agg(window, self.tick, self.requests, self.status, &self.hist)
    }
}

/// Builds the aggregate of a `window`-second window ending at `tick`.
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

/// Adds the per-class counts of `src` onto `dst`.
fn add_status(dst: &mut [u64; 5], src: [u64; 5]) {
    for (dst, src) in dst.iter_mut().zip(src) {
        *dst += src;
    }
}

/// Returns the sample of a second without any request.
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

/// Returns the current Unix time in seconds, or `0` when the clock is before
/// the epoch.
fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs() as i64)
}

/// Returns the `(bucket, status class index)` a request of `ns` nanoseconds with
/// status class `status_class` (`1` to `5`) is recorded under.
///
/// The class index is `None` for a class outside `1xx` to `5xx`, which has no
/// counter.
pub(crate) fn sample_of(ns: u64, status_class: u8) -> (usize, Option<usize>) {
    let class = (1..=5)
        .contains(&status_class)
        .then(|| usize::from(status_class - 1));
    (bucket_of(ns), class)
}

/// Returns the index of the bucket that holds a latency of `ns` nanoseconds.
fn bucket_of(ns: u64) -> usize {
    let value = ns.clamp(1, MAX_LATENCY_NS);
    if value < SUB {
        return value as usize;
    }
    let log = value.ilog2();
    let shift = log.saturating_sub(SUB_BITS);
    let significant = (value >> shift) as usize;
    let index = SUB as usize
        + (log.saturating_sub(SUB_BITS) as usize) * SUB as usize
        + significant.saturating_sub(SUB as usize);
    index.min(BUCKETS - 1)
}

/// Returns the largest latency in nanoseconds that falls into bucket `index`.
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
        .map_or(u64::MAX, |value| value.saturating_sub(1));
    high.min(MAX_LATENCY_NS)
}

/// Returns the `(4xx, 5xx)` shares of `requests`, or `None` for both when there
/// are no requests.
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
