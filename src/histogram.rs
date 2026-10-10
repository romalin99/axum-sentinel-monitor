//! Lock-free sliding-window latency histogram over one-second slots.
//!
//! Samples are first recorded into a thread-private stage that holds the
//! current second of one thread; the stage owner moves it into the shared ring
//! when it next records a later second. Snapshots read the ring and the stages
//! together and validate every stage read against its tick, so no sample is
//! ever counted twice.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering, fence};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::shard::{MAX_STAGES, live_indices};

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
/// Oldest age in seconds of a stage that is still moved into the ring.
///
/// Moving a stage adds its counters to the ring slot of its second, which the
/// first thread to reach the second `WINDOW_SECS` later recycles. One second
/// of margin keeps a move clear of that recycle unless the moving thread's
/// clock reading is more than a second stale, which only a thread stalled for
/// that long between taking the clock and moving its stage could produce. A
/// stage one second older than this is still inside the window and is left
/// where it is until it expires; the sample that found it bypasses the stage.
const FLUSH_MAX_AGE: u64 = WINDOW_SECS - 2;
/// Attempts a fold makes to read the stages consistently.
///
/// A fold yields before its first retries and then sleeps between attempts,
/// about thirty milliseconds in all, so a thread preempted in the middle of a
/// stage move gets to finish it even when it is queued on another CPU. A stage
/// still mid-move on the last attempt is left out of that one snapshot.
const MAX_FOLD_ATTEMPTS: usize = 64;
/// Pause between the fold attempts after the yielding ones.
const FOLD_SHORT_PAUSE: Duration = Duration::from_micros(50);
/// Pause between the fold attempts after the tenth.
const FOLD_LONG_PAUSE: Duration = Duration::from_micros(500);

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

/// Lock-free counters of one second, used both as a ring slot and as a stage.
///
/// Aligned to 128 bytes, which covers every supported CPU's cache line, so a
/// stage never shares a line with its neighbours.
#[repr(align(128))]
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

    /// Moves every counter into `dst`, leaving this slot zeroed; the caller must
    /// be the stage's owner and hold this slot in [`TICK_RESETTING`].
    fn drain_into(&self, dst: &Slot) {
        move_count(&self.requests, &dst.requests);
        for (src, dst) in self.status.iter().zip(&dst.status) {
            move_count(src, dst);
        }
        for (src, dst) in self.buckets.iter().zip(&dst.buckets) {
            move_count(src, dst);
        }
    }

    /// Adds one sample to a ring slot, which several threads write; the caller
    /// must have checked the tick.
    fn add(&self, bucket: usize, class: Option<usize>) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        if let Some(class) = class {
            self.status[class].fetch_add(1, Ordering::Relaxed);
        }
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
    }

    /// Adds one sample to a stage, which only its owning thread writes, so a
    /// load and a store replace each locked read-modify-write.
    fn add_owned(&self, bucket: usize, class: Option<usize>) {
        bump(&self.requests);
        if let Some(class) = class {
            bump(&self.status[class]);
        }
        bump(&self.buckets[bucket]);
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
            fence(Ordering::Acquire);
            return (self.tick.load(Ordering::Relaxed) == expected_tick).then_some((0, [0; 5]));
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
        // Pairs with the release fence a writer issues after marking the slot
        // `TICK_RESETTING`: a read that saw any counter of a reset or a drain
        // also sees the tick change and is discarded.
        fence(Ordering::Acquire);
        if self.tick.load(Ordering::Relaxed) != expected_tick {
            hist.reset();
            return None;
        }
        Some((requests, status))
    }
}

/// Increments a counter that only the calling thread writes.
fn bump(count: &AtomicU32) {
    count.store(
        count.load(Ordering::Relaxed).wrapping_add(1),
        Ordering::Relaxed,
    );
}

/// Moves the count of `src`, a stage counter only the calling thread writes,
/// onto `dst`, a ring counter, skipping both writes when it is zero.
fn move_count(src: &AtomicU32, dst: &AtomicU32) {
    let count = src.load(Ordering::Relaxed);
    if count != 0 {
        src.store(0, Ordering::Relaxed);
        dst.fetch_add(count, Ordering::Relaxed);
    }
}

/// Ring of one-second [`Slot`]s covering the trailing [`WINDOW_SECS`] seconds,
/// plus one stage per recording thread.
///
/// A tick is the number of whole seconds since `origin`; slot `tick % WINDOW_SECS`
/// is recycled by the first writer that reaches a newer second. A thread with an
/// index below [`MAX_STAGES`] records into its own stage, which holds one second,
/// and moves the stage into the ring slot of that second when it records the
/// next one. Nothing but that thread writes its stage, so recording touches no
/// cache line another core writes; threads without a stage record straight into
/// the ring.
pub(crate) struct SlidingWindow {
    /// Instant that tick zero is measured from.
    origin: Instant,
    /// Seconds added to the tick; non-zero only when tests advance time.
    extra_secs: Arc<AtomicU64>,
    /// One slot per second of the ring, on the heap so that creating a window
    /// never moves the 100 KiB ring by value.
    slots: Box<[Slot]>,
    /// Stage of each thread index, allocated by the thread's first record.
    stages: [OnceLock<Box<Slot>>; MAX_STAGES],
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

/// Samples of one second read out of a thread's stage.
struct Staged {
    /// Second the stage holds.
    tick: u64,
    /// Requests completed in that second on that thread.
    requests: u64,
    /// Responses per status class, indexed `1xx` to `5xx`.
    status: [u64; 5],
    /// Samples per latency bucket.
    hist: LatencyHist,
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
            slots: (0..WINDOW_SECS).map(|_| Slot::new()).collect(),
            stages: std::array::from_fn(|_| OnceLock::new()),
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
        self.record(
            crate::shard::thread_index(),
            self.current_tick(),
            bucket,
            class,
        );
    }

    /// Records one request in the second `tick` on behalf of the thread with
    /// index `thread`.
    ///
    /// `bucket` and `class` come from [`sample_of`], computed once by the caller
    /// and shared by the global and the per-route window. The sample goes into
    /// the thread's stage when it has one and the stage holds `tick` or an older
    /// second; otherwise it goes straight into the ring, where it is dropped
    /// when the slot has already moved on to a newer second.
    pub(crate) fn record(
        &self,
        thread: Option<usize>,
        tick: u64,
        bucket: usize,
        class: Option<usize>,
    ) {
        let Some(stage) = thread
            .filter(|&index| index < MAX_STAGES)
            .map(|index| self.stage(index))
        else {
            self.record_direct(tick, bucket, class);
            return;
        };
        // Only this thread writes the stage's tick, and the index pool's lock
        // ordered a previous owner's last write before this thread got the
        // index, so a relaxed load is current.
        let current = stage.tick.load(Ordering::Relaxed);
        if current != tick {
            // A stage past `tick` (which includes the sentinel `TICK_RESETTING`)
            // cannot take a sample of an earlier second.
            if current != TICK_EMPTY && current > tick {
                self.record_direct(tick, bucket, class);
                return;
            }
            // A second still inside the window but too close to the ring's wrap
            // to move safely stays in the stage for snapshots to read until it
            // expires; this sample bypasses the stage.
            if current != TICK_EMPTY
                && tick - current > FLUSH_MAX_AGE
                && tick - current < WINDOW_SECS
            {
                self.record_direct(tick, bucket, class);
                return;
            }
            self.advance_stage(stage, current, tick);
        }
        stage.add_owned(bucket, class);
    }

    /// Records one request in the ring slot of `tick`.
    ///
    /// The sample is dropped when the slot has already moved on to a newer second.
    fn record_direct(&self, tick: u64, bucket: usize, class: Option<usize>) {
        let Some(slot) = self.slot_for(tick) else {
            return;
        };
        if slot.tick.load(Ordering::Acquire) != tick {
            return;
        }
        slot.add(bucket, class);
    }

    /// Returns the stage of thread `index`, allocating it on first use.
    fn stage(&self, index: usize) -> &Slot {
        self.stages[index].get_or_init(|| Box::new(Slot::new()))
    }

    /// Moves `stage` from the second `current` to the second `tick`, first
    /// moving the samples it holds into the ring slot of `current`.
    ///
    /// Only the stage's owning thread calls this, so no other writer can be
    /// adding to the stage while it is drained. A second already outside the
    /// window is discarded instead of moved. Nothing in here can panic, which
    /// is what keeps the stage from being left in `TICK_RESETTING`.
    #[cold]
    #[inline(never)]
    fn advance_stage(&self, stage: &Slot, current: u64, tick: u64) {
        stage.tick.store(TICK_RESETTING, Ordering::Relaxed);
        // Pairs with the acquire fence in `Slot::read`: a snapshot that observes
        // any counter change below also observes the `TICK_RESETTING` mark and
        // discards its read of the stage.
        fence(Ordering::Release);
        if current != TICK_EMPTY {
            // The owner is the only writer, so `current <= tick` holds here.
            let slot = (tick - current <= FLUSH_MAX_AGE)
                .then(|| self.slot_for(current))
                .flatten();
            match slot {
                Some(slot) => stage.drain_into(slot),
                None => stage.clear(),
            }
        }
        stage.tick.store(tick, Ordering::Release);
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

    /// Folds the ring and the stages into the window aggregates and, when
    /// `with_series` is set, the per-second series.
    ///
    /// The stages are read before the ring and their ticks checked again after
    /// it; a stage that moved meanwhile has pushed samples into the ring that
    /// were also read from the stage, so the whole fold starts over. Threads
    /// move a stage at most once per second, so a retry is rare. A stage caught
    /// in the middle of a move is retried for as long as [`MAX_FOLD_ATTEMPTS`]
    /// allows and then left out of this one snapshot, which under-reports that
    /// thread's staged seconds once; nothing is ever counted twice.
    fn fold(&self, with_series: bool, tick: u64) -> TrafficSnapshot {
        let unix = with_series.then(unix_now);
        let mut staged = Vec::new();
        let mut observed = [TICK_EMPTY; MAX_STAGES];
        for attempt in 0..MAX_FOLD_ATTEMPTS {
            if attempt != 0 {
                back_off(attempt);
            }
            let stable = self.read_stages(tick, &mut staged, &mut observed);
            let snapshot = Fold::run(self, tick, unix, &staged);
            // Pairs with the release fence of `advance_stage`: a fold that read
            // moved samples out of the ring also sees that stage's tick change.
            fence(Ordering::Acquire);
            if self.stages_unchanged(&observed) && (stable || attempt + 1 == MAX_FOLD_ATTEMPTS) {
                return snapshot;
            }
        }
        staged.clear();
        Fold::run(self, tick, unix, &staged)
    }

    /// Reads every stage holding a second of the window ending at `tick` into
    /// `staged`, one entry per distinct second, recording the tick of each stage
    /// read into `observed`.
    ///
    /// Returns `false` when a stage was being moved and was skipped, so that the
    /// caller tries again once the move is over. Only stages that contributed
    /// samples are recorded in `observed`: a stage that was empty, ahead of
    /// `tick`, or already outside the window cannot move samples into a second
    /// this fold counts.
    fn read_stages(
        &self,
        tick: u64,
        staged: &mut Vec<Staged>,
        observed: &mut [u64; MAX_STAGES],
    ) -> bool {
        staged.clear();
        observed.fill(TICK_EMPTY);
        let mut stable = true;
        let mut scratch = LatencyHist::default();
        for (index, cell) in self.stages.iter().enumerate().take(live_indices()) {
            let Some(stage) = cell.get() else {
                continue;
            };
            let stage_tick = stage.tick.load(Ordering::Acquire);
            if stage_tick == TICK_EMPTY {
                continue;
            }
            if stage_tick == TICK_RESETTING {
                stable = false;
                continue;
            }
            if stage_tick > tick || tick - stage_tick >= WINDOW_SECS {
                continue;
            }
            match stage.read(stage_tick, &mut scratch) {
                Some((requests, status)) if requests != 0 => {
                    let entry = staged_second(staged, stage_tick);
                    entry.requests += requests;
                    add_status(&mut entry.status, status);
                    entry.hist.add_from(&scratch);
                    observed[index] = stage_tick;
                }
                Some(_) => {}
                None => stable = false,
            }
        }
        stable
    }

    /// Returns `true` when no stage recorded in `observed` has moved since.
    fn stages_unchanged(&self, observed: &[u64; MAX_STAGES]) -> bool {
        self.stages.iter().zip(observed).all(|(cell, &before)| {
            before == TICK_EMPTY
                || cell
                    .get()
                    .is_some_and(|stage| stage.tick.load(Ordering::Relaxed) == before)
        })
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
                    // Pairs with the acquire fence in `Slot::read`, so a reader
                    // that sees the zeroed counters also sees the tick change.
                    fence(Ordering::Release);
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

/// Running state of one [`SlidingWindow::fold`] attempt.
struct Fold<'a> {
    /// Ring being folded.
    window: &'a SlidingWindow,
    /// Tick the windows end at.
    tick: u64,
    /// Unix time of `tick`, or `None` when no series is built.
    unix: Option<i64>,
    /// Stage contents to merge into the seconds they belong to.
    staged: &'a [Staged],
    /// Scratch histogram the current second is loaded into.
    slot_hist: LatencyHist,
    /// Running total of every second walked so far.
    hist: LatencyHist,
    /// Running per-class status counts.
    status: [u64; 5],
    /// Running request count.
    requests: u64,
    /// Per-second samples, newest first while walking.
    series: Vec<SecondSample>,
}

impl<'a> Fold<'a> {
    /// Walks the ring once, newest second first, accumulating all three windows
    /// and, when `unix` is set, the per-second series.
    ///
    /// The windows nest, so one running total serves all three: it is the 30s
    /// window after 30 seconds, the 60s window after 60, and the 90s window at
    /// the end. Each second is therefore merged once instead of up to three
    /// times.
    fn run(
        window: &'a SlidingWindow,
        tick: u64,
        unix: Option<i64>,
        staged: &'a [Staged],
    ) -> TrafficSnapshot {
        let mut fold = Self {
            window,
            tick,
            unix,
            staged,
            slot_hist: LatencyHist::default(),
            hist: LatencyHist::default(),
            status: [0; 5],
            requests: 0,
            series: Vec::with_capacity(if unix.is_some() {
                WINDOW_SECS as usize
            } else {
                0
            }),
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

    /// Merges the seconds of the given ages, youngest first, into the running
    /// totals.
    ///
    /// A second is the ring slot plus every stage holding that second; the
    /// stages are merged before the per-second percentiles are taken, so the
    /// newest second, which usually lives in the stages alone, reports its
    /// latency like any other.
    fn walk(&mut self, ages: std::ops::Range<u64>) {
        for age in ages {
            let mut present = false;
            let mut requests = 0;
            let mut status = [0u64; 5];
            self.slot_hist.reset();
            if self.tick >= age {
                let slot_tick = self.tick - age;
                if let Some((slot_requests, slot_status)) = self.window.slots
                    [(slot_tick % WINDOW_SECS) as usize]
                    .read(slot_tick, &mut self.slot_hist)
                {
                    present = true;
                    requests = slot_requests;
                    status = slot_status;
                }
                for entry in self.staged.iter().filter(|entry| entry.tick == slot_tick) {
                    present = true;
                    requests += entry.requests;
                    add_status(&mut status, entry.status);
                    self.slot_hist.add_from(&entry.hist);
                }
            }

            if let Some(unix) = self.unix {
                let unix_secs = unix.saturating_sub(age as i64);
                self.series.push(if present {
                    let [p50_ns, p95_ns, p99_ns, p999_ns] = self.slot_hist.percentiles();
                    SecondSample {
                        unix_secs,
                        requests,
                        status,
                        p50_ns,
                        p95_ns,
                        p99_ns,
                        p999_ns,
                    }
                } else {
                    empty_sample(unix_secs)
                });
            }

            if requests != 0 {
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

/// Waits before fold attempt `attempt`.
///
/// A stage move takes microseconds, so the first retries only yield; the later
/// ones sleep, which lets a mover that was preempted mid-move run again even
/// when it is queued on another CPU.
fn back_off(attempt: usize) {
    if attempt <= 2 {
        std::thread::yield_now();
    } else if attempt <= 10 {
        std::thread::sleep(FOLD_SHORT_PAUSE);
    } else {
        std::thread::sleep(FOLD_LONG_PAUSE);
    }
}

/// Returns the entry of `staged` for the second `tick`, adding an empty one when
/// the second has no entry yet.
fn staged_second(staged: &mut Vec<Staged>, tick: u64) -> &mut Staged {
    let at = match staged.iter().position(|entry| entry.tick == tick) {
        Some(at) => at,
        None => {
            staged.push(Staged {
                tick,
                requests: 0,
                status: [0; 5],
                hist: LatencyHist::default(),
            });
            staged.len() - 1
        }
    };
    &mut staged[at]
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
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    use crate::shard::warm_indices;

    use super::*;

    /// Returns the tick the ring slot of `tick` currently holds.
    fn ring_tick(window: &SlidingWindow, tick: u64) -> u64 {
        window.slots[(tick % WINDOW_SECS) as usize]
            .tick
            .load(Ordering::Relaxed)
    }

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
    fn stage_holds_the_current_second_and_moves_into_the_ring() {
        warm_indices(1);
        let window = SlidingWindow::new();
        let tick = 10;
        let (bucket, class) = sample_of(1_000_000, 2);
        window.record(Some(0), tick, bucket, class);

        let stage = window.stages[0].get().expect("stage allocated");
        assert_eq!(stage.tick.load(Ordering::Relaxed), tick);
        assert_eq!(stage.requests.load(Ordering::Relaxed), 1);
        assert_eq!(ring_tick(&window, tick), TICK_EMPTY);

        let snap = window.fold(true, tick);
        assert_eq!(snap.window_30.requests, 1);
        assert!(snap.window_30.p50_ns.is_some());
        assert_eq!(snap.series[89].requests, 1);
        assert!(snap.series[89].p50_ns.is_some());
        let (window_30, _, window_90) = window.snapshot_windows_at(tick);
        assert_eq!(window_30.requests, 1);
        assert!(window_90.p50_ns.is_some());

        let (bucket, class) = sample_of(2_000_000, 4);
        window.record(Some(0), tick + 1, bucket, class);
        assert_eq!(stage.tick.load(Ordering::Relaxed), tick + 1);
        assert_eq!(stage.requests.load(Ordering::Relaxed), 1);
        assert_eq!(ring_tick(&window, tick), tick);
        let moved = &window.slots[(tick % WINDOW_SECS) as usize];
        assert_eq!(moved.requests.load(Ordering::Relaxed), 1);
        assert_eq!(moved.status[1].load(Ordering::Relaxed), 1);

        let snap = window.fold(true, tick + 1);
        assert_eq!(snap.window_90.requests, 2);
        assert_eq!(snap.window_90.status[1], 1);
        assert_eq!(snap.window_90.status[3], 1);
        assert_eq!(snap.series[88].requests, 1);
        assert_eq!(snap.series[89].requests, 1);
        assert!(snap.series[88].p50_ns.is_some());
    }

    #[test]
    fn second_at_the_oldest_movable_age_is_moved_into_the_ring() {
        warm_indices(1);
        let window = SlidingWindow::new();
        let (bucket, class) = sample_of(1_000_000, 2);
        let tick = 7;
        window.record(Some(0), tick, bucket, class);
        window.record(Some(0), tick + FLUSH_MAX_AGE, bucket, class);
        assert_eq!(ring_tick(&window, tick), tick);
        let stage = window.stages[0].get().expect("stage allocated");
        assert_eq!(stage.tick.load(Ordering::Relaxed), tick + FLUSH_MAX_AGE);
        let snap = window.fold(true, tick + FLUSH_MAX_AGE);
        assert_eq!(snap.window_90.requests, 2);
        assert_eq!(snap.series[1].requests, 1);
        assert_eq!(snap.series[89].requests, 1);
    }

    #[test]
    fn a_second_split_between_the_ring_and_a_stage_is_merged() {
        warm_indices(1);
        let window = SlidingWindow::new();
        let (fast, class) = sample_of(1_000_000, 2);
        let (slow, _) = sample_of(50_000_000, 2);
        window.record(None, 20, fast, class);
        window.record(Some(0), 20, slow, class);
        let snap = window.fold(true, 20);
        assert_eq!(snap.series[89].requests, 2);
        assert!(snap.series[89].p50_ns.unwrap() < 50_000_000);
        assert!(snap.series[89].p99_ns.unwrap() >= 50_000_000);
        assert_eq!(snap.window_30.requests, 2);
        assert!(snap.window_30.p99_ns.unwrap() >= 50_000_000);
    }

    #[test]
    fn a_stage_moved_during_a_fold_is_detected_and_the_fold_repeated() {
        warm_indices(1);
        let window = SlidingWindow::new();
        let (bucket, class) = sample_of(1_000_000, 2);
        window.record(Some(0), 10, bucket, class);
        window.record(Some(0), 10, bucket, class);
        let mut staged = Vec::new();
        let mut observed = [TICK_EMPTY; MAX_STAGES];
        assert!(window.read_stages(11, &mut staged, &mut observed));
        assert_eq!(staged.len(), 1);

        // The stage moves after it was read: the ring now holds the two samples
        // the fold also took from the stage, so a fold that trusted its read
        // would count them twice.
        window.record(Some(0), 11, bucket, class);
        let torn = Fold::run(&window, 11, None, &staged);
        assert_eq!(torn.window_90.requests, 4);
        assert!(!window.stages_unchanged(&observed));
        assert_eq!(window.fold(false, 11).window_90.requests, 3);
    }

    #[test]
    fn samples_without_a_stage_go_straight_into_the_ring() {
        let window = SlidingWindow::new();
        let tick = window.current_tick();
        let (bucket, class) = sample_of(1_000_000, 2);
        window.record(None, tick, bucket, class);
        window.record(Some(MAX_STAGES), tick, bucket, class);
        assert_eq!(ring_tick(&window, tick), tick);
        assert!(window.stages.iter().all(|cell| cell.get().is_none()));
        assert_eq!(window.snapshot().window_90.requests, 2);
    }

    #[test]
    fn second_near_the_ring_wrap_is_kept_until_it_expires() {
        warm_indices(1);
        let window = SlidingWindow::new();
        let (bucket, class) = sample_of(1_000_000, 2);
        let tick = 5;
        window.record(Some(0), tick, bucket, class);
        let stage = window.stages[0].get().expect("stage allocated");

        // One second before the ring wraps the old second is still in the
        // window: it stays in the stage, its ring slot is untouched, and the new
        // sample goes straight into the ring.
        let near = tick + FLUSH_MAX_AGE + 1;
        window.record(Some(0), near, bucket, class);
        assert_eq!(stage.tick.load(Ordering::Relaxed), tick);
        assert_eq!(ring_tick(&window, tick), TICK_EMPTY);
        assert_eq!(ring_tick(&window, near), near);
        let snap = window.fold(true, near);
        assert_eq!(snap.window_90.requests, 2);
        assert_eq!(snap.series[0].requests, 1);
        assert_eq!(snap.series[89].requests, 1);

        // Once the old second has expired the stage is cleared, not moved.
        let expired = tick + WINDOW_SECS;
        window.record(Some(0), expired, bucket, class);
        assert_eq!(stage.tick.load(Ordering::Relaxed), expired);
        assert_eq!(stage.requests.load(Ordering::Relaxed), 1);
        assert_eq!(ring_tick(&window, tick), TICK_EMPTY);
        let snap = window.fold(true, expired);
        assert_eq!(snap.window_90.requests, 2);
        assert_eq!(snap.series[88].requests, 1);
        assert_eq!(snap.series[89].requests, 1);
    }

    #[test]
    fn fold_keeps_the_stable_stages_when_one_is_stuck_mid_move() {
        warm_indices(2);
        let window = SlidingWindow::new();
        let (bucket, class) = sample_of(1_000_000, 2);
        window.record(Some(0), 30, bucket, class);
        window.record(Some(1), 30, bucket, class);
        let stage = window.stages[0].get().expect("stage allocated");

        // A mover preempted in the middle of its move leaves its stage marked;
        // the snapshot waits, then reports every other stage.
        stage.tick.store(TICK_RESETTING, Ordering::Relaxed);
        let disturbed = window.fold(true, 30);
        stage.tick.store(30, Ordering::Relaxed);
        assert_eq!(disturbed.window_90.requests, 1);
        assert_eq!(window.fold(true, 30).window_90.requests, 2);
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
    fn snapshots_taken_during_stage_moves_count_every_sample_once() {
        const THREADS: usize = 3;
        let window = Arc::new(SlidingWindow::new());
        // Each recorder counts a sample before and after recording it, so the
        // samples a snapshot may see lie between the finished total read before
        // it and the started total read after it.
        let started_count: Arc<Vec<AtomicU64>> =
            Arc::new((0..THREADS).map(|_| AtomicU64::new(0)).collect());
        let finished_count: Arc<Vec<AtomicU64>> =
            Arc::new((0..THREADS).map(|_| AtomicU64::new(0)).collect());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let recorders: Vec<_> = (0..THREADS)
            .map(|index| {
                let window = Arc::clone(&window);
                let started_count = Arc::clone(&started_count);
                let finished_count = Arc::clone(&finished_count);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        started_count[index].fetch_add(1, Ordering::SeqCst);
                        window.observe(1_000_000, 2);
                        finished_count[index].fetch_add(1, Ordering::SeqCst);
                    }
                })
            })
            .collect();
        let sum = |counts: &[AtomicU64]| -> u64 {
            counts
                .iter()
                .map(|count| count.load(Ordering::SeqCst))
                .sum()
        };

        // Advancing the clock makes every recorder move its stage, so the
        // snapshots below often overlap stage moves.
        let started = Instant::now();
        let mut rounds = 0;
        while started.elapsed() < Duration::from_millis(400) {
            if rounds % 5 == 0 {
                window.advance_secs(1);
            }
            let finished_before = sum(&finished_count);
            let snapshot = window.snapshot();
            let started_after = sum(&started_count);
            let series: u64 = snapshot.series.iter().map(|sample| sample.requests).sum();
            assert!(
                snapshot.window_90.requests >= finished_before,
                "lost samples"
            );
            assert!(
                snapshot.window_90.requests <= started_after,
                "double counted samples"
            );
            assert_eq!(series, snapshot.window_90.requests);
            rounds += 1;
            std::thread::sleep(Duration::from_millis(1));
        }
        stop.store(true, Ordering::Relaxed);
        for recorder in recorders {
            recorder.join().unwrap();
        }
        // At rest every total agrees; while recording, a status class and the
        // request count of one second are read a few instructions apart and may
        // differ by the samples in progress, as they always could.
        let total = sum(&finished_count);
        let snapshot = window.snapshot();
        assert_eq!(snapshot.window_90.requests, total);
        assert_eq!(snapshot.window_90.status[1], total);
    }

    #[test]
    fn slot_layout_matches_the_crate_docs() {
        assert_eq!(std::mem::size_of::<Slot>(), 1_152);
        assert_eq!(std::mem::align_of::<Slot>(), 128);
        // A ring: 90 slots on the heap plus the window with its stage cells,
        // about 102 KiB in all.
        let ring = WINDOW_SECS as usize * std::mem::size_of::<Slot>()
            + std::mem::size_of::<SlidingWindow>();
        assert!(ring <= 105 * 1024);
        assert!(std::mem::size_of::<SlidingWindow>() <= 2 * 1024);
    }
}
