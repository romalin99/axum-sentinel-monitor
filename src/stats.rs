//! Shared monitor state: live HTTP counters and the cached snapshot.

use std::cell::Cell;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, PoisonError, RwLock, TryLockError};
use std::time::{Duration, Instant};

use axum::http::StatusCode;

use crate::collect::Collector;
use crate::endpoints::{EndpointSet, RouteHandle};
use crate::histogram::{SlidingWindow, sample_of};
use crate::snapshot::Snapshot;

/// Number of shards the global counters are split into.
///
/// Every request increments the request counter and the in-flight gauge, so
/// with a single copy all cores would bounce the same cache line. A thread
/// writes to the shard it was assigned on first use; snapshots sum the shards.
const COUNTER_SHARDS: usize = 16;

/// Source of the shard indices handed to threads.
static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// Shard of the global counters this thread writes to.
    static SHARD: Cell<usize> = {
        Cell::new(NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % COUNTER_SHARDS)
    };
}

/// Returns the shard of the current thread, or shard zero while the thread is
/// shutting down and its thread-local storage is gone.
fn current_shard() -> usize {
    SHARD.try_with(Cell::get).unwrap_or(0)
}

/// State shared by a [`crate::Monitor`] and its layers: live HTTP counters plus
/// the snapshot collector and its cache.
pub(crate) struct SharedStats {
    /// Counters updated on the request path.
    http: HttpMetrics,
    /// Collector and snapshot cache.
    collect: RwLock<CollectState>,
    /// Lifetime of a cached snapshot.
    refresh: Duration,
    /// Monitor route, whose requests are not recorded.
    skip_path: Box<str>,
}

/// Collector and the snapshot it produced last, guarded by one lock.
struct CollectState {
    /// Sampler that keeps the previous CPU and network readings.
    collector: Collector,
    /// Snapshot collected last, if any.
    cache: Option<CacheEntry>,
}

/// Snapshot kept until it is older than the refresh interval.
struct CacheEntry {
    /// Cached snapshot.
    snapshot: Arc<Snapshot>,
    /// Instant the snapshot was collected.
    cached_at: Instant,
}

/// One shard of the lifetime counters, padded to its own cache line so that
/// threads on different shards never share a line.
#[repr(align(64))]
struct CounterShard {
    /// Requests started since creation.
    requests: AtomicU64,
    /// Requests started minus requests finished.
    ///
    /// Signed because a request may start on one shard and finish on another
    /// when its task migrates between worker threads; only the sum over all
    /// shards is meaningful.
    in_flight: AtomicI64,
    /// Responses per status class since creation, indexed `1xx` to `5xx`.
    status: [AtomicU64; 5],
}

impl CounterShard {
    /// Creates a zeroed shard.
    fn new() -> Self {
        Self {
            requests: AtomicU64::new(0),
            in_flight: AtomicI64::new(0),
            status: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

/// Lock-free HTTP counters updated on the request path.
pub(crate) struct HttpMetrics {
    /// Lifetime counters, one shard per group of threads.
    shards: [CounterShard; COUNTER_SHARDS],
    /// Completed requests of all routes over the trailing window.
    latency: SlidingWindow,
    /// Per-route metrics on the same time base as `latency`.
    endpoints: EndpointSet,
}

impl SharedStats {
    /// Creates the shared state with an empty snapshot cache.
    pub(crate) fn new(refresh: Duration, skip_path: &str) -> Arc<Self> {
        Arc::new(Self {
            http: HttpMetrics::new(),
            collect: RwLock::new(CollectState {
                collector: Collector::new(),
                cache: None,
            }),
            refresh,
            skip_path: Box::from(skip_path),
        })
    }

    /// Returns the live HTTP counters.
    pub(crate) fn http(&self) -> &HttpMetrics {
        &self.http
    }

    /// Returns the monitor route, whose requests are not recorded.
    pub(crate) fn skip_path(&self) -> &str {
        &self.skip_path
    }

    /// Returns an owned copy of the latest snapshot.
    pub(crate) fn snapshot(&self) -> Snapshot {
        Arc::unwrap_or_clone(self.snapshot_arc())
    }

    /// Returns the cached snapshot while it is fresh, without collecting.
    pub(crate) fn cached_snapshot(&self) -> Option<Arc<Snapshot>> {
        let state = self.collect.read().unwrap_or_else(PoisonError::into_inner);
        self.fresh_snapshot(&state)
    }

    /// Returns the cached snapshot while it is fresh, or `None` when it has
    /// expired or another thread is collecting right now.
    ///
    /// Unlike [`Self::cached_snapshot`] this never blocks, so an async task can
    /// call it and move a cold collection to the blocking pool instead of
    /// stalling its worker behind one that is already in progress.
    pub(crate) fn try_cached_snapshot(&self) -> Option<Arc<Snapshot>> {
        let state = match self.collect.try_read() {
            Ok(state) => state,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => return None,
        };
        self.fresh_snapshot(&state)
    }

    /// Returns the snapshot of `state` while it is younger than the refresh
    /// interval.
    fn fresh_snapshot(&self, state: &CollectState) -> Option<Arc<Snapshot>> {
        state
            .cache
            .as_ref()
            .filter(|entry| entry.cached_at.elapsed() < self.refresh)
            .map(|entry| Arc::clone(&entry.snapshot))
    }

    /// Returns the cached snapshot, collecting a new one when it has expired.
    pub(crate) fn snapshot_arc(&self) -> Arc<Snapshot> {
        if let Some(snapshot) = self.cached_snapshot() {
            return snapshot;
        }
        let mut state = self.collect.write().unwrap_or_else(PoisonError::into_inner);
        if let Some(snapshot) = self.fresh_snapshot(&state) {
            return snapshot;
        }
        let snapshot = Arc::new(state.collector.collect(&self.http));
        state.cache = Some(CacheEntry {
            snapshot: Arc::clone(&snapshot),
            cached_at: Instant::now(),
        });
        snapshot
    }
}

impl HttpMetrics {
    /// Creates zeroed counters whose windows start now.
    fn new() -> Self {
        let latency = SlidingWindow::new();
        let endpoints = EndpointSet::with_clock(latency.origin(), latency.extra_secs());
        Self {
            shards: std::array::from_fn(|_| CounterShard::new()),
            latency,
            endpoints,
        }
    }

    /// Counts a request that started at `now` and resolves its route slot.
    pub(crate) fn begin_request(&self, method: &str, path: &str, now: Instant) -> RouteHandle {
        let shard = &self.shards[current_shard()];
        shard.requests.fetch_add(1, Ordering::Relaxed);
        shard.in_flight.fetch_add(1, Ordering::Relaxed);
        self.endpoints.begin(method, path, now)
    }

    /// Records the status class and latency of a request that started at
    /// `started` and completed at `now`.
    pub(crate) fn finish(
        &self,
        route: &RouteHandle,
        started: Instant,
        now: Instant,
        status: StatusCode,
    ) {
        let ns =
            u64::try_from(now.saturating_duration_since(started).as_nanos()).unwrap_or(u64::MAX);
        let (bucket, class) = sample_of(ns, (status.as_u16() / 100) as u8);
        // Classes outside `1xx` to `5xx` have no counter and are ignored.
        if let Some(class) = class {
            self.shards[current_shard()].status[class].fetch_add(1, Ordering::Relaxed);
        }
        let tick = self.latency.tick_at(now);
        self.latency.record(tick, bucket, class);
        self.endpoints.observe(route, tick, bucket, class);
    }

    /// Returns the requests started since creation.
    pub(crate) fn requests(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.requests.load(Ordering::Relaxed))
            .sum()
    }

    /// Returns the requests currently being handled.
    pub(crate) fn in_flight(&self) -> u64 {
        let total: i64 = self
            .shards
            .iter()
            .map(|shard| shard.in_flight.load(Ordering::Relaxed))
            .sum();
        // Starts and finishes of one request may be read from different shards
        // at different moments, so the sum can briefly dip below zero.
        u64::try_from(total).unwrap_or(0)
    }

    /// Returns the responses per status class since creation, indexed `1xx` to
    /// `5xx`.
    pub(crate) fn status_counts(&self) -> [u64; 5] {
        std::array::from_fn(|index| {
            self.shards
                .iter()
                .map(|shard| shard.status[index].load(Ordering::Relaxed))
                .sum()
        })
    }

    /// Returns the sliding window of all routes.
    pub(crate) fn latency(&self) -> &SlidingWindow {
        &self.latency
    }

    /// Returns the per-route table.
    pub(crate) fn endpoints(&self) -> &EndpointSet {
        &self.endpoints
    }

    /// Decrements the global and per-route in-flight gauges.
    pub(crate) fn end_in_flight(&self, route: &RouteHandle) {
        self.shards[current_shard()]
            .in_flight
            .fetch_sub(1, Ordering::Relaxed);
        route.end();
    }
}

/// Releases the in-flight gauges on drop, so a request that is cancelled or
/// never polled is still accounted for.
pub(crate) struct InFlightGuard {
    /// State whose global gauge is released.
    pub(crate) stats: Arc<SharedStats>,
    /// Route whose gauge is released.
    pub(crate) route: RouteHandle,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.stats.http.end_in_flight(&self.route);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_flight_decrement_saturates_at_zero() {
        let stats = SharedStats::new(Duration::from_secs(1), "/monitor");
        let route = stats.http.begin_request("GET", "/", Instant::now());
        stats.http.end_in_flight(&route);
        stats.http.end_in_flight(&route);
        assert_eq!(stats.http.in_flight(), 0);
        assert_eq!(stats.http.endpoints().snapshot()[0].in_flight, 0);
    }

    #[test]
    fn counters_sum_across_shards() {
        let stats = SharedStats::new(Duration::from_secs(1), "/monitor");
        let threads: Vec<_> = (0..COUNTER_SHARDS * 2)
            .map(|_| {
                let stats = Arc::clone(&stats);
                std::thread::spawn(move || {
                    let started = Instant::now();
                    let route = stats.http.begin_request("GET", "/", started);
                    stats
                        .http
                        .finish(&route, started, Instant::now(), StatusCode::OK);
                    stats.http.end_in_flight(&route);
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let expected = (COUNTER_SHARDS * 2) as u64;
        assert_eq!(stats.http.requests(), expected);
        assert_eq!(stats.http.in_flight(), 0);
        assert_eq!(stats.http.status_counts()[1], expected);
        assert_eq!(stats.http.latency().snapshot().window_90.requests, expected);
    }
}
