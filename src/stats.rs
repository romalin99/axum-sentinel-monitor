//! Shared monitor state: live HTTP counters and the cached snapshot.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant};

use axum::http::StatusCode;

use crate::collect::Collector;
use crate::endpoints::{EndpointSet, RouteHandle, saturating_dec};
use crate::histogram::SlidingWindow;
use crate::snapshot::Snapshot;

/// State shared by a [`crate::Monitor`] and its layers: live HTTP counters plus
/// the snapshot collector and its cache.
pub(crate) struct SharedStats {
    /// Counters updated on the request path.
    http: HttpMetrics,
    /// Collector and snapshot cache.
    collect: RwLock<CollectState>,
    /// Lifetime of a cached snapshot.
    refresh: Duration,
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

/// Lock-free HTTP counters updated on the request path.
pub(crate) struct HttpMetrics {
    /// Requests started since creation.
    requests: AtomicU64,
    /// Requests currently being handled.
    in_flight: AtomicU64,
    /// Responses per status class since creation, indexed `1xx` to `5xx`.
    status: [AtomicU64; 5],
    /// Completed requests of all routes over the trailing window.
    latency: SlidingWindow,
    /// Per-route metrics on the same time base as `latency`.
    endpoints: EndpointSet,
}

impl SharedStats {
    /// Creates the shared state with an empty snapshot cache.
    pub(crate) fn new(refresh: Duration) -> Arc<Self> {
        Arc::new(Self {
            http: HttpMetrics::new(),
            collect: RwLock::new(CollectState {
                collector: Collector::new(),
                cache: None,
            }),
            refresh,
        })
    }

    /// Returns the live HTTP counters.
    pub(crate) fn http(&self) -> &HttpMetrics {
        &self.http
    }

    /// Returns an owned copy of the latest snapshot.
    pub(crate) fn snapshot(&self) -> Snapshot {
        Arc::unwrap_or_clone(self.snapshot_arc())
    }

    /// Returns the cached snapshot while it is fresh, without collecting.
    pub(crate) fn cached_snapshot(&self) -> Option<Arc<Snapshot>> {
        let state = self.collect.read().unwrap_or_else(PoisonError::into_inner);
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
        if let Some(entry) = &state.cache
            && entry.cached_at.elapsed() < self.refresh
        {
            return Arc::clone(&entry.snapshot);
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
            requests: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            status: std::array::from_fn(|_| AtomicU64::new(0)),
            latency,
            endpoints,
        }
    }

    /// Counts a request as started and resolves its route slot.
    pub(crate) fn begin_request(&self, method: &str, path: &str) -> RouteHandle {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        self.endpoints.begin(method, path)
    }

    /// Records the status class and latency of a completed request.
    pub(crate) fn finish(&self, route: &RouteHandle, elapsed: Duration, status: StatusCode) {
        let ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        let class = (status.as_u16() / 100) as u8;
        // Classes outside `1xx` to `5xx` have no counter and are ignored.
        if let Some(counter) = usize::from(class)
            .checked_sub(1)
            .and_then(|index| self.status.get(index))
        {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        let tick = self.latency.current_tick();
        self.latency.observe_at(tick, ns, class);
        self.endpoints.observe(route, tick, ns, class);
    }

    /// Returns the requests started since creation.
    pub(crate) fn requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    /// Returns the requests currently being handled.
    pub(crate) fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Returns the responses per status class since creation, indexed `1xx` to
    /// `5xx`.
    pub(crate) fn status_counts(&self) -> [u64; 5] {
        std::array::from_fn(|index| self.status[index].load(Ordering::Relaxed))
    }

    /// Returns the sliding window of all routes.
    pub(crate) fn latency(&self) -> &SlidingWindow {
        &self.latency
    }

    /// Returns the per-route table.
    pub(crate) fn endpoints(&self) -> &EndpointSet {
        &self.endpoints
    }

    /// Decrements the global and per-route in-flight gauges, saturating at zero.
    pub(crate) fn end_in_flight(&self, route: &RouteHandle) {
        saturating_dec(&self.in_flight);
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
        let stats = SharedStats::new(Duration::from_secs(1));
        let route = stats.http.begin_request("GET", "/");
        stats.http.end_in_flight(&route);
        stats.http.end_in_flight(&route);
        assert_eq!(stats.http.in_flight(), 0);
        assert_eq!(stats.http.endpoints().snapshot()[0].in_flight, 0);
    }
}
