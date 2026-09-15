use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use axum::http::StatusCode;

use crate::collect::Collector;
use crate::endpoints::{EndpointSet, RouteHandle};
use crate::histogram::SlidingWindow;
use crate::snapshot::Snapshot;

pub(crate) struct SharedStats {
    http: HttpMetrics,
    collect: RwLock<CollectState>,
    refresh: Duration,
}

struct CollectState {
    collector: Collector,
    cache: Option<CacheEntry>,
}

struct CacheEntry {
    snapshot: Arc<Snapshot>,
    cached_at: Instant,
}

pub(crate) struct HttpMetrics {
    requests: AtomicU64,
    in_flight: AtomicU64,
    status1: AtomicU64,
    status2: AtomicU64,
    status3: AtomicU64,
    status4: AtomicU64,
    status5: AtomicU64,
    latency: SlidingWindow,
    endpoints: EndpointSet,
}

impl SharedStats {
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

    pub(crate) fn http(&self) -> &HttpMetrics {
        &self.http
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        Arc::unwrap_or_clone(self.snapshot_arc())
    }

    pub(crate) fn cached_snapshot(&self) -> Option<Arc<Snapshot>> {
        let state = self
            .collect
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state
            .cache
            .as_ref()
            .filter(|entry| entry.cached_at.elapsed() < self.refresh)
            .map(|entry| Arc::clone(&entry.snapshot))
    }

    pub(crate) fn snapshot_arc(&self) -> Arc<Snapshot> {
        if let Some(snapshot) = self.cached_snapshot() {
            return snapshot;
        }
        let mut state = self
            .collect
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
    fn new() -> Self {
        let latency = SlidingWindow::new();
        let endpoints = EndpointSet::with_clock(latency.origin(), latency.extra_secs());
        Self {
            requests: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            status1: AtomicU64::new(0),
            status2: AtomicU64::new(0),
            status3: AtomicU64::new(0),
            status4: AtomicU64::new(0),
            status5: AtomicU64::new(0),
            latency,
            endpoints,
        }
    }

    pub(crate) fn begin_request(&self, method: &str, path: &str) -> RouteHandle {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        self.endpoints.begin(method, path)
    }

    pub(crate) fn finish(&self, route: &RouteHandle, elapsed: Duration, status: StatusCode) {
        self.record_status(status);
        let ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        let class = (status.as_u16() / 100) as u8;
        let tick = self.latency.current_tick();
        self.latency.observe_at(tick, ns, class);
        self.endpoints.observe(route, tick, ns, class);
    }

    fn record_status(&self, status: StatusCode) {
        match status.as_u16() / 100 {
            1 => self.status1.fetch_add(1, Ordering::Relaxed),
            2 => self.status2.fetch_add(1, Ordering::Relaxed),
            3 => self.status3.fetch_add(1, Ordering::Relaxed),
            4 => self.status4.fetch_add(1, Ordering::Relaxed),
            5 => self.status5.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
    }

    pub(crate) fn requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    pub(crate) fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    pub(crate) fn status1(&self) -> u64 {
        self.status1.load(Ordering::Relaxed)
    }

    pub(crate) fn status2(&self) -> u64 {
        self.status2.load(Ordering::Relaxed)
    }

    pub(crate) fn status3(&self) -> u64 {
        self.status3.load(Ordering::Relaxed)
    }

    pub(crate) fn status4(&self) -> u64 {
        self.status4.load(Ordering::Relaxed)
    }

    pub(crate) fn status5(&self) -> u64 {
        self.status5.load(Ordering::Relaxed)
    }

    pub(crate) fn latency(&self) -> &SlidingWindow {
        &self.latency
    }

    pub(crate) fn endpoints(&self) -> &EndpointSet {
        &self.endpoints
    }

    pub(crate) fn end_in_flight(&self, route: &RouteHandle) {
        let _ = self
            .in_flight
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_sub(1)
            });
        route.end();
    }
}

pub(crate) struct InFlightGuard {
    pub(crate) stats: Arc<SharedStats>,
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
