use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::http::StatusCode;

use crate::collect::Collector;
use crate::histogram::LatencyHistogram;
use crate::snapshot::Snapshot;

pub(crate) struct SharedStats {
    http: HttpMetrics,
    collect: Mutex<CollectState>,
    refresh: Duration,
}

struct CollectState {
    collector: Collector,
    cache: Option<CacheEntry>,
}

struct CacheEntry {
    snapshot: Snapshot,
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
    latency: LatencyHistogram,
}

impl SharedStats {
    pub(crate) fn new(refresh: Duration) -> Arc<Self> {
        Arc::new(Self {
            http: HttpMetrics::new(),
            collect: Mutex::new(CollectState {
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
        let mut state = self
            .collect
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(entry) = &state.cache {
            if entry.cached_at.elapsed() < self.refresh {
                return entry.snapshot.clone();
            }
        }
        let snapshot = state.collector.collect(&self.http);
        state.cache = Some(CacheEntry {
            snapshot: snapshot.clone(),
            cached_at: Instant::now(),
        });
        snapshot
    }
}

impl HttpMetrics {
    fn new() -> Self {
        Self {
            requests: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            status1: AtomicU64::new(0),
            status2: AtomicU64::new(0),
            status3: AtomicU64::new(0),
            status4: AtomicU64::new(0),
            status5: AtomicU64::new(0),
            latency: LatencyHistogram::default(),
        }
    }

    pub(crate) fn begin_request(&self) -> u64 {
        let sequence = self.requests.fetch_add(1, Ordering::Relaxed) + 1;
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        sequence
    }

    pub(crate) fn finish(&self, sequence: u64, elapsed: Duration, status: StatusCode) {
        self.record_status(status);
        let ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.latency.observe_sharded(ns, sequence);
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

    pub(crate) fn latency(&self) -> &LatencyHistogram {
        &self.latency
    }

    pub(crate) fn end_in_flight(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(crate) struct InFlightGuard(pub(crate) Arc<SharedStats>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.http.end_in_flight();
    }
}
