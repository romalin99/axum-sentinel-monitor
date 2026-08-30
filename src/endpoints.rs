use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::histogram::{SlidingWindow, WindowAgg};

/// Upper bound on tracked routes. When the table is full a row is evicted in
/// this order of preference: rows with no requests in the trailing 60s window
/// first, then the rest, least recently used first within each group. Rows with
/// in-flight requests are never evicted, so a long-running request keeps its row
/// until it completes.
const MAX_ENDPOINTS: usize = 64;
const MAX_PATH_CHARS: usize = 128;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RouteKey {
    method: String,
    path: String,
}

struct RouteMetrics {
    window: SlidingWindow,
    in_flight: AtomicU64,
    last_used: AtomicU64,
}

impl RouteMetrics {
    fn touch(&self, clock: &AtomicU64) {
        let tick = clock.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        self.last_used.store(tick, Ordering::Relaxed);
    }
}

pub(crate) struct EndpointSet {
    origin: Instant,
    extra_secs: Arc<AtomicU64>,
    clock: AtomicU64,
    routes: Mutex<HashMap<RouteKey, Arc<RouteMetrics>>>,
}

#[derive(Clone, Debug)]
pub(crate) struct EndpointTraffic {
    pub method: String,
    pub path: String,
    pub in_flight: u64,
    pub window_30: WindowAgg,
    pub window_60: WindowAgg,
}

impl EndpointSet {
    pub(crate) fn with_clock(origin: Instant, extra_secs: Arc<AtomicU64>) -> Self {
        Self {
            origin,
            extra_secs,
            clock: AtomicU64::new(0),
            routes: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn begin(&self, method: &str, path: &str) {
        self.route_metrics(method, path)
            .in_flight
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn end(&self, method: &str, path: &str) {
        let key = RouteKey {
            method: normalize_method(method),
            path: normalize_path(path),
        };
        let overflow = RouteKey {
            method: "*".into(),
            path: "/...".into(),
        };
        let routes = self
            .routes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let metrics = routes.get(&key).or_else(|| routes.get(&overflow)).cloned();
        drop(routes);
        if let Some(metrics) = metrics {
            saturating_dec(&metrics.in_flight);
        }
    }

    pub(crate) fn observe(&self, method: &str, path: &str, ns: u64, status_class: u8) {
        self.route_metrics(method, path)
            .window
            .observe(ns, status_class);
    }

    pub(crate) fn snapshot(&self) -> Vec<EndpointTraffic> {
        let routes = self
            .routes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut rows: Vec<EndpointTraffic> = routes
            .iter()
            .map(|(key, metrics)| {
                let traffic = metrics.window.snapshot();
                EndpointTraffic {
                    method: key.method.clone(),
                    path: key.path.clone(),
                    in_flight: metrics.in_flight.load(Ordering::Relaxed),
                    window_30: traffic.window_30,
                    window_60: traffic.window_60,
                }
            })
            .collect();
        rows.sort_by(|left, right| {
            right
                .in_flight
                .cmp(&left.in_flight)
                .then_with(|| right.window_60.requests.cmp(&left.window_60.requests))
                .then_with(|| left.method.cmp(&right.method))
                .then_with(|| left.path.cmp(&right.path))
        });
        rows
    }

    fn route_metrics(&self, method: &str, path: &str) -> Arc<RouteMetrics> {
        let key = RouteKey {
            method: normalize_method(method),
            path: normalize_path(path),
        };
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = routes.get(&key) {
            existing.touch(&self.clock);
            return Arc::clone(existing);
        }
        if routes.len() >= MAX_ENDPOINTS && !evict_one(&mut routes) {
            let overflow = RouteKey {
                method: "*".into(),
                path: "/...".into(),
            };
            let metrics = routes
                .entry(overflow)
                .or_insert_with(|| self.new_metrics())
                .clone();
            metrics.touch(&self.clock);
            return metrics;
        }
        let created = self.new_metrics();
        created.touch(&self.clock);
        routes.insert(key, Arc::clone(&created));
        created
    }

    fn new_metrics(&self) -> Arc<RouteMetrics> {
        Arc::new(RouteMetrics {
            window: SlidingWindow::with_clock(self.origin, Arc::clone(&self.extra_secs)),
            in_flight: AtomicU64::new(0),
            last_used: AtomicU64::new(0),
        })
    }
}

/// Drops one idle row, preferring routes that saw no request in the trailing
/// 60s window and breaking ties by least recently used. Returns `false` when
/// every row is in flight — the caller then falls back to the shared overflow
/// row so the table stays bounded either way.
fn evict_one(routes: &mut HashMap<RouteKey, Arc<RouteMetrics>>) -> bool {
    let mut cold: Option<(RouteKey, u64)> = None;
    let mut warm: Option<(RouteKey, u64)> = None;
    for (key, metrics) in routes.iter() {
        if metrics.in_flight.load(Ordering::Relaxed) != 0 {
            continue;
        }
        let last_used = metrics.last_used.load(Ordering::Relaxed);
        let group = if metrics.window.recent_requests() == 0 {
            &mut cold
        } else {
            &mut warm
        };
        if group
            .as_ref()
            .is_none_or(|(_, oldest)| last_used < *oldest)
        {
            *group = Some((key.clone(), last_used));
        }
    }
    let Some((victim, _)) = cold.or(warm) else {
        return false;
    };
    routes.remove(&victim);
    true
}

fn saturating_dec(value: &AtomicU64) {
    let mut current = value.load(Ordering::Relaxed);
    while current > 0 {
        match value.compare_exchange_weak(
            current,
            current - 1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return,
            Err(seen) => current = seen,
        }
    }
}

pub(crate) fn normalize_method(method: &str) -> String {
    let method = method.trim();
    if method.is_empty() {
        return "GET".into();
    }
    method
        .chars()
        .take(16)
        .map(|ch| ch.to_ascii_uppercase())
        .collect()
}

pub(crate) fn normalize_path(path: &str) -> String {
    let path = path.split(['?', '#']).next().unwrap_or("/");
    let mut out = String::from("/");
    let mut wrote = false;
    for segment in path.split('/') {
        if segment.is_empty() {
            continue;
        }
        if wrote {
            out.push('/');
        }
        wrote = true;
        if looks_like_id(segment) || is_route_param(segment) {
            out.push_str(":id");
        } else {
            for ch in segment.chars().take(48) {
                out.push(ch);
            }
        }
        if out.len() > MAX_PATH_CHARS {
            out.truncate(MAX_PATH_CHARS);
            break;
        }
    }
    if !wrote {
        return "/".into();
    }
    out
}

fn is_route_param(segment: &str) -> bool {
    (segment.starts_with('{') && segment.ends_with('}') && segment.len() >= 3)
        || (segment.starts_with(':') && segment.len() > 1)
}

fn looks_like_id(segment: &str) -> bool {
    if segment.is_empty() {
        return false;
    }
    if segment.chars().all(|ch| ch.is_ascii_digit()) {
        return true;
    }
    let dash = segment.bytes().filter(|byte| *byte == b'-').count();
    if segment.len() == 36 && dash == 4 {
        return segment
            .chars()
            .all(|ch| ch.is_ascii_hexdigit() || ch == '-');
    }
    let len = segment.len();
    (8..=32).contains(&len) && segment.chars().all(|ch| ch.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use super::*;

    #[test]
    fn collapses_ids_and_keeps_named_segments() {
        assert_eq!(
            normalize_path("/users/42/orders/ab12cd34ef56aa00"),
            "/users/:id/orders/:id"
        );
        assert_eq!(normalize_path("/api/v2/account?x=1"), "/api/v2/account");
        assert_eq!(normalize_path("/items/{id}"), "/items/:id");
        assert_eq!(normalize_path("/"), "/");
        assert_eq!(normalize_method("post"), "POST");
    }

    #[test]
    fn sorts_busiest_endpoint_first() {
        let set = EndpointSet::with_clock(Instant::now(), Arc::new(AtomicU64::new(0)));
        set.observe("GET", "/work", 1_000_000, 2);
        set.observe("GET", "/work", 1_000_000, 2);
        set.observe("GET", "/slow", 8_000_000, 2);
        set.observe("POST", "/user", 2_000_000, 2);
        set.observe("GET", "/fail", 3_000_000, 5);
        let rows = set.snapshot();
        assert_eq!(rows[0].method, "GET");
        assert_eq!(rows[0].path, "/work");
        assert_eq!(rows[0].window_60.requests, 2);
        assert_eq!(rows[0].in_flight, 0);
        assert_eq!(rows[1].path, "/fail");
        assert_eq!(rows[1].window_60.status[4], 1);
        assert!(rows.iter().any(|row| row.path == "/slow"));
        assert!(rows[0].window_60.p50_ns.is_some());
        assert!(rows[0].window_30.rps > 0.0);
    }

    #[test]
    fn tracks_in_flight_ahead_of_completed_traffic() {
        let set = EndpointSet::with_clock(Instant::now(), Arc::new(AtomicU64::new(0)));
        set.observe("GET", "/work", 1_000_000, 2);
        set.observe("GET", "/work", 1_000_000, 2);
        set.begin("GET", "/hold");
        set.begin("GET", "/hold");
        let rows = set.snapshot();
        assert_eq!(rows[0].path, "/hold");
        assert_eq!(rows[0].in_flight, 2);
        assert_eq!(rows[0].window_60.requests, 0);
        assert_eq!(rows[1].path, "/work");
        assert_eq!(rows[1].window_60.requests, 2);
        set.end("GET", "/hold");
        assert_eq!(set.snapshot()[0].in_flight, 1);
        set.end("GET", "/hold");
        let idle = set.snapshot();
        assert_eq!(
            idle.iter()
                .find(|row| row.path == "/hold")
                .unwrap()
                .in_flight,
            0
        );
        assert_eq!(idle[0].path, "/work");
    }

    #[test]
    fn expired_window_drops_endpoint_counts() {
        let extra = Arc::new(AtomicU64::new(0));
        let set = EndpointSet::with_clock(Instant::now(), Arc::clone(&extra));
        set.observe(
            "GET",
            "/work",
            Duration::from_millis(4).as_nanos() as u64,
            2,
        );
        extra.fetch_add(61, Ordering::Relaxed);
        let expired = set.snapshot();
        assert_eq!(expired[0].window_60.requests, 0);
        assert!(expired[0].window_60.p50_ns.is_none());
        assert_eq!(expired[0].window_30.requests, 0);
        assert_eq!(expired[0].in_flight, 0);
    }

    #[test]
    fn evicts_least_recently_used_when_full() {
        let set = EndpointSet::with_clock(Instant::now(), Arc::new(AtomicU64::new(0)));
        for index in 0..MAX_ENDPOINTS {
            set.observe("GET", &format!("/route-{index}"), 1_000_000, 2);
        }
        assert_eq!(set.snapshot().len(), MAX_ENDPOINTS);

        // `/route-0` was the oldest; touching it hands the LRU slot to `/route-1`.
        set.observe("GET", "/route-0", 1_000_000, 2);
        set.observe("GET", "/fresh", 1_000_000, 2);

        let paths: Vec<String> = set.snapshot().into_iter().map(|row| row.path).collect();
        assert_eq!(paths.len(), MAX_ENDPOINTS);
        assert!(paths.iter().any(|path| path == "/fresh"));
        assert!(paths.iter().any(|path| path == "/route-0"));
        assert!(!paths.iter().any(|path| path == "/route-1"));
        assert!(!paths.iter().any(|path| path == "/..."));
    }

    #[test]
    fn never_evicts_rows_with_in_flight_requests() {
        let set = EndpointSet::with_clock(Instant::now(), Arc::new(AtomicU64::new(0)));
        set.begin("GET", "/hold");
        for index in 0..(MAX_ENDPOINTS * 2) {
            set.observe("GET", &format!("/route-{index}"), 1_000_000, 2);
        }

        let rows = set.snapshot();
        assert_eq!(rows.len(), MAX_ENDPOINTS);
        let hold = rows
            .iter()
            .find(|row| row.path == "/hold")
            .expect("in-flight row survives eviction");
        assert_eq!(hold.in_flight, 1);

        set.end("GET", "/hold");
        let idle = set.snapshot();
        assert_eq!(
            idle.iter().find(|row| row.path == "/hold").unwrap().in_flight,
            0
        );
    }

    #[test]
    fn falls_back_to_overflow_when_every_row_is_in_flight() {
        let set = EndpointSet::with_clock(Instant::now(), Arc::new(AtomicU64::new(0)));
        for index in 0..MAX_ENDPOINTS {
            set.begin("GET", &format!("/hold-{index}"));
        }
        set.begin("GET", "/extra");

        let rows = set.snapshot();
        let overflow = rows
            .iter()
            .find(|row| row.path == "/...")
            .expect("overflow row when nothing is evictable");
        assert_eq!(overflow.in_flight, 1);

        set.end("GET", "/extra");
        let drained = set.snapshot();
        assert_eq!(
            drained.iter().find(|row| row.path == "/...").unwrap().in_flight,
            0
        );
    }

    #[test]
    fn evicts_rows_without_recent_traffic_before_the_lru_row() {
        let extra = Arc::new(AtomicU64::new(0));
        let set = EndpointSet::with_clock(Instant::now(), Arc::clone(&extra));
        set.observe("GET", "/stale", 1_000_000, 2);
        extra.fetch_add(61, Ordering::Relaxed);
        for index in 0..(MAX_ENDPOINTS - 1) {
            set.observe("GET", &format!("/route-{index}"), 1_000_000, 2);
        }
        assert_eq!(set.snapshot().len(), MAX_ENDPOINTS);

        // `begin`/`end` refresh the LRU position without recording a request, so
        // `/stale` is now the most recently used row yet still has an empty window.
        set.begin("GET", "/stale");
        set.end("GET", "/stale");
        set.observe("GET", "/fresh", 1_000_000, 2);

        let paths: Vec<String> = set.snapshot().into_iter().map(|row| row.path).collect();
        assert_eq!(paths.len(), MAX_ENDPOINTS);
        assert!(paths.iter().any(|path| path == "/fresh"));
        assert!(!paths.iter().any(|path| path == "/stale"));
        // The true LRU row survives because a cold row outranks it for eviction.
        assert!(paths.iter().any(|path| path == "/route-0"));
    }
}
