use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::histogram::{SlidingWindow, WindowAgg};

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
}

pub(crate) struct EndpointSet {
    origin: Instant,
    extra_secs: Arc<AtomicU64>,
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
            return Arc::clone(existing);
        }
        if routes.len() >= MAX_ENDPOINTS {
            let overflow = RouteKey {
                method: "*".into(),
                path: "/...".into(),
            };
            return routes
                .entry(overflow)
                .or_insert_with(|| self.new_metrics())
                .clone();
        }
        let created = self.new_metrics();
        routes.insert(key, Arc::clone(&created));
        created
    }

    fn new_metrics(&self) -> Arc<RouteMetrics> {
        Arc::new(RouteMetrics {
            window: SlidingWindow::with_clock(self.origin, Arc::clone(&self.extra_secs)),
            in_flight: AtomicU64::new(0),
        })
    }
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
}
