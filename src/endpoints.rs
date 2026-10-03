//! Bounded per-route HTTP metrics with cold-first, LRU eviction.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Instant;

use crate::histogram::{SlidingWindow, WINDOW_SECS, WindowAgg};

/// Upper bound on tracked routes.
///
/// When the table is full a row is evicted in this order of preference: rows
/// with no requests in the trailing 90s window first, then the rest, least
/// recently used first within each group. Rows with in-flight requests are never
/// evicted, so a long-running request keeps its row until it completes.
const MAX_ENDPOINTS: usize = 64;

/// Upper bound in bytes on a normalized path; longer paths are truncated.
const MAX_PATH_CHARS: usize = 128;

/// Route table key: `"<METHOD> <normalized path>"` in one string.
///
/// A single string lets a lookup use a borrowed `&str` built in a reusable
/// per-thread buffer — no allocation and no exclusive lock on the hot path. The
/// two halves are split back out for snapshots.
type RouteKey = String;

/// Separator between the method and the path inside a [`RouteKey`].
const KEY_SEPARATOR: char = ' ';

/// Key of the shared row that absorbs new routes while every row is in flight.
const OVERFLOW_KEY: &str = "* /...";

thread_local! {
    /// Per-thread scratch buffer in which lookup keys are built.
    static KEY_BUF: RefCell<String> = RefCell::new(String::with_capacity(MAX_PATH_CHARS + 24));
}

/// Splits a [`RouteKey`] back into `(method, path)`.
fn split_key(key: &str) -> (&str, &str) {
    key.split_once(KEY_SEPARATOR).unwrap_or((key, "/"))
}

/// Metrics of one tracked route.
struct RouteMetrics {
    /// Completed requests of this route over the trailing window.
    window: SlidingWindow,
    /// Requests of this route currently being handled.
    in_flight: AtomicU64,
    /// Value of the table's logical clock at the last lookup; orders rows for LRU
    /// eviction.
    last_used: AtomicU64,
    /// Tick of the most recent request, biased by one so `0` means "never".
    ///
    /// Lets eviction test the 90s window with a single load instead of walking
    /// all [`WINDOW_SECS`] slots of the route's histogram.
    last_observe: AtomicU64,
}

impl RouteMetrics {
    /// Marks this row as the most recently used one.
    fn touch(&self, clock: &AtomicU64) {
        let tick = clock.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        self.last_used.store(tick, Ordering::Relaxed);
    }

    /// Returns `true` when no request completed inside the window ending at `tick`.
    fn is_cold(&self, tick: u64) -> bool {
        match self.last_observe.load(Ordering::Relaxed) {
            0 => true,
            stamp => tick.saturating_sub(stamp - 1) >= WINDOW_SECS,
        }
    }
}

/// Resolved route slot handed to the caller for the lifetime of one request.
///
/// Holding it means the method and path are normalized and looked up once
/// instead of once per begin/observe/end step.
pub(crate) struct RouteHandle(Arc<RouteMetrics>);

impl RouteHandle {
    /// Decrements the route's in-flight gauge, saturating at zero.
    pub(crate) fn end(&self) {
        saturating_dec(&self.0.in_flight);
    }
}

/// Bounded table of per-route metrics, keyed by method and normalized path.
pub(crate) struct EndpointSet {
    /// Instant that tick zero is measured from.
    origin: Instant,
    /// Seconds added to the tick; non-zero only when tests advance time.
    extra_secs: Arc<AtomicU64>,
    /// Logical clock incremented on every lookup, the source of LRU order.
    clock: AtomicU64,
    /// Rows by [`RouteKey`], never more than [`MAX_ENDPOINTS`] plus the overflow row.
    routes: RwLock<HashMap<RouteKey, Arc<RouteMetrics>>>,
}

/// Traffic of one route as captured by [`EndpointSet::snapshot`].
#[derive(Clone, Debug)]
pub(crate) struct EndpointTraffic {
    /// Upper-case HTTP method.
    pub method: String,
    /// Normalized path.
    pub path: String,
    /// Requests currently being handled.
    pub in_flight: u64,
    /// Aggregate of the trailing 30 seconds.
    pub window_30: WindowAgg,
    /// Aggregate of the trailing 60 seconds.
    pub window_60: WindowAgg,
    /// Aggregate of the trailing 90 seconds.
    pub window_90: WindowAgg,
}

impl EndpointSet {
    /// Creates an empty table that shares its time base with the global window.
    pub(crate) fn with_clock(origin: Instant, extra_secs: Arc<AtomicU64>) -> Self {
        Self {
            origin,
            extra_secs,
            clock: AtomicU64::new(0),
            routes: RwLock::new(HashMap::new()),
        }
    }

    /// Resolves the route of a starting request and counts it as in flight.
    pub(crate) fn begin(&self, method: &str, path: &str) -> RouteHandle {
        let metrics = self.route_metrics(method, path);
        metrics.in_flight.fetch_add(1, Ordering::Relaxed);
        RouteHandle(metrics)
    }

    /// Records a completed request of `route` in the slot of `tick`.
    pub(crate) fn observe(&self, route: &RouteHandle, tick: u64, ns: u64, status_class: u8) {
        route
            .0
            .last_observe
            .store(tick.saturating_add(1), Ordering::Relaxed);
        route.0.window.observe_at(tick, ns, status_class);
    }

    /// Records a completed request for `method` and `path` at the current tick.
    #[cfg(test)]
    fn observe_path(&self, method: &str, path: &str, ns: u64, status_class: u8) {
        let route = RouteHandle(self.route_metrics(method, path));
        self.observe(&route, self.tick(), ns, status_class);
    }

    /// Returns the current tick: whole seconds since `origin`.
    fn tick(&self) -> u64 {
        self.origin.elapsed().as_secs() + self.extra_secs.load(Ordering::Relaxed)
    }

    /// Returns every tracked route, in-flight first, then busiest, then by name.
    pub(crate) fn snapshot(&self) -> Vec<EndpointTraffic> {
        let tick = self.tick();
        let routes: Vec<_> = {
            let routes = self.routes.read().unwrap_or_else(PoisonError::into_inner);
            routes
                .iter()
                .map(|(key, metrics)| {
                    let (method, path) = split_key(key);
                    (method.to_owned(), path.to_owned(), Arc::clone(metrics))
                })
                .collect()
        };
        let mut rows: Vec<EndpointTraffic> = routes
            .into_iter()
            .map(|(method, path, metrics)| {
                let (window_30, window_60, window_90) = if metrics.is_cold(tick) {
                    SlidingWindow::empty_windows(tick)
                } else {
                    metrics.window.snapshot_windows_at(tick)
                };
                EndpointTraffic {
                    method,
                    path,
                    in_flight: metrics.in_flight.load(Ordering::Relaxed),
                    window_30,
                    window_60,
                    window_90,
                }
            })
            .collect();
        rows.sort_by(|left, right| {
            right
                .in_flight
                .cmp(&left.in_flight)
                .then_with(|| right.window_90.requests.cmp(&left.window_90.requests))
                .then_with(|| left.method.cmp(&right.method))
                .then_with(|| left.path.cmp(&right.path))
        });
        rows
    }

    /// Returns the row for `method` and `path`, inserting it when absent.
    ///
    /// A full table evicts one idle row; when every row is in flight the request
    /// is attributed to the shared `* /...` overflow row instead.
    fn route_metrics(&self, method: &str, path: &str) -> Arc<RouteMetrics> {
        KEY_BUF.with(|buf| {
            let mut key = buf.borrow_mut();
            key.clear();
            write_normalized_method(&mut key, method);
            key.push(KEY_SEPARATOR);
            write_normalized_path(&mut key, path);
            // Hot path: a known route is a shared read lock + one hash lookup, zero allocation.
            {
                let routes = self.routes.read().unwrap_or_else(PoisonError::into_inner);
                if let Some(existing) = routes.get(key.as_str()) {
                    existing.touch(&self.clock);
                    return Arc::clone(existing);
                }
            }
            let mut routes = self.routes.write().unwrap_or_else(PoisonError::into_inner);
            // Re-check: another thread may have inserted the row between the two locks.
            if let Some(existing) = routes.get(key.as_str()) {
                existing.touch(&self.clock);
                return Arc::clone(existing);
            }
            if routes.len() >= MAX_ENDPOINTS && !evict_one(&mut routes, self.tick()) {
                let metrics = if let Some(existing) = routes.get(OVERFLOW_KEY) {
                    Arc::clone(existing)
                } else {
                    let created = self.new_metrics();
                    routes.insert(OVERFLOW_KEY.to_owned(), Arc::clone(&created));
                    created
                };
                metrics.touch(&self.clock);
                return metrics;
            }
            let created = self.new_metrics();
            created.touch(&self.clock);
            routes.insert(key.clone(), Arc::clone(&created));
            created
        })
    }

    /// Creates an empty row on this table's time base.
    fn new_metrics(&self) -> Arc<RouteMetrics> {
        Arc::new(RouteMetrics {
            window: SlidingWindow::with_clock(self.origin, Arc::clone(&self.extra_secs)),
            in_flight: AtomicU64::new(0),
            last_used: AtomicU64::new(0),
            last_observe: AtomicU64::new(0),
        })
    }
}

/// Drops one idle row, preferring routes that saw no request in the trailing
/// 90s window and breaking ties by least recently used.
///
/// Returns `false` when every row is in flight — the caller then falls back to
/// the shared overflow row so the table stays bounded either way.
fn evict_one(routes: &mut HashMap<RouteKey, Arc<RouteMetrics>>, tick: u64) -> bool {
    let mut cold: Option<(&RouteKey, u64)> = None;
    let mut warm: Option<(&RouteKey, u64)> = None;
    for (key, metrics) in routes.iter() {
        if metrics.in_flight.load(Ordering::Relaxed) != 0 {
            continue;
        }
        let last_used = metrics.last_used.load(Ordering::Relaxed);
        let group = if metrics.is_cold(tick) {
            &mut cold
        } else {
            &mut warm
        };
        if group.as_ref().is_none_or(|(_, oldest)| last_used < *oldest) {
            *group = Some((key, last_used));
        }
    }
    let Some((victim, _)) = cold.or(warm) else {
        return false;
    };
    let victim = victim.clone();
    routes.remove(&victim);
    true
}

/// Decrements `value` unless it is already zero.
pub(crate) fn saturating_dec(value: &AtomicU64) {
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

/// Returns the canonical form of `method` as an owned string.
#[cfg(test)]
fn normalize_method(method: &str) -> String {
    let mut out = String::new();
    write_normalized_method(&mut out, method);
    out
}

/// Appends the canonical (trimmed, upper-case, ≤16 chars) method to `out`.
fn write_normalized_method(out: &mut String, method: &str) {
    let method = method.trim();
    if method.is_empty() {
        out.push_str("GET");
        return;
    }
    // Nearly every request already carries a canonical upper-case method.
    if method.len() <= 16 && method.bytes().all(|byte| byte.is_ascii_uppercase()) {
        out.push_str(method);
        return;
    }
    out.extend(method.chars().take(16).map(|ch| ch.to_ascii_uppercase()));
}

/// Returns the normalized form of `path` as an owned string.
#[cfg(test)]
fn normalize_path(path: &str) -> String {
    let mut out = String::new();
    write_normalized_path(&mut out, path);
    out
}

/// Appends the normalized path (query stripped, id-like segments collapsed to `:id`,
/// bounded to [`MAX_PATH_CHARS`]) to `out`.
fn write_normalized_path(out: &mut String, path: &str) {
    let path = path.split(['?', '#']).next().unwrap_or("/");
    let start = out.len();
    out.push('/');
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
            out.extend(segment.chars().take(48));
        }
        if out.len() - start > MAX_PATH_CHARS {
            let mut cut = start + MAX_PATH_CHARS;
            while !out.is_char_boundary(cut) {
                cut -= 1;
            }
            out.truncate(cut);
            break;
        }
    }
}

/// Returns `true` for a route-template parameter such as `{id}` or `:id`.
fn is_route_param(segment: &str) -> bool {
    let bytes = segment.as_bytes();
    (bytes.len() >= 3 && bytes[0] == b'{' && bytes[bytes.len() - 1] == b'}')
        || (bytes.len() > 1 && bytes[0] == b':')
}

/// Returns `true` when `segment` looks like an identifier rather than a route
/// name, so that it is collapsed to `:id`.
fn looks_like_id(segment: &str) -> bool {
    let bytes = segment.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    if bytes.iter().all(u8::is_ascii_digit) {
        return true;
    }
    let dash = bytes.iter().filter(|byte| **byte == b'-').count();
    if bytes.len() == 36 && dash == 4 {
        return bytes
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() || *byte == b'-');
    }
    let len = bytes.len();
    (8..=32).contains(&len) && bytes.iter().all(u8::is_ascii_hexdigit)
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
        set.observe_path("GET", "/work", 1_000_000, 2);
        set.observe_path("GET", "/work", 1_000_000, 2);
        set.observe_path("GET", "/slow", 8_000_000, 2);
        set.observe_path("POST", "/user", 2_000_000, 2);
        set.observe_path("GET", "/fail", 3_000_000, 5);
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
        assert_eq!(rows[0].window_90.requests, 2);
    }

    #[test]
    fn tracks_in_flight_ahead_of_completed_traffic() {
        let set = EndpointSet::with_clock(Instant::now(), Arc::new(AtomicU64::new(0)));
        set.observe_path("GET", "/work", 1_000_000, 2);
        set.observe_path("GET", "/work", 1_000_000, 2);
        let first = set.begin("GET", "/hold");
        let second = set.begin("GET", "/hold");
        let rows = set.snapshot();
        assert_eq!(rows[0].path, "/hold");
        assert_eq!(rows[0].in_flight, 2);
        assert_eq!(rows[0].window_60.requests, 0);
        assert_eq!(rows[1].path, "/work");
        assert_eq!(rows[1].window_60.requests, 2);
        first.end();
        assert_eq!(set.snapshot()[0].in_flight, 1);
        second.end();
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
        set.observe_path(
            "GET",
            "/work",
            Duration::from_millis(4).as_nanos() as u64,
            2,
        );
        extra.fetch_add(91, Ordering::Relaxed);
        let expired = set.snapshot();
        assert_eq!(expired[0].window_60.requests, 0);
        assert_eq!(expired[0].window_90.requests, 0);
        assert!(expired[0].window_60.p50_ns.is_none());
        assert_eq!(expired[0].window_30.requests, 0);
        assert_eq!(expired[0].in_flight, 0);
    }

    #[test]
    fn evicts_least_recently_used_when_full() {
        let set = EndpointSet::with_clock(Instant::now(), Arc::new(AtomicU64::new(0)));
        for index in 0..MAX_ENDPOINTS {
            set.observe_path("GET", &format!("/route-{index}"), 1_000_000, 2);
        }
        assert_eq!(set.snapshot().len(), MAX_ENDPOINTS);

        // `/route-0` was the oldest; touching it hands the LRU slot to `/route-1`.
        set.observe_path("GET", "/route-0", 1_000_000, 2);
        set.observe_path("GET", "/fresh", 1_000_000, 2);

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
        let hold = set.begin("GET", "/hold");
        for index in 0..(MAX_ENDPOINTS * 2) {
            set.observe_path("GET", &format!("/route-{index}"), 1_000_000, 2);
        }

        let rows = set.snapshot();
        assert_eq!(rows.len(), MAX_ENDPOINTS);
        let row = rows
            .iter()
            .find(|row| row.path == "/hold")
            .expect("in-flight row survives eviction");
        assert_eq!(row.in_flight, 1);

        hold.end();
        let idle = set.snapshot();
        assert_eq!(
            idle.iter()
                .find(|row| row.path == "/hold")
                .unwrap()
                .in_flight,
            0
        );
    }

    #[test]
    fn falls_back_to_overflow_when_every_row_is_in_flight() {
        let set = EndpointSet::with_clock(Instant::now(), Arc::new(AtomicU64::new(0)));
        for index in 0..MAX_ENDPOINTS {
            let _held = set.begin("GET", &format!("/hold-{index}"));
        }
        let extra = set.begin("GET", "/extra");

        let rows = set.snapshot();
        let overflow = rows
            .iter()
            .find(|row| row.path == "/...")
            .expect("overflow row when nothing is evictable");
        assert_eq!(overflow.in_flight, 1);

        extra.end();
        let drained = set.snapshot();
        assert_eq!(
            drained
                .iter()
                .find(|row| row.path == "/...")
                .unwrap()
                .in_flight,
            0
        );
    }

    #[test]
    fn evicts_rows_without_recent_traffic_before_the_lru_row() {
        let extra = Arc::new(AtomicU64::new(0));
        let set = EndpointSet::with_clock(Instant::now(), Arc::clone(&extra));
        set.observe_path("GET", "/stale", 1_000_000, 2);
        extra.fetch_add(91, Ordering::Relaxed);
        for index in 0..(MAX_ENDPOINTS - 1) {
            set.observe_path("GET", &format!("/route-{index}"), 1_000_000, 2);
        }
        assert_eq!(set.snapshot().len(), MAX_ENDPOINTS);

        // `begin`/`end` refresh the LRU position without recording a request, so
        // `/stale` is now the most recently used row yet still has an empty window.
        set.begin("GET", "/stale").end();
        set.observe_path("GET", "/fresh", 1_000_000, 2);

        let paths: Vec<String> = set.snapshot().into_iter().map(|row| row.path).collect();
        assert_eq!(paths.len(), MAX_ENDPOINTS);
        assert!(paths.iter().any(|path| path == "/fresh"));
        assert!(!paths.iter().any(|path| path == "/stale"));
        // The true LRU row survives because a cold row outranks it for eviction.
        assert!(paths.iter().any(|path| path == "/route-0"));
    }

    #[test]
    fn concurrent_registration_creates_one_route() {
        const THREADS: usize = 16;
        let set = Arc::new(EndpointSet::with_clock(
            Instant::now(),
            Arc::new(AtomicU64::new(0)),
        ));
        let barrier = Arc::new(std::sync::Barrier::new(THREADS));
        let threads: Vec<_> = (0..THREADS)
            .map(|_| {
                let set = Arc::clone(&set);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    set.begin("GET", "/shared").end();
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let routes = set.routes.read().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(routes.len(), 1);
        assert!(routes.contains_key("GET /shared"));
    }
}
