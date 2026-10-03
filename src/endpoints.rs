//! Bounded per-route HTTP metrics with cold-first, LRU eviction.

use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
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

/// Upper bound in characters on one path segment; longer segments are truncated.
const MAX_SEGMENT_CHARS: usize = 48;

/// Upper bound in characters on a method name; longer names are truncated.
const MAX_METHOD_CHARS: usize = 16;

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

/// Hash map of route keys using [`FxHasher`].
type RouteMap = HashMap<RouteKey, Arc<RouteMetrics>, BuildHasherDefault<FxHasher>>;

/// Source of the identifiers that tell [`EndpointSet`] instances apart in the
/// per-thread cache.
static NEXT_SET_ID: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// Per-thread scratch buffer and route cache.
    static LOCAL: RefCell<LocalRoutes> = RefCell::new(LocalRoutes::new());
}

/// Per-thread lookup state: the buffer lookup keys are built in, plus a cache of
/// the rows this thread resolved recently.
///
/// The cache turns the hot path of a known route into a hash lookup without the
/// shared read lock. It mirrors the table of one [`EndpointSet`] and is cleared
/// whenever the set's generation changes, that is, whenever a row was evicted,
/// so a stale row is used at most until the thread's next lookup. The rows it
/// holds stay allocated until then, even after their set is dropped; that is at
/// most [`MAX_ENDPOINTS`] rows of one set per thread.
struct LocalRoutes {
    /// Scratch buffer in which lookup keys are built.
    key: String,
    /// Identifier of the [`EndpointSet`] the cache mirrors.
    set_id: u64,
    /// Generation of that set when the cache was last cleared.
    generation: u64,
    /// Rows resolved on this thread since the last clear.
    cache: RouteMap,
}

impl LocalRoutes {
    /// Creates an empty cache bound to no set.
    fn new() -> Self {
        Self {
            key: String::with_capacity(MAX_PATH_CHARS + MAX_METHOD_CHARS + 8),
            set_id: u64::MAX,
            generation: 0,
            cache: RouteMap::default(),
        }
    }

    /// Clears the cache unless it already mirrors `set` at its current
    /// generation.
    fn sync(&mut self, set: &EndpointSet) {
        let generation = set.generation.load(Ordering::Acquire);
        if self.set_id != set.id || self.generation != generation {
            self.cache.clear();
            self.set_id = set.id;
            self.generation = generation;
        }
    }
}

/// Fx hash, the multiplicative hash used by `rustc`.
///
/// Route keys are short and the table is bounded, so the collision resistance
/// of the default `SipHash` buys nothing here while costing several times the
/// instructions per lookup.
#[derive(Default)]
struct FxHasher {
    /// Running hash state.
    hash: u64,
}

/// Odd multiplier of [`FxHasher`], chosen by `rustc` for its bit dispersion.
const FX_SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

impl FxHasher {
    /// Folds one word into the state.
    fn add(&mut self, word: u64) {
        self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(FX_SEED);
    }
}

impl Hasher for FxHasher {
    fn write(&mut self, bytes: &[u8]) {
        let (words, remainder) = bytes.as_chunks::<8>();
        for word in words {
            self.add(u64::from_le_bytes(*word));
        }
        for &byte in remainder {
            self.add(u64::from(byte));
        }
    }

    fn write_u8(&mut self, value: u8) {
        self.add(u64::from(value));
    }

    fn write_u64(&mut self, value: u64) {
        self.add(value);
    }

    fn write_usize(&mut self, value: usize) {
        self.add(value as u64);
    }

    fn finish(&self) -> u64 {
        self.hash
    }
}

/// Splits a [`RouteKey`] back into `(method, path)`.
fn split_key(key: &str) -> (&str, &str) {
    key.split_once(KEY_SEPARATOR).unwrap_or((key, "/"))
}

/// Metrics of one tracked route.
///
/// The layout is fixed so the three gauges share one cache line with the `Arc`
/// reference counts in front of them: a request then touches a single line of
/// this row on the way in and out, and the 100 KiB ring stays untouched until
/// the request completes.
#[repr(C)]
struct RouteMetrics {
    /// Requests of this route currently being handled.
    in_flight: AtomicU64,
    /// Nanoseconds since the table's origin at the last lookup; orders rows for
    /// LRU eviction.
    last_used: AtomicU64,
    /// Tick of the most recent request, biased by one so `0` means "never".
    ///
    /// Lets eviction test the 90s window with a single load instead of walking
    /// all [`WINDOW_SECS`] slots of the route's histogram.
    last_observe: AtomicU64,
    /// Completed requests of this route over the trailing window.
    window: SlidingWindow,
}

impl RouteMetrics {
    /// Marks this row as used at `stamp` nanoseconds since the table's origin.
    fn touch(&self, stamp: u64) {
        self.last_used.store(stamp, Ordering::Relaxed);
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
    /// Identifier that tells this set apart from others in the per-thread cache.
    id: u64,
    /// Instant that tick zero is measured from.
    origin: Instant,
    /// Seconds added to the tick; non-zero only when tests advance time.
    extra_secs: Arc<AtomicU64>,
    /// Incremented whenever a row is evicted; invalidates the per-thread caches.
    generation: AtomicU64,
    /// Rows by [`RouteKey`], never more than [`MAX_ENDPOINTS`] plus the overflow row.
    routes: RwLock<RouteMap>,
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
            id: NEXT_SET_ID.fetch_add(1, Ordering::Relaxed),
            origin,
            extra_secs,
            generation: AtomicU64::new(0),
            routes: RwLock::new(RouteMap::default()),
        }
    }

    /// Resolves the route of a request starting at `now` and counts it as in
    /// flight.
    pub(crate) fn begin(&self, method: &str, path: &str, now: Instant) -> RouteHandle {
        let metrics = self.route_metrics(method, path, now);
        metrics.in_flight.fetch_add(1, Ordering::Relaxed);
        RouteHandle(metrics)
    }

    /// Records a completed request of `route` in the slot of `tick`.
    pub(crate) fn observe(
        &self,
        route: &RouteHandle,
        tick: u64,
        bucket: usize,
        class: Option<usize>,
    ) {
        route
            .0
            .last_observe
            .store(tick.saturating_add(1), Ordering::Relaxed);
        route.0.window.record(tick, bucket, class);
    }

    /// Records a completed request for `method` and `path` at the current tick.
    #[cfg(test)]
    fn observe_path(&self, method: &str, path: &str, ns: u64, status_class: u8) {
        let now = Instant::now();
        let route = RouteHandle(self.route_metrics(method, path, now));
        let (bucket, class) = crate::histogram::sample_of(ns, status_class);
        self.observe(&route, self.tick_at(now), bucket, class);
    }

    /// Returns the current tick: whole seconds since `origin`.
    fn tick(&self) -> u64 {
        self.tick_at(Instant::now())
    }

    /// Returns the tick of `now`: whole seconds since `origin`.
    fn tick_at(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.origin).as_secs()
            + self.extra_secs.load(Ordering::Relaxed)
    }

    /// Returns the LRU stamp of `now`: nanoseconds since `origin`.
    fn stamp_at(&self, now: Instant) -> u64 {
        // Truncation would take centuries of uptime.
        now.saturating_duration_since(self.origin).as_nanos() as u64
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

    /// Returns the row for `method` and `path`, inserting it when absent, and
    /// marks it as used at `now`.
    ///
    /// A known route costs one hash lookup in the per-thread cache and no shared
    /// lock. The fallback without thread-local storage only runs while the
    /// thread is shutting down.
    fn route_metrics(&self, method: &str, path: &str, now: Instant) -> Arc<RouteMetrics> {
        let stamp = self.stamp_at(now);
        let cached = LOCAL.try_with(|local| {
            let mut local = local.borrow_mut();
            let local = &mut *local;
            local.key.clear();
            write_key(&mut local.key, method, path);
            local.sync(self);
            if let Some(existing) = local.cache.get(local.key.as_str()) {
                existing.touch(stamp);
                return Arc::clone(existing);
            }
            let metrics = self.shared_route(&local.key, stamp);
            local.cache.insert(local.key.clone(), Arc::clone(&metrics));
            metrics
        });
        cached.unwrap_or_else(|_| {
            let mut key = String::new();
            write_key(&mut key, method, path);
            self.shared_route(&key, stamp)
        })
    }

    /// Returns the row of `key` from the shared table, inserting it when absent.
    ///
    /// A full table evicts one idle row; when every row is in flight the request
    /// is attributed to the shared `* /...` overflow row instead.
    fn shared_route(&self, key: &str, stamp: u64) -> Arc<RouteMetrics> {
        {
            let routes = self.routes.read().unwrap_or_else(PoisonError::into_inner);
            if let Some(existing) = routes.get(key) {
                existing.touch(stamp);
                return Arc::clone(existing);
            }
        }
        let mut routes = self.routes.write().unwrap_or_else(PoisonError::into_inner);
        // Re-check: another thread may have inserted the row between the two locks.
        if let Some(existing) = routes.get(key) {
            existing.touch(stamp);
            return Arc::clone(existing);
        }
        if routes.len() >= MAX_ENDPOINTS {
            if evict_one(&mut routes, self.tick()) {
                // Per-thread caches may still hold the evicted row; the bump makes
                // them drop it on their next lookup.
                self.generation.fetch_add(1, Ordering::Release);
            } else {
                let metrics = if let Some(existing) = routes.get(OVERFLOW_KEY) {
                    Arc::clone(existing)
                } else {
                    let created = self.new_metrics();
                    routes.insert(OVERFLOW_KEY.to_owned(), Arc::clone(&created));
                    created
                };
                metrics.touch(stamp);
                return metrics;
            }
        }
        let created = self.new_metrics();
        created.touch(stamp);
        routes.insert(key.to_owned(), Arc::clone(&created));
        created
    }

    /// Creates an empty row on this table's time base.
    fn new_metrics(&self) -> Arc<RouteMetrics> {
        Arc::new(RouteMetrics {
            in_flight: AtomicU64::new(0),
            last_used: AtomicU64::new(0),
            last_observe: AtomicU64::new(0),
            window: SlidingWindow::with_clock(self.origin, Arc::clone(&self.extra_secs)),
        })
    }
}

/// Drops one idle row, preferring routes that saw no request in the trailing
/// 90s window and breaking ties by least recently used.
///
/// Returns `false` when every row is in flight — the caller then falls back to
/// the shared overflow row so the table stays bounded either way.
fn evict_one(routes: &mut RouteMap, tick: u64) -> bool {
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
///
/// The decrement is unconditional and undone when it underflowed: one atomic
/// instead of a load plus compare-and-swap on the common path. The underflow
/// only happens on a double release, which the guards never do, so the wrapped
/// value is never observed in practice.
pub(crate) fn saturating_dec(value: &AtomicU64) {
    if value.fetch_sub(1, Ordering::Relaxed) == 0 {
        value.fetch_add(1, Ordering::Relaxed);
    }
}

/// Appends the [`RouteKey`] of `method` and `path` to `out`.
fn write_key(out: &mut String, method: &str, path: &str) {
    write_normalized_method(out, method);
    out.push(KEY_SEPARATOR);
    write_normalized_path(out, path);
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
    // Nearly every request carries a standard upper-case method, which needs
    // neither trimming nor case folding.
    if !method.is_empty()
        && method.len() <= MAX_METHOD_CHARS
        && method.bytes().all(|byte| byte.is_ascii_uppercase())
    {
        out.push_str(method);
        return;
    }
    let method = method.trim();
    if method.is_empty() {
        out.push_str("GET");
        return;
    }
    out.extend(
        method
            .chars()
            .take(MAX_METHOD_CHARS)
            .map(|ch| ch.to_ascii_uppercase()),
    );
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
    // Only the part before a query or fragment names the route. Both delimiters
    // are ASCII, so the byte position is a character boundary.
    let end = path
        .bytes()
        .position(|byte| byte == b'?' || byte == b'#')
        .unwrap_or(path.len());
    let path = &path[..end];
    let bytes = path.as_bytes();
    let start = out.len();
    out.push('/');
    let mut wrote = false;
    let mut index = 0;
    // Segments are scanned by byte: `/` is ASCII, so every cut is a character
    // boundary, and this avoids the per-call search setup of `str::split`.
    while index < bytes.len() {
        if bytes[index] == b'/' {
            index += 1;
            continue;
        }
        let segment_start = index;
        while index < bytes.len() && bytes[index] != b'/' {
            index += 1;
        }
        let segment = &path[segment_start..index];
        if wrote {
            out.push('/');
        }
        wrote = true;
        if looks_like_id(segment) || is_route_param(segment) {
            out.push_str(":id");
        } else {
            out.push_str(truncate_chars(segment, MAX_SEGMENT_CHARS));
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

/// Returns the longest prefix of `text` that has at most `max_chars` characters.
fn truncate_chars(text: &str, max_chars: usize) -> &str {
    // A string of at most `max_chars` bytes cannot hold more characters.
    if text.len() <= max_chars {
        return text;
    }
    text.char_indices()
        .nth(max_chars)
        .map_or(text, |(index, _)| &text[..index])
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
    if bytes.len() == 36 {
        let dashes = bytes.iter().filter(|byte| **byte == b'-').count();
        return dashes == 4
            && bytes
                .iter()
                .all(|byte| byte.is_ascii_hexdigit() || *byte == b'-');
    }
    (8..=32).contains(&bytes.len()) && bytes.iter().all(u8::is_ascii_hexdigit)
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
        assert_eq!(normalize_path(""), "/");
        assert_eq!(normalize_path("//a///b/"), "/a/b");
        assert_eq!(
            normalize_path("/f47ac10b-58cc-4372-a567-0e02b2c3d479/x"),
            "/:id/x"
        );
        assert_eq!(normalize_path("/数据/中心#frag"), "/数据/中心");
        assert_eq!(normalize_method("post"), "POST");
        assert_eq!(normalize_method(" get "), "GET");
        assert_eq!(normalize_method(""), "GET");
    }

    #[test]
    fn bounds_segment_and_path_length_on_character_boundaries() {
        let long_segment = "é".repeat(MAX_SEGMENT_CHARS + 5);
        let normalized = normalize_path(&format!("/{long_segment}"));
        assert_eq!(normalized.chars().count(), MAX_SEGMENT_CHARS + 1);

        let long_path: String = (0..20).map(|_| "/ééééééééé").collect();
        let normalized = normalize_path(&long_path);
        assert!(normalized.len() <= MAX_PATH_CHARS);
        assert!(normalized.is_char_boundary(normalized.len()));
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
        let first = set.begin("GET", "/hold", Instant::now());
        let second = set.begin("GET", "/hold", Instant::now());
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
    fn evicted_row_leaves_the_thread_cache() {
        let set = EndpointSet::with_clock(Instant::now(), Arc::new(AtomicU64::new(0)));
        for index in 0..MAX_ENDPOINTS {
            set.observe_path("GET", &format!("/route-{index}"), 1_000_000, 2);
        }
        // Evicts `/route-0`, the least recently used row.
        set.observe_path("GET", "/fresh", 1_000_000, 2);
        // A cached handle to the evicted row would record into a row the
        // snapshot no longer lists.
        set.observe_path("GET", "/route-0", 1_000_000, 2);
        let rows = set.snapshot();
        let revived = rows
            .iter()
            .find(|row| row.path == "/route-0")
            .expect("re-inserted route");
        assert_eq!(revived.window_90.requests, 1);
    }

    #[test]
    fn never_evicts_rows_with_in_flight_requests() {
        let set = EndpointSet::with_clock(Instant::now(), Arc::new(AtomicU64::new(0)));
        let hold = set.begin("GET", "/hold", Instant::now());
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
            let _held = set.begin("GET", &format!("/hold-{index}"), Instant::now());
        }
        let extra = set.begin("GET", "/extra", Instant::now());

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
        set.begin("GET", "/stale", Instant::now()).end();
        set.observe_path("GET", "/fresh", 1_000_000, 2);

        let paths: Vec<String> = set.snapshot().into_iter().map(|row| row.path).collect();
        assert_eq!(paths.len(), MAX_ENDPOINTS);
        assert!(paths.iter().any(|path| path == "/fresh"));
        assert!(!paths.iter().any(|path| path == "/stale"));
        // The true LRU row survives because a cold row outranks it for eviction.
        assert!(paths.iter().any(|path| path == "/route-0"));
    }

    #[test]
    fn separate_sets_do_not_share_the_thread_cache() {
        let first = EndpointSet::with_clock(Instant::now(), Arc::new(AtomicU64::new(0)));
        let second = EndpointSet::with_clock(Instant::now(), Arc::new(AtomicU64::new(0)));
        first.observe_path("GET", "/shared", 1_000_000, 2);
        second.observe_path("GET", "/shared", 1_000_000, 2);
        first.observe_path("GET", "/shared", 1_000_000, 2);
        assert_eq!(first.snapshot()[0].window_90.requests, 2);
        assert_eq!(second.snapshot()[0].window_90.requests, 1);
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
                    set.begin("GET", "/shared", Instant::now()).end();
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
