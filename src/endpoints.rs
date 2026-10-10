//! Bounded per-route HTTP metrics with cold-first, LRU eviction.

use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Instant;

use crate::histogram::{SlidingWindow, WINDOW_SECS, WindowAgg};
use crate::shard::{MAX_STAGES, gauge_line, live_indices};

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

/// Upper bound on entries of a thread's route cache; the cache is emptied when
/// an insertion would exceed it.
///
/// Together with the clear on every eviction this keeps a thread from holding
/// more than a few hundred keys, whatever the cardinality of the paths it sees.
const LOCAL_CACHE_LIMIT: usize = 256;

/// Route table key: `"<METHOD> <normalized path>"` in one string.
///
/// A single string lets a lookup use a borrowed `&str` built in a reusable
/// per-thread buffer — no allocation and no exclusive lock on the hot path. The
/// two halves are split back out for snapshots.
type RouteKey = String;

/// Separator between the method and the path inside a [`RouteKey`].
const KEY_SEPARATOR: char = ' ';

/// Separator between the method and the route template inside a raw cache key.
///
/// It differs from [`KEY_SEPARATOR`] so that a raw key can never equal the
/// normalized key of another route: a template that is a valid normalized path
/// but normalizes to something else (a literal segment of 48 digits, say) would
/// otherwise share a cache entry with the request paths that normalize to it.
const RAW_KEY_SEPARATOR: char = '\0';

/// Key of the shared row that absorbs new routes while every row is in flight.
const OVERFLOW_KEY: &str = "* /...";

/// Hash map of route keys using [`FxHasher`].
type RouteMap = HashMap<RouteKey, Arc<RouteMetrics>, BuildHasherDefault<FxHasher>>;

/// Source of the identifiers that tell [`EndpointSet`] instances apart in the
/// per-thread cache.
static NEXT_SET_ID: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// Per-thread scratch buffers and route cache.
    static LOCAL: RefCell<LocalRoutes> = RefCell::new(LocalRoutes::new());
}

/// Per-thread lookup state: the buffers lookup keys are built in, plus a cache
/// of the rows this thread resolved recently.
///
/// The cache turns the hot path of a known route into a hash lookup without the
/// shared read lock. It mirrors the table of one [`EndpointSet`] and is cleared
/// whenever the set's generation changes, that is, whenever a row was evicted,
/// so a stale row is used at most until the thread's next lookup; it is also
/// cleared when it reaches [`LOCAL_CACHE_LIMIT`] entries. The rows it holds stay
/// allocated until then, even after their set is dropped.
///
/// A request that carries the route template Axum matched is cached under the
/// raw method and template joined by a NUL byte, so a hit skips path
/// normalization; the template space is bounded by the router's routes. Other
/// requests are cached under their normalized key.
struct LocalRoutes {
    /// Scratch buffer in which lookup keys are built.
    key: String,
    /// Scratch buffer in which the normalized key is built on a cache miss.
    normalized: String,
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
            normalized: String::with_capacity(MAX_PATH_CHARS + MAX_METHOD_CHARS + 8),
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

/// Returns the raw cache key of a matched route template.
#[cfg(test)]
pub(crate) fn raw_key(method: &str, template: &str) -> String {
    format!("{method}{RAW_KEY_SEPARATOR}{template}")
}

/// Returns `true` when the current thread's route cache holds `key`.
#[cfg(test)]
pub(crate) fn thread_cache_has(key: &str) -> bool {
    LOCAL.with(|local| local.borrow().cache.contains_key(key))
}

/// Stamps of one route written by the threads mapped to one line.
///
/// Each thread writes the line of its own index, so a request touches no stamp
/// line another core writes; the row's values are maxima over the lines. The
/// line is 128 bytes, which covers every supported CPU's cache line.
#[repr(align(128))]
struct RouteGauge {
    /// Nanoseconds since the table's origin at the last lookup on this line;
    /// the maximum over the lines orders rows for LRU eviction.
    last_used: AtomicU64,
    /// Tick of the most recent request on this line, biased by one so `0`
    /// means "never".
    ///
    /// Lets eviction test the 90s window with one load per line instead of
    /// walking all [`WINDOW_SECS`] slots of the route's histogram.
    last_observe: AtomicU64,
}

impl RouteGauge {
    /// Creates a zeroed gauge line.
    fn new() -> Self {
        Self {
            last_used: AtomicU64::new(0),
            last_observe: AtomicU64::new(0),
        }
    }
}

/// Bytes that separate the in-flight gauge from the rest of a row.
///
/// Together with the `Arc` counts (16 bytes) and the gauge (8 bytes) they fill
/// 128 bytes, wider than the cache lines of the CPUs this crate is tested on,
/// so the fields a request only reads never share a line with the counts every
/// request writes.
const ROW_HEADER_PADDING: usize = 128 - 16 - 8;

/// Metrics of one tracked route.
///
/// The layout is fixed so the in-flight gauge directly follows the `Arc`
/// reference counts. A heap allocation is only 16-byte aligned, so the gauge
/// shares the counts' cache line for about three rows in four and lies on the
/// next line otherwise. Every request writes the counts when it clones and
/// drops its handle, so keeping the gauge next to them adds little cross-core
/// traffic, while a single counter makes eviction read an exact value: a row
/// with a request in flight is never evicted.
#[repr(C)]
struct RouteMetrics {
    /// Requests of this route currently being handled.
    in_flight: AtomicU64,
    /// Keeps the fields below off the line of the counts and the gauge.
    header_padding: [u8; ROW_HEADER_PADDING],
    /// Completed requests of this route over the trailing window.
    window: SlidingWindow,
    /// One stamp line per thread index.
    gauges: Box<[RouteGauge; MAX_STAGES]>,
}

impl RouteMetrics {
    /// Marks this row as used at `stamp` nanoseconds since the table's origin on
    /// the line of thread `thread`.
    fn touch(&self, thread: Option<usize>, stamp: u64) {
        self.gauges[gauge_line(thread)]
            .last_used
            .store(stamp, Ordering::Relaxed);
    }

    /// Returns the requests currently being handled.
    fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Returns the stamp lines of every thread index handed out so far.
    fn used_gauges(&self) -> &[RouteGauge] {
        &self.gauges[..live_indices()]
    }

    /// Returns the LRU stamp of the most recent lookup on any line.
    fn last_used(&self) -> u64 {
        self.used_gauges()
            .iter()
            .map(|gauge| gauge.last_used.load(Ordering::Relaxed))
            .max()
            .unwrap_or(0)
    }

    /// Returns `true` when no request completed inside the window ending at `tick`.
    fn is_cold(&self, tick: u64) -> bool {
        let last_observe = self
            .used_gauges()
            .iter()
            .map(|gauge| gauge.last_observe.load(Ordering::Relaxed))
            .max()
            .unwrap_or(0);
        match last_observe {
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

    /// Resolves the route of a request starting at `now` on thread `thread` and
    /// counts it as in flight.
    ///
    /// `matched` says whether `path` is the route template Axum matched rather
    /// than the request path.
    pub(crate) fn begin(
        &self,
        method: &str,
        path: &str,
        matched: bool,
        thread: Option<usize>,
        now: Instant,
    ) -> RouteHandle {
        let metrics = self.route_metrics(method, path, matched, thread, now);
        metrics.in_flight.fetch_add(1, Ordering::Relaxed);
        RouteHandle(metrics)
    }

    /// Records a completed request of `route` in the second `tick` on behalf of
    /// thread `thread`.
    pub(crate) fn observe(
        &self,
        route: &RouteHandle,
        thread: Option<usize>,
        tick: u64,
        bucket: usize,
        class: Option<usize>,
    ) {
        route.0.gauges[gauge_line(thread)]
            .last_observe
            .store(tick.saturating_add(1), Ordering::Relaxed);
        route.0.window.record(thread, tick, bucket, class);
    }

    /// Records a completed request for `method` and `path` at the current tick.
    #[cfg(test)]
    fn observe_path(&self, method: &str, path: &str, ns: u64, status_class: u8) {
        let now = Instant::now();
        let thread = crate::shard::thread_index();
        let route = RouteHandle(self.route_metrics(method, path, false, thread, now));
        let (bucket, class) = crate::histogram::sample_of(ns, status_class);
        self.observe(&route, thread, self.tick_at(now), bucket, class);
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
                    in_flight: metrics.in_flight(),
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
    fn route_metrics(
        &self,
        method: &str,
        path: &str,
        matched: bool,
        thread: Option<usize>,
        now: Instant,
    ) -> Arc<RouteMetrics> {
        let stamp = self.stamp_at(now);
        let cached = LOCAL.try_with(|local| {
            let mut local = local.borrow_mut();
            let local = &mut *local;
            local.sync(self);
            local.key.clear();
            let raw = write_lookup_key(&mut local.key, method, path, matched);
            if let Some(existing) = local.cache.get(local.key.as_str()) {
                existing.touch(thread, stamp);
                return Arc::clone(existing);
            }
            let normalized = if raw {
                local.normalized.clear();
                write_key(&mut local.normalized, method, path);
                local.normalized.as_str()
            } else {
                local.key.as_str()
            };
            let (metrics, cacheable) = self.shared_route(normalized, thread, stamp);
            if cacheable {
                if local.cache.len() >= LOCAL_CACHE_LIMIT {
                    local.cache.clear();
                }
                local.cache.insert(local.key.clone(), Arc::clone(&metrics));
            }
            metrics
        });
        cached.unwrap_or_else(|_| {
            let mut key = String::new();
            write_key(&mut key, method, path);
            self.shared_route(&key, thread, stamp).0
        })
    }

    /// Returns the row of `key` from the shared table, inserting it when absent,
    /// and whether the row is the one `key` names.
    ///
    /// A full table evicts one idle row; when every row is in flight the request
    /// is attributed to the shared `* /...` overflow row instead, which must not
    /// be cached under `key`: the next lookup should try again for a row of its
    /// own.
    fn shared_route(
        &self,
        key: &str,
        thread: Option<usize>,
        stamp: u64,
    ) -> (Arc<RouteMetrics>, bool) {
        {
            let routes = self.routes.read().unwrap_or_else(PoisonError::into_inner);
            if let Some(existing) = routes.get(key) {
                existing.touch(thread, stamp);
                return (Arc::clone(existing), true);
            }
        }
        let mut routes = self.routes.write().unwrap_or_else(PoisonError::into_inner);
        // Re-check: another thread may have inserted the row between the two locks.
        if let Some(existing) = routes.get(key) {
            existing.touch(thread, stamp);
            return (Arc::clone(existing), true);
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
                metrics.touch(thread, stamp);
                return (metrics, false);
            }
        }
        let created = self.new_metrics();
        created.touch(thread, stamp);
        routes.insert(key.to_owned(), Arc::clone(&created));
        (created, true)
    }

    /// Creates an empty row on this table's time base.
    fn new_metrics(&self) -> Arc<RouteMetrics> {
        Arc::new(RouteMetrics {
            in_flight: AtomicU64::new(0),
            header_padding: [0; ROW_HEADER_PADDING],
            window: SlidingWindow::with_clock(self.origin, Arc::clone(&self.extra_secs)),
            gauges: Box::new(std::array::from_fn(|_| RouteGauge::new())),
        })
    }
}

/// Decrements `value` unless it is already zero.
///
/// The decrement is unconditional and undone when it underflowed: one atomic
/// instead of a load plus compare-and-swap on the common path. The underflow
/// only happens on a double release, which the guards never do, so the wrapped
/// value is never observed in practice.
fn saturating_dec(value: &AtomicU64) {
    if value.fetch_sub(1, Ordering::Relaxed) == 0 {
        value.fetch_add(1, Ordering::Relaxed);
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
        if metrics.in_flight() != 0 {
            continue;
        }
        let last_used = metrics.last_used();
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

/// Appends the cache lookup key of a request to `out` and returns whether it is
/// the raw `"<METHOD>\0<template>"` form rather than the normalized key.
///
/// The raw form is used only for a matched route template with a plain
/// upper-case method and a bounded length: the template space is then bounded
/// by the router's routes, and the method needs no folding, so the raw key
/// names the same route as the normalized one. Its separator keeps it apart
/// from every normalized key.
fn write_lookup_key(out: &mut String, method: &str, path: &str, matched: bool) -> bool {
    if matched && is_plain_method(method) && path.len() <= MAX_PATH_CHARS {
        out.push_str(method);
        out.push(RAW_KEY_SEPARATOR);
        out.push_str(path);
        return true;
    }
    write_key(out, method, path);
    false
}

/// Appends the [`RouteKey`] of `method` and `path` to `out`.
fn write_key(out: &mut String, method: &str, path: &str) {
    write_normalized_method(out, method);
    out.push(KEY_SEPARATOR);
    write_normalized_path(out, path);
}

/// Returns `true` when `method` is already in canonical form: non-empty,
/// upper-case ASCII, and at most [`MAX_METHOD_CHARS`] long.
fn is_plain_method(method: &str) -> bool {
    !method.is_empty()
        && method.len() <= MAX_METHOD_CHARS
        && method.bytes().all(|byte| byte.is_ascii_uppercase())
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
    if is_plain_method(method) {
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

    use crate::shard::{thread_index, warm_indices};

    use super::*;

    /// Creates a table on a fresh time base.
    fn new_set() -> EndpointSet {
        EndpointSet::with_clock(Instant::now(), Arc::new(AtomicU64::new(0)))
    }

    /// Returns the number of entries this thread's cache holds for `set`.
    fn local_cache_len(set: &EndpointSet) -> usize {
        LOCAL.with(|local| {
            let local = local.borrow();
            if local.set_id == set.id {
                local.cache.len()
            } else {
                0
            }
        })
    }

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
    fn matched_templates_are_cached_raw_and_reported_normalized() {
        let set = new_set();
        let thread = thread_index();
        let first = set.begin("GET", "/items/{id}", true, thread, Instant::now());
        let second = set.begin("GET", "/items/{id}", true, thread, Instant::now());
        // A hit on the raw template key is what lets a matched request skip
        // normalization.
        assert!(Arc::ptr_eq(&first.0, &second.0));
        assert!(thread_cache_has(&raw_key("GET", "/items/{id}")));
        // A request without a template for the same route shares the row.
        let third = set.begin("GET", "/items/42", false, thread, Instant::now());
        assert!(Arc::ptr_eq(&first.0, &third.0));
        first.end();
        second.end();
        third.end();

        let rows = set.snapshot();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].path, "/items/:id");
        assert_eq!(rows[0].in_flight, 0);
    }

    #[test]
    fn raw_template_keys_never_collide_with_normalized_keys() {
        let set = new_set();
        let thread = thread_index();
        // A literal route whose last segment is 48 digits normalizes to `:id`,
        // while a request path one character longer is cut back to exactly the
        // template text by the segment limit.
        let template = format!("/keys/{}", "1".repeat(MAX_SEGMENT_CHARS));
        let unmatched = format!("{template}x");
        set.begin("GET", &unmatched, false, thread, Instant::now())
            .end();
        set.begin("GET", &template, true, thread, Instant::now())
            .end();
        set.begin("GET", &template, true, thread, Instant::now())
            .end();
        let mut paths: Vec<String> = set.snapshot().into_iter().map(|row| row.path).collect();
        paths.sort();
        assert_eq!(paths, vec![template, "/keys/:id".to_owned()]);
    }

    #[test]
    fn gauge_lines_and_in_flight_have_the_documented_layout() {
        assert_eq!(std::mem::size_of::<RouteGauge>(), 128);
        assert_eq!(std::mem::align_of::<RouteGauge>(), 128);
        // The in-flight gauge is the first field, right behind the `Arc` counts,
        // and everything else starts one full line later.
        assert_eq!(std::mem::offset_of!(RouteMetrics, in_flight), 0);
        assert_eq!(std::mem::offset_of!(RouteMetrics, window), 128 - 16);
        assert!(std::mem::align_of::<RouteMetrics>() <= 16);
    }

    #[test]
    fn unusual_methods_and_long_templates_skip_the_raw_cache() {
        let set = new_set();
        let thread = thread_index();
        let long = format!("/{}", "a".repeat(MAX_PATH_CHARS + 1));
        set.begin("get", "/items/{id}", true, thread, Instant::now())
            .end();
        set.begin("GET", &long, true, thread, Instant::now()).end();
        assert!(thread_cache_has("GET /items/:id"));
        assert!(!thread_cache_has(&raw_key("get", "/items/{id}")));
        assert!(!thread_cache_has(&raw_key("GET", &long)));
    }

    #[test]
    fn thread_cache_is_emptied_when_it_reaches_its_limit() {
        let set = new_set();
        let thread = thread_index();
        let mut peak = 0;
        // Every template normalizes to the same route, so the table never
        // evicts and only the limit can empty the cache.
        for index in 0..(LOCAL_CACHE_LIMIT * 4) {
            set.begin(
                "GET",
                &format!("/v/{index:08x}"),
                true,
                thread,
                Instant::now(),
            )
            .end();
            let len = local_cache_len(&set);
            assert!(len <= LOCAL_CACHE_LIMIT);
            peak = peak.max(len);
        }
        assert_eq!(peak, LOCAL_CACHE_LIMIT);
        assert_eq!(set.snapshot().len(), 1);
    }

    #[test]
    fn lru_order_uses_the_newest_stamp_over_all_lines() {
        warm_indices(2);
        let set = new_set();
        // Row A is used on line 0 first and on line 1 last, with row B in
        // between on line 0: A's newest stamp is not on the line it started on.
        set.begin("GET", "/a", false, Some(0), Instant::now()).end();
        set.begin("GET", "/b", false, Some(0), Instant::now()).end();
        set.begin("GET", "/a", false, Some(1), Instant::now()).end();
        for index in 0..(MAX_ENDPOINTS - 2) {
            set.begin(
                "GET",
                &format!("/route-{index}"),
                false,
                Some(0),
                Instant::now(),
            )
            .end();
        }
        assert_eq!(set.snapshot().len(), MAX_ENDPOINTS);
        set.begin("GET", "/fresh", false, Some(0), Instant::now())
            .end();
        let paths: Vec<String> = set.snapshot().into_iter().map(|row| row.path).collect();
        assert_eq!(paths.len(), MAX_ENDPOINTS);
        assert!(paths.iter().any(|path| path == "/a"));
        assert!(!paths.iter().any(|path| path == "/b"));
    }

    #[test]
    fn thread_cache_stays_bounded_under_high_cardinality_paths() {
        let set = new_set();
        for index in 0..(LOCAL_CACHE_LIMIT * 10) {
            set.observe_path("GET", &format!("/user/name-{index}"), 1_000_000, 2);
            assert!(local_cache_len(&set) <= LOCAL_CACHE_LIMIT);
        }
        assert_eq!(set.snapshot().len(), MAX_ENDPOINTS);
    }

    #[test]
    fn sorts_busiest_endpoint_first() {
        let set = new_set();
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
        let set = new_set();
        let thread = thread_index();
        set.observe_path("GET", "/work", 1_000_000, 2);
        set.observe_path("GET", "/work", 1_000_000, 2);
        let first = set.begin("GET", "/hold", false, thread, Instant::now());
        let second = set.begin("GET", "/hold", false, thread, Instant::now());
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
    fn in_flight_survives_finishing_on_another_thread() {
        let set = Arc::new(new_set());
        let handle = set.begin("GET", "/migrate", false, thread_index(), Instant::now());
        assert_eq!(set.snapshot()[0].in_flight, 1);
        let ended = {
            let set = Arc::clone(&set);
            std::thread::spawn(move || {
                handle.end();
                set.snapshot()[0].in_flight
            })
            .join()
            .unwrap()
        };
        assert_eq!(ended, 0);
        // A request that finished on another thread leaves the gauge exact.
        let again = set.begin("GET", "/migrate", false, thread_index(), Instant::now());
        assert_eq!(set.snapshot()[0].in_flight, 1);
        again.end();
        assert_eq!(set.snapshot()[0].in_flight, 0);
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
        let set = new_set();
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
        let set = new_set();
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
        let set = new_set();
        let thread = thread_index();
        let hold = set.begin("GET", "/hold", false, thread, Instant::now());
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
        let set = new_set();
        let thread = thread_index();
        for index in 0..MAX_ENDPOINTS {
            let _held = set.begin(
                "GET",
                &format!("/hold-{index}"),
                false,
                thread,
                Instant::now(),
            );
        }
        let extra = set.begin("GET", "/extra", false, thread, Instant::now());
        // The overflow row is never cached under the key it absorbed.
        assert!(!LOCAL.with(|local| local.borrow().cache.contains_key("GET /extra")));

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
        let thread = thread_index();
        set.observe_path("GET", "/stale", 1_000_000, 2);
        extra.fetch_add(91, Ordering::Relaxed);
        for index in 0..(MAX_ENDPOINTS - 1) {
            set.observe_path("GET", &format!("/route-{index}"), 1_000_000, 2);
        }
        assert_eq!(set.snapshot().len(), MAX_ENDPOINTS);

        // `begin`/`end` refresh the LRU position without recording a request, so
        // `/stale` is now the most recently used row yet still has an empty window.
        set.begin("GET", "/stale", false, thread, Instant::now())
            .end();
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
        let first = new_set();
        let second = new_set();
        let thread = thread_index();
        first.observe_path("GET", "/shared", 1_000_000, 2);
        second.observe_path("GET", "/shared", 1_000_000, 2);
        first.observe_path("GET", "/shared", 1_000_000, 2);
        assert_eq!(first.snapshot()[0].window_90.requests, 2);
        assert_eq!(second.snapshot()[0].window_90.requests, 1);

        // The same holds for raw template keys.
        first
            .begin("GET", "/t/{id}", true, thread, Instant::now())
            .end();
        second
            .begin("GET", "/t/{id}", true, thread, Instant::now())
            .end();
        first
            .begin("GET", "/t/{id}", true, thread, Instant::now())
            .end();
        assert_eq!(first.snapshot().len(), 2);
        assert_eq!(second.snapshot().len(), 2);
    }

    #[test]
    fn concurrent_registration_creates_one_route() {
        const THREADS: usize = 16;
        let set = Arc::new(new_set());
        let barrier = Arc::new(std::sync::Barrier::new(THREADS));
        let threads: Vec<_> = (0..THREADS)
            .map(|_| {
                let set = Arc::clone(&set);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    let thread = thread_index();
                    set.begin("GET", "/shared", false, thread, Instant::now())
                        .end();
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
