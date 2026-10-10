//! An embeddable runtime monitor for Axum, inspired by Fiber Monitor v3.
//!
//! [`Monitor::router`] serves an HTML dashboard (or JSON when requested), while
//! [`Monitor::layer`] records HTTP metrics for application traffic. Requests to
//! the monitor endpoint itself are not counted. HTTP QPS and latency percentiles
//! are computed in-process from a 90-second ring; samples older than
//! [`HTTP_WINDOW`] are discarded. The dashboard API tab also lists per-route
//! in-flight calls plus 30s/60s/90s QPS and P50/P95/P99/P999. At most 64 routes are
//! tracked: once the table is full, routes with no request in the trailing 90s
//! window are evicted first and the least recently used row goes first within
//! that group, while rows with in-flight requests are never evicted.
//!
//! # Recording cost
//!
//! A request on a known route takes no lock. Every thread that records takes
//! an index, and indices are reused when threads exit; the 64 lowest indices
//! each own a stage in every ring and a stamp line in every route, so for up
//! to 64 recording threads alive at once the counters a request updates live on
//! cache lines of their own. What a request still shares with other threads is
//! the reference count of the route row and of the monitor state, plus the
//! row's in-flight gauge, which usually sits on the line the reference count
//! already touches. A stage holds one second of one thread; the thread moves it
//! into the shared ring when it next records a later second, so an idle
//! thread's last second stays in its stage until it expires. Threads with
//! higher indices record straight into the shared ring, which is exact but
//! contended, and a route the thread has not seen since the last eviction is
//! resolved under the route table's lock.
//!
//! Snapshots read the rings and the stages together and validate every stage
//! read, so no sample is ever counted twice. A thread caught in the middle of a
//! stage move is waited for, up to about thirty milliseconds; after that, a
//! stage still moving, or moved while the snapshot read it, is left out of that
//! one snapshot.
//!
//! Memory per ring is about 102 KiB plus 1,152 bytes per recording thread;
//! each route adds a ring and 8 KiB of stamp lines, so a full table of 64 routes
//! holds about 8 MiB with 8 recording threads and about 12 MiB with 64. Rows
//! are freed when they are evicted, except that a thread's route cache, of at
//! most 256 entries, keeps the rows it resolved alive until that thread's next
//! lookup after an eviction or in another monitor.
//!
//! # Examples
//!
//! ```no_run
//! use axum::{Router, routing::get};
//! use axum_sentinel_monitor::Monitor;
//!
//! # async fn run() -> std::io::Result<()> {
//! let monitor = Monitor::default();
//! let app = Router::new()
//!     .route("/", get(|| async { "Hello from Axum" }))
//!     .merge(monitor.router())
//!     .layer(monitor.layer());
//!
//! let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
//! axum::serve(listener, app).await
//! # }
//! ```

mod collect;
mod config;
mod dashboard;
mod endpoints;
mod histogram;
mod json;
mod layer;
mod shard;
mod snapshot;
mod stats;

use std::sync::{Arc, Mutex, PoisonError};

use axum::{
    Router,
    body::Bytes,
    http::{
        HeaderMap, HeaderValue, Method, StatusCode,
        header::{ACCEPT, ALLOW, CACHE_CONTROL, CONTENT_TYPE},
    },
    response::{IntoResponse, Response},
    routing::any,
};

pub use config::{Config, HTTP_WINDOW, MIN_REFRESH};
pub use json::{SonicJson, SonicJsonRejection};
pub use layer::{MonitorFuture, MonitorLayer, MonitorService};
pub use snapshot::{
    CollectionStats, HttpEndpointStats, HttpRateStats, HttpSecondSample, HttpStats,
    HttpStatusStats, HttpWindowStats, HttpWindows, LatencyStats, ProcessStats, RuntimeStats,
    Snapshot, SystemStats,
};

/// Shared monitor handle used to create the endpoint and request-counting layer.
#[derive(Clone)]
pub struct Monitor {
    /// Normalized configuration.
    config: Arc<Config>,
    /// Counters and snapshot cache shared with every layer of this monitor.
    stats: Arc<stats::SharedStats>,
    /// Pre-rendered dashboard page; empty in `api_only` mode.
    dashboard: Bytes,
    /// JSON encoding of the snapshot served last.
    encoded: Arc<Mutex<Option<EncodedSnapshot>>>,
}

/// JSON encoding of the snapshot it was produced from, kept so that polls
/// hitting the same cached snapshot share one serialization.
struct EncodedSnapshot {
    /// Snapshot the bytes were encoded from, compared by pointer.
    source: Arc<Snapshot>,
    /// Encoded JSON document.
    body: Bytes,
}

impl Default for Monitor {
    fn default() -> Self {
        Self::new(Config::default())
    }
}

impl Monitor {
    /// Creates a monitor from `config`.
    ///
    /// Snapshots are collected on demand and cached for [`Config::refresh`].
    pub fn new(config: Config) -> Self {
        let config = Arc::new(config.normalized());
        let stats = stats::SharedStats::new(config.refresh, &config.route);
        let dashboard = if config.api_only {
            Bytes::new()
        } else {
            Bytes::from(dashboard::render(&config))
        };
        Self {
            config,
            stats,
            dashboard,
            encoded: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns a router exposing the configured monitor route.
    ///
    /// GET requests return HTML by default. Requests with an
    /// `Accept: application/json` header return the current snapshot.
    pub fn router(&self) -> Router {
        let route = self.config.route.clone();
        let monitor = self.clone();
        Router::new().route(
            &route,
            any(move |method: Method, headers: HeaderMap| {
                let monitor = monitor.clone();
                async move { monitor.respond(method, headers).await }
            }),
        )
    }

    /// Returns a Tower layer that records HTTP metrics for every request except
    /// the monitor route.
    pub fn layer(&self) -> MonitorLayer {
        MonitorLayer {
            stats: Arc::clone(&self.stats),
        }
    }

    /// Returns the latest metrics snapshot, collecting when the cache is cold.
    pub fn stats(&self) -> Snapshot {
        self.stats.snapshot()
    }

    /// Returns the latest metrics snapshot; an alias of [`Self::stats`].
    pub fn snapshot(&self) -> Snapshot {
        self.stats()
    }

    /// Returns a shared snapshot without cloning its HTTP series and endpoint rows.
    ///
    /// Prefer this over [`Self::snapshot`] for frequent programmatic polling.
    pub fn snapshot_arc(&self) -> Arc<Snapshot> {
        self.stats.snapshot_arc()
    }

    /// Returns the normalized monitor configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns the current snapshot encoded as JSON.
    ///
    /// Cold collection and serialization run on the blocking pool. Cache hits
    /// return shared encoded bytes without scheduling another blocking task.
    /// The cache probe never blocks: while another poll is collecting, this one
    /// waits for the result on the blocking pool rather than on its worker.
    async fn collect_json(&self) -> Result<Bytes, String> {
        if let Some(snapshot) = self.stats.try_cached_snapshot() {
            return encode_snapshot(&self.encoded, snapshot);
        }
        let stats = Arc::clone(&self.stats);
        let encoded = Arc::clone(&self.encoded);
        match tokio::task::spawn_blocking(move || encode_snapshot(&encoded, stats.snapshot_arc()))
            .await
        {
            Ok(body) => body,
            // The blocking pool is gone (runtime shutting down); collect inline
            // rather than failing the request.
            Err(_) => encode_snapshot(&self.encoded, self.stats.snapshot_arc()),
        }
    }

    /// Serves the monitor route: JSON when negotiated or `api_only`, HTML otherwise.
    async fn respond(&self, method: Method, headers: HeaderMap) -> Response {
        if method != Method::GET {
            return (
                StatusCode::METHOD_NOT_ALLOWED,
                [(ALLOW, HeaderValue::from_static("GET"))],
            )
                .into_response();
        }

        let wants_json = self.config.api_only || prefers_json(&headers);

        let mut response = if wants_json {
            match self.collect_json().await {
                Ok(body) => {
                    let mut response = body.into_response();
                    response.headers_mut().insert(
                        CONTENT_TYPE,
                        HeaderValue::from_static("application/json; charset=utf-8"),
                    );
                    response
                }
                Err(error) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    [(
                        CONTENT_TYPE,
                        HeaderValue::from_static("text/plain; charset=utf-8"),
                    )],
                    error,
                )
                    .into_response(),
            }
        } else {
            let mut response = self.dashboard.clone().into_response();
            response.headers_mut().insert(
                CONTENT_TYPE,
                HeaderValue::from_static("text/html; charset=utf-8"),
            );
            response
        };
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response.headers_mut().insert(
            "x-content-type-options",
            HeaderValue::from_static("nosniff"),
        );
        response
    }
}

/// Serializes `snapshot`, reusing the cached bytes when it is the snapshot that
/// was encoded last.
///
/// The lock is held only to read or replace the cache entry, never while
/// serializing, so a cache hit on an async worker is not stalled behind a
/// serialization on the blocking pool. Two polls that miss at the same time
/// both serialize the same snapshot and store the same bytes, which is correct
/// and rare.
fn encode_snapshot(
    cache: &Mutex<Option<EncodedSnapshot>>,
    snapshot: Arc<Snapshot>,
) -> Result<Bytes, String> {
    {
        let cache = cache.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(encoded) = cache.as_ref()
            && Arc::ptr_eq(&encoded.source, &snapshot)
        {
            return Ok(encoded.body.clone());
        }
    }
    let body = sonic_rs::to_vec(&*snapshot)
        .map(Bytes::from)
        .map_err(|error| error.to_string())?;
    let mut cache = cache.lock().unwrap_or_else(PoisonError::into_inner);
    *cache = Some(EncodedSnapshot {
        source: snapshot,
        body: body.clone(),
    });
    Ok(body)
}

/// Returns `true` when the `Accept` header ranks `application/json` above
/// `text/html`.
fn prefers_json(headers: &HeaderMap) -> bool {
    let Some(accept) = headers.get(ACCEPT).and_then(|value| value.to_str().ok()) else {
        return false;
    };
    let ranges = parse_accept(accept);
    let json = candidate_quality(&ranges, "application", "json");
    let html = candidate_quality(&ranges, "text", "html");
    match (json, html) {
        (Some(json), Some(html)) => json.0 > 0.0 && json > html,
        (Some((quality, _)), None) => quality > 0.0,
        _ => false,
    }
}

/// Splits an `Accept` header into `(type, subtype, quality)` media ranges.
fn parse_accept(value: &str) -> Vec<(&str, &str, f32)> {
    value
        .split(',')
        .filter_map(|item| {
            let mut parts = item.trim().split(';');
            let (kind, subtype) = parts.next()?.trim().split_once('/')?;
            let quality = parts
                .find_map(|parameter| {
                    let (name, value) = parameter.trim().split_once('=')?;
                    name.eq_ignore_ascii_case("q")
                        .then(|| value.trim().parse::<f32>().ok())
                        .flatten()
                })
                .unwrap_or(1.0)
                .clamp(0.0, 1.0);
            Some((kind.trim(), subtype.trim(), quality))
        })
        .collect()
}

/// Returns the `(quality, specificity)` of the most specific range matching the
/// candidate media type, or `None` when no range matches.
fn candidate_quality(
    ranges: &[(&str, &str, f32)],
    candidate_kind: &str,
    candidate_subtype: &str,
) -> Option<(f32, u8)> {
    ranges
        .iter()
        .filter_map(|(kind, subtype, quality)| {
            if (*kind == "*" || kind.eq_ignore_ascii_case(candidate_kind))
                && (*subtype == "*" || subtype.eq_ignore_ascii_case(candidate_subtype))
            {
                let specificity = u8::from(*kind != "*") + u8::from(*subtype != "*");
                Some((*quality, specificity))
            } else {
                None
            }
        })
        .max_by(|left, right| {
            left.1
                .cmp(&right.1)
                .then_with(|| left.0.total_cmp(&right.0))
        })
}

#[cfg(test)]
mod tests {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        routing::get,
    };
    use http_body_util::BodyExt;
    use sonic_rs::{JsonContainerTrait, JsonValueTrait};
    use tower::{Service, ServiceExt};

    use super::*;

    #[tokio::test]
    async fn serves_html_by_default() {
        let monitor = Monitor::default();
        let response = monitor
            .router()
            .oneshot(Request::get("/monitor").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("--accent: #67e8f9"));
        assert!(html.contains("canvas"));
        assert!(!html.contains("Chart.js"));
        assert!(html.contains("P999"));
        assert!(html.contains("data-page=\"api\""));
        assert!(html.contains("Endpoints"));
        assert!(html.contains("data-samples=\"90\""));
    }

    #[tokio::test]
    async fn serves_process_runtime_system_http_json() {
        let monitor = Monitor::default();
        let app = Router::new()
            .route("/", get(|| async { "ok" }))
            .merge(monitor.router())
            .layer(monitor.layer());

        let _ = app
            .clone()
            .oneshot(Request::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::get("/monitor")
                    .header(ACCEPT, "application/json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let value: sonic_rs::Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(value["http"]["requests"], 1);
        assert_eq!(value["http"]["status"]["2xx"], 1);
        assert_eq!(value["http"]["window_seconds"], 90);
        assert_eq!(
            value["http"]["series"].as_array().map(|rows| rows.len()),
            Some(90)
        );
        assert_eq!(value["http"]["windows"]["60"]["requests"], 1);
        assert_eq!(value["http"]["windows"]["60"]["status"]["2xx"], 1);
        assert_eq!(value["http"]["windows"]["90"]["requests"], 1);
        assert_eq!(value["http"]["endpoints"][0]["path"], "/");
        assert_eq!(value["http"]["endpoints"][0]["method"], "GET");
        assert_eq!(value["http"]["endpoints"][0]["in_flight"], 0);
        assert_eq!(
            value["http"]["endpoints"][0]["windows"]["60"]["requests"],
            1
        );
        assert_eq!(
            value["http"]["endpoints"][0]["windows"]["90"]["requests"],
            1
        );
        assert!(value["http"]["latency"]["p50_ns"].is_u64());
        assert!(value["http"]["latency"]["p95_ns"].is_u64());
        assert!(value["http"]["latency"]["p999_ns"].is_u64());
        assert!(value["http"]["rps"].is_number());
        assert!(value["process"]["uptime_seconds"].is_u64());
        assert!(value["runtime"]["goroutines"].is_u64());
        assert!(value["collected_at"].is_str());
        assert!(value["collection"]["errors"].is_array());
    }

    #[tokio::test]
    async fn sorts_endpoints_by_recent_request_count() {
        let monitor = Monitor::default();
        let app = Router::new()
            .route("/", get(|| async { "ok" }))
            .route("/work", get(|| async { "work" }))
            .route("/fail", get(|| async { StatusCode::INTERNAL_SERVER_ERROR }))
            .route("/items/{id}", get(|| async { "item" }))
            .merge(monitor.router())
            .layer(monitor.layer());

        for _ in 0..3 {
            let _ = app
                .clone()
                .oneshot(Request::get("/work").body(Body::empty()).unwrap())
                .await
                .unwrap();
        }
        let _ = app
            .clone()
            .oneshot(Request::get("/fail").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let _ = app
            .clone()
            .oneshot(Request::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let _ = app
            .clone()
            .oneshot(Request::get("/items/42").body(Body::empty()).unwrap())
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::get("/monitor")
                    .header(ACCEPT, "application/json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let value: sonic_rs::Value = sonic_rs::from_slice(&body).unwrap();
        let endpoints = value["http"]["endpoints"].as_array().expect("endpoints");
        assert!(endpoints.len() >= 3);
        assert_eq!(endpoints[0]["method"], "GET");
        assert_eq!(endpoints[0]["path"], "/work");
        assert_eq!(endpoints[0]["windows"]["60"]["requests"], 3);
        assert_eq!(endpoints[0]["in_flight"], 0);
        assert!(endpoints[0]["windows"]["60"]["latency"]["p50_ns"].is_u64());
        assert!(endpoints[0]["windows"]["30"]["rps"].is_number());
        let fail = endpoints
            .iter()
            .find(|row| row["path"] == "/fail")
            .expect("fail endpoint");
        assert_eq!(fail["windows"]["60"]["status"]["5xx"], 1);
        let item = endpoints
            .iter()
            .find(|row| row["path"] == "/items/:id")
            .expect("collapsed item path");
        assert_eq!(item["windows"]["60"]["requests"], 1);
    }

    #[tokio::test]
    async fn records_in_flight_per_endpoint() {
        let monitor = Monitor::default();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
        let started_tx = std::sync::Arc::new(std::sync::Mutex::new(Some(started_tx)));
        let app = Router::new()
            .route("/hold", {
                let started_tx = std::sync::Arc::clone(&started_tx);
                get(move || {
                    let started_tx = std::sync::Arc::clone(&started_tx);
                    async move {
                        if let Some(tx) = started_tx.lock().ok().and_then(|mut slot| slot.take()) {
                            let _ = tx.send(());
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                        "held"
                    }
                })
            })
            .merge(monitor.router())
            .layer(monitor.layer());

        let pending = tokio::spawn(app.oneshot(Request::get("/hold").body(Body::empty()).unwrap()));
        started_rx.await.expect("handler started");
        let snapshot = monitor.snapshot();
        assert_eq!(snapshot.http.in_flight, 1);
        let hold = snapshot
            .http
            .endpoints
            .iter()
            .find(|row| row.path == "/hold")
            .expect("hold endpoint");
        assert_eq!(hold.method, "GET");
        assert_eq!(hold.in_flight, 1);
        pending.abort();
    }

    #[tokio::test]
    async fn layer_caches_matched_routes_under_their_template() {
        let monitor = Monitor::default();
        let app = Router::new()
            .route("/items/{id}", get(|| async { "item" }))
            .layer(monitor.layer());
        let _ = app
            .oneshot(Request::get("/items/42").body(Body::empty()).unwrap())
            .await
            .unwrap();
        // The request ran on this thread, so its cache holds the template Axum
        // matched rather than the normalized request path.
        assert!(endpoints::thread_cache_has(&endpoints::raw_key(
            "GET",
            "/items/{id}"
        )));
    }

    #[tokio::test]
    async fn dropping_unpolled_request_releases_in_flight() {
        let monitor = Monitor::default();
        let mut app = Router::new()
            .route("/x", get(|| async { "ok" }))
            .layer(monitor.layer());
        let future = Service::<Request<Body>>::call(
            &mut app,
            Request::get("/x").body(Body::empty()).unwrap(),
        );
        drop(future);
        assert_eq!(monitor.snapshot().http.in_flight, 0);
    }

    #[tokio::test]
    async fn monitor_endpoint_is_not_application_traffic() {
        let monitor = Monitor::default();
        let app = monitor.router().layer(monitor.layer());
        let response = app
            .oneshot(
                Request::get("/monitor")
                    .header(ACCEPT, "application/json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let value: sonic_rs::Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(value["http"]["requests"], 0);
    }

    #[tokio::test]
    async fn rejects_non_get_requests() {
        let response = Monitor::default()
            .router()
            .oneshot(Request::post("/monitor").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers().get(ALLOW).unwrap(), "GET");
    }

    #[test]
    fn negotiates_quality_and_specificity() {
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            "text/html;q=0.4, application/json;q=0.8".parse().unwrap(),
        );
        assert!(prefers_json(&headers));
        headers.insert(ACCEPT, "application/json;q=0, */*;q=1".parse().unwrap());
        assert!(!prefers_json(&headers));
    }

    #[test]
    fn reuses_encoded_snapshot_bytes_within_cache_ttl() {
        let monitor = Monitor::default();
        let snapshot = monitor.snapshot_arc();
        assert!(Arc::ptr_eq(&snapshot, &monitor.snapshot_arc()));
        let first = encode_snapshot(&monitor.encoded, Arc::clone(&snapshot)).unwrap();
        let second = encode_snapshot(&monitor.encoded, snapshot).unwrap();
        assert_eq!(first.as_ptr(), second.as_ptr());
        assert_eq!(first.len(), second.len());
    }
}
