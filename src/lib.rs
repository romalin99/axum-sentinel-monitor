//! An embeddable runtime monitor for Axum, inspired by Fiber Monitor v3.
//!
//! [`Monitor::router`] serves an HTML dashboard (or JSON when requested), while
//! [`Monitor::layer`] records HTTP metrics for application traffic. Requests to
//! the monitor endpoint itself are not counted. HTTP QPS and latency percentiles
//! are computed in-process from a 60-second ring; samples older than
//! [`HTTP_WINDOW`] are discarded. The dashboard API tab also lists per-route
//! in-flight calls plus 30s/60s QPS and P50/P95/P99/P999.

mod collect;
mod config;
mod dashboard;
mod endpoints;
mod histogram;
mod json;
mod layer;
mod snapshot;
mod stats;

use std::sync::Arc;

use axum::{
    Router,
    http::{
        HeaderMap, HeaderValue, Method, StatusCode,
        header::{ACCEPT, ALLOW, CACHE_CONTROL, CONTENT_TYPE},
    },
    response::{Html, IntoResponse, Response},
    routing::any,
};

pub use config::{Config, HTTP_WINDOW, MIN_REFRESH};
pub use json::{SonicJson, SonicJsonRejection};
pub use layer::{MonitorLayer, MonitorService};
pub use snapshot::{
    CollectionStats, HttpEndpointStats, HttpRateStats, HttpSecondSample, HttpStats,
    HttpStatusStats, HttpWindowStats, HttpWindows, LatencyStats, ProcessStats, RuntimeStats,
    Snapshot, SystemStats,
};

/// Shared monitor handle used to create the endpoint and request-counting layer.
#[derive(Clone)]
pub struct Monitor {
    config: Arc<Config>,
    stats: Arc<stats::SharedStats>,
}

impl Default for Monitor {
    fn default() -> Self {
        Self::new(Config::default())
    }
}

impl Monitor {
    /// Creates a monitor. Snapshots are collected on demand and cached for `refresh`.
    pub fn new(config: Config) -> Self {
        let config = Arc::new(config.normalized());
        let stats = stats::SharedStats::new(config.refresh);
        Self { config, stats }
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
                std::future::ready(monitor.respond(method, headers))
            }),
        )
    }

    /// Returns a Tower layer that records HTTP metrics for every request except
    /// the monitor route.
    pub fn layer(&self) -> MonitorLayer {
        MonitorLayer {
            stats: Arc::clone(&self.stats),
            skip_path: self.config.route.clone(),
        }
    }

    /// Returns the latest metrics snapshot, collecting when the cache is cold.
    pub fn stats(&self) -> Snapshot {
        self.stats.snapshot()
    }

    /// Returns the latest metrics snapshot, collecting when the cache is cold.
    pub fn snapshot(&self) -> Snapshot {
        self.stats()
    }

    /// Returns the normalized monitor configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    fn respond(&self, method: Method, headers: HeaderMap) -> Response {
        if method != Method::GET {
            return (
                StatusCode::METHOD_NOT_ALLOWED,
                [(ALLOW, HeaderValue::from_static("GET"))],
            )
                .into_response();
        }

        let wants_json = self.config.api_only || prefers_json(&headers);

        let mut response = if wants_json {
            SonicJson(self.stats()).into_response()
        } else {
            Html(dashboard::render(&self.config)).into_response()
        };

        if wants_json {
            response.headers_mut().insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/json; charset=utf-8"),
            );
        } else {
            response.headers_mut().insert(
                CONTENT_TYPE,
                HeaderValue::from_static("text/html; charset=utf-8"),
            );
        }
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
    use tower::ServiceExt;

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
        assert!(!html.contains("data-samples=\"90\""));
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
        assert_eq!(value["http"]["window_seconds"], 60);
        assert_eq!(
            value["http"]["series"].as_array().map(|rows| rows.len()),
            Some(60)
        );
        assert_eq!(value["http"]["windows"]["60"]["requests"], 1);
        assert_eq!(value["http"]["windows"]["60"]["status"]["2xx"], 1);
        assert_eq!(value["http"]["endpoints"][0]["path"], "/");
        assert_eq!(value["http"]["endpoints"][0]["method"], "GET");
        assert_eq!(value["http"]["endpoints"][0]["in_flight"], 0);
        assert_eq!(
            value["http"]["endpoints"][0]["windows"]["60"]["requests"],
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
}
