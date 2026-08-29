//! An embeddable runtime monitor for Axum, inspired by Fiber Monitor v3.
//!
//! [`Monitor::router`] serves an HTML dashboard (or JSON when requested), while
//! [`Monitor::layer`] records HTTP metrics for application traffic. Requests to
//! the monitor endpoint itself are not counted.

mod collect;
mod config;
mod dashboard;
mod histogram;
mod json;
mod layer;
mod snapshot;
mod stats;

use std::sync::Arc;

use axum::{
    http::{
        header::{ACCEPT, ALLOW, CACHE_CONTROL, CONTENT_TYPE},
        HeaderMap, HeaderValue, Method, StatusCode,
    },
    response::{Html, IntoResponse, Response},
    routing::any,
    Router,
};

pub use config::{Config, MIN_REFRESH};
pub use json::{SonicJson, SonicJsonRejection};
pub use layer::{MonitorLayer, MonitorService};
pub use snapshot::{
    CollectionStats, HttpRateStats, HttpStats, HttpStatusStats, LatencyStats, ProcessStats,
    RuntimeStats, Snapshot, SystemStats,
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
    let Some(accept) = headers
        .get(ACCEPT)
        .and_then(|value| value.to_str().ok())
    else {
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
    use sonic_rs::JsonValueTrait;
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
        assert!(value["process"]["uptime_seconds"].is_u64());
        assert!(value["runtime"]["goroutines"].is_u64());
        assert!(value["collected_at"].is_str());
        assert!(value["collection"]["errors"].is_array());
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
