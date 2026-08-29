use std::time::Duration;

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
    routing::get,
};
use axum_sentinel_monitor::{Config, Monitor};
use http_body_util::BodyExt;
use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};
use tower::ServiceExt;

async fn body(response: axum::response::Response) -> String {
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("read response")
        .to_bytes();
    String::from_utf8(bytes.to_vec()).expect("UTF-8 response")
}

#[tokio::test]
async fn serves_default_and_custom_dashboard_safely() {
    let default = Monitor::default();
    let response = default
        .router()
        .oneshot(Request::get("/monitor").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/html; charset=utf-8"
    );
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    let html = body(response).await;
    assert!(html.contains("Axum Sentinel Monitor"));
    for metric in ["Process", "Runtime", "System", "Tasks", "API", "QPS"] {
        assert!(html.contains(metric), "missing dashboard metric: {metric}");
    }
    assert!(html.contains("data-theme=\"dark\""));
    assert!(html.contains("canvas"));
    assert!(!html.contains("Chart.js"));
    assert!(html.contains("data-page=\"api\""));
    assert!(html.contains("Endpoints"));
    assert!(html.contains("data-filter=\"success\""));
    assert!(html.contains("endpoint-stats"));
    assert!(html.contains(">P50<"));
    assert!(html.contains(">P999<"));
    assert!(html.contains(">30s<"));
    assert!(!html.contains("data-samples=\"90\""));
    assert!(!html.contains("Location"));
    assert!(!html.contains("Day of week"));
    assert!(!html.contains("__MONITOR_TITLE__"));
    assert!(!html.contains("__MONITOR_REFRESH_MS__"));

    let custom = Monitor::new(Config {
        title: "<script>bad('title')</script>".to_owned(),
        custom_head: "<meta name=\"trusted\" content=\"yes\">".to_owned(),
        ..Config::default()
    });
    let response = custom
        .router()
        .oneshot(Request::get("/monitor").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let html = body(response).await;
    assert!(html.contains("&lt;script&gt;bad(&#39;title&#39;)&lt;/script&gt;"));
    assert!(!html.contains("<script>bad('title')</script>"));
    assert!(!html.contains("<meta name=\"trusted\" content=\"yes\">"));
}

#[tokio::test]
async fn serves_documented_json_schema_with_accept_negotiation() {
    let monitor = Monitor::default();
    let response = monitor
        .router()
        .oneshot(
            Request::get("/monitor")
                .header(
                    header::ACCEPT,
                    "text/html;q=0.4, application/json; charset=utf-8; q=0.9",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/json; charset=utf-8"
    );
    let value: Value = sonic_rs::from_str(&body(response).await).unwrap();
    assert!(value["process"]["uptime_seconds"].is_number());
    assert!(value["runtime"]["goroutines"].is_number());
    assert!(value["runtime"]["workers"].is_number());
    assert!(
        value["system"]["memory_total_bytes"].is_u64()
            || value["system"]["memory_total_bytes"].is_null()
    );
    assert_eq!(value["http"]["requests"], 0);
    assert!(value["http"]["in_flight"].is_u64());
    assert!(value["http"]["status"]["2xx"].is_u64());
    assert_eq!(value["http"]["window_seconds"], 60);
    assert_eq!(
        value["http"]["series"].as_array().map(|rows| rows.len()),
        Some(60)
    );
    assert_eq!(value["http"]["windows"]["30"]["seconds"], 30);
    assert_eq!(value["http"]["windows"]["60"]["seconds"], 60);
    assert!(value["http"]["windows"]["60"]["status"].is_object());
    assert!(value["http"]["endpoints"].is_array());
    assert!(value["http"]["latency"].is_object());
    assert!(value["collected_at"].is_str());
    assert!(value["collection"]["errors"].is_array());
}

#[tokio::test]
async fn supports_api_only_and_rejects_every_non_get_method() {
    let monitor = Monitor::new(Config {
        api_only: true,
        ..Config::default()
    });
    let response = monitor
        .router()
        .oneshot(Request::get("/monitor").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/json; charset=utf-8"
    );

    let response = monitor
        .router()
        .oneshot(Request::post("/monitor").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(response.headers()[header::ALLOW], "GET");
}

#[tokio::test]
async fn clamps_refresh_counts_requests_and_isolates_instances() {
    let first = Monitor::new(Config {
        refresh: Duration::from_millis(1),
        ..Config::default()
    });
    let second = Monitor::default();
    assert_eq!(first.config().refresh, Config::MIN_REFRESH);

    let app = Router::new()
        .route("/", get(|| async { "ok" }))
        .merge(first.router())
        .layer(first.layer());
    app.oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(first.snapshot().http.requests, 1);
    assert_eq!(second.snapshot().http.requests, 0);
}

#[tokio::test]
async fn monitor_endpoint_is_not_application_traffic() {
    let monitor = Monitor::default();
    let app = monitor.router().layer(monitor.layer());
    let response = app
        .oneshot(
            Request::get("/monitor")
                .header(header::ACCEPT, "application/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let value: Value = sonic_rs::from_str(&body(response).await).unwrap();
    assert_eq!(value["http"]["requests"], 0);
}
