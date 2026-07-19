use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
    routing::get,
};
use axum_sentinel_monitor::{Config, MetricSample, MetricsCollector, Monitor};
use http_body_util::BodyExt;
use sonic_rs::{JsonValueTrait, Value};
use tower::ServiceExt;

#[derive(Clone)]
struct FixedCollector {
    calls: Arc<AtomicUsize>,
}

impl FixedCollector {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl MetricsCollector for FixedCollector {
    fn collect(&mut self) -> MetricSample {
        self.calls.fetch_add(1, Ordering::SeqCst);
        MetricSample {
            process_cpu: Some(1.5),
            process_ram: Some(1024),
            process_connections: Some(2),
            process_threads: Some(3),
            process_uptime: Some(12),
            system_cpu: Some(4.5),
            system_ram: Some(2048),
            system_total_ram: Some(4096),
            system_load_average: Some(0.75),
            system_connections: Some(6),
        }
    }
}

fn monitor(config: Config) -> Monitor {
    Monitor::with_collector(config, FixedCollector::new())
}

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
    let default = monitor(Config::default());
    let response = default
        .router()
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
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
    // Keep the complete Fiber v3 dashboard metric set and resolve all
    // server-side template placeholders before returning the document.
    for metric in [
        "CPU Usage",
        "Memory Usage",
        "Response Time",
        "Open Connections",
        "Total requests",
        "Requests / sample",
        "Threads",
        "Uptime",
    ] {
        assert!(html.contains(metric), "missing dashboard metric: {metric}");
    }
    assert!(html.contains("const limit=Number(\"51\")"));
    assert!(!html.contains("__POLL_MS__"));
    assert!(!html.contains("__HISTORY_POINTS__"));

    let custom = monitor(Config {
        title: "<script>bad('title')</script>".to_owned(),
        font_url: "\" onload=\"bad()".to_owned(),
        chart_js_url: String::new(),
        custom_head: "<meta name=\"trusted\" content=\"yes\">".to_owned(),
        ..Config::default()
    });
    let response = custom
        .router()
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let html = body(response).await;
    assert!(html.contains("&lt;script&gt;bad(&#39;title&#39;)&lt;/script&gt;"));
    assert!(html.contains("&quot; onload=&quot;bad()"));
    assert!(html.contains("<meta name=\"trusted\" content=\"yes\">"));
    assert!(!html.contains("<script>bad('title')</script>"));
}

#[tokio::test]
async fn serves_documented_json_schema_with_accept_negotiation() {
    let monitor = monitor(Config::default());
    let response = monitor
        .router()
        .oneshot(
            Request::get("/")
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
    for field in ["cpu", "ram", "conns", "goroutines", "uptime"] {
        assert!(value["pid"][field].is_number(), "pid.{field}");
    }
    assert!(value["pid"]["requests"].is_str());
    for field in ["cpu", "ram", "total_ram", "load_avg", "conns"] {
        assert!(value["os"][field].is_number(), "os.{field}");
    }
}

#[tokio::test]
async fn supports_api_only_and_rejects_every_non_get_method() {
    let monitor = monitor(Config {
        api_only: true,
        ..Config::default()
    });
    let response = monitor
        .router()
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/json; charset=utf-8"
    );

    let response = monitor
        .router()
        .oneshot(Request::post("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(response.headers()[header::ALLOW], "GET");
}

#[tokio::test]
async fn clamps_refresh_counts_requests_and_isolates_instances() {
    let first = monitor(Config {
        refresh: Duration::from_millis(1),
        ..Config::default()
    });
    let second = monitor(Config::default());
    assert_eq!(first.config().refresh, Config::MIN_REFRESH);

    let app = Router::new()
        .route("/", get(|| async { "ok" }))
        .layer(first.layer());
    app.oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(first.snapshot().pid.requests, "1");
    assert_eq!(second.snapshot().pid.requests, "0");
}

struct PartialCollector {
    first: bool,
}

impl MetricsCollector for PartialCollector {
    fn collect(&mut self) -> MetricSample {
        if self.first {
            self.first = false;
            MetricSample {
                process_cpu: Some(9.0),
                process_threads: Some(7),
                system_total_ram: Some(1234),
                ..MetricSample::default()
            }
        } else {
            MetricSample::default()
        }
    }
}

#[tokio::test]
async fn retains_successful_values_and_stops_explicitly() {
    let collector = FixedCollector::new();
    let calls = Arc::clone(&collector.calls);
    let monitor = Monitor::with_collector(
        Config {
            refresh: Config::MIN_REFRESH,
            ..Config::default()
        },
        collector,
    );
    tokio::time::sleep(Duration::from_millis(430)).await;
    assert!(calls.load(Ordering::SeqCst) >= 3);
    monitor.close();
    tokio::time::sleep(Duration::from_millis(30)).await;
    let after_close = calls.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(calls.load(Ordering::SeqCst), after_close);

    let partial = Monitor::with_collector(
        Config {
            refresh: Config::MIN_REFRESH,
            ..Config::default()
        },
        PartialCollector { first: true },
    );
    tokio::time::sleep(Duration::from_millis(230)).await;
    let snapshot = partial.snapshot();
    assert_eq!(snapshot.pid.cpu, 9.0);
    assert_eq!(snapshot.pid.goroutines, 7);
    assert_eq!(snapshot.os.total_ram, 1234);
}

#[tokio::test]
async fn dropping_the_last_clone_ends_sampling() {
    let collector = FixedCollector::new();
    let calls = Arc::clone(&collector.calls);
    {
        let monitor = Monitor::with_collector(
            Config {
                refresh: Config::MIN_REFRESH,
                ..Config::default()
            },
            collector,
        );
        let clone = monitor.clone();
        tokio::time::sleep(Duration::from_millis(220)).await;
        drop(monitor);
        tokio::time::sleep(Duration::from_millis(220)).await;
        assert!(calls.load(Ordering::SeqCst) >= 3);
        drop(clone);
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    let after_drop = calls.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(calls.load(Ordering::SeqCst), after_drop);
}
