use std::{
    io::ErrorKind,
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use axum::{
    extract::Query,
    http::StatusCode,
    routing::{get, post},
    Router,
};
use axum_sentinel_monitor::{Config, Monitor, SonicJson};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;

static NEXT_USER_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Deserialize)]
struct SearchParams {
    name: String,
    age: u32,
}

#[derive(Serialize)]
struct SearchResponse {
    name: String,
    age: u32,
}

async fn search(Query(params): Query<SearchParams>) -> SonicJson<SearchResponse> {
    SonicJson(SearchResponse {
        name: format!("{}你好", params.name),
        age: params.age.saturating_add(1),
    })
}

#[derive(Deserialize)]
struct CreateUser {
    name: String,
    age: u32,
}

#[derive(Serialize)]
struct User {
    id: u64,
    name: String,
    age: u32,
}

async fn create_user(SonicJson(input): SonicJson<CreateUser>) -> SonicJson<User> {
    SonicJson(User {
        id: NEXT_USER_ID.fetch_add(1, Ordering::Relaxed),
        name: input.name,
        age: input.age,
    })
}

#[tokio::main]
async fn main() {
    let monitor = Monitor::new(Config {
        title: "Axum Sentinel Monitor".into(),
        description: "Live process, runtime, system, and HTTP metrics for this Axum service."
            .into(),
        footer: "Powered by axum-sentinel-monitor.".into(),
        refresh: Duration::from_secs(2),
        ..Config::default()
    });

    let app = Router::new()
        .route("/", get(|| async { "Hello from Axum" }))
        .route("/search", get(search))
        .route("/user", post(create_user))
        .route(
            "/slow",
            get(|| async {
                sleep(Duration::from_millis(40)).await;
                "slow"
            }),
        )
        .route(
            "/work",
            get(|| async {
                sleep(Duration::from_millis(8)).await;
                "work"
            }),
        )
        .route("/client-error", get(|| async { StatusCode::BAD_REQUEST }))
        .route("/fail", get(|| async { StatusCode::INTERNAL_SERVER_ERROR }))
        .merge(monitor.router())
        .layer(monitor.layer());

    let address = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("bind server");
    println!("app: http://{address}");
    println!("monitor: http://{address}/monitor");
    println!("search: http://{address}/search?name=Tom&age=18");
    println!("create user: POST http://{address}/user");
    tokio::spawn(generate_traffic(address));
    axum::serve(listener, app).await.expect("serve app");
}

async fn generate_traffic(address: SocketAddr) {
    sleep(Duration::from_millis(250)).await;
    let paths = ["/", "/work", "/work", "/slow", "/client-error", "/fail"];
    loop {
        for path in paths {
            let _ = http_get(address, path).await;
            sleep(Duration::from_millis(180)).await;
        }
    }
}

async fn http_get(address: SocketAddr, path: &str) -> std::io::Result<()> {
    let mut stream = TcpStream::connect(address).await?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut buf = [0u8; 256];
    match stream.read(&mut buf).await {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::ConnectionReset => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{header, Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use sonic_rs::{json, JsonValueTrait, Value};
    use tower::ServiceExt;

    async fn json_body(response: axum::response::Response) -> Value {
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        sonic_rs::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn search_transforms_query_values() {
        let response = Router::new()
            .route("/search", get(search))
            .oneshot(
                Request::get("/search?name=Tom&age=18")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            json_body(response).await,
            json!({"name": "Tom你好", "age": 19})
        );
    }

    #[tokio::test]
    async fn user_accepts_and_returns_json() {
        let response = Router::new()
            .route("/user", post(create_user))
            .oneshot(
                Request::post("/user")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"name":"Alice","age":20}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert!(body["id"].is_u64());
        assert_eq!(body["name"], "Alice");
        assert_eq!(body["age"], 20);
    }

    #[tokio::test]
    async fn user_rejects_non_json_and_malformed_json() {
        let app = Router::new().route("/user", post(create_user));
        let response = app
            .clone()
            .oneshot(
                Request::post("/user")
                    .body(Body::from(r#"{"name":"Alice","age":20}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let response = app
            .oneshot(
                Request::post("/user")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"name":"Alice","age":"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
