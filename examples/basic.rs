use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use axum::{
    Router,
    extract::Query,
    routing::{get, post},
};
use axum_sentinel_monitor::{Config, Monitor, SonicJson};
use serde::{Deserialize, Serialize};

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
        title: "Example Service".to_owned(),
        refresh: Duration::from_secs(2),
        ..Config::default()
    });

    let app = Router::new()
        .route("/", get(|| async { "Hello from Axum" }))
        .route("/search", get(search))
        .route("/user", post(create_user))
        .nest("/monitor", monitor.router())
        .layer(monitor.layer());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .expect("bind server");
    println!("app: http://127.0.0.1:3000");
    println!("monitor: http://127.0.0.1:3000/monitor");
    println!("search: http://127.0.0.1:3000/search?name=Tom&age=18");
    println!("create user: POST http://127.0.0.1:3000/user");
    axum::serve(listener, app).await.expect("serve app");
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode, header},
    };
    use http_body_util::BodyExt;
    use sonic_rs::{JsonValueTrait, Value, json};
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
