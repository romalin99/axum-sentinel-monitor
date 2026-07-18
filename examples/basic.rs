use std::time::Duration;

use axum::{Router, routing::get};
use axum_sentinel_monitor::{Config, Monitor};

#[tokio::main]
async fn main() {
    let monitor = Monitor::new(Config {
        title: "Example Service".to_owned(),
        refresh: Duration::from_secs(2),
        ..Config::default()
    });

    let app = Router::new()
        .route("/", get(|| async { "Hello from Axum" }))
        .nest("/monitor", monitor.router())
        .layer(monitor.layer());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .expect("bind server");
    println!("app: http://127.0.0.1:3000");
    println!("monitor: http://127.0.0.1:3000/monitor");
    axum::serve(listener, app).await.expect("serve app");
}
