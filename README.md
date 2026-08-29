# axum-sentinel-monitor

An embeddable Axum runtime monitor inspired by
[Fiber Monitor v3](https://docs.gofiber.io/contrib/monitor/). It exposes
real-time process, Tokio runtime, system, and HTTP metrics through a built-in
dashboard and JSON snapshot.

The dashboard is a dependency-free UI with the same layout, trend panels, and
dark/light palette as the Fiber Monitor preview. It does not persist metrics,
run a background collection loop, or replace Prometheus, OpenTelemetry, or an APM.

## Features

- Process CPU, RSS, threads, file descriptors/handles, and uptime
- Tokio live tasks, worker count, and allocator heap details
- System CPU, memory, application-filesystem usage, load averages, and network rates
- HTTP requests, in-flight, 1xx–5xx classes, RPS, 4xx/5xx rates, and P50/P95/P99 latency
- Seven trend charts plus Heap, disk, and status-code detail views
- Light/Dark toggle and a 30/60/90 sample window, persisted in local storage
- HTML dashboard or API-only operation
- SIMD-accelerated JSON extraction and serialization through `sonic-rs`

## Usage

```toml
[dependencies]
axum = "0.8"
axum-sentinel-monitor = "0.1"
tokio = { version = "1", features = ["full"] }
```

```rust
use axum::{Router, routing::get};
use axum_sentinel_monitor::{Config, Monitor};

#[tokio::main]
async fn main() {
    let monitor = Monitor::new(Config {
        title: "My Service".into(),
        description: "Live runtime and HTTP statistics.".into(),
        ..Config::default()
    });

    let app = Router::new()
        .route("/", get(|| async { "ok" }))
        .merge(monitor.router())
        // Apply last so every application route is recorded. The monitor
        // path itself is skipped.
        .layer(monitor.layer());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}
```

Open <http://127.0.0.1:3000/monitor>. Request the same endpoint with
`Accept: application/json` to receive the current snapshot:

```json
{
  "collected_at": "2026-08-29T00:00:00Z",
  "collection": { "partial": false, "errors": [] },
  "process": {},
  "runtime": {},
  "system": {},
  "http": {}
}
```

A route-only mount still serves the dashboard and JSON snapshot, but application
HTTP metrics stay empty until `Monitor::layer()` is applied. Monitor endpoint
requests are not included in HTTP metrics.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `title` | `Axum Sentinel Monitor` | Document title and dashboard heading |
| `description` | Live process, runtime, system, and HTTP metrics… | Text below the heading |
| `footer` | `Powered by axum-sentinel-monitor.` | Footer text |
| `favicon_url` | Built-in SVG | Root-relative path or absolute HTTP(S) URL |
| `refresh` | 3 seconds | Browser polling interval and snapshot cache TTL; values below 1s are clamped |
| `api_only` | `false` | Always return JSON |
| `custom_head` | empty | Deprecated; ignored by the embedded dashboard |
| `font_url` | Google Fonts Roboto | Deprecated; no external font is loaded |
| `chart_js_url` | Chart.js 2.9 CDN | Deprecated; charts use the built-in Canvas implementation |
| `route` | `/monitor` | Route created by `Monitor::router()` |

## Metrics

| Group | Metrics |
| --- | --- |
| Process | CPU, RSS, threads, file descriptors/handles, runtime since monitor initialization |
| Runtime | Tokio live tasks (`goroutines`), heap allocation/system/in-use/idle/released memory, worker count (`workers`) |
| System | CPU, used/available/total memory, application-filesystem usage/type/free space, 1/5/15-minute load averages, aggregate network rates |
| HTTP | Requests, in-flight requests, 1xx–5xx status classes, RPS, 4xx/5xx rates, P50/P95/P99 latency |

Unsupported, failed, and not-yet-available window metrics are encoded as `null`,
not as a synthetic zero. CPU, network, request-rate, status-rate, and latency
values need two collection windows; their first snapshot is `null`.

Snapshots are collected only when JSON is requested and are shared within the
configured refresh TTL. The dashboard keeps at most 90 trend samples in browser
memory and displays 60 by default.

`runtime.goroutines` is the number of live Tokio tasks (or OS threads when no
Tokio runtime is available). `runtime.workers` is the Tokio worker count. Rust
has no garbage collector, so GC metrics are not collected or shown.

## Security

Runtime metrics can reveal process and host information. Do not expose the
monitor route publicly without authentication or network-level access control.
The crate sets `Cache-Control: no-store` and `X-Content-Type-Options: nosniff`,
but authorization remains the application's responsibility.

Monitor records aggregate counters only. It does not collect request or response
bodies, headers, cookies, query strings, client IPs, filesystem paths, or
device names.

## Fiber Monitor compatibility

The dashboard layout, colors, trend panels, and JSON groups match
[Fiber Monitor v3](https://github.com/gofiber/contrib/tree/main/v3/monitor).
Axum routing and middleware are separate, so this crate uses:

- `Monitor::router()` to expose the dashboard/API route
- `Monitor::layer()` to record application HTTP metrics

## License

Licensed under the MIT license.
