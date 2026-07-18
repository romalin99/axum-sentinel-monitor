# Axum Sentinel Monitor

An embeddable, real-time system dashboard for [Axum](https://github.com/tokio-rs/axum).
It follows the feature set of Fiber v3 Monitor while using instance-local state and
normal Tower/Axum composition.

## Features

- One route serves an HTML dashboard or JSON, selected with `Accept: application/json`
- Process CPU, RSS, TCP connections, thread/task count, uptime and request count
- Host CPU, used/total memory, one-minute load average and TCP connections
- CPU, memory, browser-observed response time, connections, requests and threads charts
- Configurable refresh interval, title, font URL, Chart.js URL and trusted custom head HTML
- Isolated monitor instances, graceful metric degradation and an explicit collector shutdown
- No WebSocket, SSE, persistence or telemetry export; updates use ordinary HTTP polling

## Fiber v3 correspondence

This crate follows the current
[Fiber v3 monitor](https://docs.gofiber.io/contrib/monitor/) metric and dashboard model:

- Fiber's six charts map to process/OS CPU, process/OS/total memory, browser-observed
  response time, process/OS TCP connections, request delta, and goroutine/thread count.
- The browser polls at `refresh - 200ms`, with a 200ms floor, and retains the latest
  51 points. Chart labels use real timestamps, as in Fiber v3.
- `pid.requests` stays a decimal string. The browser uses `BigInt`, clamps the delta to
  JavaScript's safe integer range, and treats a negative delta as a process restart.
- Fiber's process/OS/total memory presentation and 1024-based `formatBytes` display are
  retained. Chart values follow Fiber v3's decimal-MB convention.
- `uptime` is shown separately from the six charts, matching the Fiber v3 page structure.

Intentional Axum differences are annotated in the implementation: `MonitorLayer` replaces
Fiber's app-wide middleware plus `Next`, each monitor owns isolated state instead of Fiber's
package singleton, content negotiation accepts normal weighted `Accept` headers, and
configuration values are escaped before insertion into HTML. Fiber's goroutine value maps
to the process thread/task count available on the current platform.

## Quick start

```rust
use axum::{Router, routing::get};
use axum_sentinel_monitor::{Config, Monitor};

#[tokio::main]
async fn main() {
    let monitor = Monitor::new(Config::default());
    let app = Router::new()
        .route("/", get(|| async { "hello" }))
        .nest("/monitor", monitor.router())
        .layer(monitor.layer());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
```

Run the included example with `cargo run --example basic`, then open
`http://127.0.0.1:3000/monitor`.

`monitor.layer()` counts requests passing through that layer. Move the layer onto a
sub-router if only part of the application should be counted. The monitor route itself is
counted in the example because the layer wraps the complete application.

## Configuration

| Field | Default | Notes |
| --- | --- | --- |
| `title` | `Axum Sentinel Monitor` | Empty values use the default; HTML escaped |
| `refresh` | 3 seconds | Values below 200ms are raised to 200ms |
| `api_only` | `false` | Always return JSON when enabled |
| `font_url` | Google Fonts Roboto | Empty disables the stylesheet |
| `chart_js_url` | Chart.js 2.9 on jsDelivr | Empty disables charts |
| `custom_head` | empty | Inserted as raw HTML; trusted application configuration only |

The default page loads two third-party resources. For an offline or stricter deployment,
host them yourself and set the two URLs, or set them to empty strings. Inline CSS and
JavaScript mean that a strict Content Security Policy must allow the page explicitly.

## JSON API

Send `Accept: application/json` to the mounted monitor URL:

```json
{
  "pid": {
    "cpu": 0.0,
    "ram": 0,
    "conns": 0,
    "goroutines": 0,
    "requests": "42",
    "uptime": 12
  },
  "os": {
    "cpu": 0.0,
    "ram": 0,
    "total_ram": 0,
    "load_avg": 0.0,
    "conns": 0
  }
}
```

`requests` is a decimal string so JavaScript does not lose 64-bit integer precision.
Response time is measured in the browser around the polling request, matching Fiber
Monitor; it is not server handler latency. Request chart points are counts since the prior
poll, not requests per second.

## Security

The endpoint reveals process and host information. It has no built-in authentication so it
can compose with the authorization middleware already used by an application. Mount it on
an internal listener or wrap the monitor router in an authentication layer before exposing
it. Do not accept `custom_head` from users.

For example, apply your existing authentication middleware only to the monitor router:

```rust,ignore
let protected_monitor = monitor
    .router()
    .route_layer(axum::middleware::from_fn(require_admin));
let app = Router::new().nest("/monitor", protected_monitor);
```

For infrastructure-only access, serve `monitor.router()` from a second listener bound to a
private interface instead of adding it to the public application router.

Responses include `Cache-Control: no-store` and `X-Content-Type-Options: nosniff`.
Configuration values used in text and URL attributes are escaped.

## Platform behavior

The collector targets Linux, macOS and Windows. Some operating systems, containers and
sandboxes deny process details or TCP socket enumeration. Failed samples retain the latest
successful value (or zero before the first success) and never make the HTTP endpoint fail.
Host values reflect what the operating system exposes and are not necessarily container
resource limits.

Fiber's `goroutines` field is represented as the current Rust process thread/task count.
Unlike Fiber's package-global collector, each `Monitor` has its own refresh interval and
request count. Call `monitor.close()` for explicit early shutdown; otherwise the sampler
ends after the final instance clone is dropped.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

This project is independently implemented with reference to
`github.com/gofiber/contrib/v3/monitor` v1.1.1. See
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
