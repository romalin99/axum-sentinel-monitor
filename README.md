# axum-sentinel-monitor

An embeddable Axum runtime monitor inspired by
[Fiber Monitor v3](https://docs.gofiber.io/contrib/monitor/). It exposes
real-time process, Tokio runtime, system, and HTTP metrics through a built-in
dashboard and JSON snapshot.

The dashboard is a dependency-free UI with the same layout, trend panels, and
dark/light palette as the Fiber Monitor preview. HTTP analytics stay in-process:
no remote analytics service, Prometheus scrape, or other third-party backend is
required. It does not persist metrics beyond the 60-second HTTP ring, run a
background collection loop, or replace Prometheus, OpenTelemetry, or an APM.

## Features

- Process CPU, RSS, threads, file descriptors/handles, and uptime
- Tokio live tasks, worker count, and allocator heap details
- System CPU, memory, application-filesystem usage, load averages, and network rates
- HTTP requests, in-flight, 1xx–5xx classes, and in-process QPS / P50 / P95 / P99 / P999
- Per-route Endpoints list with in-flight, 30s/60s QPS, and P50 / P99 / P999
- HTTP samples live in a 60-second ring (one slot per second); data older than 60s is discarded
- Seven trend charts plus Heap, disk, and status-code detail views
- Light/Dark toggle and a 30s/60s HTTP window, persisted in local storage
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
| HTTP | Lifetime request/status totals, in-flight, 30s/60s QPS, 4xx/5xx rates, P50/P95/P99/P999, a 60-point 1s series, and per-route windows |

HTTP QPS and latency are computed inside the process from a 60-second ring. There is no Prometheus, SaaS analytics, or other remote backend. Slots older than `HTTP_WINDOW` (60 seconds) are overwritten. The dashboard has a **Runtime** tab (process/system) and an **API** tab (QPS, latency percentiles, error rate, status codes, and an Endpoints list). Each route is shown like a volume list: count, method, path, and a status-colored bar. In-flight calls use the same row. 30s/60s QPS, P50, P99, and P999 sit on the right. Numeric, UUID, and `{param}` path segments are collapsed to `:id`. At most 64 routes are retained. The API tab 30s/60s toggle shows the most recent 30 or 60 seconds of that ring. It does not keep more than 60 seconds, and it does not collect location, device, usage-time, or day-of-week dimensions.

Unsupported, failed, and not-yet-available process/system window metrics are encoded as `null`,
not as a synthetic zero. CPU and network rates need two collection windows; their first snapshot is `null`. HTTP QPS is `0` until the first completed request, and latency percentiles are `null` while the selected window has no samples.

Snapshots are collected only when JSON is requested and are shared within the
configured refresh TTL. Process/system trend charts keep at most 60 poll samples in the browser. HTTP charts are drawn from `http.series` (oldest-first, 60 one-second points) so a newly opened dashboard already shows the last minute.

`runtime.goroutines` is the number of live Tokio tasks (or OS threads when no
Tokio runtime is available). `runtime.workers` is the Tokio worker count. Rust
has no garbage collector, so GC metrics are not collected or shown.

Heap figures follow Go's `runtime.MemStats` names. On Linux/glibc they come from
`mallinfo2`: `heap_alloc_bytes` = `uordblks` (in use), `heap_idle_bytes` =
`fordblks` (free chunks), `heap_sys_bytes` = `arena + hblkhd`. `heap_sys_bytes`
is a high-water mark of address space the allocator obtained — `malloc_trim`
returns pages with `MADV_DONTNEED` but never shrinks that number, so it cannot be
compared with RSS directly. `heap_released_bytes` closes that gap: it is
`heap_sys_bytes` minus the process's resident anonymous memory (`RssAnon` from
`/proc/self/status`), clamped into `released <= idle <= sys`, i.e. the heap pages
the kernel no longer keeps resident. `heap_sys_bytes - heap_released_bytes`
(shown as **Heap Resident** in the Heap details) therefore tracks
`process.rss_bytes` minus file-backed mappings, and drops after a trim. Because
`RssAnon` also counts thread stacks and non-malloc anonymous mappings, released
is a lower bound. On macOS the default zone statistics are used and
`heap_released_bytes` is `0`; other targets report zeros.

When the host binary uses `tikv-jemallocator` as its global allocator, enable the
`jemalloc` feature: heap figures then come from jemalloc's own `stats.*`
(`tikv-jemalloc-ctl`) — `heap_alloc_bytes` = allocated, `heap_inuse_bytes` =
active, `heap_sys_bytes` = mapped + retained, `heap_released_bytes` = retained +
(mapped − resident), `heap_idle_bytes` = sys − active — so **Heap Resident**
equals jemalloc's `resident`. Without the feature, a jemalloc-backed process makes
the glibc-based `heap_released_bytes` meaningless (jemalloc's anonymous pages swamp
`RssAnon`). Under the feature the native-library (glibc) heap is no longer part of
heap_*; `process.rss_bytes` still covers everything.

## Security

Runtime metrics can reveal process and host information. Do not expose the
monitor route publicly without authentication or network-level access control.
The crate sets `Cache-Control: no-store` and `X-Content-Type-Options: nosniff`,
but authorization remains the application's responsibility.

Monitor records aggregate counters and per-route 30s/60s windows, including in-flight. It does not collect request or response
bodies, headers, cookies, query strings, client IPs, filesystem paths, or
device names. Numeric, UUID, and `{param}` path segments are stored as `:id`.

## Fiber Monitor compatibility

The dashboard layout, colors, trend panels, and JSON groups match
[Fiber Monitor v3](https://github.com/gofiber/contrib/tree/main/v3/monitor).
Axum routing and middleware are separate, so this crate uses:

- `Monitor::router()` to expose the dashboard/API route
- `Monitor::layer()` to record application HTTP metrics

## License

Licensed under the MIT license.
