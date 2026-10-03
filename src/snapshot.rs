//! Serializable snapshot types returned by the monitor endpoint and API.

use chrono::{DateTime, Utc};
use serde::Serialize;

/// JSON snapshot served to the dashboard and API clients.
#[derive(Clone, Debug, Serialize)]
pub struct Snapshot {
    /// Wall-clock time at which this snapshot was collected.
    pub collected_at: DateTime<Utc>,
    /// Outcome of the collection pass that produced this snapshot.
    pub collection: CollectionStats,
    /// Metrics of the current process.
    pub process: ProcessStats,
    /// Async runtime and allocator metrics.
    pub runtime: RuntimeStats,
    /// Host-wide metrics.
    pub system: SystemStats,
    /// In-process HTTP traffic metrics.
    pub http: HttpStats,
}

/// Outcome of one collection pass.
#[derive(Clone, Debug, Serialize)]
pub struct CollectionStats {
    /// `true` when at least one metric could not be collected.
    pub partial: bool,
    /// Names of the metrics that could not be collected, such as `process.threads`.
    pub errors: Vec<String>,
}

/// Metrics of the current process.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ProcessStats {
    /// CPU usage in percent of the whole machine (0–100), or `None` on the first
    /// collection, before a usage delta exists.
    pub cpu_percent: Option<f64>,
    /// Resident set size in bytes, or `None` when the process could not be read.
    pub rss_bytes: Option<u64>,
    /// OS thread count, or `None` when the platform does not report it.
    pub threads: Option<i32>,
    /// Open file descriptors (handles on Windows), or `None` when the platform
    /// does not report them.
    pub open_descriptors: Option<i32>,
    /// Seconds elapsed since the monitor was created.
    pub uptime_seconds: u64,
}

/// Async runtime and allocator metrics, named after Go's `runtime.MemStats`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct RuntimeStats {
    /// Tokio live tasks, or OS threads when no runtime is available.
    pub goroutines: u64,
    /// Bytes handed out by the allocator and still in use.
    pub heap_alloc_bytes: u64,
    /// Address space in bytes the allocator has obtained from the OS.
    ///
    /// On glibc this is `mallinfo2().arena + hblkhd`, a high-water mark that
    /// `malloc_trim` does not shrink; `heap_sys_bytes - heap_released_bytes` is
    /// the resident heap.
    pub heap_sys_bytes: u64,
    /// Page-granular in-use bytes.
    ///
    /// This is jemalloc `stats.active` under the `jemalloc` feature; otherwise it
    /// equals `heap_alloc_bytes` (glibc and macOS expose no such figure).
    pub heap_inuse_bytes: u64,
    /// Bytes of free chunks inside the heap, including pages already returned to
    /// the OS.
    pub heap_idle_bytes: u64,
    /// Bytes of heap pages the kernel no longer keeps resident (Go's
    /// `HeapReleased`).
    ///
    /// On glibc it is derived as `heap_sys_bytes` minus `RssAnon`, so it moves with
    /// `malloc_trim` / `MADV_DONTNEED` while `heap_sys_bytes` stays put.
    pub heap_released_bytes: u64,
    /// Tokio worker threads.
    pub workers: i32,
}

/// Host-wide metrics.
#[derive(Clone, Debug, Default, Serialize)]
pub struct SystemStats {
    /// CPU usage across all cores in percent (0–100), or `None` on the first
    /// collection, before a usage delta exists.
    pub cpu_percent: Option<f64>,
    /// Used memory in percent of total memory, or `None` when memory could not be
    /// read.
    pub memory_used_percent: Option<f64>,
    /// Used memory in bytes, or `None` when memory could not be read.
    pub memory_used_bytes: Option<u64>,
    /// Total memory in bytes, or `None` when memory could not be read.
    pub memory_total_bytes: Option<u64>,
    /// Available memory in bytes, or `None` when memory could not be read.
    pub memory_available_bytes: Option<u64>,
    /// Used space in percent on the disk holding the working directory, or `None`
    /// when no mount matches.
    pub disk_used_percent: Option<f64>,
    /// Used space in bytes on that disk, or `None` when no mount matches.
    pub disk_used_bytes: Option<u64>,
    /// Total space in bytes on that disk, or `None` when no mount matches.
    pub disk_total_bytes: Option<u64>,
    /// Space in bytes available to the process on that disk, or `None` when no
    /// mount matches.
    pub disk_free_bytes: Option<u64>,
    /// File-system name of that disk, or `None` when it is unknown.
    pub disk_fstype: Option<String>,
    /// One-minute load average, or `None` on Windows.
    pub load1: Option<f64>,
    /// Five-minute load average, or `None` on Windows.
    pub load5: Option<f64>,
    /// Fifteen-minute load average, or `None` on Windows.
    pub load15: Option<f64>,
    /// Bytes received per second across all interfaces, or `None` on the first
    /// collection and after a counter reset.
    pub network_receive_bps: Option<f64>,
    /// Bytes sent per second across all interfaces, or `None` on the first
    /// collection and after a counter reset.
    pub network_send_bps: Option<f64>,
}

/// In-process HTTP traffic metrics recorded by [`crate::MonitorLayer`].
#[derive(Clone, Debug, Default, Serialize)]
pub struct HttpStats {
    /// Requests started since the monitor was created.
    pub requests: u64,
    /// Requests currently being handled.
    pub in_flight: u64,
    /// Requests per second over the last 60 seconds of in-process samples.
    pub rps: Option<f64>,
    /// Completed responses by status class since the monitor was created.
    pub status: HttpStatusStats,
    /// Error rates over the last 60 seconds.
    pub rates: HttpRateStats,
    /// Latency percentiles over the last 60 seconds.
    pub latency: LatencyStats,
    /// Maximum HTTP sample retention, in seconds.
    ///
    /// Older slots are discarded.
    pub window_seconds: u32,
    /// Aggregates over the trailing 30, 60, and 90 seconds.
    pub windows: HttpWindows,
    /// One point per second for the retained window, oldest first.
    pub series: Vec<HttpSecondSample>,
    /// Per-route 30s/60s/90s stats, in-flight first then busiest.
    ///
    /// Samples older than 90s are dropped.
    pub endpoints: Vec<HttpEndpointStats>,
}

/// Method + path traffic for the same 30s/60s/90s ring as [`HttpStats`].
#[derive(Clone, Debug, Default, Serialize)]
pub struct HttpEndpointStats {
    /// Upper-case HTTP method.
    pub method: String,
    /// Normalized route path, with id-like segments collapsed to `:id`.
    pub path: String,
    /// Requests currently being handled for this route.
    pub in_flight: u64,
    /// Aggregates over the trailing 30, 60, and 90 seconds for this route.
    pub windows: HttpWindows,
}

/// 30-second, 60-second, and 90-second views of the same in-process ring.
#[derive(Clone, Debug, Default, Serialize)]
pub struct HttpWindows {
    /// Trailing 30 seconds, serialized as `"30"`.
    #[serde(rename = "30")]
    pub secs_30: HttpWindowStats,
    /// Trailing 60 seconds, serialized as `"60"`.
    #[serde(rename = "60")]
    pub secs_60: HttpWindowStats,
    /// Trailing 90 seconds, serialized as `"90"`.
    #[serde(rename = "90")]
    pub secs_90: HttpWindowStats,
}

/// Aggregated HTTP traffic for a sliding window that never exceeds 90 seconds.
#[derive(Clone, Debug, Default, Serialize)]
pub struct HttpWindowStats {
    /// Nominal window length in seconds.
    pub seconds: u32,
    /// Seconds of the window that have elapsed since the monitor was created;
    /// smaller than `seconds` only during warm-up.
    pub covered_seconds: u32,
    /// Requests completed inside the window.
    pub requests: u64,
    /// Requests per second, computed over `covered_seconds`.
    pub rps: f64,
    /// Completed responses by status class inside the window.
    pub status: HttpStatusStats,
    /// Error rates inside the window.
    pub rates: HttpRateStats,
    /// Latency percentiles inside the window.
    pub latency: LatencyStats,
}

/// Completed requests in a single one-second slot.
#[derive(Clone, Debug, Default, Serialize)]
pub struct HttpSecondSample {
    /// Unix timestamp of the slot, in seconds.
    pub t: i64,
    /// Requests completed in the slot.
    pub requests: u64,
    /// Completed responses by status class in the slot.
    pub status: HttpStatusStats,
    /// Median latency in nanoseconds, or `None` when the slot is empty.
    pub p50_ns: Option<u64>,
    /// 95th-percentile latency in nanoseconds, or `None` when the slot is empty.
    pub p95_ns: Option<u64>,
    /// 99th-percentile latency in nanoseconds, or `None` when the slot is empty.
    pub p99_ns: Option<u64>,
    /// 99.9th-percentile latency in nanoseconds, or `None` when the slot is empty.
    pub p999_ns: Option<u64>,
}

/// Response counts by HTTP status class.
#[derive(Clone, Debug, Default, Serialize)]
pub struct HttpStatusStats {
    /// Informational responses, serialized as `"1xx"`.
    #[serde(rename = "1xx")]
    pub status_1xx: u64,
    /// Successful responses, serialized as `"2xx"`.
    #[serde(rename = "2xx")]
    pub status_2xx: u64,
    /// Redirects, serialized as `"3xx"`.
    #[serde(rename = "3xx")]
    pub status_3xx: u64,
    /// Client errors, serialized as `"4xx"`.
    #[serde(rename = "4xx")]
    pub status_4xx: u64,
    /// Server errors, serialized as `"5xx"`.
    #[serde(rename = "5xx")]
    pub status_5xx: u64,
}

/// Error responses as a fraction (0–1) of the requests in a window.
#[derive(Clone, Debug, Default, Serialize)]
pub struct HttpRateStats {
    /// Share of client errors, or `None` when the window is empty.
    ///
    /// Serialized as `"4xx"`.
    #[serde(rename = "4xx")]
    pub status_4xx: Option<f64>,
    /// Share of server errors, or `None` when the window is empty.
    ///
    /// Serialized as `"5xx"`.
    #[serde(rename = "5xx")]
    pub status_5xx: Option<f64>,
}

/// Latency percentiles of a window, capped at 60 seconds.
///
/// Each value is the upper bound of the histogram bucket holding that rank.
#[derive(Clone, Debug, Default, Serialize)]
pub struct LatencyStats {
    /// Median latency in nanoseconds, or `None` when the window is empty.
    pub p50_ns: Option<u64>,
    /// 95th-percentile latency in nanoseconds, or `None` when the window is empty.
    pub p95_ns: Option<u64>,
    /// 99th-percentile latency in nanoseconds, or `None` when the window is empty.
    pub p99_ns: Option<u64>,
    /// 99.9th-percentile latency in nanoseconds, or `None` when the window is empty.
    pub p999_ns: Option<u64>,
}
