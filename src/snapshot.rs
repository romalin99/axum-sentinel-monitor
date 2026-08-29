use chrono::{DateTime, Utc};
use serde::Serialize;

/// JSON snapshot served to the dashboard and API clients.
#[derive(Clone, Debug, Serialize)]
pub struct Snapshot {
    pub collected_at: DateTime<Utc>,
    pub collection: CollectionStats,
    pub process: ProcessStats,
    pub runtime: RuntimeStats,
    pub system: SystemStats,
    pub http: HttpStats,
}

#[derive(Clone, Debug, Serialize)]
pub struct CollectionStats {
    pub partial: bool,
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ProcessStats {
    pub cpu_percent: Option<f64>,
    pub rss_bytes: Option<u64>,
    pub threads: Option<i32>,
    pub open_descriptors: Option<i32>,
    pub uptime_seconds: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct RuntimeStats {
    /// Tokio live tasks, or OS threads when no runtime is available.
    pub goroutines: u64,
    pub heap_alloc_bytes: u64,
    pub heap_sys_bytes: u64,
    pub heap_inuse_bytes: u64,
    pub heap_idle_bytes: u64,
    pub heap_released_bytes: u64,
    /// Tokio worker threads.
    pub workers: i32,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct SystemStats {
    pub cpu_percent: Option<f64>,
    pub memory_used_percent: Option<f64>,
    pub memory_used_bytes: Option<u64>,
    pub memory_total_bytes: Option<u64>,
    pub memory_available_bytes: Option<u64>,
    pub disk_used_percent: Option<f64>,
    pub disk_used_bytes: Option<u64>,
    pub disk_total_bytes: Option<u64>,
    pub disk_free_bytes: Option<u64>,
    pub disk_fstype: Option<String>,
    pub load1: Option<f64>,
    pub load5: Option<f64>,
    pub load15: Option<f64>,
    pub network_receive_bps: Option<f64>,
    pub network_send_bps: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct HttpStats {
    pub requests: u64,
    pub in_flight: u64,
    /// Requests per second over the last 60 seconds of in-process samples.
    pub rps: Option<f64>,
    pub status: HttpStatusStats,
    pub rates: HttpRateStats,
    pub latency: LatencyStats,
    /// Maximum HTTP sample retention, in seconds. Older slots are discarded.
    pub window_seconds: u32,
    pub windows: HttpWindows,
    /// One point per second for the retained window, oldest first.
    pub series: Vec<HttpSecondSample>,
    /// Per-route 30s/60s stats, in-flight first then busiest. Samples older than 60s are dropped.
    pub endpoints: Vec<HttpEndpointStats>,
}

/// Method + path traffic for the same 30s/60s ring as [`HttpStats`].
#[derive(Clone, Debug, Default, Serialize)]
pub struct HttpEndpointStats {
    pub method: String,
    pub path: String,
    /// Requests currently being handled for this route.
    pub in_flight: u64,
    pub windows: HttpWindows,
}

/// 30-second and 60-second views of the same in-process ring.
#[derive(Clone, Debug, Default, Serialize)]
pub struct HttpWindows {
    #[serde(rename = "30")]
    pub secs_30: HttpWindowStats,
    #[serde(rename = "60")]
    pub secs_60: HttpWindowStats,
}

/// Aggregated HTTP traffic for a sliding window that never exceeds 60 seconds.
#[derive(Clone, Debug, Default, Serialize)]
pub struct HttpWindowStats {
    pub seconds: u32,
    pub covered_seconds: u32,
    pub requests: u64,
    pub rps: f64,
    pub status: HttpStatusStats,
    pub rates: HttpRateStats,
    pub latency: LatencyStats,
}

/// Completed requests in a single one-second slot.
#[derive(Clone, Debug, Default, Serialize)]
pub struct HttpSecondSample {
    pub t: i64,
    pub requests: u64,
    pub status: HttpStatusStats,
    pub p50_ns: Option<u64>,
    pub p95_ns: Option<u64>,
    pub p99_ns: Option<u64>,
    pub p999_ns: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct HttpStatusStats {
    #[serde(rename = "1xx")]
    pub status_1xx: u64,
    #[serde(rename = "2xx")]
    pub status_2xx: u64,
    #[serde(rename = "3xx")]
    pub status_3xx: u64,
    #[serde(rename = "4xx")]
    pub status_4xx: u64,
    #[serde(rename = "5xx")]
    pub status_5xx: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct HttpRateStats {
    #[serde(rename = "4xx")]
    pub status_4xx: Option<f64>,
    #[serde(rename = "5xx")]
    pub status_5xx: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct LatencyStats {
    pub p50_ns: Option<u64>,
    pub p95_ns: Option<u64>,
    pub p99_ns: Option<u64>,
    pub p999_ns: Option<u64>,
}
