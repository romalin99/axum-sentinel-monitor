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
    pub rps: Option<f64>,
    pub status: HttpStatusStats,
    pub rates: HttpRateStats,
    pub latency: LatencyStats,
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
}

impl HttpStatusStats {
    pub(crate) fn completed(&self) -> u64 {
        self.status_1xx + self.status_2xx + self.status_3xx + self.status_4xx + self.status_5xx
    }
}
