use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::Utc;
use sysinfo::{
    CpuRefreshKind, DiskRefreshKind, Disks, Networks, Pid, ProcessRefreshKind, ProcessesToUpdate,
    System,
};

use crate::histogram::{WINDOW_SECS, window_rates};
use crate::snapshot::{
    CollectionStats, HttpEndpointStats, HttpRateStats, HttpSecondSample, HttpStats,
    HttpStatusStats, HttpWindowStats, HttpWindows, LatencyStats, ProcessStats, RuntimeStats,
    Snapshot, SystemStats,
};
use crate::stats::HttpMetrics;

/// Disk usage is sampled at most this often. Reading it costs ~20ms per call
/// (the per-mount `statfs` dominates, not the enumeration — refreshing a
/// resident `Disks` is no cheaper than rebuilding it), and the figure moves far
/// too slowly to be worth that on every dashboard poll.
const DISK_TTL: Duration = Duration::from_secs(30);

pub(crate) struct Collector {
    system: System,
    networks: Networks,
    disks: Disks,
    disk_root: Option<PathBuf>,
    disk_cache: Option<DiskUsage>,
    disk_at: Option<Instant>,
    pid: Pid,
    num_cpu: usize,
    started: Instant,
    process_cpu_seen: bool,
    system_cpu_seen: bool,
    network_seen: bool,
    network_received: u64,
    network_sent: u64,
    network_at: Instant,
}

impl Collector {
    pub(crate) fn new() -> Self {
        let mut system = System::new();
        system.refresh_cpu_list(CpuRefreshKind::nothing().with_cpu_usage());
        let networks = Networks::new_with_refreshed_list();
        // Mount points and file-system names are enumerated once: re-listing disks
        // costs tens of milliseconds and would run on the caller's thread.
        let disks = Disks::new_with_refreshed_list_specifics(
            DiskRefreshKind::nothing().with_kind().with_storage(),
        );
        let disk_root = std::env::current_dir()
            .ok()
            .map(|cwd| cwd.canonicalize().unwrap_or(cwd));
        let now = Instant::now();
        Self {
            system,
            networks,
            disks,
            disk_root,
            disk_cache: None,
            disk_at: None,
            pid: Pid::from_u32(std::process::id()),
            num_cpu: 1,
            started: now,
            process_cpu_seen: false,
            system_cpu_seen: false,
            network_seen: false,
            network_received: 0,
            network_sent: 0,
            network_at: now,
        }
    }

    pub(crate) fn collect(&mut self, http: &HttpMetrics) -> Snapshot {
        let now = Instant::now();
        let mut errors = Vec::new();
        let process = self.collect_process(&mut errors);
        let system = self.collect_system(now, &mut errors);
        Snapshot {
            collected_at: Utc::now(),
            collection: CollectionStats {
                partial: !errors.is_empty(),
                errors,
            },
            process,
            runtime: collect_runtime(),
            system,
            http: self.collect_http(http),
        }
    }

    fn collect_process(&mut self, errors: &mut Vec<String>) -> ProcessStats {
        let mut stats = ProcessStats {
            uptime_seconds: self.started.elapsed().as_secs(),
            ..ProcessStats::default()
        };

        self.system.refresh_cpu_usage();
        self.system.refresh_memory();
        self.system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[self.pid]),
            true,
            ProcessRefreshKind::nothing().with_cpu().with_memory(),
        );
        self.num_cpu = self.system.cpus().len().max(1);

        let Some(process) = self.system.process(self.pid) else {
            errors.extend([
                "process.cpu".into(),
                "process.memory".into(),
                "process.threads".into(),
                "process.descriptors".into(),
            ]);
            return stats;
        };

        if self.process_cpu_seen {
            stats.cpu_percent = Some(clamp_percent(
                f64::from(process.cpu_usage()) / self.num_cpu as f64,
            ));
        } else {
            self.process_cpu_seen = true;
        }
        stats.rss_bytes = Some(process.memory());

        match num_threads::num_threads() {
            Some(threads) => stats.threads = Some(threads.get() as i32),
            None => errors.push("process.threads".into()),
        }

        match open_descriptors() {
            Some(count) => stats.open_descriptors = Some(count),
            None => errors.push("process.descriptors".into()),
        }

        stats
    }

    fn collect_system(&mut self, now: Instant, errors: &mut Vec<String>) -> SystemStats {
        let mut stats = SystemStats::default();

        if self.system_cpu_seen {
            stats.cpu_percent = Some(clamp_percent(f64::from(self.system.global_cpu_usage())));
        } else {
            self.system_cpu_seen = true;
        }

        let total = self.system.total_memory();
        let used = self.system.used_memory();
        let available = self.system.available_memory();
        if total > 0 {
            stats.memory_used_percent = Some((used as f64 / total as f64) * 100.0);
            stats.memory_used_bytes = Some(used);
            stats.memory_total_bytes = Some(total);
            stats.memory_available_bytes = Some(available);
        } else {
            errors.push("system.memory".into());
        }

        match self.application_disk() {
            Some(disk) => {
                stats.disk_used_percent = Some(disk.used_percent);
                stats.disk_used_bytes = Some(disk.used);
                stats.disk_total_bytes = Some(disk.total);
                stats.disk_free_bytes = Some(disk.free);
                if !disk.fs_type.is_empty() {
                    stats.disk_fstype = Some(disk.fs_type);
                }
            }
            None => errors.push("system.disk".into()),
        }

        if !cfg!(windows) {
            let load = System::load_average();
            stats.load1 = Some(load.one);
            stats.load5 = Some(load.five);
            stats.load15 = Some(load.fifteen);
        }

        self.networks.refresh(true);
        let received: u64 = self
            .networks
            .values()
            .map(|data| data.total_received())
            .sum();
        let sent: u64 = self
            .networks
            .values()
            .map(|data| data.total_transmitted())
            .sum();
        if self.network_seen {
            let (rx, tx) = network_rates(
                self.network_received,
                self.network_sent,
                received,
                sent,
                now.saturating_duration_since(self.network_at),
            );
            stats.network_receive_bps = rx;
            stats.network_send_bps = tx;
        } else {
            self.network_seen = true;
        }
        if self.networks.is_empty() {
            errors.push("system.network".into());
        }
        self.network_received = received;
        self.network_sent = sent;
        self.network_at = now;
        stats
    }

    fn collect_http(&self, http: &HttpMetrics) -> HttpStats {
        let status = HttpStatusStats {
            status_1xx: http.status1(),
            status_2xx: http.status2(),
            status_3xx: http.status3(),
            status_4xx: http.status4(),
            status_5xx: http.status5(),
        };
        let traffic = http.latency().snapshot();
        let window_30 = to_window_stats(&traffic.window_30);
        let window_60 = to_window_stats(&traffic.window_60);
        HttpStats {
            requests: http.requests(),
            in_flight: http.in_flight(),
            rps: Some(window_60.rps),
            status,
            rates: window_60.rates.clone(),
            latency: window_60.latency.clone(),
            window_seconds: WINDOW_SECS as u32,
            windows: HttpWindows {
                secs_30: window_30,
                secs_60: window_60,
            },
            series: traffic
                .series
                .into_iter()
                .map(|sample| HttpSecondSample {
                    t: sample.unix_secs,
                    requests: sample.requests,
                    status: status_from_counts(sample.status),
                    p50_ns: sample.p50_ns,
                    p95_ns: sample.p95_ns,
                    p99_ns: sample.p99_ns,
                    p999_ns: sample.p999_ns,
                })
                .collect(),
            endpoints: http
                .endpoints()
                .snapshot()
                .into_iter()
                .map(|endpoint| HttpEndpointStats {
                    method: endpoint.method,
                    path: endpoint.path,
                    in_flight: endpoint.in_flight,
                    windows: HttpWindows {
                        secs_30: to_window_stats(&endpoint.window_30),
                        secs_60: to_window_stats(&endpoint.window_60),
                    },
                })
                .collect(),
        }
    }
}

fn to_window_stats(agg: &crate::histogram::WindowAgg) -> HttpWindowStats {
    let (rate_4xx, rate_5xx) = window_rates(&agg.status, agg.requests);
    HttpWindowStats {
        seconds: agg.seconds,
        covered_seconds: agg.covered_seconds,
        requests: agg.requests,
        rps: agg.rps,
        status: status_from_counts(agg.status),
        rates: HttpRateStats {
            status_4xx: rate_4xx,
            status_5xx: rate_5xx,
        },
        latency: LatencyStats {
            p50_ns: agg.p50_ns,
            p95_ns: agg.p95_ns,
            p99_ns: agg.p99_ns,
            p999_ns: agg.p999_ns,
        },
    }
}

fn status_from_counts(counts: [u64; 5]) -> HttpStatusStats {
    HttpStatusStats {
        status_1xx: counts[0],
        status_2xx: counts[1],
        status_3xx: counts[2],
        status_4xx: counts[3],
        status_5xx: counts[4],
    }
}

#[derive(Clone)]
struct DiskUsage {
    used_percent: f64,
    used: u64,
    total: u64,
    free: u64,
    fs_type: String,
}

impl Collector {
    /// Returns the cached disk usage, refreshing it at most once per
    /// [`DISK_TTL`]. Mount points and file-system names are those captured at
    /// startup; only storage figures are re-read.
    fn application_disk(&mut self) -> Option<DiskUsage> {
        if let Some(at) = self.disk_at
            && at.elapsed() < DISK_TTL
        {
            return self.disk_cache.clone();
        }
        self.disks
            .refresh_specifics(false, DiskRefreshKind::nothing().with_storage());
        let usage = self.read_disk();
        // Stamped even when the lookup fails, so a missing mount does not turn
        // into a ~20ms probe on every collect.
        self.disk_at = Some(Instant::now());
        self.disk_cache = usage.clone();
        usage
    }

    fn read_disk(&self) -> Option<DiskUsage> {
        let root = self.disk_root.as_deref()?;
        let disk = self
            .disks
            .list()
            .iter()
            .filter(|disk| path_on_mount(root, disk.mount_point()))
            .max_by_key(|disk| disk.mount_point().as_os_str().len())?;
        let total = disk.total_space();
        if total == 0 {
            return None;
        }
        let free = disk.available_space();
        let used = total.saturating_sub(free);
        Some(DiskUsage {
            used_percent: used as f64 / total as f64 * 100.0,
            used,
            total,
            free,
            fs_type: disk.file_system().to_string_lossy().into_owned(),
        })
    }
}

fn path_on_mount(path: &Path, mount: &Path) -> bool {
    let mount = if mount.as_os_str().is_empty() {
        PathBuf::from(mount)
    } else {
        mount.to_path_buf()
    };
    path.starts_with(&mount)
}

fn collect_runtime() -> RuntimeStats {
    let (tasks, workers) = tokio_runtime_counts();
    let heap = allocator_stats();
    RuntimeStats {
        goroutines: tasks,
        heap_alloc_bytes: heap.alloc_bytes,
        heap_sys_bytes: heap.sys_bytes,
        heap_inuse_bytes: heap.alloc_bytes,
        heap_idle_bytes: heap.idle_bytes,
        heap_released_bytes: heap.released_bytes,
        workers,
    }
}

#[derive(Default)]
struct AllocatorStats {
    alloc_bytes: u64,
    sys_bytes: u64,
    idle_bytes: u64,
    released_bytes: u64,
}

fn tokio_runtime_counts() -> (u64, i32) {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            let metrics = handle.metrics();
            (
                metrics.num_alive_tasks() as u64,
                i32::try_from(metrics.num_workers()).unwrap_or(i32::MAX),
            )
        }
        Err(_) => {
            let tasks = num_threads::num_threads()
                .map(|threads| threads.get() as u64)
                .unwrap_or(0);
            let workers = std::thread::available_parallelism()
                .map(|value| value.get() as i32)
                .unwrap_or(1);
            (tasks, workers)
        }
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn allocator_stats() -> AllocatorStats {
    #[repr(C)]
    struct Mallinfo2 {
        arena: usize,
        ordblks: usize,
        smblks: usize,
        hblks: usize,
        hblkhd: usize,
        usmblks: usize,
        fsmblks: usize,
        uordblks: usize,
        fordblks: usize,
        keepcost: usize,
    }
    unsafe extern "C" {
        fn mallinfo2() -> Mallinfo2;
    }
    let info = unsafe { mallinfo2() };
    AllocatorStats {
        alloc_bytes: info.uordblks as u64,
        sys_bytes: info.arena.saturating_add(info.hblkhd) as u64,
        idle_bytes: info.fordblks as u64,
        released_bytes: info.keepcost as u64,
    }
}

#[cfg(target_os = "macos")]
fn allocator_stats() -> AllocatorStats {
    #[repr(C)]
    struct MallocStatistics {
        blocks_in_use: u32,
        size_in_use: usize,
        max_size_in_use: usize,
        size_allocated: usize,
    }
    enum MallocZone {}
    unsafe extern "C" {
        fn malloc_default_zone() -> *mut MallocZone;
        fn malloc_zone_statistics(zone: *mut MallocZone, stats: *mut MallocStatistics);
    }
    unsafe {
        let zone = malloc_default_zone();
        if zone.is_null() {
            return AllocatorStats::default();
        }
        let mut stats = MallocStatistics {
            blocks_in_use: 0,
            size_in_use: 0,
            max_size_in_use: 0,
            size_allocated: 0,
        };
        malloc_zone_statistics(zone, &mut stats);
        AllocatorStats {
            alloc_bytes: stats.size_in_use as u64,
            sys_bytes: stats.size_allocated as u64,
            idle_bytes: stats.size_allocated.saturating_sub(stats.size_in_use) as u64,
            released_bytes: 0,
        }
    }
}

#[cfg(not(any(all(target_os = "linux", target_env = "gnu"), target_os = "macos")))]
fn allocator_stats() -> AllocatorStats {
    AllocatorStats::default()
}

#[cfg(unix)]
fn open_descriptors() -> Option<i32> {
    let path = if cfg!(target_os = "linux") {
        "/proc/self/fd"
    } else {
        "/dev/fd"
    };
    std::fs::read_dir(path).ok().map(iter_count_saturating)
}

#[cfg(windows)]
fn open_descriptors() -> Option<i32> {
    use std::os::raw::{c_int, c_void};
    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut c_void;
        fn GetProcessHandleCount(process: *mut c_void, count: *mut u32) -> c_int;
    }
    let mut count = 0u32;
    let ok = unsafe { GetProcessHandleCount(GetCurrentProcess(), &mut count) };
    if ok != 0 { Some(count as i32) } else { None }
}

#[cfg(not(any(unix, windows)))]
fn open_descriptors() -> Option<i32> {
    None
}

#[cfg(unix)]
fn iter_count_saturating<T>(iter: impl Iterator<Item = T>) -> i32 {
    let count = iter.count();
    i32::try_from(count).unwrap_or(i32::MAX)
}

fn network_rates(
    previous_received: u64,
    previous_sent: u64,
    current_received: u64,
    current_sent: u64,
    elapsed: std::time::Duration,
) -> (Option<f64>, Option<f64>) {
    if elapsed.is_zero() || current_received < previous_received || current_sent < previous_sent {
        return (None, None);
    }
    let seconds = elapsed.as_secs_f64();
    (
        Some((current_received - previous_received) as f64 / seconds),
        Some((current_sent - previous_sent) as f64 / seconds),
    )
}

fn clamp_percent(value: f64) -> f64 {
    value.clamp(0.0, 100.0)
}
