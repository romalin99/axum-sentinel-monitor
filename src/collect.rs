//! Collection of process, runtime, system, and HTTP metrics into a snapshot.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::Utc;
use sysinfo::{
    CpuRefreshKind, DiskRefreshKind, Disks, Networks, Pid, ProcessRefreshKind, ProcessesToUpdate,
    System,
};

use crate::histogram::{WINDOW_SECS, WindowAgg, window_rates};
use crate::snapshot::{
    CollectionStats, HttpEndpointStats, HttpRateStats, HttpSecondSample, HttpStats,
    HttpStatusStats, HttpWindowStats, HttpWindows, LatencyStats, ProcessStats, RuntimeStats,
    Snapshot, SystemStats,
};
use crate::stats::HttpMetrics;

/// Minimum interval between disk usage samples.
///
/// Reading disk usage costs ~20ms per call (the per-mount `statfs` dominates,
/// not the enumeration — refreshing a resident `Disks` is no cheaper than
/// rebuilding it), and the figure moves far too slowly to be worth that on every
/// dashboard poll.
const DISK_TTL: Duration = Duration::from_secs(30);

/// Minimum interval between open-descriptor counts.
///
/// The listing walks `/proc/self/fd` (or `/dev/fd`), so the cost grows with
/// connection count. The figure is stable enough to share across several
/// dashboard polls.
const FD_TTL: Duration = Duration::from_secs(5);

/// Samples process, runtime, system, and HTTP metrics into a [`Snapshot`].
///
/// CPU and network figures are deltas against the previous collection, so the
/// collector keeps the last sample and reports `None` on the first pass.
pub(crate) struct Collector {
    /// CPU, memory, and process sampler.
    system: System,
    /// Network interface counters.
    networks: Networks,
    /// Disks enumerated at startup.
    disks: Disks,
    /// Canonical working directory, used to pick the disk to report.
    disk_root: Option<PathBuf>,
    /// Disk usage read last, or `None` when the lookup failed.
    disk_cache: Option<DiskUsage>,
    /// Instant `disk_cache` was read, or `None` before the first read.
    disk_at: Option<Instant>,
    /// Open-descriptor count read last, or `None` when the listing failed.
    fd_cache: Option<i32>,
    /// Instant `fd_cache` was read, or `None` before the first read.
    fd_at: Option<Instant>,
    /// Identifier of this process.
    pid: Pid,
    /// Logical CPU count, used to scale process CPU usage to the whole machine.
    num_cpu: usize,
    /// Instant the collector was created; the origin of the reported uptime.
    started: Instant,
    /// Whether a process CPU sample exists to diff against.
    process_cpu_seen: bool,
    /// Whether a system CPU sample exists to diff against.
    system_cpu_seen: bool,
    /// Whether a network sample exists to diff against.
    network_seen: bool,
    /// Total bytes received as of the previous collection.
    network_received: u64,
    /// Total bytes sent as of the previous collection.
    network_sent: u64,
    /// Instant of the previous collection.
    network_at: Instant,
}

impl Collector {
    /// Creates a collector, enumerating CPUs, network interfaces, and disks once.
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
            fd_cache: None,
            fd_at: None,
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

    /// Collects a snapshot, listing every metric that failed in
    /// [`CollectionStats::errors`] instead of failing as a whole.
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

    /// Samples this process, pushing the name of each unavailable metric onto
    /// `errors`.
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

        match self.cached_descriptors() {
            Some(count) => stats.open_descriptors = Some(count),
            None => errors.push("process.descriptors".into()),
        }

        stats
    }

    /// Samples the host, pushing the name of each unavailable metric onto `errors`.
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

    /// Converts the live HTTP counters and windows into their snapshot form.
    fn collect_http(&self, http: &HttpMetrics) -> HttpStats {
        let status = status_from_counts(http.status_counts());
        let traffic = http.latency().snapshot();
        let window_30 = to_window_stats(&traffic.window_30);
        let window_60 = to_window_stats(&traffic.window_60);
        let window_90 = to_window_stats(&traffic.window_90);
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
                secs_90: window_90,
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
                        secs_90: to_window_stats(&endpoint.window_90),
                    },
                })
                .collect(),
        }
    }
}

/// Converts an internal window aggregate into its public snapshot form.
fn to_window_stats(agg: &WindowAgg) -> HttpWindowStats {
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

/// Maps per-class counts, indexed `1xx` to `5xx`, onto named fields.
fn status_from_counts(counts: [u64; 5]) -> HttpStatusStats {
    HttpStatusStats {
        status_1xx: counts[0],
        status_2xx: counts[1],
        status_3xx: counts[2],
        status_4xx: counts[3],
        status_5xx: counts[4],
    }
}

/// Usage of the disk that holds the process working directory.
#[derive(Clone)]
struct DiskUsage {
    /// Used space in percent of the total.
    used_percent: f64,
    /// Used space in bytes.
    used: u64,
    /// Total space in bytes.
    total: u64,
    /// Space in bytes available to the process.
    free: u64,
    /// File-system name, possibly empty.
    fs_type: String,
}

impl Collector {
    /// Returns the cached disk usage, refreshing it at most once per [`DISK_TTL`].
    ///
    /// Mount points and file-system names are those captured at startup; only
    /// storage figures are re-read.
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
        self.disk_cache.clone_from(&usage);
        usage
    }

    /// Returns the cached open-descriptor count, refreshing it at most once per
    /// [`FD_TTL`].
    fn cached_descriptors(&mut self) -> Option<i32> {
        if let Some(at) = self.fd_at
            && at.elapsed() < FD_TTL
        {
            return self.fd_cache;
        }
        let count = open_descriptors();
        self.fd_at = Some(Instant::now());
        self.fd_cache = count;
        count
    }

    /// Reads usage of the longest mount point that contains the working directory.
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

/// Returns `true` when `path` lies on `mount`, comparing whole path components.
fn path_on_mount(path: &Path, mount: &Path) -> bool {
    path.starts_with(mount)
}

/// Samples the Tokio runtime and the allocator.
fn collect_runtime() -> RuntimeStats {
    let (tasks, workers) = tokio_runtime_counts();
    let heap = allocator_stats();
    RuntimeStats {
        goroutines: tasks,
        heap_alloc_bytes: heap.alloc_bytes,
        heap_sys_bytes: heap.sys_bytes,
        heap_inuse_bytes: heap.inuse_bytes,
        heap_idle_bytes: heap.idle_bytes,
        heap_released_bytes: heap.released_bytes,
        workers,
    }
}

/// Heap figures in bytes, shaped after Go's `runtime.MemStats`.
#[derive(Default)]
struct AllocatorStats {
    /// Bytes handed out by the allocator and still in use (Go `HeapAlloc`).
    alloc_bytes: u64,
    /// Page-granular in-use bytes (Go `HeapInuse`); equals `alloc_bytes` where the
    /// allocator exposes no such figure.
    inuse_bytes: u64,
    /// Address space obtained from the OS (Go `HeapSys`).
    sys_bytes: u64,
    /// Free bytes inside the heap (Go `HeapIdle`).
    idle_bytes: u64,
    /// Bytes the kernel no longer keeps resident (Go `HeapReleased`).
    released_bytes: u64,
}

/// Reads heap figures from jemalloc's `stats.*` (feature `jemalloc`).
///
/// The Go `MemStats` mapping is: `HeapAlloc` = allocated, `HeapInuse` = active,
/// `HeapSys` = mapped + retained (address space obtained, including what was
/// handed back), `HeapReleased` = retained + (mapped − resident) (pages the
/// kernel no longer holds), `HeapIdle` = sys − active. `heap_sys − heap_released`
/// therefore equals jemalloc's own `resident`.
///
/// Statistics are cached by jemalloc and refreshed by advancing the epoch first.
/// A failed mallctl read yields zeros (the allocator is not jemalloc, or stats
/// were compiled out).
#[cfg(feature = "jemalloc")]
fn allocator_stats() -> AllocatorStats {
    use tikv_jemalloc_ctl::{epoch, stats};
    let widen = |v: Result<usize, tikv_jemalloc_ctl::Error>| v.map(|n| n as u64);
    if epoch::advance().is_err() {
        return AllocatorStats::default();
    }
    let (Ok(allocated), Ok(active), Ok(resident), Ok(mapped), Ok(retained)) = (
        widen(stats::allocated::read()),
        widen(stats::active::read()),
        widen(stats::resident::read()),
        widen(stats::mapped::read()),
        widen(stats::retained::read()),
    ) else {
        return AllocatorStats::default();
    };
    let sys_bytes = mapped.saturating_add(retained);
    AllocatorStats {
        alloc_bytes: allocated,
        inuse_bytes: active,
        sys_bytes,
        idle_bytes: sys_bytes.saturating_sub(active),
        released_bytes: retained.saturating_add(mapped.saturating_sub(resident)),
    }
}

/// Returns `(live tasks, worker threads)` of the current Tokio runtime, falling
/// back to OS threads and available parallelism outside a runtime.
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
            let tasks = num_threads::num_threads().map_or(0, |threads| threads.get() as u64);
            let workers =
                std::thread::available_parallelism().map_or(1, |value| value.get() as i32);
            (tasks, workers)
        }
    }
}

/// Reads heap figures from glibc's `mallinfo2`.
#[cfg(all(not(feature = "jemalloc"), target_os = "linux", target_env = "gnu"))]
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
    // SAFETY: `mallinfo2` takes no arguments and returns its bookkeeping by
    // value; `Mallinfo2` mirrors the layout of glibc's `struct mallinfo2`.
    let info = unsafe { mallinfo2() };
    let sys_bytes = info.arena.saturating_add(info.hblkhd) as u64;
    let idle_bytes = info.fordblks as u64;
    // glibc keeps `arena` (system_mem) at its high-water mark: `malloc_trim` / `free`
    // give pages back with MADV_DONTNEED but never shrink the bookkeeping, so
    // `heap_sys` alone cannot be reconciled with RSS. The bytes actually handed back
    // to the kernel are the part of the heap that is no longer resident, which the
    // kernel does expose (RssAnon in /proc/self/status). `keepcost` is only the
    // releasable top chunk of the main arena and is not that number.
    let released_bytes = resident_anonymous_bytes().map_or(0, |resident| {
        reconcile_released(sys_bytes, idle_bytes, resident)
    });
    AllocatorStats {
        alloc_bytes: info.uordblks as u64,
        inuse_bytes: info.uordblks as u64,
        sys_bytes,
        idle_bytes,
        released_bytes,
    }
}

/// Returns the bytes of heap address space the kernel no longer keeps resident
/// (Go's `HeapReleased`).
///
/// The value is `heap_sys - resident anonymous memory`, kept inside the
/// `released <= idle <= sys` invariant so `sys - released` never exceeds RSS.
///
/// `resident_anonymous` is `RssAnon`, which also counts thread stacks and
/// non-malloc anonymous mappings, so the result is a lower bound: a few MB of
/// stacks read as "still resident heap". On a trimmed process the error is small
/// next to the pages returned.
#[cfg(any(
    all(not(feature = "jemalloc"), target_os = "linux", target_env = "gnu"),
    test
))]
fn reconcile_released(sys_bytes: u64, idle_bytes: u64, resident_anonymous: u64) -> u64 {
    sys_bytes.saturating_sub(resident_anonymous).min(idle_bytes)
}

/// Returns `RssAnon` from `/proc/self/status`, in bytes.
#[cfg(all(not(feature = "jemalloc"), target_os = "linux", target_env = "gnu"))]
fn resident_anonymous_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    parse_rss_anon_kb(&status).map(|kb| kb.saturating_mul(1024))
}

/// Pulls the `RssAnon:` value (kB) out of a `/proc/<pid>/status` body.
#[cfg(any(
    all(not(feature = "jemalloc"), target_os = "linux", target_env = "gnu"),
    test
))]
fn parse_rss_anon_kb(status: &str) -> Option<u64> {
    status
        .lines()
        .find_map(|line| line.strip_prefix("RssAnon:"))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|value| value.parse().ok())
}

/// Reads heap figures from the default malloc zone.
#[cfg(all(not(feature = "jemalloc"), target_os = "macos"))]
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
    // SAFETY: `malloc_default_zone` takes no arguments, and its result is checked
    // for null before use. `stats` is a live, exclusively borrowed value whose
    // layout mirrors `malloc_statistics_t`, which `malloc_zone_statistics` fills.
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
            inuse_bytes: stats.size_in_use as u64,
            sys_bytes: stats.size_allocated as u64,
            idle_bytes: stats.size_allocated.saturating_sub(stats.size_in_use) as u64,
            released_bytes: 0,
        }
    }
}

/// Returns zeros: this platform's allocator exposes no heap figures.
#[cfg(not(any(
    feature = "jemalloc",
    all(target_os = "linux", target_env = "gnu"),
    target_os = "macos"
)))]
fn allocator_stats() -> AllocatorStats {
    AllocatorStats::default()
}

/// Counts the open file descriptors of this process.
#[cfg(unix)]
fn open_descriptors() -> Option<i32> {
    let path = if cfg!(target_os = "linux") {
        "/proc/self/fd"
    } else {
        "/dev/fd"
    };
    std::fs::read_dir(path).ok().map(iter_count_saturating)
}

/// Counts the open handles of this process.
#[cfg(windows)]
fn open_descriptors() -> Option<i32> {
    use std::os::raw::{c_int, c_void};
    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut c_void;
        fn GetProcessHandleCount(process: *mut c_void, count: *mut u32) -> c_int;
    }
    let mut count = 0u32;
    // SAFETY: `GetCurrentProcess` returns a pseudo-handle that is always valid,
    // and `count` is a live `u32` the call writes through.
    let ok = unsafe { GetProcessHandleCount(GetCurrentProcess(), &mut count) };
    if ok != 0 { Some(count as i32) } else { None }
}

/// Returns `None`: this platform has no descriptor listing.
#[cfg(not(any(unix, windows)))]
fn open_descriptors() -> Option<i32> {
    None
}

/// Counts the items of `iter`, saturating at `i32::MAX`.
#[cfg(unix)]
fn iter_count_saturating<T>(iter: impl Iterator<Item = T>) -> i32 {
    let count = iter.count();
    i32::try_from(count).unwrap_or(i32::MAX)
}

/// Returns `(receive, send)` rates in bytes per second, or `None` for both when
/// no time has elapsed or a counter went backwards.
fn network_rates(
    previous_received: u64,
    previous_sent: u64,
    current_received: u64,
    current_sent: u64,
    elapsed: Duration,
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

/// Clamps `value` to the 0–100 percent range.
fn clamp_percent(value: f64) -> f64 {
    value.clamp(0.0, 100.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn released_is_sys_minus_resident_within_invariants() {
        // 1,100 MB obtained, 800 MB free chunks, 400 MB still resident => 700 MB returned.
        assert_eq!(reconcile_released(1100, 800, 400), 700);
        // Nothing trimmed yet: resident covers the whole heap.
        assert_eq!(reconcile_released(1100, 150, 1100), 0);
        // RssAnon above heap_sys (large non-malloc mmaps): never negative.
        assert_eq!(reconcile_released(500, 100, 900), 0);
        // Released can never exceed the free chunks that could have been given back.
        assert_eq!(reconcile_released(1100, 100, 200), 100);
    }

    #[test]
    fn parses_rss_anon_from_proc_status() {
        let body =
            "Name:\tuss-se\nVmRSS:\t  998456 kB\nRssAnon:\t  951000 kB\nRssFile:\t   47456 kB\n";
        assert_eq!(parse_rss_anon_kb(body), Some(951_000));
        assert_eq!(parse_rss_anon_kb("VmRSS:\t 10 kB\n"), None);
    }

    #[test]
    fn mount_matching_uses_path_components() {
        assert!(path_on_mount(Path::new("/srv/app"), Path::new("/")));
        assert!(path_on_mount(Path::new("/srv/app"), Path::new("/srv")));
        assert!(!path_on_mount(
            Path::new("/srv/application"),
            Path::new("/srv/app")
        ));
        assert!(path_on_mount(Path::new("/srv/app"), Path::new("")));
    }
}
