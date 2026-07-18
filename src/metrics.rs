use std::{
    sync::{
        Arc, Condvar, Mutex, RwLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use netstat2::{AddressFamilyFlags, ProtocolFlags, ProtocolSocketInfo, iterate_sockets_info};
use serde::Serialize;
use sysinfo::{Pid, ProcessesToUpdate, System};

#[derive(Clone, Debug, Default, Serialize)]
pub struct ProcessMetrics {
    pub cpu: f32,
    pub ram: u64,
    pub conns: u64,
    pub goroutines: u64,
    pub requests: String,
    pub uptime: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct OsMetrics {
    pub cpu: f32,
    pub ram: u64,
    pub total_ram: u64,
    pub load_avg: f64,
    pub conns: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Snapshot {
    pub pid: ProcessMetrics,
    pub os: OsMetrics,
}

/// A partial sample. Unavailable values retain their last successful value.
#[derive(Clone, Debug, Default)]
pub struct MetricSample {
    pub process_cpu: Option<f32>,
    pub process_ram: Option<u64>,
    pub process_connections: Option<u64>,
    pub process_threads: Option<u64>,
    pub process_uptime: Option<u64>,
    pub system_cpu: Option<f32>,
    pub system_ram: Option<u64>,
    pub system_total_ram: Option<u64>,
    pub system_load_average: Option<f64>,
    pub system_connections: Option<u64>,
}

/// Source used by a monitor's background sampler.
pub trait MetricsCollector: Send + 'static {
    fn collect(&mut self) -> MetricSample;
}

#[derive(Debug)]
pub struct SystemCollector {
    system: System,
    pid: Pid,
}

impl Default for SystemCollector {
    fn default() -> Self {
        Self {
            system: System::new(),
            pid: Pid::from_u32(std::process::id()),
        }
    }
}

impl MetricsCollector for SystemCollector {
    fn collect(&mut self) -> MetricSample {
        self.system.refresh_cpu_usage();
        self.system.refresh_memory();
        self.system
            .refresh_processes(ProcessesToUpdate::Some(&[self.pid]), true);

        let process = self.system.process(self.pid);
        let (system_connections, process_connections) = tcp_connection_counts(self.pid);
        let logical_cpus = self.system.cpus().len().max(1) as f32;
        MetricSample {
            process_cpu: process.map(|process| process.cpu_usage() / logical_cpus),
            process_ram: process.map(|process| process.memory()),
            process_connections,
            process_threads: process
                .and_then(|process| process.tasks())
                .map(|tasks| tasks.len() as u64),
            process_uptime: process.map(|process| process.run_time()),
            system_cpu: Some(self.system.global_cpu_usage()),
            system_ram: Some(self.system.used_memory()),
            system_total_ram: Some(self.system.total_memory()),
            system_load_average: Some(System::load_average().one),
            system_connections,
        }
    }
}

fn tcp_connection_counts(pid: Pid) -> (Option<u64>, Option<u64>) {
    let sockets = match iterate_sockets_info(
        AddressFamilyFlags::IPV4 | AddressFamilyFlags::IPV6,
        ProtocolFlags::TCP,
    ) {
        Ok(sockets) => sockets,
        Err(_) => return (None, None),
    };
    let mut system_count = 0_u64;
    let mut process_count = 0_u64;
    for socket in sockets.flatten() {
        if matches!(socket.protocol_socket_info, ProtocolSocketInfo::Tcp(_)) {
            system_count = system_count.saturating_add(1);
            if socket.associated_pids.contains(&pid.as_u32()) {
                process_count = process_count.saturating_add(1);
            }
        }
    }
    (Some(system_count), Some(process_count))
}

pub(crate) struct MetricsState {
    snapshot: RwLock<Snapshot>,
    requests: AtomicU64,
    started: Instant,
}

impl MetricsState {
    fn new() -> Self {
        Self {
            snapshot: RwLock::new(Snapshot::default()),
            requests: AtomicU64::new(0),
            started: Instant::now(),
        }
    }

    pub(crate) fn increment_requests(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        let mut snapshot = self
            .snapshot
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        snapshot.pid.requests = self.requests.load(Ordering::Relaxed).to_string();
        if snapshot.pid.uptime == 0 {
            snapshot.pid.uptime = self.started.elapsed().as_secs();
        }
        snapshot
    }

    fn apply(&self, sample: MetricSample) {
        let mut snapshot = self
            .snapshot
            .write()
            .unwrap_or_else(|error| error.into_inner());
        assign(&mut snapshot.pid.cpu, sample.process_cpu);
        assign(&mut snapshot.pid.ram, sample.process_ram);
        assign(&mut snapshot.pid.conns, sample.process_connections);
        assign(&mut snapshot.pid.goroutines, sample.process_threads);
        assign(&mut snapshot.pid.uptime, sample.process_uptime);
        assign(&mut snapshot.os.cpu, sample.system_cpu);
        assign(&mut snapshot.os.ram, sample.system_ram);
        assign(&mut snapshot.os.total_ram, sample.system_total_ram);
        assign(&mut snapshot.os.load_avg, sample.system_load_average);
        assign(&mut snapshot.os.conns, sample.system_connections);
    }
}

fn assign<T>(target: &mut T, value: Option<T>) {
    if let Some(value) = value {
        *target = value;
    }
}

pub(crate) struct Sampler {
    pub(crate) state: Arc<MetricsState>,
    shutdown: Arc<(Mutex<bool>, Condvar)>,
}

impl Sampler {
    pub(crate) fn start(refresh: Duration, mut collector: Box<dyn MetricsCollector>) -> Arc<Self> {
        let state = Arc::new(MetricsState::new());
        state.apply(collector.collect());
        let sampler = Arc::new(Self {
            state,
            shutdown: Arc::new((Mutex::new(false), Condvar::new())),
        });
        let weak = Arc::downgrade(&sampler);
        thread::Builder::new()
            .name("axum-sentinel-monitor".to_owned())
            .spawn(move || sampling_loop(weak, refresh, &mut *collector))
            .expect("failed to start monitor sampler");
        sampler
    }

    pub(crate) fn close(&self) {
        let (lock, condvar) = &*self.shutdown;
        *lock.lock().unwrap_or_else(|error| error.into_inner()) = true;
        condvar.notify_all();
    }
}

fn sampling_loop(sampler: Weak<Sampler>, refresh: Duration, collector: &mut dyn MetricsCollector) {
    loop {
        let Some(current) = sampler.upgrade() else {
            break;
        };
        let shutdown = Arc::clone(&current.shutdown);
        drop(current);
        let (lock, condvar) = &*shutdown;
        let stopped = lock.lock().unwrap_or_else(|error| error.into_inner());
        let (stopped, _) = condvar
            .wait_timeout_while(stopped, refresh, |stopped| !*stopped)
            .unwrap_or_else(|error| error.into_inner());
        if *stopped {
            break;
        }
        drop(stopped);
        let Some(current) = sampler.upgrade() else {
            break;
        };
        current.state.apply(collector.collect());
    }
}

impl Drop for Sampler {
    fn drop(&mut self) {
        self.close();
    }
}
