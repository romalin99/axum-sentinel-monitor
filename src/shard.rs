//! Per-thread indices that select thread-private counters, stages, and gauges.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};

/// Number of thread-private stages per sliding window and of gauge lines per
/// route.
///
/// A thread whose index is below this bound owns one stage in every window and
/// one gauge line in every route, so its recording never touches a cache line
/// another core writes. Threads beyond it record straight into the shared ring,
/// which is exact but contended.
pub(crate) const MAX_STAGES: usize = 64;

/// Number of shards the lifetime counters are split into.
///
/// Sharing a counter shard between threads is harmless (every update is an
/// atomic add), so these are folded modulo a small constant.
pub(crate) const COUNTER_SHARDS: usize = 16;

/// Next index handed out when no released index is available.
static NEXT_INDEX: AtomicUsize = AtomicUsize::new(0);

/// Indices released by threads that exited, reused lowest first.
///
/// Reuse keeps the index space compact, so a process whose threads come and go
/// (blocking-pool threads, tests) keeps its live threads below [`MAX_STAGES`].
static RELEASED: Mutex<Vec<usize>> = Mutex::new(Vec::new());

/// Index owned by one thread for its lifetime, returned to [`RELEASED`] on exit.
struct ThreadIndex(usize);

impl ThreadIndex {
    /// Takes the lowest released index, or a fresh one.
    fn acquire() -> Self {
        let mut released = RELEASED.lock().unwrap_or_else(PoisonError::into_inner);
        // The vector is kept sorted descending, so the lowest index is last.
        match released.pop() {
            Some(index) => Self(index),
            None => Self(NEXT_INDEX.fetch_add(1, Ordering::Relaxed)),
        }
    }
}

impl Drop for ThreadIndex {
    fn drop(&mut self) {
        let mut released = RELEASED.lock().unwrap_or_else(PoisonError::into_inner);
        let position = released.partition_point(|&index| index > self.0);
        released.insert(position, self.0);
    }
}

thread_local! {
    /// Index of the current thread.
    static INDEX: ThreadIndex = ThreadIndex::acquire();
}

/// Returns the index of the current thread, or `None` while the thread's local
/// storage is being torn down.
///
/// The index is stable for the thread's lifetime and unique among live threads.
pub(crate) fn thread_index() -> Option<usize> {
    INDEX.try_with(|index| index.0).ok()
}

/// Returns the lifetime-counter shard of a thread with `index`.
pub(crate) fn counter_shard(index: Option<usize>) -> usize {
    index.map_or(0, |index| index % COUNTER_SHARDS)
}

/// Returns the gauge line of a thread with `index`.
///
/// Gauges are atomic adds, so threads beyond [`MAX_STAGES`] share lines without
/// harm.
pub(crate) fn gauge_line(index: Option<usize>) -> usize {
    index.map_or(0, |index| index % MAX_STAGES)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_is_stable_per_thread_and_released_on_exit() {
        let own = thread_index().expect("index on a live thread");
        assert_eq!(thread_index(), Some(own));

        let other = std::thread::spawn(thread_index).join().unwrap().unwrap();
        assert_ne!(other, own);
        // The other thread has exited, so its index is free again unless a
        // thread of another test took it in the meantime; either way it is no
        // longer counted as handed out.
        let released = RELEASED.lock().unwrap_or_else(PoisonError::into_inner);
        let next = NEXT_INDEX.load(Ordering::Relaxed);
        assert!(other < next);
        assert!(released.iter().all(|&index| index < next));
        assert!(released.windows(2).all(|pair| pair[0] > pair[1]));
    }

    #[test]
    fn live_threads_hold_distinct_indices() {
        const THREADS: usize = 8;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(THREADS));
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let index = thread_index().unwrap();
                    barrier.wait();
                    index
                })
            })
            .collect();
        let mut indices: Vec<usize> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        indices.sort_unstable();
        indices.dedup();
        assert_eq!(indices.len(), THREADS);
    }
}
