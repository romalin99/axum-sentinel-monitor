//! Per-thread indices that select thread-private counters, stages, and stamp
//! lines.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};

/// Number of thread-private stages per sliding window and of stamp lines per
/// route.
///
/// A thread whose index is below this bound owns one stage in every window and
/// one stamp line in every route, so the counters it updates there sit on cache
/// lines no other thread writes. Threads beyond it record straight into the
/// shared ring, which is exact but contended, and share the stamp lines.
pub(crate) const MAX_STAGES: usize = 64;

/// Number of shards the lifetime counters are split into, one per thread index.
pub(crate) const COUNTER_SHARDS: usize = MAX_STAGES;

/// Pool of thread indices: fresh indices are handed out in order and released
/// ones are reused lowest first.
///
/// Reuse keeps the index space compact, so a process whose threads come and go
/// (blocking-pool threads, tests) keeps its live indices below [`MAX_STAGES`] as
/// long as no more than that many recording threads are alive at once.
struct IndexPool {
    /// Next index handed out when no released index is available.
    next: AtomicUsize,
    /// Indices released by threads that exited, sorted descending so that the
    /// lowest is last.
    released: Mutex<Vec<usize>>,
}

impl IndexPool {
    /// Creates a pool that has handed out nothing.
    const fn new() -> Self {
        Self {
            next: AtomicUsize::new(0),
            released: Mutex::new(Vec::new()),
        }
    }

    /// Takes the lowest released index, or a fresh one.
    fn acquire(&self) -> usize {
        let mut released = self.released.lock().unwrap_or_else(PoisonError::into_inner);
        released
            .pop()
            .unwrap_or_else(|| self.next.fetch_add(1, Ordering::Relaxed))
    }

    /// Returns `index` to the pool.
    ///
    /// The lock also orders the releasing thread's last writes to its stages
    /// before the next owner's first access to them.
    fn release(&self, index: usize) {
        let mut released = self.released.lock().unwrap_or_else(PoisonError::into_inner);
        let position = released.partition_point(|&other| other > index);
        released.insert(position, index);
    }

    /// Returns how many fresh indices were handed out, which is the number of
    /// indices that have ever been in use.
    fn handed_out(&self) -> usize {
        self.next.load(Ordering::Relaxed)
    }
}

/// Pool the threads of this process draw their indices from.
static POOL: IndexPool = IndexPool::new();

/// Index owned by one thread for its lifetime, returned to [`POOL`] on exit.
struct ThreadIndex {
    /// Index of the owning thread.
    index: usize,
}

impl Drop for ThreadIndex {
    /// Returns the index to [`POOL`] when the thread's local storage is torn
    /// down.
    fn drop(&mut self) {
        POOL.release(self.index);
    }
}

thread_local! {
    /// Index of the current thread.
    static INDEX: ThreadIndex = ThreadIndex {
        index: POOL.acquire(),
    };
}

/// Returns the index of the current thread, or `None` while the thread's local
/// storage is being torn down.
///
/// The index is stable for the thread's lifetime and unique among live threads.
pub(crate) fn thread_index() -> Option<usize> {
    INDEX.try_with(|index| index.index).ok()
}

/// Returns the number of thread indices handed out so far, capped at
/// [`MAX_STAGES`].
///
/// Indices are reused, so this is the most threads that ever recorded at the
/// same time; the stages and stamp lines at or above it have never been written
/// and need not be read.
pub(crate) fn live_indices() -> usize {
    POOL.handed_out().min(MAX_STAGES)
}

/// Returns the lifetime-counter shard of a thread with `index`.
pub(crate) fn counter_shard(index: Option<usize>) -> usize {
    index.map_or(0, |index| index % COUNTER_SHARDS)
}

/// Returns the stamp line of a thread with `index`.
///
/// Stamps are plain stores read back as maxima, so threads beyond
/// [`MAX_STAGES`] share lines; a shared line can then lag by one writer's stamp,
/// which only affects the order of eviction.
pub(crate) fn gauge_line(index: Option<usize>) -> usize {
    index.map_or(0, |index| index % MAX_STAGES)
}

/// Makes sure at least `count` indices have been handed out, so that tests
/// which address stages and stamp lines by an explicit index find them read.
#[cfg(test)]
pub(crate) fn warm_indices(count: usize) {
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(count));
    let threads: Vec<_> = (0..count)
        .map(|_| {
            let barrier = std::sync::Arc::clone(&barrier);
            std::thread::spawn(move || {
                let index = thread_index();
                barrier.wait();
                index
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert!(live_indices() >= count.min(MAX_STAGES));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_is_stable_per_thread_and_distinct_from_other_live_threads() {
        let own = thread_index().expect("index on a live thread");
        assert_eq!(thread_index(), Some(own));
        let other = std::thread::spawn(thread_index).join().unwrap().unwrap();
        assert_ne!(other, own);
        assert!(live_indices() > own.min(MAX_STAGES - 1));
    }

    #[test]
    fn pool_hands_out_fresh_indices_and_reuses_released_ones_lowest_first() {
        let pool = IndexPool::new();
        assert_eq!(pool.acquire(), 0);
        assert_eq!(pool.acquire(), 1);
        assert_eq!(pool.acquire(), 2);
        assert_eq!(pool.handed_out(), 3);
        pool.release(1);
        pool.release(0);
        assert_eq!(pool.acquire(), 0);
        assert_eq!(pool.acquire(), 1);
        assert_eq!(pool.acquire(), 3);
        assert_eq!(pool.handed_out(), 4);
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
