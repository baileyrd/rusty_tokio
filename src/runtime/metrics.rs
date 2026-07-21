//! Read-only introspection into a running runtime's live state -- how
//! many tasks are queued, how busy each worker is, how many times a
//! worker has stolen work, how many blocking-pool threads are alive.
//! Exposed as [`super::Handle::metrics`], mirroring the shape of
//! tokio's own `RuntimeMetrics` (`num_workers`/`num_alive_tasks`/
//! `global_queue_depth`/per-worker `worker_local_queue_depth`/
//! `worker_steal_count`/`worker_park_count`), so `benches/scheduler.rs`
//! and `benches/timers.rs` (added for issues #8/#13) no longer have to
//! infer any of this indirectly through wall-clock timing of the public
//! API alone.
//!
//! Every accessor here is a plain atomic load (or an existing queue's
//! `Mutex::lock`, already taken on every schedule/steal regardless of
//! whether metrics are ever read), so calling any of these costs
//! nothing extra. The actual cost this issue adds on the hot scheduling
//! path itself is a handful of new relaxed `AtomicU64::fetch_add` calls
//! at each steal/park site, alongside the counter that already existed
//! (`active_tasks`, added for issue #12's graceful shutdown). Unlike
//! tokio, none of this is gated behind an `unstable` feature flag --
//! a few relaxed atomic increments on paths that already take a mutex
//! lock isn't the kind of hot-path cost that justifies withholding it
//! by default.

use super::Shared;
use std::sync::Arc;

/// A live, snapshot-on-read view into a [`crate::Runtime`]'s scheduler
/// and blocking pool. Returned by [`super::Handle::metrics`]. Cheap to
/// clone and to hold onto -- it's just a cloned `Arc`, and every method
/// re-reads the current value rather than freezing one at construction
/// time.
#[derive(Clone)]
pub struct RuntimeMetrics {
    pub(crate) shared: Arc<Shared>,
}

impl RuntimeMetrics {
    /// How many worker threads this runtime has -- fixed for its
    /// lifetime (see [`super::Builder::worker_threads`]). `1` on a
    /// [`super::Builder::new_current_thread`] runtime, matching that
    /// flavor having exactly one local queue (the calling thread's, for
    /// the duration of each `block_on`).
    pub fn num_workers(&self) -> usize {
        self.shared.num_workers()
    }

    /// Tasks spawned and not yet finished (completed, aborted, or
    /// panicked) -- the same counter
    /// [`crate::Runtime::shutdown_timeout`] waits to drain to zero.
    pub fn num_alive_tasks(&self) -> usize {
        self.shared.num_alive_tasks()
    }

    /// Tasks currently sitting in the shared injector queue -- spawned
    /// from outside the worker pool (or via `Handle::spawn` from a
    /// thread with no local queue of its own) and not yet picked up by
    /// any worker.
    pub fn global_queue_depth(&self) -> usize {
        self.shared.global_queue_depth()
    }

    /// Tasks currently sitting in `worker`'s own local run queue.
    ///
    /// # Panics
    /// Panics if `worker >= self.num_workers()`.
    pub fn worker_local_queue_depth(&self, worker: usize) -> usize {
        self.shared.worker_local_queue_depth(worker)
    }

    /// How many tasks `worker` has picked up by stealing from a
    /// sibling's local queue, cumulative since the runtime started.
    ///
    /// # Panics
    /// Panics if `worker >= self.num_workers()`.
    pub fn worker_steal_count(&self, worker: usize) -> u64 {
        self.shared.worker_steal_count(worker)
    }

    /// How many times `worker` has parked (found nothing runnable in
    /// its own queue, the injector, or any sibling's), cumulative since
    /// the runtime started. A worker parks with a bounded timeout
    /// rather than waiting to be woken precisely (see `Shared::park`'s
    /// docs), so this counts every such wait, not just ones that ran
    /// out their timeout unwoken.
    ///
    /// # Panics
    /// Panics if `worker >= self.num_workers()`.
    pub fn worker_park_count(&self, worker: usize) -> u64 {
        self.shared.worker_park_count(worker)
    }

    /// How many OS threads the `spawn_blocking` pool currently has
    /// alive. Grows lazily (one per queued job, up to
    /// [`super::Builder::max_blocking_threads`]) and shrinks back down
    /// after a thread sits idle -- see `blocking`'s module docs.
    pub fn num_blocking_threads(&self) -> usize {
        self.shared.num_blocking_threads()
    }
}
