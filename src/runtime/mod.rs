//! Two runtime flavors sharing one `Shared`/scheduling core:
//!
//! - **Multi-threaded** (the default, [`Builder::new`]/
//!   [`Builder::new_multi_thread`]): a fixed pool of worker threads,
//!   each with its own local run queue, backed by a shared injector
//!   queue (for tasks spawned from outside the pool) and able to steal
//!   from one another when idle.
//! - **Current-thread** ([`Builder::new_current_thread`]): no worker
//!   threads at all -- spawned tasks run interleaved with polls of
//!   `block_on`'s own future, entirely on whichever thread calls it. See
//!   `current_thread`'s module docs for the scheduling loop and what
//!   this flavor deliberately still doesn't do (drive I/O inline on
//!   that same thread, or accept `!Send` futures -- see issue #23's
//!   `LocalSet`/`spawn_local`).
//!
//! Both flavors share the same I/O reactor and timer driver background
//! threads that feed wakeups back in.

mod blocking;
mod context;
mod current_thread;
mod metrics;
mod worker;

pub use context::Handle;
pub use metrics::RuntimeMetrics;

use crate::io::reactor::Reactor;
use crate::sync::Notify;
use crate::task::{self, JoinHandle};
use crate::time::TimerDriver;
use blocking::BlockingPool;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// State shared by every worker thread, the reactor, and the timer
/// driver. Lives for as long as the `Runtime` (and, transitively, any
/// task holding a `Weak` back-reference to it) does.
pub(crate) struct Shared {
    injector: Mutex<std::collections::VecDeque<Arc<task::Task>>>,
    local_queues: Vec<Mutex<std::collections::VecDeque<Arc<task::Task>>>>,
    park_lock: Mutex<()>,
    park_condvar: Condvar,
    /// The *hard* shutdown flag: once set, worker threads stop picking
    /// up new tasks at all. Distinct from `shutting_down` below, which
    /// fires first and only *advises* tasks that shutdown has begun --
    /// see `Runtime::shutdown_timeout`'s doc comment for why the gap
    /// between the two matters.
    shutdown: AtomicBool,
    /// Set once, the first time any shutdown path (`Runtime::drop`,
    /// `shutdown_background`, or `shutdown_timeout`) begins. Checked by
    /// `teardown_once` so the compiler-generated `Drop::drop` that still
    /// runs after a `shutdown_background`/`shutdown_timeout` call
    /// consumes `self` by value doesn't redo (or, worse, re-wait on)
    /// work already done.
    torn_down: AtomicBool,
    /// The persistent, checkable half of the graceful-shutdown signal --
    /// see `shutdown_notified`'s doc comment for why a bare `Notify`
    /// alone (which only wakes whoever's *already* waiting) isn't
    /// enough on its own.
    shutting_down: AtomicBool,
    shutdown_signal: Notify,
    /// Tasks currently spawned and not yet finished (completed, aborted,
    /// or panicked) -- incremented in `task::spawn`, decremented from
    /// every terminal path in `task::Task::run`. The only thing this
    /// crate uses it for is `wait_for_tasks_drain`, so `Relaxed` on the
    /// increment/decrement themselves is fine; the drain wait's own
    /// mutex lock/unlock around checking it is what actually provides
    /// the necessary synchronization with whichever thread just
    /// decremented it to zero.
    active_tasks: AtomicUsize,
    drain_lock: Mutex<()>,
    drain_condvar: Condvar,
    /// Per-worker counters backing [`RuntimeMetrics`] -- how many tasks
    /// each worker has picked up by stealing from a sibling's local
    /// queue, and how many times each has parked. Indexed the same as
    /// `local_queues`; a current-thread runtime has exactly one of each
    /// (its single "worker 0"), same as `local_queues` itself.
    steal_counts: Vec<AtomicU64>,
    park_counts: Vec<AtomicU64>,
    pub(crate) reactor: Arc<Reactor>,
    pub(crate) timer: Arc<TimerDriver>,
    pub(crate) blocking_pool: BlockingPool,
    /// Whether this runtime was built via
    /// [`Builder::new_current_thread`] -- checked by `time::pause`,
    /// which (matching tokio) only makes sense on that flavor: pausing
    /// wall-clock time shared by every task on the runtime would be
    /// incoherent if other worker threads could be concurrently relying
    /// on real timing.
    is_current_thread: bool,
}

impl Shared {
    pub(crate) fn schedule(&self, task: Arc<task::Task>) {
        if let Some(idx) = worker::current_worker_index() {
            self.local_queues[idx].lock().unwrap().push_back(task);
        } else {
            self.injector.lock().unwrap().push_back(task);
        }
        self.park_condvar.notify_one();
    }

    pub(crate) fn next_task(&self, idx: usize) -> Option<Arc<task::Task>> {
        if let Some(t) = self.local_queues[idx].lock().unwrap().pop_front() {
            return Some(t);
        }
        if let Some(t) = self.injector.lock().unwrap().pop_front() {
            return Some(t);
        }
        let n = self.local_queues.len();
        for offset in 1..n {
            let victim = (idx + offset) % n;
            if let Ok(mut q) = self.local_queues[victim].try_lock() {
                if let Some(t) = q.pop_back() {
                    self.steal_counts[idx].fetch_add(1, Ordering::Relaxed);
                    return Some(t);
                }
            }
        }
        None
    }

    /// Sleep until woken by a new task, or until the timeout elapses
    /// (the timeout is a safety net, not the primary wake path -- it
    /// bounds how long a worker can go without re-checking `shutdown`).
    /// `idx` is only used to attribute the park to the right
    /// `RuntimeMetrics::worker_park_count` counter -- every worker
    /// shares the same `park_lock`/`park_condvar`.
    fn park(&self, idx: usize) {
        self.park_counts[idx].fetch_add(1, Ordering::Relaxed);
        let guard = self.park_lock.lock().unwrap();
        let _ = self
            .park_condvar
            .wait_timeout(guard, std::time::Duration::from_millis(50));
    }

    fn wake_all_parked(&self) {
        self.park_condvar.notify_all();
    }

    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Returns `true` only the first time it's called -- see
    /// `torn_down`'s field docs.
    fn teardown_once(&self) -> bool {
        !self.torn_down.swap(true, Ordering::AcqRel)
    }

    pub(crate) fn task_spawned(&self) {
        self.active_tasks.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn task_finished(&self) {
        self.active_tasks.fetch_sub(1, Ordering::Relaxed);
        // Cheap and only ever contended during a `shutdown_timeout`
        // call, same "notify without necessarily holding the paired
        // lock at this exact instant" pattern `BlockingPool`'s own
        // worker loop already relies on -- a wakeup this races is
        // caught on `wait_for_tasks_drain`'s own bounded poll interval
        // instead of instantly, not lost outright.
        self.drain_condvar.notify_all();
    }

    /// Waits until every spawned task has finished, or `deadline`
    /// passes -- whichever comes first. Returns without joining
    /// anything or touching the hard `shutdown` flag; see
    /// `Runtime::shutdown_timeout`.
    fn wait_for_tasks_drain(&self, deadline: Instant) {
        let mut guard = self.drain_lock.lock().unwrap();
        while self.active_tasks.load(Ordering::Relaxed) > 0 {
            let now = Instant::now();
            if now >= deadline {
                return;
            }
            let wait = (deadline - now).min(Duration::from_millis(50));
            guard = self.drain_condvar.wait_timeout(guard, wait).unwrap().0;
        }
    }

    /// Marks shutdown as having begun and wakes every task currently
    /// awaiting `shutdown_notified()` -- the *advisory* half of
    /// shutdown, fired before the hard `shutdown` flag so a task that's
    /// awaiting it directly (e.g. a dedicated cleanup task) gets a real
    /// chance to be scheduled and run before teardown proceeds, not just
    /// notionally "notified" a moment before being abandoned.
    pub(crate) fn begin_graceful_shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);
        self.shutdown_signal.notify_waiters();
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Acquire)
    }

    /// Resolves once `begin_graceful_shutdown` has been called --
    /// immediately, if it already has been by the time this is first
    /// polled. Plain `Notify::notify_waiters` alone only wakes whoever's
    /// *already* registered at the moment it's called (matching tokio's
    /// own semantics -- see `sync::notify`'s docs); a task that calls
    /// this *after* shutdown has already begun still needs to observe
    /// that, which is exactly what the `shutting_down` flag checked here
    /// first is for.
    pub(crate) fn shutdown_notified(&self) -> impl Future<Output = ()> + Send + '_ {
        let mut inner: Option<crate::sync::Notified<'_>> = None;
        std::future::poll_fn(move |cx| {
            if self.is_shutting_down() {
                return std::task::Poll::Ready(());
            }
            let fut = inner.get_or_insert_with(|| self.shutdown_signal.notified());
            std::pin::Pin::new(fut).poll(cx)
        })
    }

    pub(crate) fn is_current_thread(&self) -> bool {
        self.is_current_thread
    }

    // -- RuntimeMetrics accessors -- plain atomic loads (or a lock also
    // taken on every schedule/steal regardless), so calling any of these
    // costs nothing on the hot scheduling path itself.

    pub(crate) fn num_workers(&self) -> usize {
        self.local_queues.len()
    }

    pub(crate) fn num_alive_tasks(&self) -> usize {
        self.active_tasks.load(Ordering::Relaxed)
    }

    pub(crate) fn global_queue_depth(&self) -> usize {
        self.injector.lock().unwrap().len()
    }

    pub(crate) fn worker_local_queue_depth(&self, worker: usize) -> usize {
        self.local_queues[worker].lock().unwrap().len()
    }

    pub(crate) fn worker_steal_count(&self, worker: usize) -> u64 {
        self.steal_counts[worker].load(Ordering::Relaxed)
    }

    pub(crate) fn worker_park_count(&self, worker: usize) -> u64 {
        self.park_counts[worker].load(Ordering::Relaxed)
    }

    pub(crate) fn num_blocking_threads(&self) -> usize {
        self.blocking_pool.live_threads()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Flavor {
    MultiThread,
    CurrentThread,
}

/// Configures and builds a [`Runtime`].
pub struct Builder {
    flavor: Flavor,
    worker_threads: usize,
    max_blocking_threads: usize,
}

impl Builder {
    pub fn new() -> Self {
        Builder {
            flavor: Flavor::MultiThread,
            worker_threads: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
            max_blocking_threads: 32,
        }
    }

    /// Equivalent to [`Builder::new`] -- the multi-threaded,
    /// work-stealing pool. Exists so callers who want to be explicit
    /// about the flavor (to pair with [`Builder::new_current_thread`])
    /// don't have to reach for a bare `new`.
    pub fn new_multi_thread() -> Self {
        Self::new()
    }

    /// A runtime with no worker-thread pool at all: spawned tasks run
    /// interleaved with polls of `block_on`'s own future, entirely on
    /// whichever thread calls it -- see `current_thread`'s module docs
    /// for the scheduling loop. Spawned futures still need to be `Send`
    /// (the same as the multi-threaded flavor); this alone doesn't
    /// enable `!Send` futures -- that needs a `LocalSet`, filed
    /// separately as issue #23.
    pub fn new_current_thread() -> Self {
        Builder {
            flavor: Flavor::CurrentThread,
            worker_threads: 1,
            max_blocking_threads: 32,
        }
    }

    /// Only meaningful for the multi-threaded flavor -- see
    /// [`Builder::new_current_thread`].
    ///
    /// # Panics
    /// Panics if `n` is zero, or if called on a current-thread builder
    /// (where it has nothing to configure).
    pub fn worker_threads(mut self, n: usize) -> Self {
        assert!(n > 0, "a runtime needs at least one worker thread");
        assert!(
            self.flavor == Flavor::MultiThread,
            "worker_threads has no effect on a runtime built with \
             Builder::new_current_thread()"
        );
        self.worker_threads = n;
        self
    }

    /// Caps how many OS threads [`crate::spawn_blocking`] will grow the
    /// blocking pool to. Threads above what's currently needed shrink
    /// back down after sitting idle, so this is a ceiling on
    /// concurrently-running blocking work, not a fixed cost.
    pub fn max_blocking_threads(mut self, n: usize) -> Self {
        assert!(n > 0, "a runtime needs at least one blocking thread");
        self.max_blocking_threads = n;
        self
    }

    pub fn build(self) -> std::io::Result<Runtime> {
        let reactor = Arc::new(Reactor::new()?);
        reactor.start();
        let timer = Arc::new(TimerDriver::new());
        timer.start();
        let blocking_pool = BlockingPool::new(self.max_blocking_threads);

        let is_current_thread = self.flavor == Flavor::CurrentThread;
        // A current-thread runtime has exactly one local queue -- the
        // calling thread's, registered as "worker 0" for the duration
        // of each `block_on` call (see `current_thread::block_on`) --
        // and nothing to steal from or share with, so no OS threads are
        // spawned for it at all.
        let queue_count = if is_current_thread {
            1
        } else {
            self.worker_threads
        };
        let local_queues = (0..queue_count)
            .map(|_| Mutex::new(std::collections::VecDeque::new()))
            .collect();
        let steal_counts = (0..queue_count).map(|_| AtomicU64::new(0)).collect();
        let park_counts = (0..queue_count).map(|_| AtomicU64::new(0)).collect();

        let shared = Arc::new(Shared {
            injector: Mutex::new(std::collections::VecDeque::new()),
            local_queues,
            park_lock: Mutex::new(()),
            park_condvar: Condvar::new(),
            shutdown: AtomicBool::new(false),
            torn_down: AtomicBool::new(false),
            shutting_down: AtomicBool::new(false),
            shutdown_signal: Notify::new(),
            active_tasks: AtomicUsize::new(0),
            drain_lock: Mutex::new(()),
            drain_condvar: Condvar::new(),
            steal_counts,
            park_counts,
            reactor,
            timer,
            blocking_pool,
            is_current_thread,
        });

        let workers = if is_current_thread {
            None
        } else {
            Some(
                (0..self.worker_threads)
                    .map(|idx| worker::spawn_worker(shared.clone(), idx))
                    .collect(),
            )
        };

        Ok(Runtime {
            shared,
            workers,
            is_current_thread,
        })
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

/// A running instance of the hand-rolled runtime: a worker-thread pool
/// plus its I/O reactor and timer driver. Dropping it shuts everything
/// down and joins every background thread.
pub struct Runtime {
    shared: Arc<Shared>,
    workers: Option<Vec<std::thread::JoinHandle<()>>>,
    is_current_thread: bool,
}

impl Runtime {
    pub fn new() -> std::io::Result<Runtime> {
        Builder::new().build()
    }

    pub fn builder() -> Builder {
        Builder::new()
    }

    pub fn handle(&self) -> Handle {
        Handle {
            shared: self.shared.clone(),
        }
    }

    /// A live view into this runtime's scheduler and blocking pool --
    /// see [`RuntimeMetrics`] for what's on it. Equivalent to
    /// `self.handle().metrics()`.
    pub fn metrics(&self) -> RuntimeMetrics {
        RuntimeMetrics {
            shared: self.shared.clone(),
        }
    }

    /// Spawn a future onto the pool. It starts running as soon as a
    /// worker picks it up, independent of whether anything ever awaits
    /// the returned handle.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        task::spawn(&self.shared, future)
    }

    /// Run a genuinely blocking closure on a dedicated thread pool
    /// instead of stalling a worker thread. See [`crate::spawn_blocking`]
    /// for the full contract.
    pub fn spawn_blocking<F, T>(&self, f: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.handle().spawn_blocking(f)
    }

    /// Drive `future` to completion on the calling thread, parking it
    /// (not busy-spinning) whenever it's `Pending`.
    ///
    /// On the multi-threaded flavor, anything `future` spawns runs on
    /// the worker pool as usual, in the background. On the
    /// current-thread flavor (see [`Builder::new_current_thread`]),
    /// there is no worker pool -- spawned tasks run interleaved with
    /// polls of `future` itself, entirely on this call's own thread; see
    /// `current_thread`'s module docs.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        let _guard = context::enter(self.shared.clone());
        if self.is_current_thread {
            current_thread::block_on(&self.shared, future)
        } else {
            block_on_inner(future)
        }
    }

    /// The four steps every shutdown path ends with: flip the hard
    /// `shutdown` flag so worker threads stop picking up new tasks, wake
    /// anything still parked so it re-checks that flag promptly instead
    /// of waiting out its own park timeout, and tear down the reactor
    /// and timer threads. Both are simple event loops with nothing of
    /// the caller's left to wait on once told to stop, so this always
    /// joins them regardless of which shutdown path called it.
    fn stop_scheduling_and_reactor(&self) {
        self.shared.shutdown.store(true, Ordering::Release);
        self.shared.wake_all_parked();
        self.shared.reactor.shutdown();
        self.shared.timer.shutdown();
    }

    /// Signals shutdown and returns immediately -- doesn't wait for
    /// outstanding tasks to finish, the blocking pool to drain, or even
    /// the worker threads themselves to exit. Dropping the worker
    /// threads' `JoinHandle`s here (instead of joining them, as every
    /// other shutdown path does) doesn't stop or detach-kill them --
    /// they keep running in the background, finishing whatever task
    /// they're mid-poll on and then exiting once they next check the
    /// now-true `shutdown` flag, just unobserved by this call.
    ///
    /// Prefer [`Runtime::shutdown_timeout`] over this when spawned tasks
    /// might hold something worth letting finish cleanly (a flush, a
    /// file close) -- this method gives them no time at all.
    pub fn shutdown_background(mut self) {
        self.shared.begin_graceful_shutdown();
        if !self.shared.teardown_once() {
            return;
        }
        self.stop_scheduling_and_reactor();
        self.shared.blocking_pool.signal_shutdown();
        self.workers.take();
    }

    /// Signals shutdown, then waits up to `timeout` for every
    /// outstanding task to finish and the blocking pool to drain before
    /// falling back to the same abrupt teardown `Drop` gives if it
    /// doesn't happen in time.
    ///
    /// "Waits" here is deliberately advisory-plus-bounded, not a hard
    /// guarantee every task runs to completion: the hard `shutdown` flag
    /// (which makes worker threads stop picking up *new* tasks, though
    /// they still finish whichever one they're mid-poll on) isn't set
    /// until after this wait, so tasks already queued or running keep
    /// making progress the whole time -- but a task that's genuinely
    /// stuck (blocked forever on something that will never resolve, or
    /// a `spawn_blocking` closure in a blocking syscall this crate has
    /// no way to preempt) will still be here when `timeout` elapses, at
    /// which point this stops waiting and tears down anyway, the same
    /// as `shutdown_background` would.
    pub fn shutdown_timeout(mut self, timeout: Duration) {
        self.shared.begin_graceful_shutdown();
        let deadline = Instant::now() + timeout;
        self.shared.wait_for_tasks_drain(deadline);
        if !self.shared.teardown_once() {
            return;
        }
        self.stop_scheduling_and_reactor();
        self.shared.blocking_pool.signal_shutdown();
        self.shared.blocking_pool.wait_for_drain(Some(deadline));
        if let Some(workers) = self.workers.take() {
            for w in workers {
                let _ = w.join();
            }
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.shared.begin_graceful_shutdown();
        if !self.shared.teardown_once() {
            // `shutdown_background`/`shutdown_timeout` already ran this
            // (they consume `self` by value, so this `drop` still runs
            // once their body returns) -- nothing left to do.
            return;
        }
        self.stop_scheduling_and_reactor();
        self.shared.blocking_pool.shutdown();
        if let Some(workers) = self.workers.take() {
            for w in workers {
                let _ = w.join();
            }
        }
    }
}

struct BlockOnWaker {
    mutex: Mutex<bool>,
    condvar: Condvar,
}

impl BlockOnWaker {
    fn new() -> Self {
        BlockOnWaker {
            mutex: Mutex::new(false),
            condvar: Condvar::new(),
        }
    }

    fn park(&self) {
        let mut notified = self.mutex.lock().unwrap();
        while !*notified {
            notified = self.condvar.wait(notified).unwrap();
        }
        *notified = false;
    }
}

impl std::task::Wake for BlockOnWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        *self.mutex.lock().unwrap() = true;
        self.condvar.notify_one();
    }
}

fn block_on_inner<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let parker = Arc::new(BlockOnWaker::new());
    let waker = std::task::Waker::from(parker.clone());
    let mut cx = std::task::Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(v) => return v,
            std::task::Poll::Pending => parker.park(),
        }
    }
}
