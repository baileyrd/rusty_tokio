//! The multi-threaded, work-stealing runtime: a fixed pool of worker
//! threads, each with its own local run queue, backed by a shared
//! injector queue (for tasks spawned from outside the pool) and able to
//! steal from one another when idle. Plus the I/O reactor and timer
//! driver threads that feed wakeups back in.

mod context;
mod worker;

pub use context::Handle;

use crate::io::reactor::Reactor;
use crate::task::{self, JoinHandle};
use crate::time::TimerDriver;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

/// State shared by every worker thread, the reactor, and the timer
/// driver. Lives for as long as the `Runtime` (and, transitively, any
/// task holding a `Weak` back-reference to it) does.
pub(crate) struct Shared {
    injector: Mutex<std::collections::VecDeque<Arc<task::Task>>>,
    local_queues: Vec<Mutex<std::collections::VecDeque<Arc<task::Task>>>>,
    park_lock: Mutex<()>,
    park_condvar: Condvar,
    shutdown: AtomicBool,
    pub(crate) reactor: Arc<Reactor>,
    pub(crate) timer: Arc<TimerDriver>,
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
                    return Some(t);
                }
            }
        }
        None
    }

    /// Sleep until woken by a new task, or until the timeout elapses
    /// (the timeout is a safety net, not the primary wake path -- it
    /// bounds how long a worker can go without re-checking `shutdown`).
    fn park(&self) {
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
}

/// Configures and builds a [`Runtime`].
pub struct Builder {
    worker_threads: usize,
}

impl Builder {
    pub fn new() -> Self {
        Builder {
            worker_threads: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
        }
    }

    pub fn worker_threads(mut self, n: usize) -> Self {
        assert!(n > 0, "a runtime needs at least one worker thread");
        self.worker_threads = n;
        self
    }

    pub fn build(self) -> std::io::Result<Runtime> {
        let reactor = Arc::new(Reactor::new()?);
        reactor.start();
        let timer = Arc::new(TimerDriver::new());
        timer.start();

        let local_queues = (0..self.worker_threads)
            .map(|_| Mutex::new(std::collections::VecDeque::new()))
            .collect();

        let shared = Arc::new(Shared {
            injector: Mutex::new(std::collections::VecDeque::new()),
            local_queues,
            park_lock: Mutex::new(()),
            park_condvar: Condvar::new(),
            shutdown: AtomicBool::new(false),
            reactor,
            timer,
        });

        let workers = (0..self.worker_threads)
            .map(|idx| worker::spawn_worker(shared.clone(), idx))
            .collect();

        Ok(Runtime {
            shared,
            workers: Some(workers),
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

    /// Drive `future` to completion on the calling thread, parking it
    /// (not busy-spinning) whenever it's `Pending`. Anything `future`
    /// spawns runs on the worker pool as usual.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        let _guard = context::enter(self.shared.clone());
        block_on_inner(future)
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.shared.shutdown.store(true, Ordering::Release);
        self.shared.wake_all_parked();
        self.shared.reactor.shutdown();
        self.shared.timer.shutdown();
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
