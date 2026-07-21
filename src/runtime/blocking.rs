//! The blocking-task thread pool `spawn_blocking` offloads onto,
//! separate from the async worker pool. Threads are grown lazily (one
//! per queued job, up to a cap) and shrink back down after sitting idle
//! for a while, rather than either spawning a fresh OS thread per call
//! (real overhead for anything short-lived) or keeping a fixed-size
//! pool alive forever (wasted threads when nothing's blocking).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

type Job = Box<dyn FnOnce() + Send>;

struct Inner {
    queue: Mutex<VecDeque<Job>>,
    condvar: Condvar,
    shutdown: AtomicBool,
    live_threads: AtomicUsize,
    max_threads: usize,
    idle_timeout: Duration,
}

pub(crate) struct BlockingPool {
    inner: Arc<Inner>,
}

impl BlockingPool {
    pub(crate) fn new(max_threads: usize) -> Self {
        assert!(max_threads > 0, "a blocking pool needs at least one thread");
        BlockingPool {
            inner: Arc::new(Inner {
                queue: Mutex::new(VecDeque::new()),
                condvar: Condvar::new(),
                shutdown: AtomicBool::new(false),
                live_threads: AtomicUsize::new(0),
                max_threads,
                idle_timeout: Duration::from_secs(10),
            }),
        }
    }

    pub(crate) fn spawn(&self, job: Job) {
        self.inner.queue.lock().unwrap().push_back(job);
        self.inner.condvar.notify_one();
        self.grow_if_needed();
    }

    /// Adds one more worker thread if the pool is under its cap. Called
    /// on every `spawn`, so it may occasionally add a thread that turns
    /// out not to be needed (another idle thread picks up the job
    /// first) -- harmless, the extra thread just idles out later.
    fn grow_if_needed(&self) {
        loop {
            let current = self.inner.live_threads.load(Ordering::Acquire);
            if current >= self.inner.max_threads {
                return;
            }
            if self
                .inner
                .live_threads
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let inner = self.inner.clone();
                std::thread::Builder::new()
                    .name("rusty_tokio-blocking".to_string())
                    .spawn(move || Self::worker_loop(inner))
                    .expect("failed to spawn rusty_tokio blocking-pool thread");
                return;
            }
        }
    }

    fn worker_loop(inner: Arc<Inner>) {
        loop {
            let job = {
                let mut guard = inner.queue.lock().unwrap();
                loop {
                    if let Some(job) = guard.pop_front() {
                        break Some(job);
                    }
                    if inner.shutdown.load(Ordering::Acquire) {
                        break None;
                    }
                    let (new_guard, timeout) = inner
                        .condvar
                        .wait_timeout(guard, inner.idle_timeout)
                        .unwrap();
                    guard = new_guard;
                    if timeout.timed_out() && guard.is_empty() {
                        break None;
                    }
                }
            };
            match job {
                Some(job) => job(),
                None => {
                    inner.live_threads.fetch_sub(1, Ordering::AcqRel);
                    // Wake anyone in `shutdown()` waiting for the count
                    // to reach zero.
                    inner.condvar.notify_all();
                    return;
                }
            }
        }
    }

    pub(crate) fn shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::Release);
        self.inner.condvar.notify_all();
        // Threads are transient and self-terminating (unlike the fixed
        // reactor/timer threads), so there's nothing to `join` here --
        // just wait for the count to actually reach zero so a caller
        // relying on `Runtime` drop as a clean-shutdown point (tests, in
        // particular) doesn't race a still-finishing blocking job. The
        // bounded wait is a safety net in case a decrement-then-notify
        // on another thread races this wait's setup.
        let mut guard = self.inner.queue.lock().unwrap();
        while self.inner.live_threads.load(Ordering::Acquire) > 0 {
            guard = self
                .inner
                .condvar
                .wait_timeout(guard, Duration::from_millis(50))
                .unwrap()
                .0;
        }
    }
}
