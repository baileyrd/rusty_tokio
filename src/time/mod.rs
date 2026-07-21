//! Timers: a single background thread holding a min-heap of deadlines,
//! sleeping until the nearest one (or until told a nearer one just
//! arrived), and waking whichever tasks are due.

use crate::runtime::Handle;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

struct Inner {
    heap: BinaryHeap<Reverse<(Instant, u64)>>,
    wakers: HashMap<u64, Waker>,
}

pub(crate) struct TimerDriver {
    inner: Mutex<Inner>,
    condvar: Condvar,
    next_id: AtomicU64,
    shutdown: AtomicBool,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl TimerDriver {
    pub(crate) fn new() -> Self {
        TimerDriver {
            inner: Mutex::new(Inner {
                heap: BinaryHeap::new(),
                wakers: HashMap::new(),
            }),
            condvar: Condvar::new(),
            next_id: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
            thread: Mutex::new(None),
        }
    }

    pub(crate) fn start(self: &Arc<Self>) {
        let driver = self.clone();
        let handle = std::thread::Builder::new()
            .name("rusty_tokio-timer".to_string())
            .spawn(move || driver.event_loop())
            .expect("failed to spawn rusty_tokio timer thread");
        *self.thread.lock().unwrap() = Some(handle);
    }

    fn event_loop(&self) {
        loop {
            if self.shutdown.load(Ordering::Acquire) {
                return;
            }
            let guard = self.inner.lock().unwrap();
            match guard.heap.peek().copied() {
                None => {
                    // Nothing scheduled: wait to be told otherwise. The
                    // bounded wait is a safety net so `shutdown` is
                    // re-checked periodically even without a notify.
                    let _ = self
                        .condvar
                        .wait_timeout(guard, Duration::from_millis(200))
                        .unwrap();
                }
                Some(Reverse((deadline, _))) => {
                    let now = Instant::now();
                    if deadline <= now {
                        self.fire_due(guard, now);
                    } else {
                        let _ = self.condvar.wait_timeout(guard, deadline - now).unwrap();
                    }
                }
            }
        }
    }

    fn fire_due(&self, mut guard: std::sync::MutexGuard<'_, Inner>, now: Instant) {
        let mut due = Vec::new();
        while let Some(&Reverse((deadline, id))) = guard.heap.peek() {
            if deadline > now {
                break;
            }
            guard.heap.pop();
            if let Some(waker) = guard.wakers.remove(&id) {
                due.push(waker);
            }
        }
        drop(guard);
        for waker in due {
            waker.wake();
        }
    }

    fn register(&self, deadline: Instant, waker: Waker) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut guard = self.inner.lock().unwrap();
        guard.heap.push(Reverse((deadline, id)));
        guard.wakers.insert(id, waker);
        drop(guard);
        // A newly registered deadline might be sooner than whatever the
        // timer thread is currently sleeping until.
        self.condvar.notify_one();
        id
    }

    fn update_waker(&self, id: u64, waker: Waker) {
        self.inner.lock().unwrap().wakers.insert(id, waker);
    }

    fn cancel(&self, id: u64) {
        // The heap entry itself is left in place -- removing from a
        // binary heap isn't O(1) -- and simply skipped when it's popped
        // since its waker is gone by then.
        self.inner.lock().unwrap().wakers.remove(&id);
    }

    pub(crate) fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.condvar.notify_all();
        if let Some(handle) = self.thread.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

/// A future that resolves once at a specific `Instant`.
pub struct Sleep {
    deadline: Instant,
    timer: Arc<TimerDriver>,
    id: Option<u64>,
}

impl Sleep {
    fn at(deadline: Instant) -> Self {
        let timer = Handle::current().shared.timer.clone();
        Sleep {
            deadline,
            timer,
            id: None,
        }
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if Instant::now() >= self.deadline {
            return Poll::Ready(());
        }
        match self.id {
            None => {
                let id = self.timer.register(self.deadline, cx.waker().clone());
                self.id = Some(id);
            }
            Some(id) => self.timer.update_waker(id, cx.waker().clone()),
        }
        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            self.timer.cancel(id);
        }
    }
}

/// Resolves after `duration` elapses.
pub fn sleep(duration: Duration) -> Sleep {
    Sleep::at(Instant::now() + duration)
}

/// Resolves at `deadline`, or immediately if it's already passed.
pub fn sleep_until(deadline: Instant) -> Sleep {
    Sleep::at(deadline)
}

/// The error [`timeout`] resolves to when the inner future didn't
/// finish in time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed;

impl std::fmt::Display for Elapsed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "deadline elapsed before the future completed")
    }
}

impl std::error::Error for Elapsed {}

/// Race `future` against a `duration`-long timer.
pub struct Timeout<F: Future> {
    future: Pin<Box<F>>,
    sleep: Sleep,
}

pub fn timeout<F: Future>(duration: Duration, future: F) -> Timeout<F> {
    Timeout {
        future: Box::pin(future),
        sleep: sleep(duration),
    }
}

impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, Elapsed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // `Timeout<F>` is `Unpin` regardless of `F` (its only fields are
        // `Pin<Box<F>>`, always `Unpin`, and `Sleep`, itself all-`Unpin`
        // fields), so projecting via `get_mut` needs no unsafe code.
        let this = self.get_mut();
        if let Poll::Ready(v) = this.future.as_mut().poll(cx) {
            return Poll::Ready(Ok(v));
        }
        match Pin::new(&mut this.sleep).poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(Elapsed)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// What [`Interval::tick`] does when one or more ticks were missed --
/// the caller didn't call `tick()` again until after more than one
/// `period` had already elapsed since the last one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MissedTickBehavior {
    /// Keep the original schedule and fire every missed tick back-to-
    /// back with no delay between them until caught up -- the default,
    /// and this type's behavior before `MissedTickBehavior` existed.
    /// Next deadline is always the previous scheduled deadline plus one
    /// period, regardless of how late `tick()` was actually called.
    #[default]
    Burst,
    /// Give up on the original schedule and restart it from whenever
    /// this (late) tick actually fired: next deadline is `Instant::now()
    /// + period`, measured at the moment the missed tick is finally
    /// observed, not `period` after the original schedule.
    Delay,
    /// Neither burst through nor delay-and-reset: jump straight to the
    /// next deadline that's still in the future, skipping every tick
    /// that was missed without firing any of them.
    Skip,
}

/// Fires repeatedly on a fixed period, measured from the *previous
/// scheduled* tick (not from when `tick()` returned) so ticks don't
/// drift under load the way `sleep(period)` in a loop would -- see
/// [`MissedTickBehavior`] for exactly what "measured from" means once a
/// tick has actually been missed, which the three variants there each
/// treat differently.
pub struct Interval {
    period: Duration,
    next_deadline: Instant,
    sleep: Option<Sleep>,
    missed_tick_behavior: MissedTickBehavior,
}

pub fn interval(period: Duration) -> Interval {
    interval_at(Instant::now() + period, period)
}

/// Like [`interval`], but the first tick fires at `start` instead of
/// always being derived from `Instant::now() + period` -- useful for
/// aligning several independent intervals to the same wall-clock
/// moments, or for making the first tick fire sooner (or later) than a
/// full period from now.
pub fn interval_at(start: Instant, period: Duration) -> Interval {
    assert!(period > Duration::ZERO, "interval period must be positive");
    Interval {
        period,
        next_deadline: start,
        sleep: None,
        missed_tick_behavior: MissedTickBehavior::default(),
    }
}

impl Interval {
    pub fn missed_tick_behavior(&self) -> MissedTickBehavior {
        self.missed_tick_behavior
    }

    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.missed_tick_behavior = behavior;
    }

    /// Waits for the next tick, returning the `Instant` it was
    /// scheduled for.
    pub async fn tick(&mut self) -> Instant {
        std::future::poll_fn(|cx| {
            let sleep = self
                .sleep
                .get_or_insert_with(|| Sleep::at(self.next_deadline));
            match Pin::new(sleep).poll(cx) {
                Poll::Ready(()) => {
                    let fired_at = self.next_deadline;
                    self.next_deadline = match self.missed_tick_behavior {
                        MissedTickBehavior::Burst => self.next_deadline + self.period,
                        MissedTickBehavior::Delay => Instant::now() + self.period,
                        MissedTickBehavior::Skip => {
                            let now = Instant::now();
                            let mut next = self.next_deadline + self.period;
                            while next <= now {
                                next += self.period;
                            }
                            next
                        }
                    };
                    self.sleep = None;
                    Poll::Ready(fired_at)
                }
                Poll::Pending => Poll::Pending,
            }
        })
        .await
    }
}
