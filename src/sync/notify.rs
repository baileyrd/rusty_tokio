//! An async condition variable: [`Notify::notified`] parks until
//! [`Notify::notify_one`] or [`Notify::notify_waiters`] fires.
//!
//! Each `notified()` call gets its own `woken` flag, stored alongside its
//! waker while parked and set before that waker is ever called -- so a
//! poll that runs because it was woken always finds its own flag already
//! true and returns `Ready` immediately, regardless of `permits` or
//! whether anyone else registered after it. Without a per-waiter flag,
//! `notify_waiters` has a real bug: it wakes every currently-registered
//! waker but (unlike `notify_one`) leaves nothing else behind for that
//! specific waiter to see, so re-polling it afterward would find
//! `permits == 0`, register nothing new (it's already marked
//! `registered`), and return `Pending` forever -- a wakeup that fires
//! but is then silently lost on the very next poll.
//!
//! A `notify_one` with nobody currently waiting still banks a permit for
//! the next `notified()` call, so a wakeup that "arrives early" (before
//! the other side started waiting) isn't lost either.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct Inner {
    permits: usize,
    waiters: VecDeque<(Arc<AtomicBool>, Waker)>,
}

pub struct Notify {
    inner: Mutex<Inner>,
}

impl Notify {
    pub fn new() -> Self {
        Notify {
            inner: Mutex::new(Inner {
                permits: 0,
                waiters: VecDeque::new(),
            }),
        }
    }

    /// Wakes one waiter, or -- if nobody is currently waiting -- banks a
    /// permit so the very next `notified().await` returns immediately.
    pub fn notify_one(&self) {
        let mut guard = self.inner.lock().unwrap();
        if let Some((woken, waker)) = guard.waiters.pop_front() {
            drop(guard);
            // Set before waking: the whole point is that when this
            // waiter's poll runs (which the wake below triggers), it
            // must already see `woken == true`.
            woken.store(true, Ordering::Release);
            waker.wake();
        } else {
            guard.permits += 1;
        }
    }

    /// Wakes every task currently waiting -- each one's own `woken` flag
    /// is set first (see this module's docs for why that's required,
    /// unlike a naive port of `notify_one`'s wake-and-hope approach).
    /// Nothing is banked for a `notified()` call made *after* this
    /// returns; that call waits for the next notification, same as
    /// tokio's semantics.
    pub fn notify_waiters(&self) {
        let mut guard = self.inner.lock().unwrap();
        let waiters = std::mem::take(&mut guard.waiters);
        drop(guard);
        for (woken, waker) in waiters {
            woken.store(true, Ordering::Release);
            waker.wake();
        }
    }

    pub fn notified(&self) -> Notified<'_> {
        Notified {
            notify: self,
            registered: false,
            woken: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Notified<'a> {
    notify: &'a Notify,
    registered: bool,
    woken: Arc<AtomicBool>,
}

impl Future for Notified<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // Checked first, before ever touching the shared lock: this is
        // what makes re-polling after a `notify_waiters`-driven wake
        // resolve instead of registering (uselessly, since it's already
        // `registered`) and going back to sleep forever.
        if self.woken.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        let mut guard = self.notify.inner.lock().unwrap();
        if guard.permits > 0 {
            guard.permits -= 1;
            return Poll::Ready(());
        }
        if !self.registered {
            guard
                .waiters
                .push_back((self.woken.clone(), cx.waker().clone()));
            self.registered = true;
        }
        Poll::Pending
    }
}
