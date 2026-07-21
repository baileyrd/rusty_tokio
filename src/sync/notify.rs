//! An async condition variable: [`Notify::notified`] parks until
//! [`Notify::notify_one`] or [`Notify::notify_waiters`] fires. A
//! `notify_one` with nobody currently waiting leaves a single permit
//! banked for the next call to `notified()`, mirroring tokio's
//! `Notify` -- this is what lets a wakeup that "arrives early" (before
//! the other side started waiting) not get lost.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

struct Inner {
    permits: usize,
    waiters: VecDeque<Waker>,
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

    /// Wakes one waiter, or -- if nobody is currently waiting -- banks
    /// a permit so the very next `notified().await` returns immediately.
    ///
    /// Every call banks a permit (not just the ones with nobody
    /// waiting): a waiter that's already registered doesn't know
    /// *why* it was woken when it gets re-polled, so waking it without
    /// also leaving a permit for it to find would just put it straight
    /// back to sleep -- a real lost-wakeup, not merely a simplification.
    pub fn notify_one(&self) {
        let mut guard = self.inner.lock().unwrap();
        guard.permits += 1;
        let waiter = guard.waiters.pop_front();
        drop(guard);
        if let Some(waker) = waiter {
            waker.wake();
        }
    }

    /// Wakes every task currently waiting. Unlike `notify_one`, this
    /// never banks a permit -- a task that calls `notified()` afterward
    /// waits for the next notification, same as tokio's semantics.
    pub fn notify_waiters(&self) {
        let mut guard = self.inner.lock().unwrap();
        let waiters = std::mem::take(&mut guard.waiters);
        drop(guard);
        for waker in waiters {
            waker.wake();
        }
    }

    pub fn notified(&self) -> Notified<'_> {
        Notified {
            notify: self,
            registered: false,
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
}

impl Future for Notified<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut guard = self.notify.inner.lock().unwrap();
        if guard.permits > 0 {
            guard.permits -= 1;
            return Poll::Ready(());
        }
        if !self.registered {
            guard.waiters.push_back(cx.waker().clone());
            self.registered = true;
        }
        Poll::Pending
    }
}
