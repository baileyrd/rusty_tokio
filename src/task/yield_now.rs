//! [`yield_now`]: voluntarily give up a task's turn without waiting on
//! anything real, so the scheduler gets a chance to run other queued
//! work before this task continues.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Resolves on the *next* poll, not the first -- the first poll always
/// wakes itself immediately and returns `Pending`, guaranteeing this
/// task goes back through the scheduler's queue (and anything else
/// queued gets a chance to run) before it's polled again, rather than
/// completing in the same poll it was created in.
pub struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            return Poll::Ready(());
        }
        self.yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// Gives up this task's turn, letting the scheduler run other queued
/// work before it's polled again -- useful for a long-running,
/// never-actually-blocked async loop that wants to cooperate with other
/// tasks without splitting itself across multiple spawns.
pub fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}
