//! A cooperative scheduling ("coop") budget: every task gets a fixed
//! number of poll operations before it's transparently forced to yield,
//! even if it never legitimately awaits anything. Without this, a task
//! whose single `poll` call loops internally forever (a `Stream`-like
//! future that keeps handing back `Ready`, or a hand-written future that
//! does a lot of synchronous work per poll) can monopolize a worker
//! thread indefinitely -- [`crate::task::Task::run`] only calls a
//! future's `poll` once per scheduling turn, but nothing stops that one
//! call from itself never returning in practice. A tight `while let
//! Some(x) = rx.recv().await { .. }` loop over a channel that's always
//! ready is the textbook case: every individual `.await` really is
//! resolving, over and over, so nothing about it looks like a bug from
//! inside that one task -- it's still starving every other task on the
//! same worker.
//!
//! The mechanism: a thread-local counter, reset to a fresh allotment by
//! [`budget`] at the top of every top-level [`crate::task::Task::run`]
//! call, and decremented by [`poll_proceed`] at a handful of poll points
//! this crate's own primitives call through regardless -- the reactor's
//! [`crate::io::reactor::poll_io`] (so every socket read/write consumes
//! budget, covering `TcpStream`/`TcpListener`/`UdpSocket`/`UnixStream`/
//! `UnixListener` uniformly through one shared choke point), and
//! `mpsc`/`oneshot`/[`crate::sync::Notify`]'s own poll implementations.
//! Once exhausted, `poll_proceed` self-wakes (the same
//! `cx.waker().wake_by_ref(); Poll::Pending` idiom
//! [`crate::task::yield_now`] uses) and returns `Pending` *before* the
//! caller does its actual readiness check -- so a channel that already
//! has a value sitting in it still yields once budget runs out, deferring
//! the dequeue to the next poll instead of handing it over immediately.
//! Since the task is scheduled back onto the *end* of its run queue (the
//! same queue any other woken task lands on), everything else already
//! queued gets a turn first.
//!
//! Deliberately not copying tokio's exact accounting wholesale: tokio
//! charges budget for things like buffered-I/O internals this crate
//! doesn't have, and its budget applies inside nested sub-runtimes this
//! crate has no equivalent of. The four choke points above are this
//! crate's own actual poll-heavy primitives, and each is charged
//! uniformly (one unit per operation) rather than trying to weight
//! "expensive" operations differently -- simple, and sufficient to break
//! the starvation case above without needing per-primitive tuning.
//!
//! A future polled outside of `Task::run` (`Runtime::block_on`'s own
//! future, most notably) has no budget in scope at all --
//! `poll_proceed` is unconditionally `Poll::Ready(())` there, matching
//! tokio's own behavior of only enforcing coop where a runtime's task
//! system is actually driving the poll.

use std::cell::Cell;
use std::task::{Context, Poll};

/// Tokio uses the same figure for its own default budget. Not load-bearing
/// on its own -- any bound that's "large enough that a well-behaved task
/// never notices, small enough that a pathological one yields promptly"
/// works -- but there's no reason to pick a different number than one
/// that's already seen this much real-world exercise.
const DEFAULT_BUDGET: usize = 128;

thread_local! {
    /// `None` outside of any task's top-level poll (no accounting in
    /// effect); `Some(remaining)` for the duration of a
    /// [`crate::task::Task::run`] call.
    static BUDGET: Cell<Option<usize>> = const { Cell::new(None) };
}

/// Gives `f` a fresh cooperative budget for its duration, restoring
/// whatever was in scope before once it returns -- called once per
/// [`crate::task::Task::run`], wrapped directly around that call's
/// `future.poll(..)`.
pub(crate) fn budget<R>(f: impl FnOnce() -> R) -> R {
    let previous = BUDGET.with(|b| b.replace(Some(DEFAULT_BUDGET)));
    let result = f();
    BUDGET.with(|b| b.set(previous));
    result
}

/// Consumes one unit of the current task's budget. `Poll::Ready(())`
/// either if there's budget left (in which case one unit is spent) or
/// no budget tracking is in effect at all; `Poll::Pending` -- after
/// self-waking, so the caller is guaranteed to be polled again promptly
/// -- once a task's budget hits zero.
///
/// Callers check this *before* doing their actual readiness check or
/// dequeue, so a resource that's already ready still yields once budget
/// runs out rather than being handed over immediately -- see this
/// module's own docs for why that ordering is what actually breaks a
/// tight, always-ready poll loop.
pub(crate) fn poll_proceed(cx: &mut Context<'_>) -> Poll<()> {
    BUDGET.with(|b| match b.get() {
        None => Poll::Ready(()),
        Some(0) => {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
        Some(n) => {
            b.set(Some(n - 1));
            Poll::Ready(())
        }
    })
}
