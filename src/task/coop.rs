//! Public access to this crate's cooperative scheduling budget -- see
//! [`crate::coop`]'s (crate-private) module docs for the full mechanism
//! this sits on top of. Exposed for custom poll loops (via
//! [`consume_budget`]/[`has_budget_remaining`]) or code that deliberately
//! wants to opt a future out of the ambient budget (via
//! [`unconstrained`]).

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Whether the current task could still make at least one more
/// budget-charging poll right now without being forced to yield.
/// Always `true` outside of a task's own top-level poll (e.g. inside
/// [`crate::Runtime::block_on`]'s own future), since nothing enforces a
/// budget there in the first place.
pub fn has_budget_remaining() -> bool {
    crate::coop::has_budget_remaining()
}

/// Consumes one unit of the current task's cooperative budget,
/// completing immediately if any remains (or if no accounting is in
/// effect at all) and yielding back to the scheduler -- to be polled
/// again promptly -- once the budget runs out.
///
/// Useful inside a hand-written poll loop that doesn't otherwise go
/// through one of this crate's own budget-charging primitives (the
/// reactor, `mpsc`, `oneshot`, [`crate::sync::Notify`]), so a tight loop
/// there still cooperates with other tasks on the same worker.
pub fn consume_budget() -> impl Future<Output = ()> {
    ConsumeBudget { _priv: () }
}

struct ConsumeBudget {
    _priv: (),
}

impl Future for ConsumeBudget {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        crate::coop::poll_proceed(cx)
    }
}

/// Wraps `future` so that polling it never charges (or is limited by)
/// the ambient cooperative budget, however deeply nested inside other
/// budget-charging operations it's polled -- for a future that's known
/// to always make bounded, prompt progress and shouldn't be forced to
/// yield just because some unrelated sibling work already spent the
/// task's budget this turn.
pub fn unconstrained<F>(future: F) -> Unconstrained<F> {
    Unconstrained {
        inner: Box::pin(future),
    }
}

/// Future returned by [`unconstrained`].
pub struct Unconstrained<F> {
    inner: Pin<Box<F>>,
}

impl<F: Future> Future for Unconstrained<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        // `inner` is independently pinned via `Pin<Box<F>>`, so
        // reborrowing it here through `&mut self` (itself never
        // `Unpin`-required) never moves `F` out -- same pattern
        // `TaskLocalFuture` uses.
        let this = self.get_mut();
        crate::coop::with_unconstrained(|| this.inner.as_mut().poll(cx))
    }
}
