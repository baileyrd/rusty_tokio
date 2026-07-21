//! [`Barrier`]: a rendezvous point for a fixed number of tasks -- every
//! `wait()` call blocks until `n` of them have all called it, then all
//! `n` resolve together. Reusable across many rounds: the instant one
//! round completes, the barrier immediately starts accepting arrivals
//! for the next one.

use std::future::poll_fn;
use std::sync::Mutex;
use std::task::{Poll, Waker};

struct State {
    /// Arrivals counted toward the current round.
    arrived: usize,
    /// Bumped every time a round completes -- lets a parked waiter tell
    /// "my round finished" apart from "some *other* round finished"
    /// without needing a dedicated wakeup channel per round.
    generation: u64,
    /// Wakers for every non-leader arrival still waiting on the current
    /// round. Woken (and cleared) all at once the moment the round
    /// completes.
    wakers: Vec<Waker>,
}

/// A rendezvous point for a fixed number of tasks. Mirrors tokio's own
/// `sync::Barrier`: every `wait()` call blocks until `n` of them have
/// all called it, then every one of the `n` calls resolves together,
/// and the barrier immediately resets to accept the next round of `n`.
///
/// Hand-rolls its own waiter list (a `Vec<Waker>` behind one plain
/// `std::sync::Mutex`, alongside the arrival count and generation
/// counter) rather than building on [`crate::sync::Notify`]: `Notify`'s
/// own waiters queue lives behind a *separate* lock from whatever
/// external state a caller checks before registering with it, which --
/// as `Notify`'s own docs note about `notify_waiters` banking nothing
/// for a later `notified()` call -- means a caller has to be careful
/// that "check the condition" and "register to be woken" happen as one
/// atomic step relative to whatever might complete that condition
/// concurrently, on a *different* lock. Folding the waiter list into the
/// *same* lock already guarding `arrived`/`generation` here sidesteps
/// that entirely: a waiter either observes its round has already
/// completed (no need to register at all) or is guaranteed to land in
/// `wakers` before the completing arrival can possibly take and drain
/// that list, since both only ever happen while holding this one mutex
/// -- there's no window for a completion to slip in between "checked"
/// and "registered" the way there would be across two locks.
pub struct Barrier {
    n: usize,
    state: Mutex<State>,
}

/// Returned by [`Barrier::wait`]. `is_leader()` is `true` for exactly
/// one of the `n` calls in a round (arbitrarily which one) -- useful for
/// a caller that wants exactly one of the `n` tasks to do some one-time
/// per-round bookkeeping (resetting a shared counter, say) without
/// every task racing to do it redundantly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BarrierWaitResult {
    is_leader: bool,
}

impl BarrierWaitResult {
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

impl Barrier {
    /// # Panics
    /// Panics if `n` is zero -- a barrier for zero arrivals could never
    /// legitimately complete a round.
    pub fn new(n: usize) -> Barrier {
        assert!(n > 0, "Barrier::new(0) can never complete a round");
        Barrier {
            n,
            state: Mutex::new(State {
                arrived: 0,
                generation: 0,
                wakers: Vec::new(),
            }),
        }
    }

    /// Blocks until `n` calls (across however many tasks) to this
    /// method have all been made, then every one of them resolves
    /// together.
    ///
    /// If this call's own future is dropped before it resolves
    /// (cancelled via `select!`, a timeout, or simply never polled to
    /// completion), its arrival is **not** retracted -- the round still
    /// counts it as having arrived. The same "arrived, then left early"
    /// edge case every rendezvous-point implementation has to make some
    /// call on; not one this crate's own use cases (staged startup,
    /// tests waiting for N workers to be ready) are likely to hit in
    /// practice, so not worth the extra bookkeeping a fully cancel-safe
    /// version would need.
    pub async fn wait(&self) -> BarrierWaitResult {
        let my_generation = {
            let mut guard = self.state.lock().unwrap();
            let my_generation = guard.generation;
            guard.arrived += 1;
            if guard.arrived == self.n {
                // The last arrival: complete the round right here,
                // synchronously, and wake everyone else -- no need to
                // ever suspend this call at all.
                guard.arrived = 0;
                guard.generation = guard.generation.wrapping_add(1);
                let wakers = std::mem::take(&mut guard.wakers);
                drop(guard);
                for waker in wakers {
                    waker.wake();
                }
                return BarrierWaitResult { is_leader: true };
            }
            my_generation
        };

        poll_fn(|cx| {
            let mut guard = self.state.lock().unwrap();
            if guard.generation != my_generation {
                return Poll::Ready(());
            }
            guard.wakers.push(cx.waker().clone());
            Poll::Pending
        })
        .await;

        BarrierWaitResult { is_leader: false }
    }
}
