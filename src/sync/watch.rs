//! A single-latest-value broadcast: the sender always holds exactly one
//! current value, and `Receiver::changed().await` resolves once it's
//! been updated since this receiver last observed it. Unlike
//! [`super::broadcast`] (a multi-consumer channel that queues every
//! message, reporting `Lagged` if a receiver falls behind), there's no
//! queue and no lagging here -- a receiver that misses several updates
//! in a row just sees the latest value whenever it does check, not
//! every intermediate one. Useful for something like
//! "the current configuration" or "has shutdown been requested" that
//! many tasks want to observe, exactly the shape
//! [`crate::Handle::shutdown_notified`]/`is_shutting_down` hand-rolled
//! as a one-off special case before this existed.
//!
//! The version counter, the wakers waiting on `changed()`, and the
//! value itself all live under a *single* lock -- the same reasoning
//! [`super::mpsc`]'s module docs give for why its queue and wakers
//! share one lock too: checking "has the version changed" and
//! registering a waker if not need to happen atomically, or a `send`
//! landing in the gap between those two steps would update the version
//! and drain the (not-yet-registered) waker list, and this receiver
//! would then wait for a second, distinct change instead of noticing
//! the one it just missed -- a real lost wakeup, not a hypothetical one.

use std::ops::Deref;
use std::sync::{Arc, Mutex as StdMutex, MutexGuard};
use std::task::{Poll, Waker};

struct Inner<T> {
    value: T,
    version: u64,
    wakers: Vec<Waker>,
    sender_alive: bool,
    receivers_alive: usize,
}

struct Shared<T> {
    inner: StdMutex<Inner<T>>,
}

pub fn channel<T>(initial: T) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        inner: StdMutex::new(Inner {
            value: initial,
            version: 0,
            wakers: Vec::new(),
            sender_alive: true,
            receivers_alive: 1,
        }),
    });
    (
        Sender {
            shared: shared.clone(),
        },
        Receiver {
            shared,
            seen_version: 0,
        },
    )
}

pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Sender<T> {
    /// Replaces the current value and wakes every receiver waiting on
    /// [`Receiver::changed`]. Fails (handing `value` back) if every
    /// receiver has already dropped -- there would be nobody left to
    /// ever observe it.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let mut guard = self.shared.inner.lock().unwrap();
        if guard.receivers_alive == 0 {
            drop(guard);
            return Err(SendError(value));
        }
        guard.value = value;
        guard.version += 1;
        let wakers = std::mem::take(&mut guard.wakers);
        drop(guard);
        for waker in wakers {
            waker.wake();
        }
        Ok(())
    }

    /// Modifies the current value in place via `modify`, without
    /// needing to hand over a whole new value up front. Always
    /// succeeds and always counts as a change, even if `modify` happens
    /// to leave the value equal to what it was.
    pub fn send_modify<F: FnOnce(&mut T)>(&self, modify: F) {
        let mut guard = self.shared.inner.lock().unwrap();
        modify(&mut guard.value);
        guard.version += 1;
        let wakers = std::mem::take(&mut guard.wakers);
        drop(guard);
        for waker in wakers {
            waker.wake();
        }
    }

    /// Reads the current value without waiting -- the sender's own side
    /// of [`Receiver::borrow`].
    pub fn borrow(&self) -> Ref<'_, T> {
        Ref {
            guard: self.shared.inner.lock().unwrap(),
        }
    }

    pub fn receiver_count(&self) -> usize {
        self.shared.inner.lock().unwrap().receivers_alive
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut guard = self.shared.inner.lock().unwrap();
        guard.sender_alive = false;
        let wakers = std::mem::take(&mut guard.wakers);
        drop(guard);
        // Wake everyone still waiting so they observe the channel is
        // now closed instead of waiting forever.
        for waker in wakers {
            waker.wake();
        }
    }
}

pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    seen_version: u64,
}

impl<T> Receiver<T> {
    /// Reads the current value without waiting or marking it seen --
    /// a later [`changed`](Self::changed) call still reports a change
    /// if the version this receiver last *marked* as seen is still
    /// behind, regardless of how many times `borrow` has been called
    /// since.
    pub fn borrow(&self) -> Ref<'_, T> {
        Ref {
            guard: self.shared.inner.lock().unwrap(),
        }
    }

    /// Like [`borrow`](Self::borrow), but also marks the current
    /// version as seen -- equivalent to (but without the intermediate
    /// wait) calling [`changed`](Self::changed) and then `borrow`.
    pub fn borrow_and_update(&mut self) -> Ref<'_, T> {
        let guard = self.shared.inner.lock().unwrap();
        self.seen_version = guard.version;
        Ref { guard }
    }

    /// Resolves once the value has changed since this receiver last
    /// marked a version as seen (via this call or
    /// [`borrow_and_update`](Self::borrow_and_update)) -- immediately,
    /// if it already has by the time this is first polled. Fails once
    /// the sender has dropped and no further changes are possible.
    pub async fn changed(&mut self) -> Result<(), RecvError> {
        let mut registered = false;
        std::future::poll_fn(|cx| {
            let mut guard = self.shared.inner.lock().unwrap();
            if guard.version != self.seen_version {
                self.seen_version = guard.version;
                return Poll::Ready(Ok(()));
            }
            if !guard.sender_alive {
                return Poll::Ready(Err(RecvError(())));
            }
            if !registered {
                guard.wakers.push(cx.waker().clone());
                registered = true;
            }
            Poll::Pending
        })
        .await
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.shared.inner.lock().unwrap().receivers_alive += 1;
        Receiver {
            shared: self.shared.clone(),
            // Starts wherever *this* receiver's own last-seen marker
            // is, not the channel's current version -- if the original
            // hasn't yet observed the latest value, neither has the
            // clone.
            seen_version: self.seen_version,
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.inner.lock().unwrap().receivers_alive -= 1;
    }
}

/// A read guard over a [`watch`](self) channel's current value, held
/// via [`Sender::borrow`]/[`Receiver::borrow`]/
/// [`Receiver::borrow_and_update`].
pub struct Ref<'a, T> {
    guard: MutexGuard<'a, Inner<T>>,
}

impl<T> Deref for Ref<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.guard.value
    }
}

/// Every receiver has dropped, so `send`/`send_modify` would have
/// nobody left to ever observe it.
pub struct SendError<T>(pub T);

impl<T> std::fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SendError(..)")
    }
}

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sending on a watch channel with no receivers left")
    }
}

impl<T> std::error::Error for SendError<T> {}

/// The sender was dropped, so no further changes are possible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError(());

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "watch channel sender dropped, no further changes possible"
        )
    }
}

impl std::error::Error for RecvError {}
