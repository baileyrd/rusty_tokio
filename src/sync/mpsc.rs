//! A bounded, multi-producer single-consumer queue. `send().await`
//! suspends the sending task while the buffer is full instead of
//! blocking the worker thread; `recv().await` suspends while it's
//! empty. [`unbounded_channel`] below is the same idea with the
//! capacity check (and therefore any need for the sender to ever wait)
//! removed entirely -- `UnboundedSender::send` is a plain, synchronous
//! method, not `async fn`, since there's genuinely nothing to await.
//!
//! All of the queue, the wakers waiting to send, and the one waker
//! waiting to receive live under a *single* lock. That's deliberate:
//! an earlier version of this split them across separate mutexes (one
//! for the queue, one for the waiter lists), and checking "is there
//! room" under one lock and then registering a waker under a different
//! lock leaves a window where a slot can free up in between and the
//! wakeup is sent to an empty waiter list -- a real lost wakeup, not a
//! hypothetical one (it deadlocked the first version of this file's
//! test suite). One lock covering the check-and-register makes that
//! race impossible by construction instead of requiring a careful
//! recheck-after-register dance. `unbounded_channel`'s own `Inner`
//! reuses this same one-lock shape for the queue/`recv_waker` pair, just
//! without a `send_waiters` list at all -- nothing ever waits to send.

use std::cell::Cell;
use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};

struct Inner<T> {
    queue: VecDeque<T>,
    capacity: usize,
    /// Slots claimed by an outstanding [`Permit`]/[`OwnedPermit`] (or
    /// one not-yet-doled-out by a live [`PermitIterator`]) but not yet
    /// filled with an actual value -- counted separately from
    /// `queue.len()` so `reserve`'s "is there room" check sees them the
    /// same way an already-queued item would (`queue.len() + reserved
    /// < capacity`), without needing a placeholder value in `queue`
    /// itself.
    reserved: usize,
    send_waiters: VecDeque<Waker>,
    recv_waker: Option<Waker>,
    senders_alive: usize,
    receiver_alive: bool,
}

struct Shared<T> {
    inner: Mutex<Inner<T>>,
}

impl<T> Shared<T> {
    /// Gives back one previously-reserved slot that's never going to be
    /// filled after all (a [`Permit`]/[`OwnedPermit`] dropped without
    /// sending, or a [`PermitIterator`] dropped with some of its permits
    /// never handed out) -- wakes up to `n` queued senders, since that
    /// many could now have room where they didn't before.
    fn release_reserved(&self, n: usize) {
        let mut guard = self.inner.lock().unwrap();
        guard.reserved -= n;
        let mut woken = Vec::new();
        for _ in 0..n {
            match guard.send_waiters.pop_front() {
                Some(waker) => woken.push(waker),
                None => break,
            }
        }
        drop(guard);
        for waker in woken {
            waker.wake();
        }
    }

    /// Converts one previously-reserved slot into an actual queued
    /// value ([`Permit::send`]/[`OwnedPermit::send`]) -- total capacity
    /// used doesn't change, so (unlike [`release_reserved`](Self::release_reserved))
    /// this wakes the *receiver*, not another sender.
    fn fill_reserved(&self, value: T) {
        let mut guard = self.inner.lock().unwrap();
        guard.reserved -= 1;
        guard.queue.push_back(value);
        let waker = guard.recv_waker.take();
        drop(guard);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
}

pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "mpsc channel capacity must be positive");
    let shared = Arc::new(Shared {
        inner: Mutex::new(Inner {
            queue: VecDeque::new(),
            capacity,
            reserved: 0,
            send_waiters: VecDeque::new(),
            recv_waker: None,
            senders_alive: 1,
            receiver_alive: true,
        }),
    });
    (
        Sender {
            shared: shared.clone(),
        },
        Receiver { shared },
    )
}

impl<T> Sender<T> {
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        let mut value = Some(value);
        let sent = std::future::poll_fn(|cx| {
            let mut guard = self.shared.inner.lock().unwrap();
            if !guard.receiver_alive {
                return Poll::Ready(false);
            }
            if guard.queue.len() + guard.reserved < guard.capacity {
                guard
                    .queue
                    .push_back(value.take().expect("polled after completion"));
                let waker = guard.recv_waker.take();
                drop(guard);
                if let Some(waker) = waker {
                    waker.wake();
                }
                return Poll::Ready(true);
            }
            guard.send_waiters.push_back(cx.waker().clone());
            Poll::Pending
        })
        .await;

        if sent {
            Ok(())
        } else {
            Err(SendError(
                value.expect("value not consumed on failure path"),
            ))
        }
    }

    /// Waits for capacity, then reserves one slot without filling it
    /// yet -- unlike [`send`](Self::send), the value to put there
    /// doesn't need to exist yet at the time the wait for room happens.
    /// Fill it (synchronously, since the slot's already reserved) via
    /// [`Permit::send`].
    pub async fn reserve(&self) -> Result<Permit<'_, T>, SendError<()>> {
        let reserved = std::future::poll_fn(|cx| self.poll_reserve(cx, 1)).await;
        if reserved {
            Ok(Permit {
                sender: self,
                used: Cell::new(false),
            })
        } else {
            Err(SendError(()))
        }
    }

    /// Like [`reserve`](Self::reserve), but reserves `n` slots at once,
    /// handed back one at a time via the returned [`PermitIterator`].
    pub async fn reserve_many(&self, n: usize) -> Result<PermitIterator<'_, T>, SendError<()>> {
        let reserved = std::future::poll_fn(|cx| self.poll_reserve(cx, n)).await;
        if reserved {
            Ok(PermitIterator {
                sender: self,
                remaining: n,
            })
        } else {
            Err(SendError(()))
        }
    }

    /// Like [`reserve`](Self::reserve), but the returned permit owns a
    /// clone of this `Sender` instead of borrowing it -- usable past
    /// this particular `Sender`'s own lifetime (e.g. moved into a
    /// spawned task), at the cost of consuming this one to make it.
    pub async fn reserve_owned(self) -> Result<OwnedPermit<T>, SendError<()>> {
        let reserved = std::future::poll_fn(|cx| self.poll_reserve(cx, 1)).await;
        if reserved {
            Ok(OwnedPermit { sender: Some(self) })
        } else {
            Err(SendError(()))
        }
    }

    /// Reserves one slot without waiting, failing immediately if the
    /// channel is either full or closed.
    pub fn try_reserve(&self) -> Result<Permit<'_, T>, TrySendError<()>> {
        let mut guard = self.shared.inner.lock().unwrap();
        if !guard.receiver_alive {
            return Err(TrySendError::Closed(()));
        }
        if guard.queue.len() + guard.reserved < guard.capacity {
            guard.reserved += 1;
            Ok(Permit {
                sender: self,
                used: Cell::new(false),
            })
        } else {
            Err(TrySendError::Full(()))
        }
    }

    /// Like [`try_reserve`](Self::try_reserve), but owning -- and, since
    /// it consumes `self`, hands `self` back inside the error on
    /// failure rather than losing it.
    pub fn try_reserve_owned(self) -> Result<OwnedPermit<T>, TrySendError<Self>> {
        let mut guard = self.shared.inner.lock().unwrap();
        if !guard.receiver_alive {
            drop(guard);
            return Err(TrySendError::Closed(self));
        }
        if guard.queue.len() + guard.reserved < guard.capacity {
            guard.reserved += 1;
            drop(guard);
            Ok(OwnedPermit { sender: Some(self) })
        } else {
            drop(guard);
            Err(TrySendError::Full(self))
        }
    }

    /// Shared poll body for [`reserve`](Self::reserve)/
    /// [`reserve_many`](Self::reserve_many)/[`reserve_owned`](Self::reserve_owned)
    /// -- `Ready(true)` once `n` slots are reserved, `Ready(false)` if
    /// the receiver's gone (no reservation made), `Pending` (queued
    /// behind any other waiting sender) otherwise.
    fn poll_reserve(&self, cx: &mut std::task::Context<'_>, n: usize) -> Poll<bool> {
        let mut guard = self.shared.inner.lock().unwrap();
        if !guard.receiver_alive {
            return Poll::Ready(false);
        }
        if guard.queue.len() + guard.reserved + n <= guard.capacity {
            guard.reserved += n;
            return Poll::Ready(true);
        }
        guard.send_waiters.push_back(cx.waker().clone());
        Poll::Pending
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.inner.lock().unwrap().senders_alive += 1;
        Sender {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut guard = self.shared.inner.lock().unwrap();
        guard.senders_alive -= 1;
        if guard.senders_alive == 0 {
            // That was the last sender: wake the receiver so it can see
            // the channel is now closed instead of waiting forever.
            let waker = guard.recv_waker.take();
            drop(guard);
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

/// A reserved (but not yet filled) slot in a bounded channel's buffer,
/// obtained via [`Sender::reserve`]/[`Sender::reserve_many`]. Sending
/// through it (via [`send`](Self::send)) is then synchronous and
/// infallible -- the capacity wait already happened at reservation
/// time -- and dropping it unused gives the slot back for someone else
/// to reserve or send into.
#[must_use = "a Permit does nothing unless `send` is called on it -- dropping it unused just releases the reservation"]
pub struct Permit<'a, T> {
    sender: &'a Sender<T>,
    used: Cell<bool>,
}

impl<T> Permit<'_, T> {
    /// Fills this reservation with `value`. Always succeeds and never
    /// waits -- the capacity was already secured when this `Permit` was
    /// obtained.
    pub fn send(self, value: T) {
        self.used.set(true);
        self.sender.shared.fill_reserved(value);
    }
}

impl<T> Drop for Permit<'_, T> {
    fn drop(&mut self) {
        if !self.used.get() {
            self.sender.shared.release_reserved(1);
        }
    }
}

/// Like [`Permit`], but owns a clone of the `Sender` it came from
/// instead of borrowing it -- obtained via
/// [`Sender::reserve_owned`]/[`Sender::try_reserve_owned`].
#[must_use = "an OwnedPermit does nothing unless `send` (or `release`) is called on it -- dropping it unused just releases the reservation"]
pub struct OwnedPermit<T> {
    // `None` only in between `send`/`release` taking it and the
    // `OwnedPermit` itself actually finishing being dropped -- lets
    // both of those hand the underlying `Sender` back by value without
    // needing `unsafe`/`ManuallyDrop` to move out of a `Drop` type.
    sender: Option<Sender<T>>,
}

impl<T> OwnedPermit<T> {
    /// Fills this reservation with `value`, handing back the `Sender`
    /// this permit was reserved from (unlike [`Permit::send`], which has
    /// no owned `Sender` to give back).
    pub fn send(mut self, value: T) -> Sender<T> {
        let sender = self.sender.take().expect("OwnedPermit already consumed");
        sender.shared.fill_reserved(value);
        sender
    }

    /// Gives up this reservation without sending anything, handing back
    /// the `Sender` -- equivalent to dropping the permit, except the
    /// `Sender` isn't lost along with it.
    pub fn release(mut self) -> Sender<T> {
        let sender = self.sender.take().expect("OwnedPermit already consumed");
        sender.shared.release_reserved(1);
        sender
    }
}

impl<T> Drop for OwnedPermit<T> {
    fn drop(&mut self) {
        if let Some(sender) = &self.sender {
            sender.shared.release_reserved(1);
        }
    }
}

/// Hands out the `n` slots reserved by [`Sender::reserve_many`], one
/// [`Permit`] at a time. Dropping this iterator before exhausting it
/// releases whatever reservations were never handed out.
pub struct PermitIterator<'a, T> {
    sender: &'a Sender<T>,
    remaining: usize,
}

impl<'a, T> Iterator for PermitIterator<'a, T> {
    type Item = Permit<'a, T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        Some(Permit {
            sender: self.sender,
            used: Cell::new(false),
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<T> ExactSizeIterator for PermitIterator<'_, T> {}

impl<T> Drop for PermitIterator<'_, T> {
    fn drop(&mut self) {
        if self.remaining > 0 {
            self.sender.shared.release_reserved(self.remaining);
        }
    }
}

/// Why [`Sender::try_reserve`]/[`Sender::try_reserve_owned`] failed to
/// reserve a slot without waiting.
pub enum TrySendError<T> {
    /// No free capacity right now -- retry later, or use
    /// [`Sender::reserve`]/[`Sender::reserve_many`] to wait for room.
    Full(T),
    /// The receiver has already been dropped, so no capacity will ever
    /// free up again.
    Closed(T),
}

impl<T> TrySendError<T> {
    /// The value (or, for the owning variants, the `Sender`) that
    /// couldn't be used to reserve a slot.
    pub fn into_inner(self) -> T {
        match self {
            TrySendError::Full(t) | TrySendError::Closed(t) => t,
        }
    }

    pub fn is_full(&self) -> bool {
        matches!(self, TrySendError::Full(_))
    }

    pub fn is_closed(&self) -> bool {
        matches!(self, TrySendError::Closed(_))
    }
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => f.write_str("TrySendError::Full(..)"),
            TrySendError::Closed(_) => f.write_str("TrySendError::Closed(..)"),
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => write!(f, "no available capacity"),
            TrySendError::Closed(_) => write!(f, "channel closed"),
        }
    }
}

impl<T> std::error::Error for TrySendError<T> {}

impl<T> Receiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        std::future::poll_fn(|cx| {
            if crate::coop::poll_proceed(cx).is_pending() {
                return Poll::Pending;
            }
            let mut guard = self.shared.inner.lock().unwrap();
            if let Some(v) = guard.queue.pop_front() {
                let waker = guard.send_waiters.pop_front();
                drop(guard);
                if let Some(waker) = waker {
                    waker.wake();
                }
                return Poll::Ready(Some(v));
            }
            if guard.senders_alive == 0 {
                return Poll::Ready(None);
            }
            guard.recv_waker = Some(cx.waker().clone());
            Poll::Pending
        })
        .await
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut guard = self.shared.inner.lock().unwrap();
        guard.receiver_alive = false;
        let waiters = std::mem::take(&mut guard.send_waiters);
        drop(guard);
        for waker in waiters {
            waker.wake();
        }
    }
}

struct UnboundedInner<T> {
    queue: VecDeque<T>,
    recv_waker: Option<Waker>,
    senders_alive: usize,
    receiver_alive: bool,
}

struct UnboundedShared<T> {
    inner: Mutex<UnboundedInner<T>>,
}

pub struct UnboundedSender<T> {
    shared: Arc<UnboundedShared<T>>,
}

pub struct UnboundedReceiver<T> {
    shared: Arc<UnboundedShared<T>>,
}

pub fn unbounded_channel<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let shared = Arc::new(UnboundedShared {
        inner: Mutex::new(UnboundedInner {
            queue: VecDeque::new(),
            recv_waker: None,
            senders_alive: 1,
            receiver_alive: true,
        }),
    });
    (
        UnboundedSender {
            shared: shared.clone(),
        },
        UnboundedReceiver { shared },
    )
}

impl<T> UnboundedSender<T> {
    /// Pushes `value` onto the queue and returns immediately -- there's
    /// no capacity to wait on, so unlike the bounded [`Sender::send`]
    /// this is a plain synchronous method, not `async fn`.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let mut guard = self.shared.inner.lock().unwrap();
        if !guard.receiver_alive {
            drop(guard);
            return Err(SendError(value));
        }
        guard.queue.push_back(value);
        let waker = guard.recv_waker.take();
        drop(guard);
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }
}

impl<T> Clone for UnboundedSender<T> {
    fn clone(&self) -> Self {
        self.shared.inner.lock().unwrap().senders_alive += 1;
        UnboundedSender {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for UnboundedSender<T> {
    fn drop(&mut self) {
        let mut guard = self.shared.inner.lock().unwrap();
        guard.senders_alive -= 1;
        if guard.senders_alive == 0 {
            // That was the last sender: wake the receiver so it can see
            // the channel is now closed instead of waiting forever.
            let waker = guard.recv_waker.take();
            drop(guard);
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

impl<T> UnboundedReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        std::future::poll_fn(|cx| {
            if crate::coop::poll_proceed(cx).is_pending() {
                return Poll::Pending;
            }
            let mut guard = self.shared.inner.lock().unwrap();
            if let Some(v) = guard.queue.pop_front() {
                return Poll::Ready(Some(v));
            }
            if guard.senders_alive == 0 {
                return Poll::Ready(None);
            }
            guard.recv_waker = Some(cx.waker().clone());
            Poll::Pending
        })
        .await
    }
}

impl<T> Drop for UnboundedReceiver<T> {
    fn drop(&mut self) {
        // No send_waiters to wake here, unlike the bounded Receiver's
        // Drop -- an unbounded Sender never waits to send, so there's
        // never anyone parked on this channel besides (at most) the
        // receiver itself, which is what's dropping right now.
        self.shared.inner.lock().unwrap().receiver_alive = false;
    }
}

/// The receiver was dropped, so the value in this error could not be
/// delivered.
pub struct SendError<T>(pub T);

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SendError(..)")
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sending on a closed channel")
    }
}

impl<T> std::error::Error for SendError<T> {}
