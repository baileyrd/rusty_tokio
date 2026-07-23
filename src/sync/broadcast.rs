//! A multi-producer, multi-consumer channel where every receiver gets
//! its own copy of every message -- a real, distinct set of semantics
//! from [`super::mpsc`]'s (where a sent message goes to exactly one
//! receiver), not just "mpsc with multiple receivers."
//!
//! A fixed-capacity ring buffer backs the channel; each [`Receiver`]
//! tracks its own read position (a sequence number) into it rather than
//! actually removing entries, so `send` never waits for room -- once the
//! buffer is full, the oldest message is simply overwritten. A receiver
//! whose read position has fallen behind the oldest message still in
//! the buffer has been lagged: its next `recv()` reports exactly how
//! many messages it missed (`RecvError::Lagged(n)`) and jumps its
//! position forward to the oldest still-available message, rather than
//! reporting `Lagged` again on every subsequent call.
//!
//! The buffer, every receiver's parked waker, and the alive-count
//! bookkeeping all live under a single lock -- the same shape
//! `sync::mpsc`'s own module docs explain the reasoning for: checking
//! "is there something new for me" and registering a waker under
//! separate locks leaves a window where a `send` in between finds no
//! one (yet) registered to wake, a real lost wakeup rather than a
//! hypothetical one.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

struct Inner<T> {
    buffer: VecDeque<T>,
    capacity: usize,
    /// The sequence number the *next* sent message will get. The oldest
    /// message still in `buffer` (if any) has sequence number
    /// `next_seq - buffer.len()`.
    next_seq: u64,
    /// Every receiver currently parked in `recv`, waiting for a message
    /// past whatever it's already seen -- woken in full on every `send`
    /// (and once every sender drops), since a new message means every
    /// currently-parked receiver, by definition already caught up to the
    /// old `next_seq`, now has something new to read.
    waiters: Vec<Waker>,
    senders_alive: usize,
    receivers_alive: usize,
}

struct Shared<T> {
    inner: Mutex<Inner<T>>,
}

pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    next_to_read: u64,
}

/// Creates a broadcast channel with room for `capacity` unread messages
/// before a lagging receiver starts missing them.
///
/// # Panics
/// Panics if `capacity` is zero.
pub fn channel<T: Clone>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "broadcast channel capacity must be positive");
    let shared = Arc::new(Shared {
        inner: Mutex::new(Inner {
            buffer: VecDeque::with_capacity(capacity),
            capacity,
            next_seq: 0,
            waiters: Vec::new(),
            senders_alive: 1,
            receivers_alive: 1,
        }),
    });
    let sender = Sender {
        shared: shared.clone(),
    };
    let receiver = Receiver {
        shared,
        next_to_read: 0,
    };
    (sender, receiver)
}

impl<T: Clone> Sender<T> {
    /// Sends `value` to every current receiver, returning how many
    /// receivers were subscribed at the time -- not a guarantee any of
    /// them will actually observe it before falling too far behind and
    /// getting `RecvError::Lagged` instead. Never waits: once the ring
    /// buffer is full, the oldest message is simply overwritten.
    ///
    /// # Errors
    /// Fails if every receiver has already been dropped.
    pub fn send(&self, value: T) -> Result<usize, SendError<T>> {
        let mut guard = self.shared.inner.lock().unwrap();
        if guard.receivers_alive == 0 {
            drop(guard);
            return Err(SendError(value));
        }
        if guard.buffer.len() == guard.capacity {
            guard.buffer.pop_front();
        }
        guard.buffer.push_back(value);
        guard.next_seq += 1;
        let receivers = guard.receivers_alive;
        let waiters = std::mem::take(&mut guard.waiters);
        drop(guard);
        for waker in waiters {
            waker.wake();
        }
        Ok(receivers)
    }

    /// Creates a new receiver that will see every message sent *after*
    /// this call -- not anything already in the buffer, matching what a
    /// receiver returned from [`channel`] itself sees.
    pub fn subscribe(&self) -> Receiver<T> {
        let mut guard = self.shared.inner.lock().unwrap();
        guard.receivers_alive += 1;
        let next_to_read = guard.next_seq;
        drop(guard);
        Receiver {
            shared: self.shared.clone(),
            next_to_read,
        }
    }

    pub fn receiver_count(&self) -> usize {
        self.shared.inner.lock().unwrap().receivers_alive
    }

    /// A weak handle that doesn't keep the channel's sender count alive
    /// by itself -- see `sync::mpsc`'s own `WeakSender` docs for the
    /// full reasoning, identical here.
    pub fn downgrade(&self) -> WeakSender<T> {
        WeakSender {
            shared: Arc::downgrade(&self.shared),
        }
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
            // That was the last sender: wake every parked receiver so
            // they can see the channel is now closed instead of waiting
            // forever for a message that will never come.
            let waiters = std::mem::take(&mut guard.waiters);
            drop(guard);
            for waker in waiters {
                waker.wake();
            }
        }
    }
}

/// A handle to a broadcast channel's sender that doesn't keep it (or its
/// sender count) alive by itself -- obtained via [`Sender::downgrade`].
/// See `sync::mpsc::WeakSender`'s own docs for the full reasoning,
/// identical here.
pub struct WeakSender<T> {
    shared: Weak<Shared<T>>,
}

impl<T> WeakSender<T> {
    /// Hands back a real, usable `Sender` -- but only if at least one
    /// other `Sender` already exists. `None` once every real `Sender`
    /// has been dropped, even if receivers are still alive.
    pub fn upgrade(&self) -> Option<Sender<T>> {
        let shared = self.shared.upgrade()?;
        let mut guard = shared.inner.lock().unwrap();
        if guard.senders_alive == 0 {
            return None;
        }
        guard.senders_alive += 1;
        drop(guard);
        Some(Sender { shared })
    }
}

impl<T> Clone for WeakSender<T> {
    fn clone(&self) -> Self {
        WeakSender {
            shared: self.shared.clone(),
        }
    }
}

impl<T> fmt::Debug for WeakSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WeakSender").finish()
    }
}

impl<T: Clone> Receiver<T> {
    /// Waits for the next message. Reports `RecvError::Lagged(n)` (and
    /// jumps this receiver's position forward to the oldest message
    /// still buffered) if `n` messages were missed since the last call,
    /// or `RecvError::Closed` once every sender has dropped and there's
    /// nothing left unread.
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        std::future::poll_fn(|cx| self.poll_recv(cx)).await
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        let mut guard = self.shared.inner.lock().unwrap();
        let base_seq = guard.next_seq - guard.buffer.len() as u64;
        if self.next_to_read < base_seq {
            let lagged = base_seq - self.next_to_read;
            self.next_to_read = base_seq;
            return Poll::Ready(Err(RecvError::Lagged(lagged)));
        }
        if self.next_to_read < guard.next_seq {
            let idx = (self.next_to_read - base_seq) as usize;
            let value = guard.buffer[idx].clone();
            self.next_to_read += 1;
            return Poll::Ready(Ok(value));
        }
        if guard.senders_alive == 0 {
            return Poll::Ready(Err(RecvError::Closed));
        }
        guard.waiters.push(cx.waker().clone());
        Poll::Pending
    }

    /// Like [`recv`](Self::recv), but returns immediately instead of
    /// waiting if there's nothing new yet.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let guard = self.shared.inner.lock().unwrap();
        let base_seq = guard.next_seq - guard.buffer.len() as u64;
        if self.next_to_read < base_seq {
            let lagged = base_seq - self.next_to_read;
            self.next_to_read = base_seq;
            return Err(TryRecvError::Lagged(lagged));
        }
        if self.next_to_read < guard.next_seq {
            let idx = (self.next_to_read - base_seq) as usize;
            let value = guard.buffer[idx].clone();
            self.next_to_read += 1;
            return Ok(value);
        }
        if guard.senders_alive == 0 {
            return Err(TryRecvError::Closed);
        }
        Err(TryRecvError::Empty)
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // Dropping a receiver doesn't change whether any *other*
        // receiver has progress to make (there's no shared capacity
        // gated on receiver count the way mpsc's bounded `Sender::send`
        // is), so unlike mpsc's `Receiver::drop`, nothing needs waking
        // here.
        self.shared.inner.lock().unwrap().receivers_alive -= 1;
    }
}

/// Every receiver had already been dropped, so `value` could not be
/// delivered to anyone.
pub struct SendError<T>(pub T);

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SendError(..)")
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sending on a broadcast channel with no receivers left")
    }
}

impl<T> std::error::Error for SendError<T> {}

/// Why [`Receiver::recv`] didn't return a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    /// Every sender has dropped and there's nothing left unread.
    Closed,
    /// This many messages were missed -- the receiver had fallen behind
    /// the oldest message still in the ring buffer. Its read position
    /// has already been advanced to that oldest message, so the next
    /// call resumes from there rather than reporting this again.
    Lagged(u64),
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecvError::Closed => write!(f, "broadcast channel closed"),
            RecvError::Lagged(n) => write!(f, "receiver lagged behind by {n} messages"),
        }
    }
}

impl std::error::Error for RecvError {}

/// Why [`Receiver::try_recv`] didn't return a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// No message available right now, but the channel isn't closed.
    Empty,
    /// Every sender has dropped and there's nothing left unread.
    Closed,
    /// See [`RecvError::Lagged`].
    Lagged(u64),
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "no message currently available"),
            TryRecvError::Closed => write!(f, "broadcast channel closed"),
            TryRecvError::Lagged(n) => write!(f, "receiver lagged behind by {n} messages"),
        }
    }
}

impl std::error::Error for TryRecvError {}
