//! A bounded, multi-producer single-consumer queue. `send().await`
//! suspends the sending task while the buffer is full instead of
//! blocking the worker thread; `recv().await` suspends while it's
//! empty.
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
//! recheck-after-register dance.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};

struct Inner<T> {
    queue: VecDeque<T>,
    capacity: usize,
    send_waiters: VecDeque<Waker>,
    recv_waker: Option<Waker>,
    senders_alive: usize,
    receiver_alive: bool,
}

struct Shared<T> {
    inner: Mutex<Inner<T>>,
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
            if guard.queue.len() < guard.capacity {
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

impl<T> Receiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        std::future::poll_fn(|cx| {
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
