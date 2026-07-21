//! A single-value, single-use channel: exactly one [`Sender::send`],
//! exactly one `.await` on the [`Receiver`] to get it back (or find out
//! the sender was dropped without sending).

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

enum State<T> {
    Empty,
    Waiting(Waker),
    Value(T),
    Closed,
}

struct Shared<T> {
    state: Mutex<State<T>>,
}

pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State::Empty),
    });
    (
        Sender {
            shared: shared.clone(),
        },
        Receiver { shared },
    )
}

impl<T> Sender<T> {
    /// Sends the value, or hands it back if the receiver was already
    /// dropped (nobody left to receive it).
    pub fn send(self, value: T) -> Result<(), T> {
        let mut guard = self.shared.state.lock().unwrap();
        if matches!(&*guard, State::Closed) {
            return Err(value);
        }
        let old = std::mem::replace(&mut *guard, State::Value(value));
        drop(guard);
        if let State::Waiting(waker) = old {
            waker.wake();
        }
        Ok(())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut guard = self.shared.state.lock().unwrap();
        if matches!(&*guard, State::Empty | State::Waiting(_)) {
            let old = std::mem::replace(&mut *guard, State::Closed);
            drop(guard);
            if let State::Waiting(waker) = old {
                waker.wake();
            }
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        *self.shared.state.lock().unwrap() = State::Closed;
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut guard = self.shared.state.lock().unwrap();
        match &*guard {
            State::Value(_) => {
                let State::Value(v) = std::mem::replace(&mut *guard, State::Closed) else {
                    unreachable!()
                };
                Poll::Ready(Ok(v))
            }
            State::Closed => Poll::Ready(Err(RecvError)),
            State::Empty | State::Waiting(_) => {
                *guard = State::Waiting(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// The sender was dropped without ever calling [`Sender::send`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the sender was dropped without sending a value")
    }
}

impl std::error::Error for RecvError {}
