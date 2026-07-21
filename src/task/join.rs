//! [`JoinHandle`]: the awaitable, abortable handle returned by `spawn`.

use super::Task;
use std::any::Any;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

enum JoinState<T> {
    Running,
    Waiting(Waker),
    Done(Result<T, JoinErrorPayload>),
    Taken,
}

enum JoinErrorPayload {
    Cancelled,
    Panicked(Box<dyn Any + Send + 'static>),
}

/// Why a task ended without ever handing back a value through its
/// normal completion path -- either aborted (possibly before it was
/// ever polled at all) or it panicked mid-poll. Deliberately
/// non-generic so [`super::Task`] (which has no `T`) can hold a hook
/// that produces one of these without knowing the task's output type.
pub(super) enum Outcome {
    Aborted,
    Panicked(Box<dyn Any + Send>),
}

pub(super) type AbnormalHook = Box<dyn FnOnce(Outcome) + Send>;

pub(super) struct JoinInner<T> {
    state: Mutex<JoinState<T>>,
}

impl<T> JoinInner<T> {
    pub(super) fn new() -> Self {
        JoinInner {
            state: Mutex::new(JoinState::Running),
        }
    }

    pub(super) fn complete(&self, value: T) {
        self.finish(Ok(value));
    }

    pub(super) fn finish_abnormal(&self, outcome: Outcome) {
        let payload = match outcome {
            Outcome::Aborted => JoinErrorPayload::Cancelled,
            Outcome::Panicked(p) => JoinErrorPayload::Panicked(p),
        };
        self.finish(Err(payload));
    }

    fn finish(&self, result: Result<T, JoinErrorPayload>) {
        let mut guard = self.state.lock().unwrap();
        let old = std::mem::replace(&mut *guard, JoinState::Done(result));
        drop(guard);
        if let JoinState::Waiting(waker) = old {
            waker.wake();
        }
    }

    fn poll(&self, cx: &mut Context<'_>) -> Poll<Result<T, JoinError>> {
        let mut guard = self.state.lock().unwrap();
        match &*guard {
            JoinState::Done(_) => {
                let JoinState::Done(result) = std::mem::replace(&mut *guard, JoinState::Taken)
                else {
                    unreachable!()
                };
                Poll::Ready(result.map_err(JoinError::from_payload))
            }
            JoinState::Taken => panic!("JoinHandle polled after it already returned Ready"),
            JoinState::Running | JoinState::Waiting(_) => {
                *guard = JoinState::Waiting(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// An awaitable handle to a spawned task. Dropping it does **not**
/// cancel the task -- it keeps running in the background, same as
/// tokio's `JoinHandle`. Use [`JoinHandle::abort`] for that.
pub struct JoinHandle<T> {
    inner: Arc<JoinInner<T>>,
    task: Option<Weak<Task>>,
}

impl<T> JoinHandle<T> {
    pub(super) fn new(inner: Arc<JoinInner<T>>) -> Self {
        JoinHandle { inner, task: None }
    }

    pub(super) fn with_task(mut self, task: Arc<Task>) -> Self {
        self.task = Some(Arc::downgrade(&task));
        self
    }

    /// Request that the task be cancelled. This is best-effort and
    /// asynchronous: the task stops at its next `.await` point (or
    /// immediately, if it wasn't running at all) rather than mid-poll.
    /// A subsequent `.await` on this handle yields
    /// [`JoinError::is_cancelled`].
    pub fn abort(&self) {
        if let Some(task) = self.task.as_ref().and_then(Weak::upgrade) {
            task.abort();
        }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.poll(cx)
    }
}

/// Why a joined task didn't produce a value.
pub struct JoinError {
    panicked: Option<Box<dyn Any + Send + 'static>>,
}

impl JoinError {
    fn from_payload(payload: JoinErrorPayload) -> Self {
        match payload {
            JoinErrorPayload::Cancelled => JoinError { panicked: None },
            JoinErrorPayload::Panicked(p) => JoinError { panicked: Some(p) },
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.panicked.is_none()
    }

    pub fn is_panic(&self) -> bool {
        self.panicked.is_some()
    }
}

impl fmt::Debug for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_panic() {
            write!(f, "JoinError::Panic")
        } else {
            write!(f, "JoinError::Cancelled")
        }
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_panic() {
            write!(f, "task panicked")
        } else {
            write!(f, "task was cancelled")
        }
    }
}

impl std::error::Error for JoinError {}
