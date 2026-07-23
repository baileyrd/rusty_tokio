//! [`JoinHandle`]: the awaitable, abortable handle returned by `spawn`.

use super::{Task, TaskId};
use std::any::Any;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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
    /// Mirrors whether `state` has reached `Done`/`Taken`, but as a plain
    /// `Arc<AtomicBool>` rather than living inside `Mutex<JoinState<T>>`
    /// so [`AbortHandle`]'s `is_finished` closure can capture just this
    /// flag instead of the whole `Arc<JoinInner<T>>` -- capturing the
    /// latter would require `JoinInner<T>: Sync`, which requires `T:
    /// Send`, which would wrongly rule out the non-`Send` `T`s that
    /// `LocalSet::spawn_local` tasks use.
    finished: Arc<AtomicBool>,
}

impl<T> JoinInner<T> {
    pub(super) fn new() -> Self {
        JoinInner {
            state: Mutex::new(JoinState::Running),
            finished: Arc::new(AtomicBool::new(false)),
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
        self.finished.store(true, Ordering::Release);
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

    /// Non-consuming, non-blocking check -- unlike `poll`, doesn't
    /// require a `Context` and never transitions `Done` to `Taken`, so
    /// it can be called any number of times without disturbing a
    /// subsequent real `.await` of the handle.
    fn is_finished(&self) -> bool {
        matches!(
            *self.state.lock().unwrap(),
            JoinState::Done(_) | JoinState::Taken
        )
    }
}

/// An awaitable handle to a spawned task. Dropping it does **not**
/// cancel the task -- it keeps running in the background, same as
/// tokio's `JoinHandle`. Use [`JoinHandle::abort`] for that.
pub struct JoinHandle<T> {
    inner: Arc<JoinInner<T>>,
    id: TaskId,
    /// A thunk that calls the underlying task's own `abort()`, captured
    /// behind a `Weak` (not an owning reference) so holding a
    /// `JoinHandle` never keeps a finished task's state around longer
    /// than it otherwise would be. A closure rather than
    /// `Option<Weak<Task>>` directly so this same `JoinHandle` type can
    /// back tasks spawned onto the multi-threaded scheduler's own
    /// `Task` *or* [`super::local::LocalSet::spawn_local`]'s `LocalTask`
    /// without either one needing to know about the other -- `T`'s own
    /// `Send`-ness (or lack of it) still propagates correctly through
    /// `inner: Arc<JoinInner<T>>` either way, since this closure never
    /// touches `T` at all. `Arc` rather than `Box` so [`AbortHandle`]
    /// (obtained from [`abort_handle`](Self::abort_handle)) can cheaply
    /// clone the exact same thunk instead of needing its own copy.
    abort: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl<T> JoinHandle<T> {
    pub(super) fn new(inner: Arc<JoinInner<T>>, id: TaskId) -> Self {
        JoinHandle {
            inner,
            id,
            abort: None,
        }
    }

    /// This task's stable identity -- unaffected by it completing,
    /// panicking, or being aborted. See [`TaskId`]'s own docs.
    pub fn id(&self) -> TaskId {
        self.id
    }

    pub(super) fn with_task(mut self, task: Arc<Task>) -> Self {
        let weak = Arc::downgrade(&task);
        self.abort = Some(Arc::new(move || {
            if let Some(task) = weak.upgrade() {
                task.abort();
            }
        }));
        self
    }

    /// Like [`with_task`](Self::with_task), but for a task spawned via
    /// [`super::local::LocalSet::spawn_local`] rather than the
    /// multi-threaded scheduler's own `Task`.
    pub(super) fn with_local_task(mut self, task: Arc<super::local::LocalTask>) -> Self {
        let weak = Arc::downgrade(&task);
        self.abort = Some(Arc::new(move || {
            if let Some(task) = weak.upgrade() {
                task.abort();
            }
        }));
        self
    }

    /// Request that the task be cancelled. This is best-effort and
    /// asynchronous: the task stops at its next `.await` point (or
    /// immediately, if it wasn't running at all) rather than mid-poll.
    /// A subsequent `.await` on this handle yields
    /// [`JoinError::is_cancelled`].
    pub fn abort(&self) {
        if let Some(abort) = &self.abort {
            abort();
        }
    }

    /// Non-blocking check for whether the task has already finished
    /// (successfully, panicked, or aborted) -- doesn't consume the
    /// handle or require polling it.
    pub fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }

    /// A separate, cloneable, abort-only capability for this same task
    /// -- unlike this `JoinHandle` itself, an `AbortHandle` can be handed
    /// out to other code (or kept around after this `JoinHandle` drops)
    /// without also granting the ability to `.await` the task's result.
    pub fn abort_handle(&self) -> AbortHandle {
        let finished = self.inner.finished.clone();
        AbortHandle {
            id: self.id,
            abort: self
                .abort
                .clone()
                .unwrap_or_else(|| Arc::new(|| {}) as Arc<dyn Fn() + Send + Sync>),
            is_finished: Arc::new(move || finished.load(Ordering::Acquire)),
        }
    }
}

/// A cloneable, abort-only capability for a spawned task, obtained via
/// [`JoinHandle::abort_handle`]. Keeps working even after the
/// `JoinHandle` it came from is dropped (it doesn't hold or need the
/// join side at all, just a `Weak` reference to the task itself,
/// captured inside the shared `abort`/`is_finished` thunks).
pub struct AbortHandle {
    id: TaskId,
    abort: Arc<dyn Fn() + Send + Sync>,
    is_finished: Arc<dyn Fn() -> bool + Send + Sync>,
}

impl AbortHandle {
    /// This task's stable identity -- the same one `JoinHandle::id`
    /// reports for the same task.
    pub fn id(&self) -> TaskId {
        self.id
    }

    /// Request that the task be cancelled -- see [`JoinHandle::abort`]
    /// for the full semantics (best-effort, asynchronous).
    pub fn abort(&self) {
        (self.abort)();
    }

    /// Non-blocking check for whether the task has already finished.
    pub fn is_finished(&self) -> bool {
        (self.is_finished)()
    }
}

impl Clone for AbortHandle {
    fn clone(&self) -> Self {
        AbortHandle {
            id: self.id,
            abort: self.abort.clone(),
            is_finished: self.is_finished.clone(),
        }
    }
}

impl fmt::Debug for AbortHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AbortHandle").field("id", &self.id).finish()
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

    /// Consumes this error, returning the wrapped panic payload -- the
    /// same value [`std::panic::catch_unwind`] would have returned,
    /// suitable for re-raising via
    /// [`std::panic::resume_unwind`] if the caller wants the original
    /// panic to keep propagating instead of being reported as an
    /// ordinary `JoinError`.
    ///
    /// # Panics
    /// Panics if this error is [`is_cancelled`](Self::is_cancelled)
    /// rather than [`is_panic`](Self::is_panic) -- there's no payload to
    /// return for a task that was aborted rather than panicking.
    pub fn into_panic(self) -> Box<dyn Any + Send + 'static> {
        self.try_into_panic()
            .expect("`into_panic` called on a JoinError that was cancelled, not a panic")
    }

    /// Like [`into_panic`](Self::into_panic), but returns `self` back
    /// (rather than panicking) if this error doesn't actually wrap a
    /// panic.
    pub fn try_into_panic(self) -> Result<Box<dyn Any + Send + 'static>, Self> {
        self.panicked.ok_or(JoinError { panicked: None })
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
