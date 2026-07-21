//! [`LocalKey`]: the type [`crate::task_local!`] declares a `static` of
//! -- implicit, per-task context (a request ID, a connection-scoped
//! config value) that inner async calls can read via `KEY.with(...)`
//! without it being threaded through every function signature
//! explicitly. Similar to `std::thread_local!`, but scoped to a *task's*
//! execution rather than an OS thread: since one worker thread runs
//! many tasks' polls interleaved, a plain `thread_local!` value would
//! leak between unrelated tasks sharing that thread.
//!
//! [`LocalKey::scope`] is how a value actually becomes visible: the
//! returned [`TaskLocalFuture`] sets the real, underlying
//! `thread_local!` slot for its key to the scoped value for the exact
//! duration of each poll of the wrapped future, and restores whatever
//! was there before (usually nothing) immediately afterward -- even if
//! that poll panics. So a *different* task polled on the same thread in
//! between two polls of this one never sees the value, and neither does
//! this future itself while it's sitting `Pending` off-thread; only
//! this future's own call tree does, and only while actually running.

use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::task::{Context, Poll};

/// What [`crate::task_local!`] declares a `static` of. Constructed by
/// that macro, not directly -- `__inner` is a public field only because
/// the macro expands in the caller's own crate and needs to build one,
/// not an API meant to be touched directly.
pub struct LocalKey<T: 'static> {
    #[doc(hidden)]
    pub __inner: std::thread::LocalKey<RefCell<Option<T>>>,
}

impl<T: 'static> LocalKey<T> {
    /// Reads the current task's value for this key.
    ///
    /// # Panics
    /// Panics if the calling task never entered a
    /// [`scope`](Self::scope)/[`sync_scope`](Self::sync_scope) for this
    /// key -- see [`try_with`](Self::try_with) for a non-panicking form.
    pub fn with<F, R>(&'static self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        self.try_with(f)
            .expect("cannot access a task-local value outside of its scope()/sync_scope() future")
    }

    /// Like [`with`](Self::with), but returns an error instead of
    /// panicking if this key isn't currently in scope.
    pub fn try_with<F, R>(&'static self, f: F) -> Result<R, AccessError>
    where
        F: FnOnce(&T) -> R,
    {
        self.__inner
            .with(|cell| cell.borrow().as_ref().map(f))
            .ok_or(AccessError { _private: () })
    }

    /// Sets this key's value to `value` for the duration of `future` --
    /// see the module docs for exactly when it's visible. Awaiting the
    /// returned future runs `future` to completion with the value in
    /// scope.
    pub fn scope<F>(&'static self, value: T, future: F) -> TaskLocalFuture<T, F>
    where
        F: Future,
    {
        TaskLocalFuture {
            key: self,
            slot: Some(value),
            future: Box::pin(future),
        }
    }

    /// Like [`scope`](Self::scope), but for a plain synchronous closure
    /// rather than a future -- sets the value for exactly the duration
    /// of the call to `f`, restoring whatever was there before
    /// (including if `f` panics) immediately afterward.
    pub fn sync_scope<F, R>(&'static self, value: T, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let previous = self.__inner.with(|cell| cell.borrow_mut().replace(value));

        struct Restore<T: 'static> {
            // Hardcoded `'static` (not a generic lifetime parameter):
            // the only way to actually obtain a `&LocalKey<T>` is via
            // `task_local!`'s generated `static` item, so this is never
            // less than `'static` in practice -- and a `Drop` impl for a
            // type with its own lifetime parameter has to soundly cover
            // *every* possible lifetime, which would reject the
            // `'static`-only `LocalKey::with` call below even though
            // every real caller only ever passes `'static` anyway.
            key: &'static LocalKey<T>,
            previous: Option<T>,
        }
        impl<T> Drop for Restore<T> {
            fn drop(&mut self) {
                self.key
                    .__inner
                    .with(|cell| *cell.borrow_mut() = self.previous.take());
            }
        }
        let _restore = Restore {
            key: self,
            previous,
        };

        f()
    }
}

/// Why [`LocalKey::try_with`] failed -- the calling task never entered a
/// `scope()`/`sync_scope()` for this key.
#[derive(Debug, Clone, Copy)]
pub struct AccessError {
    _private: (),
}

impl fmt::Display for AccessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "task-local value not set for the current task")
    }
}

impl std::error::Error for AccessError {}

/// The future returned by [`LocalKey::scope`] -- see the module docs
/// for exactly when the scoped value is visible.
pub struct TaskLocalFuture<T: 'static, F> {
    key: &'static LocalKey<T>,
    slot: Option<T>,
    future: Pin<Box<F>>,
}

impl<T: 'static, F: Future> Future for TaskLocalFuture<T, F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        // SAFETY: `future` is already independently pinned via
        // `Pin<Box<F>>` (the `Box` provides its own heap-allocated,
        // never-moved backing regardless of whether `Self` itself
        // moves) -- calling `.as_mut()` on it below only ever reborrows
        // that existing pin, never moves `F` out. `slot`/`key` are
        // plain owned data we freely swap in and out, never pinned and
        // never moved out of the struct while it's borrowed here. So
        // this never violates the pin contract even though `T`/`F`
        // themselves aren't required to be `Unpin`.
        let this = unsafe { self.get_unchecked_mut() };
        let value = this
            .slot
            .take()
            .expect("TaskLocalFuture polled after it already completed");
        let previous = this
            .key
            .__inner
            .with(|cell| cell.borrow_mut().replace(value));

        // Wrapped in `catch_unwind` so the thread-local slot is always
        // restored -- even if the inner future's poll panics -- before
        // the panic continues propagating outward. Without this, a
        // panicking task could leave its scoped value visible to a
        // later, unrelated task polled on the same thread.
        let result = panic::catch_unwind(AssertUnwindSafe(|| this.future.as_mut().poll(cx)));

        let current = this.key.__inner.with(|cell| cell.borrow_mut().take());
        this.key.__inner.with(|cell| *cell.borrow_mut() = previous);

        match result {
            Ok(Poll::Pending) => {
                this.slot = current;
                Poll::Pending
            }
            Ok(Poll::Ready(value)) => Poll::Ready(value),
            Err(payload) => panic::resume_unwind(payload),
        }
    }
}
