//! Ambient access to "the current runtime", the way `tokio::spawn()`,
//! `tokio::time::sleep()` etc. work without you having to thread a
//! runtime handle through every function call.

use super::Shared;
use std::cell::RefCell;
use std::sync::Arc;

thread_local! {
    static CURRENT: RefCell<Option<Handle>> = const { RefCell::new(None) };
}

/// A cheap, cloneable reference to a running [`crate::Runtime`]'s
/// scheduler, reactor, and timer driver.
#[derive(Clone)]
pub struct Handle {
    pub(crate) shared: Arc<Shared>,
}

impl Handle {
    /// The handle for the runtime the calling thread is currently
    /// running inside (a worker thread, or a thread inside a
    /// `block_on` call).
    ///
    /// # Panics
    /// Panics if called from a thread with no ambient runtime.
    pub fn current() -> Handle {
        Self::try_current().expect(
            "there is no rusty_tokio runtime running on this thread -- \
             call this from within Runtime::block_on or a spawned task",
        )
    }

    pub fn try_current() -> Option<Handle> {
        CURRENT.with(|c| c.borrow().clone())
    }

    pub fn spawn<F>(&self, future: F) -> crate::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        crate::task::spawn(&self.shared, future)
    }

    /// Run a genuinely blocking closure on the blocking-task thread
    /// pool instead of stalling a worker thread. See
    /// [`crate::spawn_blocking`] for the full contract.
    ///
    /// Implemented by handing the closure to the blocking pool and
    /// spawning an ordinary async task that just awaits a `oneshot` fed
    /// by that pool -- deliberately, not a separate handle type. Doing
    /// it this way means panics, abort, and `.await` on the returned
    /// `JoinHandle` all fall out of the *existing* task machinery for
    /// free: a panicking closure gets `resume_unwind`'d inside the
    /// wrapper task's poll, which the task system already catches and
    /// turns into `JoinError::is_panic()`, the same as any other task.
    /// `abort()` detaches the wrapper task from the result (the
    /// closure's OS thread runs to completion regardless -- there's no
    /// way to preempt a blocking syscall), matching tokio's own
    /// `spawn_blocking` semantics.
    pub fn spawn_blocking<F, T>(&self, f: F) -> crate::task::JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = crate::sync::oneshot::channel::<std::thread::Result<T>>();
        self.shared.blocking_pool.spawn(Box::new(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
            let _ = tx.send(result);
        }));
        self.spawn(async move {
            match rx.await {
                Ok(Ok(value)) => value,
                Ok(Err(panic_payload)) => std::panic::resume_unwind(panic_payload),
                Err(_) => unreachable!("the blocking pool always sends before its sender drops"),
            }
        })
    }
}

/// Installs `handle` as the ambient runtime for as long as the guard
/// lives, restoring whatever was there before on drop (so nested
/// `block_on` calls -- e.g. a test harness inside a bigger runtime --
/// behave sanely).
#[must_use]
pub(crate) struct EnterGuard {
    previous: Option<Handle>,
}

pub(crate) fn enter(shared: Arc<Shared>) -> EnterGuard {
    let previous = CURRENT.with(|c| c.borrow_mut().replace(Handle { shared }));
    EnterGuard { previous }
}

impl Drop for EnterGuard {
    fn drop(&mut self) {
        CURRENT.with(|c| *c.borrow_mut() = self.previous.take());
    }
}
