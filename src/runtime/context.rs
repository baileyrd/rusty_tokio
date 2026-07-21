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
