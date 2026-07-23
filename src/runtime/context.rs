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

    /// Like [`try_current`](Self::try_current), but reports *why* there
    /// isn't a usable ambient runtime instead of collapsing every
    /// failure into one bare `None` -- see [`TryCurrentError`]. Doesn't
    /// change `try_current`'s own signature (a real, if rare, breaking
    /// change for existing callers); this is purely additive.
    pub fn try_current_detailed() -> Result<Handle, TryCurrentError> {
        match CURRENT.try_with(|c| c.borrow().clone()) {
            Ok(Some(handle)) => {
                if handle.shared.is_shutting_down() {
                    Err(TryCurrentError {
                        kind: TryCurrentErrorKind::RuntimeShutDown,
                    })
                } else {
                    Ok(handle)
                }
            }
            Ok(None) => Err(TryCurrentError {
                kind: TryCurrentErrorKind::MissingContext,
            }),
            // Only reachable calling this from within another value's
            // `Drop` impl that itself runs during this thread's own
            // shutdown, after thread-locals have started being torn
            // down -- `LocalKey::with` would otherwise just panic here.
            Err(_access_error) => Err(TryCurrentError {
                kind: TryCurrentErrorKind::ThreadLocalDestroyed,
            }),
        }
    }

    #[track_caller]
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
    #[track_caller]
    pub fn spawn_blocking<F, T>(&self, f: F) -> crate::task::JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = crate::sync::oneshot::channel::<std::thread::Result<T>>();
        // A separate `TaskId`/span from the rendezvous wrapper task
        // spawned below -- see `task::trace`'s module docs for why
        // `spawn_blocking` shows up as two independent console entries
        // rather than one.
        let blocking_id = crate::task::TaskId::next();
        // `()` when the `tracing` feature is off -- see `task::trace`'s
        // module docs for why call sites don't need to `#[cfg]` around
        // this.
        #[allow(clippy::let_unit_value)]
        let span = crate::task::trace::blocking_span(None, blocking_id.as_u64());
        self.shared.blocking_pool.spawn(Box::new(move || {
            #[allow(clippy::let_unit_value)]
            let _guard = crate::task::trace::enter(&span);
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

    /// Non-blocking check: has the runtime started shutting down (via
    /// `Runtime::drop`, `shutdown_background`, or `shutdown_timeout`)?
    /// Useful for a cooperative loop to check between chunks of its own
    /// work -- e.g. `while !handle.is_shutting_down() { ... }` -- without
    /// needing to `.await` anything. See [`Handle::shutdown_notified`]
    /// for the awaitable form.
    pub fn is_shutting_down(&self) -> bool {
        self.shared.is_shutting_down()
    }

    /// Resolves once the runtime starts shutting down -- immediately, if
    /// it already has by the time this is first polled. A task can
    /// `.await` this directly as its entire body (e.g. a dedicated
    /// cleanup task that does nothing until shutdown, then flushes a
    /// buffer or closes a file) and is guaranteed a real chance to be
    /// scheduled and run before the runtime's worker pool stops picking
    /// up tasks.
    ///
    /// A task that wants to race this against its *own* ongoing work
    /// (rather than waiting on it alone) can do so with `select!`.
    pub fn shutdown_notified(&self) -> impl std::future::Future<Output = ()> + Send + '_ {
        self.shared.shutdown_notified()
    }

    /// Whether this handle's runtime was built via
    /// [`super::Builder::new_current_thread`] -- checked by
    /// `time::pause`, which only makes sense on that flavor.
    pub(crate) fn is_current_thread(&self) -> bool {
        self.shared.is_current_thread()
    }

    /// Which scheduling flavor this handle's runtime was built with --
    /// see [`super::RuntimeFlavor`].
    pub fn runtime_flavor(&self) -> super::RuntimeFlavor {
        if self.is_current_thread() {
            super::RuntimeFlavor::CurrentThread
        } else {
            super::RuntimeFlavor::MultiThread
        }
    }

    /// An opaque identifier for this handle's runtime, unique among
    /// other currently-running [`crate::Runtime`]s -- see
    /// [`super::Id`].
    pub fn id(&self) -> super::Id {
        self.shared.id
    }

    /// This runtime's own name, set via [`super::Builder::name`] --
    /// `None` unless that was called.
    pub fn name(&self) -> Option<&str> {
        self.shared.name.as_deref()
    }

    /// A live view into this runtime's scheduler and blocking pool --
    /// queue depths, steal/park counts per worker, blocking-pool thread
    /// count. See [`super::RuntimeMetrics`] for what's on it.
    pub fn metrics(&self) -> super::RuntimeMetrics {
        super::RuntimeMetrics {
            shared: self.shared.clone(),
        }
    }

    /// Installs this runtime as the ambient one for as long as the
    /// returned [`EnterGuard`] lives (restoring whatever was ambient
    /// before once it's dropped) -- without this, constructing
    /// something that needs an ambient runtime (e.g.
    /// [`crate::time::sleep`]) outside a `block_on`/spawned task panics.
    /// Unlike `spawn`, `f` still runs synchronously on the calling
    /// thread; this just makes the runtime *reachable* for the duration
    /// of the call, not scheduled onto the worker pool.
    pub fn enter(&self) -> EnterGuard {
        enter(self.shared.clone())
    }

    /// Runs `f` inline on the calling thread, first handing its other
    /// queued work off to a freshly spawned replacement worker so the
    /// rest of the pool doesn't stall while `f` (expected to block) runs.
    /// See [`crate::task::block_in_place`] for the full contract --
    /// that's the public entry point; this is where it's actually
    /// implemented, since it needs `Shared`/worker-pool access `task`
    /// doesn't have.
    ///
    /// # Panics
    /// See [`crate::task::block_in_place`]'s doc comment.
    pub(crate) fn block_in_place<R>(&self, f: impl FnOnce() -> R) -> R {
        assert!(
            !self.is_current_thread(),
            "block_in_place is not supported on a Builder::new_current_thread() \
             runtime -- there is no worker pool to hand this thread's other \
             queued work off to, and there's only ever the one thread to begin \
             with; use spawn_blocking instead"
        );
        let idx = super::worker::current_worker_index().unwrap_or_else(|| {
            panic!(
                "block_in_place called from a thread with no ambient worker -- \
                 only valid from within a task actually running on a \
                 multi-threaded Runtime's worker pool, not directly inside \
                 block_on or a spawn_blocking closure"
            )
        });
        super::worker::block_in_place(&self.shared, idx, f)
    }
}

/// Installs `handle` as the ambient runtime for as long as the guard
/// lives, restoring whatever was there before on drop (so nested
/// `block_on` calls -- e.g. a test harness inside a bigger runtime --
/// behave sanely).
///
/// Returned by [`Handle::enter`]/[`super::Runtime::enter`] -- lets code
/// that needs the ambient runtime available (e.g. constructing a
/// `crate::time::Sleep` outside an `async fn` this runtime is already
/// driving) do so explicitly, without a full `block_on`/`spawn`.
#[must_use]
pub struct EnterGuard {
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

/// Why [`Handle::try_current_detailed`] failed to find a *usable*
/// ambient runtime -- distinguishes the cases
/// [`Handle::try_current`]'s bare `Option` collapses into one `None`:
/// never inside a runtime at all
/// ([`is_missing_context`](Self::is_missing_context)), this thread's
/// own thread-local storage for the ambient runtime already torn down
/// ([`is_thread_local_destroyed`](Self::is_thread_local_destroyed)), or
/// an ambient runtime that does exist but is itself already shutting
/// down ([`is_rt_shutdown_err`](Self::is_rt_shutdown_err)).
pub struct TryCurrentError {
    kind: TryCurrentErrorKind,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TryCurrentErrorKind {
    MissingContext,
    ThreadLocalDestroyed,
    RuntimeShutDown,
}

impl TryCurrentError {
    /// No ambient runtime at all -- the same case
    /// [`Handle::try_current`] reports as a bare `None`.
    pub fn is_missing_context(&self) -> bool {
        self.kind == TryCurrentErrorKind::MissingContext
    }

    /// This thread's own thread-local storage for the ambient runtime
    /// has already been torn down. Only reachable calling
    /// [`Handle::try_current_detailed`] from within another value's
    /// `Drop` impl that itself runs during this thread's own shutdown,
    /// after thread-locals start being destroyed.
    pub fn is_thread_local_destroyed(&self) -> bool {
        self.kind == TryCurrentErrorKind::ThreadLocalDestroyed
    }

    /// An ambient runtime does exist, but it's already begun shutting
    /// down (`Runtime::drop`/`shutdown_background`/`shutdown_timeout`)
    /// -- distinct from
    /// [`is_missing_context`](Self::is_missing_context): there's a real
    /// `Handle` to have, it's just not safe to keep relying on for new
    /// work.
    pub fn is_rt_shutdown_err(&self) -> bool {
        self.kind == TryCurrentErrorKind::RuntimeShutDown
    }
}

impl std::fmt::Debug for TryCurrentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TryCurrentError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl std::fmt::Display for TryCurrentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            TryCurrentErrorKind::MissingContext => {
                write!(f, "there is no rusty_tokio runtime running on this thread")
            }
            TryCurrentErrorKind::ThreadLocalDestroyed => write!(
                f,
                "the thread-local storage tracking the ambient rusty_tokio \
                 runtime has already been destroyed"
            ),
            TryCurrentErrorKind::RuntimeShutDown => write!(
                f,
                "the rusty_tokio runtime running on this thread has already \
                 begun shutting down"
            ),
        }
    }
}

impl std::error::Error for TryCurrentError {}
