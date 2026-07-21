//! [`OnceCell`]: initialize a value exactly once, no matter how many
//! tasks concurrently ask for it -- the first caller runs the
//! initializer; everyone else (including callers that arrive *while*
//! that initializer is still running, not just ones that arrive before
//! it starts) parks and gets back the same result instead of racing to
//! initialize independently.
//!
//! **Why this hand-rolls its own waiter list instead of building on
//! [`crate::sync::Notify`]** (the same call [`crate::sync::Barrier`]
//! already makes, for the identical reason -- see that module's own
//! docs): `Notify`'s waiters queue lives behind a *separate* lock from
//! whatever external state a caller checks first, and `Notify::
//! notify_waiters`'s own docs note it deliberately banks nothing for a
//! `notified()` call registered afterward. A caller here that observes
//! `Initializing` and *then* registers with a separately-locked `Notify`
//! has a real gap in between where the in-flight initializer can finish
//! (successfully, or via a panic resetting the cell) and call
//! `notify_waiters` -- landing in that gap means waiting for a
//! notification that will never come, since nothing calls
//! `notify_waiters` again once the cell reaches its terminal
//! `Initialized` state. Folding the waiter list into the *same* lock as
//! `state` closes this: every wait re-checks `state` and registers to be
//! woken as one atomic step under that one lock, so a waiter either sees
//! its wait is already over (no need to register at all) or is
//! guaranteed to land in the waiter list before a completing initializer
//! can possibly drain it.

use std::cell::UnsafeCell;
use std::fmt;
use std::future::{poll_fn, Future};
use std::mem::MaybeUninit;
use std::sync::Mutex as StdMutex;
use std::task::{Poll, Waker};

/// What [`OnceCell::try_claim`] found -- see that method's docs.
enum Claim {
    Ready,
    Initializing,
    Initialize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Uninit,
    Initializing,
    Initialized,
}

struct Inner {
    state: State,
    /// Wakers for every caller currently parked waiting for `state` to
    /// leave `Initializing` (whether that ends in `Initialized`, or back
    /// in `Uninit` after a panic -- either way, every waiter wakes and
    /// re-checks). Woken and cleared all at once on either transition.
    wakers: Vec<Waker>,
}

/// A cell that's initialized at most once. See the module docs for the
/// concurrent-caller behavior.
pub struct OnceCell<T> {
    inner: StdMutex<Inner>,
    // Only ever written while transitioning `inner.state` into
    // `Initialized` (in `get_or_init`/`set`), and only ever read once
    // `state` has already been observed as `Initialized` -- past which
    // point it is never written again for the rest of the cell's life.
    value: UnsafeCell<MaybeUninit<T>>,
}

// SAFETY: see `value`'s field docs -- once `state` is `Initialized`, the
// value is immutable for the cell's remaining lifetime, so handing out a
// shared `&T` derived from `&self` (not tied to the `state` mutex guard)
// is sound. The same "documented exclusivity invariant instead of
// relying on the type system alone" shape `sync::Mutex`/`sync::RwLock`
// already use for their own `unsafe impl`s.
unsafe impl<T: Send> Send for OnceCell<T> {}
unsafe impl<T: Send + Sync> Sync for OnceCell<T> {}

impl<T> OnceCell<T> {
    pub fn new() -> Self {
        OnceCell {
            inner: StdMutex::new(Inner {
                state: State::Uninit,
                wakers: Vec::new(),
            }),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    /// A cell that's already initialized with `value`.
    pub fn new_with(value: T) -> Self {
        OnceCell {
            inner: StdMutex::new(Inner {
                state: State::Initialized,
                wakers: Vec::new(),
            }),
            value: UnsafeCell::new(MaybeUninit::new(value)),
        }
    }

    /// The current value, if already initialized -- never waits or runs
    /// an initializer.
    pub fn get(&self) -> Option<&T> {
        let state = self.inner.lock().unwrap().state;
        if state == State::Initialized {
            // SAFETY: `state == Initialized` guarantees `value` holds a
            // valid, permanently-unmutated `T` -- see the struct docs.
            Some(unsafe { (*self.value.get()).assume_init_ref() })
        } else {
            None
        }
    }

    pub fn initialized(&self) -> bool {
        self.inner.lock().unwrap().state == State::Initialized
    }

    /// Sets the value if the cell is currently uninitialized. Returns
    /// the value back (inside the error) if it was already initialized,
    /// or if another caller's `get_or_init` initializer is currently in
    /// flight.
    pub fn set(&self, value: T) -> Result<(), SetError<T>> {
        let mut guard = self.inner.lock().unwrap();
        match guard.state {
            State::Initialized => Err(SetError::AlreadyInitialized(value)),
            State::Initializing => Err(SetError::InitializingElsewhere(value)),
            State::Uninit => {
                // SAFETY: `state` was `Uninit` (about to become
                // `Initialized` below), so no one else has written or
                // read `value` yet.
                unsafe { (*self.value.get()).write(value) };
                guard.state = State::Initialized;
                let wakers = std::mem::take(&mut guard.wakers);
                drop(guard);
                for waker in wakers {
                    waker.wake();
                }
                Ok(())
            }
        }
    }

    /// Returns the value, running `f`'s future to produce it if the
    /// cell isn't initialized yet. Runs at most once across however
    /// many concurrent callers: a caller that arrives while another's
    /// initializer is already in flight parks instead of running its
    /// own, then observes whatever the winner produced.
    ///
    /// If the winning initializer panics, or is dropped mid-`.await`
    /// (e.g. the task running it is aborted), the cell resets to
    /// uninitialized rather than getting stuck reporting "initializing"
    /// forever -- every other caller currently parked wakes back up and
    /// one of them becomes the new initializer.
    pub async fn get_or_init<F, Fut>(&self, f: F) -> &T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        loop {
            // Kept in its own non-async helper (rather than inlined
            // here with the `inner` lock held across the match arms) so
            // the `MutexGuard` never becomes part of this `async fn`'s
            // generated future -- otherwise the whole future stops
            // being `Send`, since `MutexGuard` isn't.
            match self.try_claim() {
                Claim::Ready => {
                    // SAFETY: see `get`.
                    return unsafe { (*self.value.get()).assume_init_ref() };
                }
                Claim::Initializing => {
                    // Re-checks `state` and registers to be woken as
                    // one atomic step under `inner`'s lock on *every*
                    // poll, including the first -- so even if `state`
                    // already changed since `try_claim` looked (the
                    // in-flight initializer finished in between), this
                    // resolves immediately instead of registering a
                    // waker nothing will ever wake. See the module docs
                    // for the two-lock race this avoids.
                    poll_fn(|cx| {
                        let mut guard = self.inner.lock().unwrap();
                        if guard.state != State::Initializing {
                            return Poll::Ready(());
                        }
                        guard.wakers.push(cx.waker().clone());
                        Poll::Pending
                    })
                    .await;
                    continue;
                }
                Claim::Initialize => break,
            }
        }

        // We're the one initializing -- reset back to `Uninit` (and
        // wake everyone currently parked, so one of them can take over)
        // unless `armed` is cleared below, which only happens after the
        // value is actually written and `state` has already moved to
        // `Initialized`.
        let mut reset_on_incomplete = ResetGuard {
            cell: self,
            armed: true,
        };
        let value = f().await;
        // SAFETY: `state` is still `Initializing` (nothing else writes
        // `value` while it is), so this is the only writer.
        unsafe { (*self.value.get()).write(value) };
        let wakers = {
            let mut guard = self.inner.lock().unwrap();
            guard.state = State::Initialized;
            std::mem::take(&mut guard.wakers)
        };
        reset_on_incomplete.armed = false;
        for waker in wakers {
            waker.wake();
        }
        // SAFETY: see `get`.
        unsafe { (*self.value.get()).assume_init_ref() }
    }

    /// Non-async: checks (and, for `Initialize`, atomically claims) the
    /// current state, entirely within one `inner` lock/unlock -- kept
    /// out of `get_or_init`'s own body so the `MutexGuard` it uses never
    /// crosses an `.await` point.
    fn try_claim(&self) -> Claim {
        let mut guard = self.inner.lock().unwrap();
        match guard.state {
            State::Initialized => Claim::Ready,
            State::Initializing => Claim::Initializing,
            State::Uninit => {
                guard.state = State::Initializing;
                Claim::Initialize
            }
        }
    }

    /// Consumes the cell, returning its value if it was initialized.
    pub fn into_inner(self) -> Option<T> {
        let initialized = self.inner.lock().unwrap().state == State::Initialized;
        if !initialized {
            return None;
        }
        // SAFETY: `state` is `Initialized`, so `value` holds a valid
        // `T`. `mem::forget` afterward skips this cell's own `Drop`
        // impl (which would otherwise try to drop this same value
        // again) -- sound to call unconditionally since we've now
        // fully consumed `self` by value and nothing else can reach it.
        let value = unsafe { self.value.get().read().assume_init() };
        std::mem::forget(self);
        Some(value)
    }
}

/// Resets a cell's state back to `Uninit` (and wakes any other callers
/// currently parked in `get_or_init`) unless `armed` is cleared first --
/// covers both a panicking initializer (this runs during unwinding) and
/// one that's simply dropped without ever completing (the task running
/// it was aborted mid-`.await`), so the cell never gets stuck reporting
/// "initializing" forever for everyone else.
struct ResetGuard<'a, T> {
    cell: &'a OnceCell<T>,
    armed: bool,
}

impl<T> Drop for ResetGuard<'_, T> {
    fn drop(&mut self) {
        if self.armed {
            let wakers = {
                let mut guard = self.cell.inner.lock().unwrap();
                guard.state = State::Uninit;
                std::mem::take(&mut guard.wakers)
            };
            for waker in wakers {
                waker.wake();
            }
        }
    }
}

impl<T> Drop for OnceCell<T> {
    fn drop(&mut self) {
        if self.inner.get_mut().unwrap().state == State::Initialized {
            // SAFETY: `state` is `Initialized`, so `value` holds a
            // valid `T` that nothing else has referenced past `&mut
            // self` being obtainable (we're the sole owner, mid-drop).
            unsafe { (*self.value.get()).assume_init_drop() };
        }
    }
}

impl<T> Default for OnceCell<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: fmt::Debug> fmt::Debug for OnceCell<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.get() {
            Some(value) => f.debug_tuple("OnceCell").field(value).finish(),
            None => f.write_str("OnceCell(uninit)"),
        }
    }
}

/// Why [`OnceCell::set`] failed -- the value passed to `set` is always
/// handed back so it isn't silently dropped.
pub enum SetError<T> {
    /// The cell was already initialized.
    AlreadyInitialized(T),
    /// Another caller's [`OnceCell::get_or_init`] initializer is
    /// currently in flight.
    InitializingElsewhere(T),
}

impl<T> fmt::Debug for SetError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SetError::AlreadyInitialized(_) => write!(f, "SetError::AlreadyInitialized(..)"),
            SetError::InitializingElsewhere(_) => write!(f, "SetError::InitializingElsewhere(..)"),
        }
    }
}

impl<T> fmt::Display for SetError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SetError::AlreadyInitialized(_) => write!(f, "OnceCell was already initialized"),
            SetError::InitializingElsewhere(_) => {
                write!(
                    f,
                    "OnceCell is currently being initialized by another caller"
                )
            }
        }
    }
}

impl<T> std::error::Error for SetError<T> {}
