//! [`OnceCell`]: initialize a value exactly once, no matter how many
//! tasks concurrently ask for it -- the first caller runs the
//! initializer; everyone else (including callers that arrive *while*
//! that initializer is still running, not just ones that arrive before
//! it starts) parks on a [`Notify`] and gets back the same result
//! instead of racing to initialize independently.

use crate::sync::Notify;
use std::cell::UnsafeCell;
use std::fmt;
use std::future::Future;
use std::mem::MaybeUninit;
use std::sync::Mutex as StdMutex;

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

/// A cell that's initialized at most once. See the module docs for the
/// concurrent-caller behavior.
pub struct OnceCell<T> {
    state: StdMutex<State>,
    // Only ever written while transitioning `state` into `Initialized`
    // (in `get_or_init`/`set`), and only ever read once `state` has
    // already been observed as `Initialized` -- past which point it is
    // never written again for the rest of the cell's life.
    value: UnsafeCell<MaybeUninit<T>>,
    notify: Notify,
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
            state: StdMutex::new(State::Uninit),
            value: UnsafeCell::new(MaybeUninit::uninit()),
            notify: Notify::new(),
        }
    }

    /// A cell that's already initialized with `value`.
    pub fn new_with(value: T) -> Self {
        OnceCell {
            state: StdMutex::new(State::Initialized),
            value: UnsafeCell::new(MaybeUninit::new(value)),
            notify: Notify::new(),
        }
    }

    /// The current value, if already initialized -- never waits or runs
    /// an initializer.
    pub fn get(&self) -> Option<&T> {
        let state = *self.state.lock().unwrap();
        if state == State::Initialized {
            // SAFETY: `state == Initialized` guarantees `value` holds a
            // valid, permanently-unmutated `T` -- see the struct docs.
            Some(unsafe { (*self.value.get()).assume_init_ref() })
        } else {
            None
        }
    }

    pub fn initialized(&self) -> bool {
        *self.state.lock().unwrap() == State::Initialized
    }

    /// Sets the value if the cell is currently uninitialized. Returns
    /// the value back (inside the error) if it was already initialized,
    /// or if another caller's `get_or_init` initializer is currently in
    /// flight.
    pub fn set(&self, value: T) -> Result<(), SetError<T>> {
        let mut guard = self.state.lock().unwrap();
        match *guard {
            State::Initialized => Err(SetError::AlreadyInitialized(value)),
            State::Initializing => Err(SetError::InitializingElsewhere(value)),
            State::Uninit => {
                // SAFETY: `state` was `Uninit` (about to become
                // `Initialized` below), so no one else has written or
                // read `value` yet.
                unsafe { (*self.value.get()).write(value) };
                *guard = State::Initialized;
                drop(guard);
                self.notify.notify_waiters();
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
            // here with the `state` lock held across the match arms)
            // so the `MutexGuard` never becomes part of this `async
            // fn`'s generated future -- otherwise the whole future
            // stops being `Send`, since `MutexGuard` isn't.
            match self.try_claim() {
                Claim::Ready => {
                    // SAFETY: see `get`.
                    return unsafe { (*self.value.get()).assume_init_ref() };
                }
                Claim::Initializing => {
                    self.notify.notified().await;
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
        *self.state.lock().unwrap() = State::Initialized;
        reset_on_incomplete.armed = false;
        self.notify.notify_waiters();
        // SAFETY: see `get`.
        unsafe { (*self.value.get()).assume_init_ref() }
    }

    /// Non-async: checks (and, for `Initialize`, atomically claims) the
    /// current state, entirely within one `state` lock/unlock -- kept
    /// out of `get_or_init`'s own body so the `MutexGuard` it uses never
    /// crosses an `.await` point.
    fn try_claim(&self) -> Claim {
        let mut guard = self.state.lock().unwrap();
        match *guard {
            State::Initialized => Claim::Ready,
            State::Initializing => Claim::Initializing,
            State::Uninit => {
                *guard = State::Initializing;
                Claim::Initialize
            }
        }
    }

    /// Consumes the cell, returning its value if it was initialized.
    pub fn into_inner(self) -> Option<T> {
        let initialized = *self.state.lock().unwrap() == State::Initialized;
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
            *self.cell.state.lock().unwrap() = State::Uninit;
            self.cell.notify.notify_waiters();
        }
    }
}

impl<T> Drop for OnceCell<T> {
    fn drop(&mut self) {
        if *self.state.get_mut().unwrap() == State::Initialized {
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
