//! [`SetOnce`]: a cell that can be set at most once, with anyone else
//! able to [`wait`](SetOnce::wait) (asynchronously) for that to happen.
//!
//! Distinct from [`super::OnceCell`]: there's no initializer closure
//! here, and so no "someone else's initializer is currently running"
//! state to track either -- exactly one caller wins a race to
//! [`set`](SetOnce::set) a value (synchronously; there's nothing to
//! await), and everyone else, including a task that started
//! [`wait`](SetOnce::wait)ing before the value even existed, just parks
//! until it shows up.

use std::cell::UnsafeCell;
use std::fmt;
use std::future::poll_fn;
use std::mem::MaybeUninit;
use std::sync::Mutex as StdMutex;
use std::task::{Poll, Waker};

struct Inner {
    initialized: bool,
    /// Wakers for every caller currently parked in
    /// [`SetOnce::wait`](SetOnce::wait), woken and cleared all at once
    /// the moment [`SetOnce::set`] succeeds.
    wakers: Vec<Waker>,
}

/// A cell that's set at most once. See the module docs for how this
/// differs from [`super::OnceCell`].
pub struct SetOnce<T> {
    inner: StdMutex<Inner>,
    // Only ever written while transitioning `inner.initialized` from
    // `false` to `true` (in `set`), and only ever read once
    // `initialized` has already been observed `true` -- past which
    // point it is never written again for the rest of the cell's life.
    value: UnsafeCell<MaybeUninit<T>>,
}

// SAFETY: see `value`'s field docs -- once `initialized` is `true`, the
// value is immutable for the cell's remaining lifetime, so handing out a
// shared `&T` derived from `&self` (not tied to the `inner` mutex guard)
// is sound. Same shape `OnceCell`'s identical `unsafe impl`s already use.
unsafe impl<T: Send> Send for SetOnce<T> {}
unsafe impl<T: Send + Sync> Sync for SetOnce<T> {}

impl<T> SetOnce<T> {
    pub fn new() -> Self {
        SetOnce {
            inner: StdMutex::new(Inner {
                initialized: false,
                wakers: Vec::new(),
            }),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    /// A cell that's already set to `value`.
    pub fn new_with(value: T) -> Self {
        SetOnce {
            inner: StdMutex::new(Inner {
                initialized: true,
                wakers: Vec::new(),
            }),
            value: UnsafeCell::new(MaybeUninit::new(value)),
        }
    }

    /// The current value, if already set -- never waits.
    pub fn get(&self) -> Option<&T> {
        if self.inner.lock().unwrap().initialized {
            // SAFETY: see the struct docs.
            Some(unsafe { (*self.value.get()).assume_init_ref() })
        } else {
            None
        }
    }

    /// Whether the cell has been set yet.
    pub fn initialized(&self) -> bool {
        self.inner.lock().unwrap().initialized
    }

    /// Sets the value if the cell is currently unset. Returns the value
    /// back (inside the error) if it was already set -- `set` never
    /// overwrites a value that's already there.
    pub fn set(&self, value: T) -> Result<(), SetOnceError<T>> {
        let mut guard = self.inner.lock().unwrap();
        if guard.initialized {
            return Err(SetOnceError(value));
        }
        // SAFETY: `initialized` was `false` (about to become `true`
        // below), so no one else has written or read `value` yet.
        unsafe { (*self.value.get()).write(value) };
        guard.initialized = true;
        let wakers = std::mem::take(&mut guard.wakers);
        drop(guard);
        for waker in wakers {
            waker.wake();
        }
        Ok(())
    }

    /// Waits until the cell is set, then returns a reference to the
    /// value -- resolves immediately if it already was set before this
    /// was even called.
    pub async fn wait(&self) -> &T {
        poll_fn(|cx| {
            // Re-checks `initialized` and registers to be woken as one
            // atomic step under `inner`'s lock on every poll (including
            // the first) -- closes the same "condition changed between
            // the check and registering to wait for it" race `OnceCell`'s
            // own docs describe, for the identical reason.
            let mut guard = self.inner.lock().unwrap();
            if guard.initialized {
                return Poll::Ready(());
            }
            guard.wakers.push(cx.waker().clone());
            Poll::Pending
        })
        .await;
        // SAFETY: see `get`.
        unsafe { (*self.value.get()).assume_init_ref() }
    }

    /// Consumes the cell, returning its value if it was set.
    pub fn into_inner(self) -> Option<T> {
        let initialized = self.inner.lock().unwrap().initialized;
        if !initialized {
            return None;
        }
        // SAFETY: `initialized` is `true`, so `value` holds a valid
        // `T`. `mem::forget` afterward skips this cell's own `Drop`
        // impl (which would otherwise try to drop this same value
        // again) -- sound since we've now fully consumed `self` by
        // value and nothing else can reach it.
        let value = unsafe { self.value.get().read().assume_init() };
        std::mem::forget(self);
        Some(value)
    }
}

impl<T> Drop for SetOnce<T> {
    fn drop(&mut self) {
        if self.inner.get_mut().unwrap().initialized {
            // SAFETY: `initialized` is `true`, so `value` holds a valid
            // `T` that nothing else has referenced past `&mut self`
            // being obtainable (we're the sole owner, mid-drop).
            unsafe { (*self.value.get()).assume_init_drop() };
        }
    }
}

impl<T> Default for SetOnce<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: fmt::Debug> fmt::Debug for SetOnce<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.get() {
            Some(value) => f.debug_tuple("SetOnce").field(value).finish(),
            None => f.write_str("SetOnce(unset)"),
        }
    }
}

/// Why [`SetOnce::set`] failed: the cell was already set. The value
/// passed to `set` is always handed back (via
/// [`into_inner`](Self::into_inner)) so it isn't silently dropped.
pub struct SetOnceError<T>(T);

impl<T> SetOnceError<T> {
    /// Recovers the value that failed to be set.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> fmt::Debug for SetOnceError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SetOnceError(..)")
    }
}

impl<T> fmt::Display for SetOnceError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SetOnce was already set")
    }
}

impl<T> std::error::Error for SetOnceError<T> {}
