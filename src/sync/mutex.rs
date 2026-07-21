//! An async-aware mutex: `lock().await` suspends the task (freeing the
//! worker thread to run other work) instead of blocking it, unlike
//! `std::sync::Mutex`.

use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::Mutex as StdMutex;
use std::task::Waker;

struct State {
    locked: bool,
    waiters: VecDeque<Waker>,
}

pub struct Mutex<T> {
    state: StdMutex<State>,
    data: UnsafeCell<T>,
}

// SAFETY: `data` is only ever accessed through a `MutexGuard`, and a
// guard only exists while `state.locked` is true and owned by exactly
// one holder -- the same exclusion contract `std::sync::Mutex` relies
// on to justify the identical unsafe impl.
unsafe impl<T: Send> Send for Mutex<T> {}
unsafe impl<T: Send> Sync for Mutex<T> {}

impl<T> Mutex<T> {
    pub fn new(value: T) -> Self {
        Mutex {
            state: StdMutex::new(State {
                locked: false,
                waiters: VecDeque::new(),
            }),
            data: UnsafeCell::new(value),
        }
    }

    pub async fn lock(&self) -> MutexGuard<'_, T> {
        std::future::poll_fn(|cx| {
            let mut guard = self.state.lock().unwrap();
            if !guard.locked {
                guard.locked = true;
                return std::task::Poll::Ready(());
            }
            guard.waiters.push_back(cx.waker().clone());
            std::task::Poll::Pending
        })
        .await;
        MutexGuard { mutex: self }
    }

    /// Locks without waiting, failing if it's currently held.
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        let mut guard = self.state.lock().unwrap();
        if guard.locked {
            None
        } else {
            guard.locked = true;
            Some(MutexGuard { mutex: self })
        }
    }

    pub fn get_mut(&mut self) -> &mut T {
        // SAFETY: `&mut self` proves exclusive access, no guard can
        // exist concurrently.
        unsafe { &mut *self.data.get() }
    }
}

pub struct MutexGuard<'a, T> {
    mutex: &'a Mutex<T>,
}

impl<T> Deref for MutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: holding a `MutexGuard` proves `state.locked` was set
        // by us and won't be released until this guard drops -- nobody
        // else can be dereferencing `data` at the same time.
        unsafe { &*self.mutex.data.get() }
    }
}

impl<T> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: see `Deref` above.
        unsafe { &mut *self.mutex.data.get() }
    }
}

impl<T> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        let mut guard = self.mutex.state.lock().unwrap();
        guard.locked = false;
        let next = guard.waiters.pop_front();
        drop(guard);
        if let Some(waker) = next {
            waker.wake();
        }
        // Note: a brand-new `lock()` call racing in right after we drop
        // `guard` above could grab the mutex before the woken waiter
        // above gets re-polled. Exclusion is still fully guaranteed
        // (only one caller's `locked = true` check-and-set can ever
        // win), just not strict FIFO fairness -- an acceptable
        // simplification for a hand-rolled mutex.
    }
}
