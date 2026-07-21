//! An async-aware reader/writer lock: many concurrent `read().await`
//! guards, or one exclusive `write().await` guard, never blocking the
//! worker thread the way `std::sync::RwLock` would.
//!
//! Write-preferring, matching tokio's own `RwLock`: once a writer is
//! waiting, later readers queue behind it too rather than jumping ahead
//! just because the write lock itself isn't held yet -- otherwise
//! constant read traffic could starve a waiting writer indefinitely.
//! Readers only ever check "is a writer queued at all" (not just "is one
//! currently holding"), which is what makes that guarantee hold.
//!
//! Release (`RwLockReadGuard`/`RwLockWriteGuard`'s `Drop`) only ever
//! *clears* this lock's own contribution and wakes whichever waiters
//! are now eligible -- it never mutates `readers`/`writer_locked` on
//! their behalf. Each woken waiter's own next poll re-checks and
//! self-grants independently, the same principle [`super::Mutex`]'s
//! `Drop` already relies on (see its own doc comment) for why: pre-
//! granting state in the release path, ahead of the waiter's own poll
//! actually running, risks two different tasks both believing they
//! hold the lock if their re-polls happen to interleave unexpectedly.
//! The trade-off is the same one `Mutex` accepts too -- a brand-new
//! `read()`/`write()` call racing in right after a release could
//! acquire before an already-queued waiter gets re-polled; correctness
//! (never two writers, or a writer and a reader, active together) is
//! still fully guaranteed, just not strict FIFO fairness against a
//! fresh (non-queued) caller.

use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::Mutex as StdMutex;
use std::task::{Poll, Waker};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Read,
    Write,
}

struct State {
    writer_locked: bool,
    readers: usize,
    waiters: VecDeque<(Mode, Waker)>,
}

impl State {
    fn writer_pending(&self) -> bool {
        self.waiters.iter().any(|(mode, _)| *mode == Mode::Write)
    }

    /// Pops and returns the wakers of every waiter now eligible to run,
    /// given the current (already-updated) state -- called from both
    /// guards' `Drop` after clearing their own contribution, at which
    /// point `writer_locked` is always already `false` (either just
    /// cleared by the write guard calling this, or -- for the read
    /// guard's case -- it could only still be `true` if a writer were
    /// somehow active *and* readers had just reached zero, which can't
    /// happen: a writer never holds while any reader is active). Never
    /// mutates `readers`/`writer_locked` itself; see this module's docs
    /// for why that matters.
    fn wake_eligible(&mut self) -> Vec<Waker> {
        let mut woken = Vec::new();
        while matches!(self.waiters.front(), Some((Mode::Read, _))) {
            woken.push(self.waiters.pop_front().unwrap().1);
        }
        // Only ever check for a queued writer in the *same* release
        // event when no readers were just woken above -- waking a
        // writer alongside a just-woken reader batch would mean
        // deciding "readers == 0" before any of those readers have
        // actually re-polled and incremented it themselves, which could
        // let the writer and that reader batch both believe they hold
        // the lock if their re-polls happen to interleave unexpectedly.
        // The writer gets its turn one release later instead, once the
        // last of that reader batch's own guards actually drops.
        if woken.is_empty() && self.readers == 0 {
            if let Some((Mode::Write, _)) = self.waiters.front() {
                woken.push(self.waiters.pop_front().unwrap().1);
            }
        }
        woken
    }
}

pub struct RwLock<T> {
    state: StdMutex<State>,
    data: UnsafeCell<T>,
}

// SAFETY: `data` is only ever accessed through a guard, and a write
// guard only exists while `writer_locked` is exclusively held -- the
// same exclusion contract `std::sync::RwLock` relies on for its
// identical bounds (`Send` needs only `T: Send`; `Sync` -- letting
// `&RwLock<T>` itself cross threads, which is what lets multiple
// readers share `&T` concurrently -- additionally needs `T: Sync`).
unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send + Sync> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    pub fn new(value: T) -> Self {
        RwLock {
            state: StdMutex::new(State {
                writer_locked: false,
                readers: 0,
                waiters: VecDeque::new(),
            }),
            data: UnsafeCell::new(value),
        }
    }

    pub async fn read(&self) -> RwLockReadGuard<'_, T> {
        std::future::poll_fn(|cx| {
            let mut guard = self.state.lock().unwrap();
            if !guard.writer_locked && !guard.writer_pending() {
                guard.readers += 1;
                return Poll::Ready(());
            }
            guard.waiters.push_back((Mode::Read, cx.waker().clone()));
            Poll::Pending
        })
        .await;
        RwLockReadGuard { lock: self }
    }

    pub async fn write(&self) -> RwLockWriteGuard<'_, T> {
        std::future::poll_fn(|cx| {
            let mut guard = self.state.lock().unwrap();
            if !guard.writer_locked && guard.readers == 0 {
                guard.writer_locked = true;
                return Poll::Ready(());
            }
            guard.waiters.push_back((Mode::Write, cx.waker().clone()));
            Poll::Pending
        })
        .await;
        RwLockWriteGuard { lock: self }
    }

    /// Acquires a read lock without waiting, failing if a writer
    /// currently holds it or is queued (see this module's docs for why
    /// a *queued* writer also fails this, not just a holding one).
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        let mut guard = self.state.lock().unwrap();
        if !guard.writer_locked && !guard.writer_pending() {
            guard.readers += 1;
            Some(RwLockReadGuard { lock: self })
        } else {
            None
        }
    }

    /// Acquires the write lock without waiting, failing if it's
    /// currently held or any readers are active.
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        let mut guard = self.state.lock().unwrap();
        if !guard.writer_locked && guard.readers == 0 {
            guard.writer_locked = true;
            Some(RwLockWriteGuard { lock: self })
        } else {
            None
        }
    }

    pub fn get_mut(&mut self) -> &mut T {
        // SAFETY: `&mut self` proves exclusive access, no guard can
        // exist concurrently.
        unsafe { &mut *self.data.get() }
    }
}

pub struct RwLockReadGuard<'a, T> {
    lock: &'a RwLock<T>,
}

impl<T> Deref for RwLockReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: holding a read guard proves `readers` was incremented
        // by us and won't be invalidated by a concurrent writer until
        // every read guard (including this one) drops.
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> Drop for RwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        let mut guard = self.lock.state.lock().unwrap();
        guard.readers -= 1;
        let woken = if guard.readers == 0 {
            guard.wake_eligible()
        } else {
            Vec::new()
        };
        drop(guard);
        for waker in woken {
            waker.wake();
        }
    }
}

pub struct RwLockWriteGuard<'a, T> {
    lock: &'a RwLock<T>,
}

impl<T> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: see `DerefMut` below.
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: holding a write guard proves `writer_locked` was set
        // by us and won't be released until this guard drops -- nobody
        // else (reader or writer) can be accessing `data` at the same
        // time.
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<T> Drop for RwLockWriteGuard<'_, T> {
    fn drop(&mut self) {
        let mut guard = self.lock.state.lock().unwrap();
        guard.writer_locked = false;
        let woken = guard.wake_eligible();
        drop(guard);
        for waker in woken {
            waker.wake();
        }
    }
}
