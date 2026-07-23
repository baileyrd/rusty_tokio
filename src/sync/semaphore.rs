//! A counting semaphore: `acquire().await` suspends the task (instead
//! of blocking the worker thread) until a permit is available, capping
//! how many callers can hold one at a time -- e.g. "at most 10
//! concurrent outbound requests."
//!
//! Fair (FIFO) like tokio's own `Semaphore`: an `acquire`/`acquire_many`
//! call only takes the fast (immediate) path when *no one* is already
//! queued, so a caller that arrives while others are waiting always
//! queues behind them rather than possibly jumping ahead just because
//! enough permits happen to be free at that instant.
//!
//! Unlike [`super::Mutex`]/[`super::RwLock`]'s release logic (see their
//! own doc comments for why they specifically *avoid* this), releasing
//! permits back here genuinely does decide, and commit to, exactly
//! which queued waiters get how many permits -- directly in the release
//! path, before waking any of them. That's safe here in a way it isn't
//! for a binary locked/unlocked flag: each waiter gets its own
//! independent `granted` flag, set at most once, by whichever release
//! event decides to grant it; nothing else can ever un-decide or
//! re-decide that later. A waiter's own poll only ever checks its own
//! flag, never re-derives eligibility from the shared permit count the
//! way a `Mutex` waiter re-checks the shared `locked` bit -- so there's
//! no path by which two different decisions could end up made for the
//! same waiter.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Poll, Waker};

struct Waiter {
    needed: usize,
    granted: Arc<AtomicBool>,
    waker: Waker,
}

struct State {
    permits: usize,
    waiters: VecDeque<Waiter>,
}

pub struct Semaphore {
    state: StdMutex<State>,
}

impl Semaphore {
    pub fn new(permits: usize) -> Self {
        Semaphore {
            state: StdMutex::new(State {
                permits,
                waiters: VecDeque::new(),
            }),
        }
    }

    pub fn available_permits(&self) -> usize {
        self.state.lock().unwrap().permits
    }

    /// Adds `n` permits to the semaphore's capacity, waking any queued
    /// waiters that can now proceed -- useful for a semaphore whose
    /// capacity isn't fixed at creation (e.g. starting at zero and
    /// being fed permits as some external resource becomes available).
    pub fn add_permits(&self, n: usize) {
        Self::release(&self.state, n);
    }

    pub async fn acquire(&self) -> SemaphorePermit<'_> {
        self.acquire_many(1).await
    }

    pub async fn acquire_many(&self, n: u32) -> SemaphorePermit<'_> {
        self.acquire_permits(n as usize).await;
        SemaphorePermit {
            semaphore: self,
            permits: n as usize,
        }
    }

    /// Acquires a permit without waiting, failing if fewer than one is
    /// immediately available (or anyone else is already queued -- see
    /// this module's docs on fairness).
    pub fn try_acquire(&self) -> Option<SemaphorePermit<'_>> {
        self.try_acquire_many(1)
    }

    pub fn try_acquire_many(&self, n: u32) -> Option<SemaphorePermit<'_>> {
        let needed = n as usize;
        let mut guard = self.state.lock().unwrap();
        if guard.waiters.is_empty() && guard.permits >= needed {
            guard.permits -= needed;
            Some(SemaphorePermit {
                semaphore: self,
                permits: needed,
            })
        } else {
            None
        }
    }

    /// Like [`acquire`](Self::acquire), but the returned permit owns an
    /// `Arc` clone of the semaphore instead of borrowing it -- usable
    /// past this semaphore's own lifetime, e.g. held across a spawned
    /// task boundary without the call site needing its own separate
    /// `Arc` juggling.
    pub async fn acquire_owned(self: &Arc<Self>) -> OwnedSemaphorePermit {
        self.acquire_many_owned(1).await
    }

    pub async fn acquire_many_owned(self: &Arc<Self>, n: u32) -> OwnedSemaphorePermit {
        self.acquire_permits(n as usize).await;
        OwnedSemaphorePermit {
            semaphore: self.clone(),
            permits: n as usize,
        }
    }

    pub fn try_acquire_owned(self: &Arc<Self>) -> Option<OwnedSemaphorePermit> {
        self.try_acquire_many_owned(1)
    }

    pub fn try_acquire_many_owned(self: &Arc<Self>, n: u32) -> Option<OwnedSemaphorePermit> {
        let needed = n as usize;
        let mut guard = self.state.lock().unwrap();
        if guard.waiters.is_empty() && guard.permits >= needed {
            guard.permits -= needed;
            drop(guard);
            Some(OwnedSemaphorePermit {
                semaphore: self.clone(),
                permits: needed,
            })
        } else {
            None
        }
    }

    /// The actual wait: resolves once `needed` permits have been
    /// reserved for this call, either taken immediately (nobody queued,
    /// enough available) or granted later by a release -- see this
    /// module's docs for why that later grant is safe to decide (and
    /// commit to) directly in the release path.
    async fn acquire_permits(&self, needed: usize) {
        assert!(needed > 0, "must acquire at least one permit");
        let granted = Arc::new(AtomicBool::new(false));
        let mut registered = false;
        std::future::poll_fn(|cx| {
            if granted.load(Ordering::Acquire) {
                return Poll::Ready(());
            }
            let mut guard = self.state.lock().unwrap();
            if !registered {
                if guard.waiters.is_empty() && guard.permits >= needed {
                    guard.permits -= needed;
                    return Poll::Ready(());
                }
                guard.waiters.push_back(Waiter {
                    needed,
                    granted: granted.clone(),
                    waker: cx.waker().clone(),
                });
                registered = true;
            }
            Poll::Pending
        })
        .await
    }

    /// Gives `n` permits back (a guard's `Drop`, or [`add_permits`]),
    /// then grants as many queued waiters, in FIFO order, as the
    /// resulting permit count allows -- stopping at the first one that
    /// doesn't fit, since granting out of order would break the
    /// fairness this module's docs describe.
    fn release(state: &StdMutex<State>, n: usize) {
        let mut guard = state.lock().unwrap();
        guard.permits += n;
        let mut woken = Vec::new();
        while let Some(front) = guard.waiters.front() {
            if front.needed > guard.permits {
                break;
            }
            let waiter = guard.waiters.pop_front().unwrap();
            guard.permits -= waiter.needed;
            waiter.granted.store(true, Ordering::Release);
            woken.push(waiter.waker);
        }
        drop(guard);
        for waker in woken {
            waker.wake();
        }
    }
}

pub struct SemaphorePermit<'a> {
    semaphore: &'a Semaphore,
    permits: usize,
}

impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        Semaphore::release(&self.semaphore.state, self.permits);
    }
}

pub struct OwnedSemaphorePermit {
    semaphore: Arc<Semaphore>,
    permits: usize,
}

impl OwnedSemaphorePermit {
    /// How many permits this one holds (e.g. from
    /// [`acquire_many_owned`](Semaphore::acquire_many_owned)).
    pub fn num_permits(&self) -> usize {
        self.permits
    }

    /// The `Arc`-owned `Semaphore` this permit was acquired from.
    pub fn semaphore(&self) -> &Arc<Semaphore> {
        &self.semaphore
    }

    /// Merges `other`'s permits into `self`, so dropping `self`
    /// afterward releases both at once -- `other` itself is consumed
    /// without releasing anything on its own.
    ///
    /// # Panics
    /// Panics if `other` was acquired from a different `Semaphore`.
    pub fn merge(&mut self, other: Self) {
        assert!(
            Arc::ptr_eq(&self.semaphore, &other.semaphore),
            "merge called with permits from different Semaphores"
        );
        self.permits += other.permits;
        // Skip `other`'s own `Drop` -- it would release its permits
        // back, which its count has instead just been folded into
        // `self` to release later, together.
        std::mem::forget(other);
    }
}

impl Drop for OwnedSemaphorePermit {
    fn drop(&mut self) {
        Semaphore::release(&self.semaphore.state, self.permits);
    }
}
