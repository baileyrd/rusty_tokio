//! An async condition variable: [`Notify::notified`] parks until
//! [`Notify::notify_one`] or [`Notify::notify_waiters`] fires.
//!
//! Each `notified()` call gets its own [`WaiterState`], holding a
//! `woken` flag and the waker to fire once notified -- stored behind a
//! shared `Arc` (not just copied once at registration time) so
//! [`Notified::enable`]/[`OwnedNotified::enable`] can register interest
//! *before* a real waker exists yet (nothing's actually parked, waiting,
//! at that point -- `enable` just wants a concurrent `notify_one`/
//! `notify_waiters` to see this waiter and mark it, closing the race
//! between "check some condition" and "start waiting" a caller might
//! otherwise have), with the real waker filled in later once `poll` is
//! actually called and one exists.
//!
//! A poll that runs because it was woken always finds its own flag
//! already true and returns `Ready` immediately, regardless of
//! `permits` or whether anyone else registered after it. Without a
//! per-waiter flag, `notify_waiters` has a real bug: it wakes every
//! currently-registered waker but (unlike `notify_one`) leaves nothing
//! else behind for that specific waiter to see, so re-polling it
//! afterward would find `permits == 0`, register nothing new (it's
//! already marked `registered`), and return `Pending` forever -- a
//! wakeup that fires but is then silently lost on the very next poll.
//!
//! A `notify_one` with nobody currently waiting still banks a permit for
//! the next `notified()` call, so a wakeup that "arrives early" (before
//! the other side started waiting) isn't lost either.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// Shared per-waiter state, so [`Notified::enable`]/
/// [`OwnedNotified::enable`] can register a waiter before any real
/// waker exists (see this module's own docs), with the real waker
/// filled in later once `poll` actually runs.
struct WaiterState {
    woken: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl WaiterState {
    fn new() -> Arc<Self> {
        Arc::new(WaiterState {
            woken: AtomicBool::new(false),
            waker: Mutex::new(None),
        })
    }
}

struct Inner {
    permits: usize,
    waiters: VecDeque<Arc<WaiterState>>,
}

pub struct Notify {
    inner: Mutex<Inner>,
}

impl Notify {
    pub fn new() -> Self {
        Notify {
            inner: Mutex::new(Inner {
                permits: 0,
                waiters: VecDeque::new(),
            }),
        }
    }

    /// Wakes one waiter, or -- if nobody is currently waiting -- banks a
    /// permit so the very next `notified().await` returns immediately.
    pub fn notify_one(&self) {
        let mut guard = self.inner.lock().unwrap();
        if let Some(state) = guard.waiters.pop_front() {
            drop(guard);
            Self::fire(&state);
        } else {
            guard.permits += 1;
        }
    }

    /// Wakes every task currently waiting -- each one's own `woken` flag
    /// is set first (see this module's docs for why that's required,
    /// unlike a naive port of `notify_one`'s wake-and-hope approach).
    /// Nothing is banked for a `notified()` call made *after* this
    /// returns; that call waits for the next notification, same as
    /// tokio's semantics.
    pub fn notify_waiters(&self) {
        let mut guard = self.inner.lock().unwrap();
        let waiters = std::mem::take(&mut guard.waiters);
        drop(guard);
        for state in waiters {
            Self::fire(&state);
        }
    }

    /// Marks `state` notified and wakes whatever real waker (if any)
    /// has been stored there -- `None` if [`Notified::enable`]
    /// registered it but nothing has actually polled (and thus stored a
    /// real waker) yet, in which case there's nothing to wake: the next
    /// poll will see `woken` already set and return `Ready` immediately
    /// without ever needing a wakeup delivered.
    fn fire(state: &Arc<WaiterState>) {
        state.woken.store(true, Ordering::Release);
        if let Some(waker) = state.waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    pub fn notified(&self) -> Notified<'_> {
        Notified {
            notify: self,
            registered: false,
            state: WaiterState::new(),
        }
    }

    /// Like [`notified`](Self::notified), but the returned future owns
    /// an `Arc` clone of this `Notify` instead of borrowing it -- usable
    /// past this `Notify`'s own lifetime, e.g. moved into a spawned
    /// task.
    pub fn notified_owned(self: &Arc<Self>) -> OwnedNotified {
        OwnedNotified {
            notify: self.clone(),
            registered: false,
            state: WaiterState::new(),
        }
    }
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared poll body for [`Notified`]/[`OwnedNotified`] -- identical
/// either way, since both only ever need `&Notify`.
fn poll_notified(
    notify: &Notify,
    registered: &mut bool,
    state: &Arc<WaiterState>,
    cx: &mut Context<'_>,
) -> Poll<()> {
    if crate::coop::poll_proceed(cx).is_pending() {
        return Poll::Pending;
    }
    // Checked first, before ever touching the shared lock: this is what
    // makes re-polling after a `notify_waiters`-driven wake resolve
    // instead of registering (uselessly, since it's already
    // `registered`) and going back to sleep forever.
    if state.woken.load(Ordering::Acquire) {
        return Poll::Ready(());
    }
    let mut guard = notify.inner.lock().unwrap();
    if !*registered {
        if guard.permits > 0 {
            guard.permits -= 1;
            return Poll::Ready(());
        }
        guard.waiters.push_back(state.clone());
        *registered = true;
    }
    drop(guard);
    // Always (re-)store the real waker -- whether just registered above
    // or already registered earlier (by a previous `poll` whose waker
    // may be stale, or by `enable`, which registers with no waker at
    // all yet).
    *state.waker.lock().unwrap() = Some(cx.waker().clone());
    // Re-check after storing it: a `notify_one`/`notify_waiters` call
    // may have already popped and fired this exact entry in the window
    // between the check above and now.
    if state.woken.load(Ordering::Acquire) {
        return Poll::Ready(());
    }
    Poll::Pending
}

/// Registers `state` as a waiter (if it isn't already), *without* a
/// waker -- see this module's own docs for why that's still useful:
/// closes the race between "check some other condition" and "actually
/// start waiting" without needing anything to poll (and thus have a
/// real waker for) yet. Returns whether this waiter is already
/// notified, either because it already was before this call, or
/// because a banked permit was immediately available.
fn enable(notify: &Notify, registered: &mut bool, state: &Arc<WaiterState>) -> bool {
    if state.woken.load(Ordering::Acquire) {
        return true;
    }
    if *registered {
        return false;
    }
    let mut guard = notify.inner.lock().unwrap();
    if guard.permits > 0 {
        guard.permits -= 1;
        state.woken.store(true, Ordering::Release);
        return true;
    }
    guard.waiters.push_back(state.clone());
    *registered = true;
    false
}

pub struct Notified<'a> {
    notify: &'a Notify,
    registered: bool,
    state: Arc<WaiterState>,
}

impl Notified<'_> {
    /// Registers this waiter now, without waiting for an actual
    /// `.await`/`poll` -- see this module's docs for why that's useful.
    /// Returns whether it's already notified (a banked permit, or an
    /// already-fired notification).
    pub fn enable(self: Pin<&mut Self>) -> bool {
        let this = self.get_mut();
        enable(this.notify, &mut this.registered, &this.state)
    }
}

impl Future for Notified<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        poll_notified(this.notify, &mut this.registered, &this.state, cx)
    }
}

/// The [`Arc`]-owned counterpart of [`Notified`], returned by
/// [`Notify::notified_owned`].
pub struct OwnedNotified {
    notify: Arc<Notify>,
    registered: bool,
    state: Arc<WaiterState>,
}

impl OwnedNotified {
    /// See [`Notified::enable`] -- identical semantics here.
    pub fn enable(self: Pin<&mut Self>) -> bool {
        let this = self.get_mut();
        enable(&this.notify, &mut this.registered, &this.state)
    }
}

impl Future for OwnedNotified {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        poll_notified(&this.notify, &mut this.registered, &this.state, cx)
    }
}
