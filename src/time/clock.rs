//! [`Clock`]: the "what time is it right now" source every timer
//! deadline is computed against and compared to. Real by default
//! (`Instant::now()`); [`super::pause`]/[`super::advance`] switch a
//! specific runtime's clock to a manually-driven virtual one instead,
//! for deterministic timer tests that don't want to wait on real wall
//! time.
//!
//! A paused clock's "now" is represented as a plain `Instant` (there's
//! no way to construct an arbitrary `Instant` from scratch in stable
//! `std`, only to add/subtract a `Duration` from an existing one) --
//! frozen at whatever real `Instant::now()` was at the moment `pause()`
//! was called, then only ever moved forward explicitly by `advance()`.
//! Timer deadlines themselves are always plain `Instant` values
//! regardless of which kind of "now" computed them, so nothing
//! registered before a pause (or after a resume) needs reconciling --
//! comparing an `Instant` against another `Instant` works the same
//! either way.

use std::sync::Mutex;
use std::time::Instant;

pub(crate) struct Clock {
    /// `Some(virtual_now)` while paused; `None` for the default,
    /// real-time behavior.
    paused: Mutex<Option<Instant>>,
}

impl Clock {
    pub(crate) fn new() -> Self {
        Clock {
            paused: Mutex::new(None),
        }
    }

    pub(crate) fn now(&self) -> Instant {
        match *self.paused.lock().unwrap() {
            Some(virtual_now) => virtual_now,
            None => Instant::now(),
        }
    }

    pub(crate) fn is_paused(&self) -> bool {
        self.paused.lock().unwrap().is_some()
    }

    /// # Panics
    /// Panics if already paused.
    pub(crate) fn pause(&self) {
        let mut guard = self.paused.lock().unwrap();
        assert!(guard.is_none(), "time is already paused");
        *guard = Some(Instant::now());
    }

    /// Unfreezes the clock -- `now()` goes back to tracking real time.
    /// Any timer whose deadline was computed relative to the frozen
    /// virtual clock and has since fallen behind real time simply fires
    /// on the next check, the same as any other now-elapsed deadline;
    /// there's no special "catch-up" behavior beyond that.
    ///
    /// # Panics
    /// Panics if not currently paused.
    pub(crate) fn resume(&self) {
        let mut guard = self.paused.lock().unwrap();
        assert!(guard.is_some(), "time is not paused");
        *guard = None;
    }

    /// Moves the frozen virtual clock directly to `instant`.
    ///
    /// # Panics
    /// Panics if not currently paused.
    pub(crate) fn set(&self, instant: Instant) {
        let mut guard = self.paused.lock().unwrap();
        assert!(guard.is_some(), "time is not paused");
        *guard = Some(instant);
    }
}
