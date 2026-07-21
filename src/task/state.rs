//! The atomic bit-state backing [`super::Task`]. See the module docs on
//! `task` for why this exists instead of a plain channel-of-`Arc`
//! design.

use std::sync::atomic::{AtomicU8, Ordering};

const QUEUED: u8 = 1 << 0;
const RUNNING: u8 = 1 << 1;
const NOTIFIED: u8 = 1 << 2;
const COMPLETE: u8 = 1 << 3;
const ABORTED: u8 = 1 << 4;

pub(super) struct State(AtomicU8);

#[derive(Debug, PartialEq, Eq)]
pub(super) enum StateSnapshot {
    ShouldSchedule,
    NoOp,
}

impl State {
    pub(super) fn new() -> Self {
        // A freshly spawned task is handed straight to the scheduler by
        // its creator, so it starts life already QUEUED.
        State(AtomicU8::new(QUEUED))
    }

    pub(super) fn is_aborted(&self) -> bool {
        self.0.load(Ordering::Acquire) & ABORTED != 0
    }

    /// Transition QUEUED -> RUNNING. Returns `false` if the task was
    /// aborted before ever being run (nothing to poll).
    pub(super) fn begin_poll(&self) -> bool {
        let state = self.0.load(Ordering::Acquire);
        debug_assert!(state & QUEUED != 0 && state & RUNNING == 0);
        let new = (state & !(QUEUED | NOTIFIED)) | RUNNING;
        // We are the only thread that can be performing this exact
        // transition (we're the one who just dequeued the task), so a
        // single compare_exchange -- not a retry loop -- is correct:
        // any concurrent `wake()` either observes the old state (still
        // QUEUED, so it no-ops) or the new one (RUNNING, so it sets
        // NOTIFIED instead of touching QUEUED).
        self.0
            .compare_exchange(state, new, Ordering::AcqRel, Ordering::Acquire)
            .expect("begin_poll: task was not in the expected QUEUED state");
        state & ABORTED == 0
    }

    /// Called after a poll returns. `ready` is true for completion
    /// (including a panic, which is treated as completion). Returns
    /// `true` if the caller must re-schedule the task itself because a
    /// wake arrived while it was running.
    pub(super) fn end_poll(&self, ready: bool) -> bool {
        if ready {
            self.0.fetch_or(COMPLETE, Ordering::AcqRel);
            self.0.fetch_and(!RUNNING, Ordering::AcqRel);
            return false;
        }

        let mut state = self.0.load(Ordering::Acquire);
        loop {
            if state & NOTIFIED != 0 {
                let new = (state & !(RUNNING | NOTIFIED)) | QUEUED;
                match self
                    .0
                    .compare_exchange(state, new, Ordering::AcqRel, Ordering::Acquire)
                {
                    Ok(_) => return true,
                    Err(actual) => {
                        state = actual;
                        continue;
                    }
                }
            } else {
                let new = state & !RUNNING;
                match self
                    .0
                    .compare_exchange(state, new, Ordering::AcqRel, Ordering::Acquire)
                {
                    Ok(_) => return false,
                    Err(actual) => {
                        state = actual;
                        continue;
                    }
                }
            }
        }
    }

    /// A waker fired. Returns whether the caller now owns the
    /// responsibility of pushing the task onto a run queue.
    pub(super) fn wake(&self) -> StateSnapshot {
        let mut state = self.0.load(Ordering::Acquire);
        loop {
            if state & COMPLETE != 0 {
                return StateSnapshot::NoOp;
            }
            if state & RUNNING != 0 {
                if state & NOTIFIED != 0 {
                    return StateSnapshot::NoOp;
                }
                match self.0.compare_exchange(
                    state,
                    state | NOTIFIED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return StateSnapshot::NoOp,
                    Err(actual) => {
                        state = actual;
                        continue;
                    }
                }
            }
            if state & QUEUED != 0 {
                return StateSnapshot::NoOp;
            }
            match self.0.compare_exchange(
                state,
                state | QUEUED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return StateSnapshot::ShouldSchedule,
                Err(actual) => {
                    state = actual;
                    continue;
                }
            }
        }
    }

    /// Mark the task as wanting to be dropped instead of polled again,
    /// piggy-backing on the same wake state machine to guarantee it
    /// gets scheduled (if idle) or re-checked (if running/queued).
    pub(super) fn request_abort(&self) -> bool {
        let state = self.0.fetch_or(ABORTED, Ordering::AcqRel);
        if state & COMPLETE != 0 || state & ABORTED != 0 {
            return false;
        }
        self.wake() == StateSnapshot::ShouldSchedule
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_state_is_queued_and_pollable() {
        let s = State::new();
        assert!(s.begin_poll());
    }

    #[test]
    fn wake_while_running_defers_to_notified() {
        let s = State::new();
        assert!(s.begin_poll());
        // Woken mid-poll: not our job to schedule, the poller will.
        assert_eq!(s.wake(), StateSnapshot::NoOp);
        assert!(s.end_poll(false));
    }

    #[test]
    fn wake_while_idle_schedules() {
        let s = State::new();
        assert!(s.begin_poll());
        assert!(!s.end_poll(false));
        assert_eq!(s.wake(), StateSnapshot::ShouldSchedule);
    }

    #[test]
    fn double_wake_only_schedules_once() {
        let s = State::new();
        assert!(s.begin_poll());
        assert!(!s.end_poll(false));
        assert_eq!(s.wake(), StateSnapshot::ShouldSchedule);
        assert_eq!(s.wake(), StateSnapshot::NoOp);
    }

    #[test]
    fn abort_before_first_poll() {
        let s = State::new();
        assert!(!s.request_abort()); // already queued -> no extra schedule needed
        assert!(!s.begin_poll());
    }
}
