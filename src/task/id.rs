//! [`TaskId`]: a stable identity for a spawned task, independent of
//! holding any reference to it -- assigned once at spawn time from a
//! process-global monotonic counter (mirroring the `next_id` pattern
//! `time::TimerDriver` already uses for its own timer registrations).
//! Survives the task completing, panicking, or being aborted, since
//! it's just a plain number, not tied to the task's own lifetime.
//!
//! Also the thread-local machinery behind [`try_id`]/[`try_name`]: set
//! for the exact duration of a task's own poll call (see
//! [`super::Task::run`]/`LocalTask::run`), so code running *inside* a
//! spawned future's body can read its own task's identity without it
//! being threaded through explicitly as a parameter -- the same shape
//! `runtime::context`'s ambient `Handle::current()` already uses for a
//! different piece of ambient state.

use std::cell::RefCell;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// A stable identity for a spawned task, unique across the whole
/// process (not just one runtime). The only guarantee is uniqueness --
/// IDs happen to be handed out in increasing order, but that's an
/// implementation detail, not something to rely on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(u64);

impl TaskId {
    pub(crate) fn next() -> Self {
        TaskId(NEXT_ID.fetch_add(1, Ordering::Relaxed))
    }

    /// The raw numeric ID -- used as the `task.id` field on this task's
    /// `tracing` span (see `task::trace`) when the `tracing` feature is
    /// enabled; purely a display value there, not a correlation key.
    #[cfg_attr(not(feature = "tracing"), allow(dead_code))]
    pub(crate) fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone)]
pub(crate) struct CurrentTask {
    pub(crate) id: TaskId,
    pub(crate) name: Option<Arc<str>>,
}

thread_local! {
    static CURRENT: RefCell<Option<CurrentTask>> = const { RefCell::new(None) };
}

/// Installs `current` as the ambient task identity for as long as the
/// guard lives, restoring whatever was there before on drop -- mirrors
/// `runtime::context::EnterGuard`. Scoped tightly around a single poll
/// call by `Task::run`/`LocalTask::run`, not the task's whole lifetime,
/// since that's the only span in which "the task currently running on
/// this thread" is actually well-defined.
#[must_use]
pub(crate) struct EnterGuard {
    previous: Option<CurrentTask>,
}

pub(crate) fn enter(current: CurrentTask) -> EnterGuard {
    let previous = CURRENT.with(|c| c.borrow_mut().replace(current));
    EnterGuard { previous }
}

impl Drop for EnterGuard {
    fn drop(&mut self) {
        CURRENT.with(|c| *c.borrow_mut() = self.previous.take());
    }
}

/// The ID of the task currently running on this thread, if called from
/// inside one -- `None` from a plain background thread, or from
/// `Runtime::block_on`'s own top-level future directly (that future
/// isn't itself a spawned task, so it has no ID of its own).
pub fn try_id() -> Option<TaskId> {
    CURRENT.with(|c| c.borrow().as_ref().map(|t| t.id))
}

/// The name given to the currently running task via
/// [`super::Builder::name`], if any, and if called from inside one --
/// see [`try_id`] for when this returns `None` regardless.
pub fn try_name() -> Option<Arc<str>> {
    CURRENT.with(|c| c.borrow().as_ref().and_then(|t| t.name.clone()))
}
