//! Worker thread bodies: pop a task from the local queue, else the
//! global injector, else steal from a sibling; run it; repeat until
//! shutdown.

use super::{context, Shared};
use std::cell::Cell;
use std::sync::Arc;

thread_local! {
    static WORKER_INDEX: Cell<Option<usize>> = const { Cell::new(None) };
    /// Set by [`block_in_place`] once this thread's worker slot has been
    /// permanently handed off to a freshly spawned replacement -- checked
    /// by [`run`]'s own loop so this thread retires (exits) as soon as
    /// its current task finishes, instead of looping back to service the
    /// same `idx` a second, redundant time alongside the replacement.
    static RETIRED: Cell<bool> = const { Cell::new(false) };
}

pub(super) fn current_worker_index() -> Option<usize> {
    WORKER_INDEX.with(Cell::get)
}

/// Installs `idx` as the calling thread's worker index for as long as
/// the returned guard lives, restoring whatever was there before on
/// drop -- mirrors `context::enter`'s `EnterGuard`. Used by the
/// current-thread runtime flavor's `block_on`, which registers the
/// calling thread itself as "worker 0" (so a nested `spawn()` from a
/// task running there enqueues locally rather than via the injector,
/// reusing `schedule`/`next_task` unchanged) rather than spawning a
/// dedicated thread the way [`spawn_worker`] does -- a background
/// worker thread never needs to restore anything, since it simply
/// exits once its own `run` loop returns.
#[must_use]
pub(super) struct WorkerIndexGuard {
    previous: Option<usize>,
}

pub(super) fn enter_as_worker(idx: usize) -> WorkerIndexGuard {
    let previous = WORKER_INDEX.with(|c| c.replace(Some(idx)));
    WorkerIndexGuard { previous }
}

impl Drop for WorkerIndexGuard {
    fn drop(&mut self) {
        WORKER_INDEX.with(|c| c.set(self.previous));
    }
}

pub(super) fn spawn_worker(shared: Arc<Shared>, idx: usize) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name(format!("rusty_tokio-worker-{idx}"))
        .spawn(move || {
            WORKER_INDEX.with(|c| c.set(Some(idx)));
            let _guard = context::enter(shared.clone());
            run(&shared, idx);
        })
        .expect("failed to spawn rusty_tokio worker thread")
}

fn run(shared: &Arc<Shared>, idx: usize) {
    while !shared.is_shutdown() {
        match shared.next_task(idx) {
            Some(task) => task.run(),
            None => shared.park(idx),
        }
        if RETIRED.with(Cell::get) {
            return;
        }
    }
}

/// Runs `f` -- expected to block, unlike ordinary async code -- on the
/// calling thread, first handing this worker's other queued work off to
/// a freshly spawned replacement so the rest of the pool doesn't stall
/// waiting on it. See [`crate::task::block_in_place`]'s doc comment for
/// the full contract this backs; the caller (`Handle::block_in_place`)
/// has already confirmed `idx` really is the calling thread's own
/// worker index.
///
/// Deliberately simple rather than reusing tokio's actual approach
/// (handing the "core" back and forth, potentially reusing the blocked
/// thread as a *future* replacement rather than retiring it): this
/// always spawns a genuinely fresh OS thread and always retires the
/// calling one once its current task finishes, at the cost of one extra
/// thread spawn per call -- an explicit trade-off for a much simpler
/// implementation, matching how `BlockingPool` already documents its own
/// growth trade-offs elsewhere in this crate. The replacement's
/// `JoinHandle` is intentionally not tracked or joined by `Runtime`
/// itself -- it observes the same `shared.is_shutdown()` flag as every
/// other worker and exits on its own, so shutdown still fully quiesces
/// task execution; it just isn't one of the threads
/// `Runtime::shutdown_timeout`'s final join loop explicitly waits on.
///
/// A second (or later) `block_in_place` call from *within* the same
/// still-blocked stack (nested, or called more than once before this
/// task's poll returns) reuses the one replacement already spawned for
/// this thread's eventual retirement, rather than spawning another --
/// harmless either way, but pointless churn to repeat.
pub(super) fn block_in_place<R>(shared: &Arc<Shared>, idx: usize, f: impl FnOnce() -> R) -> R {
    if !RETIRED.with(Cell::get) {
        spawn_worker(shared.clone(), idx);
        RETIRED.with(|r| r.set(true));
    }
    f()
}
