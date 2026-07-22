//! Worker thread bodies: pop a task from the local queue, else the
//! global injector, else steal from a sibling; run it; repeat until
//! shutdown.

use super::{context, Shared};
use crate::task::Task;
use crossbeam_deque::{Injector, Stealer, Worker as LocalWorker};
use std::cell::{Cell, RefCell};
use std::sync::Arc;

thread_local! {
    static WORKER_INDEX: Cell<Option<usize>> = const { Cell::new(None) };
    /// This thread's own `crossbeam_deque::Worker` for the worker index
    /// it's currently registered as, if any -- `Worker` is `!Sync`, so
    /// (unlike `WORKER_INDEX`) it can't live in `Shared` itself; each
    /// worker thread (and the current-thread flavor's `block_on` caller,
    /// for its single "worker 0") gets its own instance moved in here at
    /// registration time. `None` on any thread that isn't currently
    /// registered as a multi-threaded worker (a current-thread runtime
    /// uses its own separate `Mutex`-guarded queue instead -- see
    /// `LocalQueues::CurrentThread`).
    static LOCAL_QUEUE: RefCell<Option<LocalWorker<Arc<Task>>>> = const { RefCell::new(None) };
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

/// Pushes onto the calling thread's own local queue. Only valid to call
/// when `current_worker_index()` is `Some` on a multi-threaded runtime
/// (checked by `Shared::schedule` before calling this), so the
/// thread-local is guaranteed to already be populated.
pub(super) fn push_local(task: Arc<Task>) {
    LOCAL_QUEUE.with(|q| {
        q.borrow()
            .as_ref()
            .expect("push_local called on a thread with no local queue installed")
            .push(task);
    });
}

/// Pops from the calling thread's own local queue, if it has one.
pub(super) fn pop_local() -> Option<Arc<Task>> {
    LOCAL_QUEUE.with(|q| q.borrow().as_ref().and_then(LocalWorker::pop))
}

/// Steals a batch from the shared injector into the calling thread's own
/// local queue, then pops one -- `None` if the calling thread has no
/// local queue, or if the injector had nothing to steal.
pub(super) fn steal_from_injector(injector: &Injector<Arc<Task>>) -> Option<Arc<Task>> {
    LOCAL_QUEUE.with(|q| {
        let borrowed = q.borrow();
        let local = borrowed.as_ref()?;
        super::steal_loop(|| injector.steal_batch_and_pop(local))
    })
}

/// Steals a batch from `sibling` into the calling thread's own local
/// queue, then pops one -- `None` if the calling thread has no local
/// queue, or if `sibling` had nothing to steal.
pub(super) fn steal_from_sibling(sibling: &Stealer<Arc<Task>>) -> Option<Arc<Task>> {
    LOCAL_QUEUE.with(|q| {
        let borrowed = q.borrow();
        let local = borrowed.as_ref()?;
        super::steal_loop(|| sibling.steal_batch_and_pop(local))
    })
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

pub(super) fn spawn_worker(
    shared: Arc<Shared>,
    idx: usize,
    local: LocalWorker<Arc<Task>>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name(format!("rusty_tokio-worker-{idx}"))
        .spawn(move || {
            WORKER_INDEX.with(|c| c.set(Some(idx)));
            LOCAL_QUEUE.with(|q| *q.borrow_mut() = Some(local));
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
            // This thread's own `Worker` is about to be dropped along
            // with its thread-local -- anything still sitting in it
            // would otherwise just be silently dropped too (never run,
            // its `JoinHandle` hanging forever). The replacement thread
            // spawned in `block_in_place` got a brand-new, empty
            // `Worker` of its own (see that function's docs for why it
            // can't simply inherit this one), so hand off what's left
            // here the same way any other idle worker would pick it
            // up: through the shared injector.
            while let Some(task) = pop_local() {
                shared.requeue_to_injector(task);
            }
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
///
/// The replacement gets a brand-new, empty local queue rather than
/// somehow inheriting this thread's -- `crossbeam_deque::Worker` (see
/// issue #8) only ever has one owning thread at a time, and this
/// (retiring) thread is still the one actively using its own `Worker`
/// for the rest of `f`'s call and whatever's left of its current task's
/// poll. Anything still queued in *this* thread's `Worker` when it
/// finally does retire gets handed to the shared injector instead (see
/// `run`'s own retire-time drain) rather than silently dropped; the one
/// real, deliberately-accepted cost is that this specific worker index's
/// queue isn't stealable-from by any sibling again until (if ever)
/// another `block_in_place` call replaces it -- `Shared`'s `stealers`
/// list keeps pointing at this thread's now-idle `Stealer`, since
/// nothing else needs to invalidate or replace it for the drain to be
/// correct. A rare, already-simplicity-trade-off-accepting path (see the
/// rest of this function's docs), not a hot path worth an `RwLock`
/// around every steal attempt on every worker just to keep it fully
/// rebalanceable too.
pub(super) fn block_in_place<R>(shared: &Arc<Shared>, idx: usize, f: impl FnOnce() -> R) -> R {
    if !RETIRED.with(Cell::get) {
        spawn_worker(shared.clone(), idx, LocalWorker::new_fifo());
        RETIRED.with(|r| r.set(true));
    }
    f()
}
