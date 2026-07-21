//! Worker thread bodies: pop a task from the local queue, else the
//! global injector, else steal from a sibling; run it; repeat until
//! shutdown.

use super::{context, Shared};
use std::cell::Cell;
use std::sync::Arc;

thread_local! {
    static WORKER_INDEX: Cell<Option<usize>> = const { Cell::new(None) };
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
    }
}
