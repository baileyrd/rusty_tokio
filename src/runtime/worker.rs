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
            None => shared.park(),
        }
    }
}
