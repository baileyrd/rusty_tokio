use rusty_tokio::Runtime;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn coop_budget_lets_a_sibling_task_run_despite_an_always_ready_recv_loop() {
    // A single worker, so there's nothing to steal from -- the only way
    // `other` (below) ever runs at all is if `hog`'s tight loop actually
    // yields the worker back to the scheduler on its own.
    let rt = Runtime::builder().worker_threads(1).build().unwrap();

    let other_ran = Arc::new(AtomicBool::new(false));
    let other_ran_for_hog = other_ran.clone();

    rt.block_on(async move {
        // Spawned from *inside* a task already running on the worker (not
        // from `block_on`'s own thread), so `hog` and `other` below land
        // on that worker's local queue, in that order -- see
        // `tests/runtime.rs`'s `yield_now_lets_two_same_queue_tasks_interleave`
        // for why that placement (rather than both going through the
        // shared injector) is what makes their relative ordering
        // deterministic on a single-worker runtime.
        let driver = rusty_tokio::spawn(async move {
            let (tx, mut rx) = rusty_tokio::sync::mpsc::unbounded_channel::<()>();
            tx.send(()).unwrap();

            // Never legitimately awaits anything -- every `recv()`
            // resolves immediately, because the loop re-feeds its own
            // channel right before looping again. Without a coop budget
            // forcing a yield, this is a genuine infinite loop that
            // monopolizes the sole worker forever; `other` would then
            // never run, `other_ran` would never flip, and this loop
            // would never break.
            let hog = rusty_tokio::spawn(async move {
                loop {
                    rx.recv().await.unwrap();
                    if other_ran_for_hog.load(Ordering::SeqCst) {
                        break;
                    }
                    tx.send(()).unwrap();
                }
            });

            rusty_tokio::spawn(async move {
                other_ran.store(true, Ordering::SeqCst);
            });

            hog.await.unwrap();
        });

        // A generous bound, not a tight timing assertion -- the coop
        // budget is only 128 poll operations, so on a correct
        // implementation this resolves almost immediately. If the coop
        // budget doesn't actually force a yield, `hog` above loops
        // forever and this times out instead of hanging the whole test
        // suite.
        let result = rusty_tokio::time::timeout(Duration::from_secs(5), driver).await;
        assert!(
            result.is_ok(),
            "coop budget should force the always-ready recv loop to yield \
             within a bounded number of iterations, letting the sibling \
             task run and flip the flag that ends the loop -- timing out \
             means it never yielded"
        );
        result.unwrap().unwrap();
    });
}
