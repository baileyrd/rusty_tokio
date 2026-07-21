use rusty_tokio::{task, Builder, Runtime};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn block_in_place_returns_the_closures_value() {
    // Only valid from within a task actually running on the worker
    // pool -- not directly inside `block_on`'s own future, which has no
    // "other queued work" of the kind `block_in_place` hands off (see
    // `block_in_place_panics_outside_any_worker` below).
    let rt = Runtime::builder().worker_threads(2).build().unwrap();
    rt.block_on(async {
        let out = rusty_tokio::spawn(async { task::block_in_place(|| 6 * 7) })
            .await
            .unwrap();
        assert_eq!(out, 42);
    });
}

#[test]
fn block_in_place_lets_a_sibling_task_run_while_it_blocks() {
    // A single worker: without a replacement worker, `other` (queued
    // right behind `hog`, both landing in the shared injector since
    // they're spawned from `block_on`'s own thread) has nowhere to run
    // until `hog`'s 200ms sleep finishes -- so it would only notify well
    // after this test's 150ms timeout. If `block_in_place` actually
    // hands `hog`'s worker's other work off to a replacement, the
    // replacement can pick `other` up out of the injector directly and
    // this resolves almost immediately instead.
    let rt = Runtime::builder().worker_threads(1).build().unwrap();
    rt.block_on(async {
        let notify = Arc::new(rusty_tokio::sync::Notify::new());
        let notify2 = notify.clone();

        let hog = rusty_tokio::spawn(async move {
            task::block_in_place(|| std::thread::sleep(Duration::from_millis(200)));
        });
        let other = rusty_tokio::spawn(async move {
            notify2.notify_one();
        });

        let woke_in_time = rusty_tokio::time::timeout(Duration::from_millis(150), async {
            notify.notified().await;
        })
        .await;

        hog.await.unwrap();
        other.await.unwrap();
        assert!(
            woke_in_time.is_ok(),
            "`other` should have run on a replacement worker while `hog` was \
             still blocked inline, well within the 150ms timeout -- timing \
             out means it had to wait for `hog`'s own worker to free up \
             instead"
        );
    });
}

#[test]
#[should_panic(expected = "current_thread")]
fn block_in_place_panics_on_a_current_thread_runtime() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        task::block_in_place(|| ());
    });
}

#[test]
#[should_panic(expected = "no ambient worker")]
fn block_in_place_panics_outside_any_worker() {
    let rt = Runtime::builder().worker_threads(2).build().unwrap();
    // Called directly inside `block_on`'s own future, never inside a
    // spawned task -- on the multi-threaded flavor, that thread is never
    // registered as a worker.
    rt.block_on(async {
        task::block_in_place(|| ());
    });
}

#[test]
fn block_in_place_panics_inside_a_spawn_blocking_closure() {
    // Unlike the two `#[should_panic]` cases above, a panic inside a
    // `spawn_blocking` closure is caught and reported through the
    // `JoinHandle` as a `JoinError` (see `tests/blocking.rs`'s
    // `a_panicking_blocking_closure_reports_a_join_error`), not
    // propagated directly out of `block_on` -- so this checks
    // `is_panic()` instead of matching the panic message text, which
    // `JoinError`'s own `Debug` deliberately doesn't preserve.
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let result = rusty_tokio::spawn_blocking(|| {
            task::block_in_place(|| ());
        })
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_panic());
    });
}
