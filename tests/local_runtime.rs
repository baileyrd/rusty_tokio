use rusty_tokio::{Builder, LocalOptions};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

#[test]
fn block_on_returns_the_futures_output() {
    let rt = Builder::new_current_thread()
        .build_local(&LocalOptions::new())
        .unwrap();
    let out = rt.block_on(async { 1 + 1 });
    assert_eq!(out, 2);
}

#[test]
fn spawn_local_accepts_a_non_send_future_and_runs_it() {
    let rt = Builder::new_current_thread()
        .build_local(&LocalOptions::new())
        .unwrap();
    let counter = Rc::new(RefCell::new(0));
    let handle = {
        let counter = counter.clone();
        // `Rc` is `!Send` -- this couldn't be spawned onto a plain
        // multi-threaded `Runtime` via `spawn()`.
        rt.spawn_local(async move {
            *counter.borrow_mut() += 1;
        })
    };
    rt.block_on(async move {
        handle.await.unwrap();
    });
    assert_eq!(*counter.borrow(), 1);
}

#[test]
fn timers_and_io_work_inside_a_local_runtime() {
    let rt = Builder::new_current_thread()
        .build_local(&LocalOptions::new())
        .unwrap();
    rt.block_on(async {
        let started = std::time::Instant::now();
        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(started.elapsed() >= Duration::from_millis(20));
    });
}

#[test]
fn block_on_can_be_called_more_than_once() {
    let rt = Builder::new_current_thread()
        .build_local(&LocalOptions::new())
        .unwrap();
    let a = rt.block_on(async { 1 });
    let b = rt.block_on(async { 2 });
    assert_eq!((a, b), (1, 2));
}

#[test]
fn spawned_local_tasks_survive_across_separate_block_on_calls() {
    let rt = Builder::new_current_thread()
        .build_local(&LocalOptions::new())
        .unwrap();
    let counter = Rc::new(RefCell::new(0));
    let handle = {
        let counter = counter.clone();
        rt.spawn_local(async move {
            *counter.borrow_mut() += 1;
            42
        })
    };
    // Queued by `spawn_local` before any `block_on` call at all --
    // still runs once one actually drives it.
    let result = rt.block_on(handle);
    assert_eq!(result.unwrap(), 42);
    assert_eq!(*counter.borrow(), 1);
}

#[test]
#[should_panic(expected = "build_local only makes sense for")]
fn build_local_panics_on_a_multi_threaded_builder() {
    let _ = Builder::new_multi_thread().build_local(&LocalOptions::new());
}

#[test]
fn metrics_and_handle_are_reachable() {
    let rt = Builder::new_current_thread()
        .build_local(&LocalOptions::new())
        .unwrap();
    assert_eq!(rt.metrics().num_workers(), 1);
    let handle = rt.handle();
    rt.block_on(async {
        handle.spawn(async { 1 + 1 }).await.unwrap();
    });
}
