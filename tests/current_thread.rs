use rusty_tokio::sync::oneshot;
use rusty_tokio::Builder;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn block_on_runs_a_plain_future_with_no_spawns() {
    let rt = Builder::new_current_thread().build().unwrap();
    let value = rt.block_on(async { 1 + 1 });
    assert_eq!(value, 2);
}

#[test]
fn spawned_tasks_run_interleaved_with_the_main_future() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        let (tx, rx) = oneshot::channel::<i32>();
        rusty_tokio::spawn(async move {
            let _ = tx.send(42);
        });
        assert_eq!(rx.await.unwrap(), 42);
    });
}

#[test]
fn many_spawned_tasks_all_complete_on_the_single_thread() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        let mut set = rusty_tokio::task::JoinSet::new();
        for i in 0..50 {
            set.spawn(async move { i });
        }
        let mut sum = 0;
        while let Some(r) = set.join_next().await {
            sum += r.unwrap();
        }
        assert_eq!(sum, (0..50).sum());
    });
}

#[test]
fn timers_and_the_reactor_still_work_on_a_current_thread_runtime() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        let started = std::time::Instant::now();
        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(started.elapsed() >= Duration::from_millis(20));
    });
}

#[test]
fn spawn_from_another_thread_before_block_on_runs_once_block_on_starts() {
    let rt = Builder::new_current_thread().build().unwrap();
    let handle = rt.handle();
    let ran = Arc::new(AtomicUsize::new(0));
    let ran_clone = ran.clone();

    // Spawn from a foreign thread, before `block_on` is ever called --
    // the task should just queue until something drives the runtime.
    let spawner = std::thread::spawn(move || {
        handle.spawn(async move {
            ran_clone.fetch_add(1, Ordering::SeqCst);
        });
    });
    spawner.join().unwrap();

    assert_eq!(ran.load(Ordering::SeqCst), 0);
    rt.block_on(async {
        rusty_tokio::task::yield_now().await;
    });
    assert_eq!(ran.load(Ordering::SeqCst), 1);
}

#[test]
fn sequential_block_on_calls_share_the_same_runtime() {
    let rt = Builder::new_current_thread().build().unwrap();
    let a = rt.block_on(async { 1 });
    let b = rt.block_on(async { 2 });
    assert_eq!((a, b), (1, 2));
}

#[test]
#[should_panic(expected = "worker_threads has no effect")]
fn worker_threads_panics_on_a_current_thread_builder() {
    Builder::new_current_thread().worker_threads(4);
}

#[test]
fn dropping_the_runtime_shuts_down_cleanly_with_no_worker_threads() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        rusty_tokio::time::sleep(Duration::from_millis(1)).await;
    });
    drop(rt);
}
