use rusty_tokio::Runtime;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[test]
fn is_shutting_down_reflects_current_state() {
    let rt = Runtime::new().unwrap();
    let handle = rt.handle();
    assert!(!handle.is_shutting_down());
    rt.shutdown_background();
    assert!(handle.is_shutting_down());
}

#[test]
fn shutdown_notified_gives_a_task_a_real_chance_to_run_cleanup() {
    let rt = Runtime::new().unwrap();
    let handle = rt.handle();
    let cleaned_up = Arc::new(AtomicBool::new(false));

    rt.block_on({
        let cleaned_up = cleaned_up.clone();
        let handle = handle.clone();
        async move {
            rusty_tokio::spawn(async move {
                handle.shutdown_notified().await;
                cleaned_up.store(true, Ordering::SeqCst);
            });
            // Give the cleanup task a chance to actually start polling
            // (and register itself as a waiter) before shutdown fires.
            rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });

    rt.shutdown_timeout(Duration::from_secs(1));
    assert!(
        cleaned_up.load(Ordering::SeqCst),
        "a task awaiting shutdown_notified() should run its cleanup before shutdown_timeout returns"
    );
}

#[test]
fn shutdown_timeout_waits_for_an_outstanding_task_to_finish_naturally() {
    let rt = Runtime::new().unwrap();
    let finished = Arc::new(AtomicBool::new(false));

    rt.block_on({
        let finished = finished.clone();
        async move {
            rusty_tokio::spawn(async move {
                rusty_tokio::time::sleep(Duration::from_millis(50)).await;
                finished.store(true, Ordering::SeqCst);
            });
        }
    });

    rt.shutdown_timeout(Duration::from_secs(1));
    assert!(
        finished.load(Ordering::SeqCst),
        "shutdown_timeout should wait for an outstanding task to finish before tearing down"
    );
}

#[test]
fn shutdown_timeout_gives_up_after_its_deadline_on_a_stuck_task() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        rusty_tokio::spawn(async {
            // Never resolves: nothing ever calls notify_one/notify_waiters.
            let notify = rusty_tokio::sync::Notify::new();
            notify.notified().await;
        });
    });

    let start = Instant::now();
    rt.shutdown_timeout(Duration::from_millis(100));
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "shutdown_timeout should stop waiting once its deadline passes, not hang on a task that never finishes"
    );
}

#[test]
fn shutdown_background_returns_immediately_without_waiting_for_outstanding_tasks() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        rusty_tokio::spawn(async {
            rusty_tokio::time::sleep(Duration::from_secs(5)).await;
        });
    });

    let start = Instant::now();
    rt.shutdown_background();
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "shutdown_background should never wait on outstanding tasks"
    );
}
