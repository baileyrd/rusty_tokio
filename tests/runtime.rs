use rusty_tokio::Runtime;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn block_on_returns_the_futures_output() {
    let rt = Runtime::new().unwrap();
    let out = rt.block_on(async { 1 + 1 });
    assert_eq!(out, 2);
}

#[test]
fn spawn_runs_concurrently_across_worker_threads() {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    let counter = Arc::new(AtomicUsize::new(0));

    rt.block_on(async {
        let mut handles = Vec::new();
        for _ in 0..200 {
            let counter = counter.clone();
            handles.push(rusty_tokio::spawn(async move {
                counter.fetch_add(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    });

    assert_eq!(counter.load(Ordering::SeqCst), 200);
}

#[test]
fn join_handle_yields_the_output() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let handle = rusty_tokio::spawn(async { 41 + 1 });
        assert_eq!(handle.await.unwrap(), 42);
    });
}

#[test]
fn abort_prevents_the_task_from_completing() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let ran = Arc::new(AtomicUsize::new(0));
        let ran2 = ran.clone();
        let handle = rusty_tokio::spawn(async move {
            rusty_tokio::time::sleep(Duration::from_secs(60)).await;
            ran2.fetch_add(1, Ordering::SeqCst);
        });
        // Give the task a chance to actually start (register its sleep)
        // before aborting it.
        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        handle.abort();
        let result = handle.await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_cancelled());
        assert_eq!(ran.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn a_panicking_task_reports_a_join_error_without_killing_the_runtime() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let handle = rusty_tokio::spawn(async {
            panic!("deliberate test panic");
        });
        let result = handle.await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_panic());

        // The runtime itself must still be usable afterward.
        let still_works = rusty_tokio::spawn(async { 7 }).await.unwrap();
        assert_eq!(still_works, 7);
    });
}

#[test]
fn nested_spawns_and_many_wakeups_all_complete() {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    rt.block_on(async {
        let handle = rusty_tokio::spawn(async {
            let inner = rusty_tokio::spawn(async {
                let mut total = 0u64;
                for i in 0..1000u64 {
                    // Force a real yield-and-rewake cycle, not just a
                    // tight synchronous loop.
                    rusty_tokio::time::sleep(Duration::from_micros(1)).await;
                    total += i;
                }
                total
            });
            inner.await.unwrap()
        });
        assert_eq!(handle.await.unwrap(), (0..1000u64).sum::<u64>());
    });
}
