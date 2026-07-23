use rusty_tokio::Runtime;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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
fn join_error_into_panic_returns_the_original_payload() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let handle = rusty_tokio::spawn(async {
            panic!("original payload");
        });
        let err = handle.await.unwrap_err();
        let payload = err.into_panic();
        let message = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
            .expect("panic payload was not a string");
        assert_eq!(message, "original payload");
    });
}

#[test]
fn join_error_try_into_panic_succeeds_for_an_actual_panic() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let handle = rusty_tokio::spawn(async {
            panic!("boom");
        });
        let err = handle.await.unwrap_err();
        assert!(err.try_into_panic().is_ok());
    });
}

#[test]
fn join_error_try_into_panic_hands_the_error_back_for_a_cancellation() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let handle = rusty_tokio::spawn(async {
            rusty_tokio::time::sleep(Duration::from_secs(60)).await;
        });
        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        handle.abort();
        let err = handle.await.unwrap_err();
        assert!(err.is_cancelled());

        let err = err.try_into_panic().unwrap_err();
        assert!(err.is_cancelled());
    });
}

#[test]
#[should_panic(expected = "into_panic")]
fn join_error_into_panic_panics_for_a_cancellation() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let handle = rusty_tokio::spawn(async {
            rusty_tokio::time::sleep(Duration::from_secs(60)).await;
        });
        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        handle.abort();
        let err = handle.await.unwrap_err();
        let _ = err.into_panic();
    });
}

#[test]
fn yield_now_actually_gets_the_task_repolled() {
    // A broken yield_now (one that never actually re-wakes the task)
    // would just hang here forever -- the point of this test is that
    // it doesn't.
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut count = 0;
        for _ in 0..50 {
            rusty_tokio::task::yield_now().await;
            count += 1;
        }
        assert_eq!(count, 50);
    });
}

#[test]
fn yield_now_lets_two_same_queue_tasks_interleave() {
    // Both `a` and `b` are spawned from *within* another task (not from
    // `block_on`'s own thread) so they land on that worker's local
    // queue in FIFO order -- interleaving depends only on the local
    // queue's own FIFO order, not on any fairness between the local
    // queue and the injector (a worker always drains its own local
    // queue before ever touching the injector, so two tasks racing
    // across *different* queues wouldn't reliably interleave the way
    // this test wants to demonstrate).
    let rt = Runtime::builder().worker_threads(1).build().unwrap();
    let order = Arc::new(Mutex::new(Vec::new()));

    rt.block_on(async {
        let driver_order = order.clone();
        rusty_tokio::spawn(async move {
            let order_a = driver_order.clone();
            let a = rusty_tokio::spawn(async move {
                for i in 0..3 {
                    order_a.lock().unwrap().push(('a', i));
                    rusty_tokio::task::yield_now().await;
                }
            });
            let order_b = driver_order.clone();
            let b = rusty_tokio::spawn(async move {
                for i in 0..3 {
                    order_b.lock().unwrap().push(('b', i));
                    rusty_tokio::task::yield_now().await;
                }
            });
            a.await.unwrap();
            b.await.unwrap();
        })
        .await
        .unwrap();
    });

    assert_eq!(
        *order.lock().unwrap(),
        vec![('a', 0), ('b', 0), ('a', 1), ('b', 1), ('a', 2), ('b', 2)],
        "yield_now should let two same-queue tasks interleave one iteration at a time"
    );
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

#[test]
fn thread_name_applies_to_worker_threads() {
    let rt = Runtime::builder()
        .worker_threads(2)
        .thread_name("my-custom-worker")
        .build()
        .unwrap();
    rt.block_on(async {
        let name = rusty_tokio::spawn(async { std::thread::current().name().unwrap().to_string() })
            .await
            .unwrap();
        assert_eq!(name, "my-custom-worker");
    });
}

#[test]
fn thread_name_fn_is_called_once_per_spawned_thread() {
    let counter = Arc::new(AtomicUsize::new(0));
    let rt = {
        let counter = counter.clone();
        Runtime::builder()
            .worker_threads(3)
            .thread_name_fn(move || format!("counted-{}", counter.fetch_add(1, Ordering::SeqCst)))
            .build()
            .unwrap()
    };
    // Each worker's name is generated synchronously, on the calling
    // thread, while `build()` spawns it -- before any task has run, so
    // this is already deterministic without needing to observe which
    // worker a given task happens to land on (nothing here forces even
    // distribution of quick tasks across workers, only that stealing
    // *can* happen when a worker is actually idle).
    assert_eq!(counter.load(Ordering::SeqCst), 3);

    rt.block_on(async {
        let name = rusty_tokio::spawn(async { std::thread::current().name().unwrap().to_string() })
            .await
            .unwrap();
        assert!(
            name.starts_with("counted-"),
            "expected a name from thread_name_fn, got {name:?}"
        );
    });
}

#[test]
fn thread_stack_size_can_be_configured_without_erroring() {
    let rt = Runtime::builder()
        .worker_threads(1)
        .thread_stack_size(4 * 1024 * 1024)
        .build()
        .unwrap();
    rt.block_on(async {
        let result = rusty_tokio::spawn(async { 1 + 1 }).await.unwrap();
        assert_eq!(result, 2);
    });
}

#[test]
fn thread_keep_alive_shrinks_the_blocking_pool_after_the_configured_idle_time() {
    let rt = Runtime::builder()
        .thread_keep_alive(Duration::from_millis(50))
        .build()
        .unwrap();
    let metrics = rt.metrics();
    rt.block_on(async move {
        rusty_tokio::spawn_blocking(|| ()).await.unwrap();
        assert_eq!(metrics.num_blocking_threads(), 1);

        // Comfortably past the configured 50ms idle timeout.
        rusty_tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(metrics.num_blocking_threads(), 0);
    });
}
