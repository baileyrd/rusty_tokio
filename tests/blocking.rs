use rusty_tokio::Runtime;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[test]
fn spawn_blocking_returns_the_closures_value() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let out = rusty_tokio::spawn_blocking(|| 6 * 7).await.unwrap();
        assert_eq!(out, 42);
    });
}

#[test]
fn spawn_blocking_runs_off_the_async_worker_threads() {
    // A single-worker runtime: if spawn_blocking ran on the async pool
    // (or serialized on one blocking thread), this future and the
    // blocking call would contend for the same one worker and the
    // `notify` below would never fire until the blocking call returns --
    // i.e. this test times out on a broken implementation instead of
    // failing an assertion.
    let rt = Runtime::builder().worker_threads(1).build().unwrap();
    rt.block_on(async {
        let notify = Arc::new(rusty_tokio::sync::Notify::new());
        let notify2 = notify.clone();
        let blocking = rusty_tokio::spawn_blocking(move || {
            std::thread::sleep(Duration::from_millis(200));
            notify2.notify_one();
        });

        // While the above is blocking a pool thread, this task must
        // still be schedulable on the runtime's one async worker.
        let woke_in_time = rusty_tokio::time::timeout(Duration::from_millis(150), async {
            notify.notified().await;
        })
        .await;

        blocking.await.unwrap();
        assert!(
            woke_in_time.is_err(),
            "the notify fires at ~200ms, after the 150ms timeout -- this just \
             confirms the async worker was free to run this task concurrently \
             with the blocking call, not stuck behind it"
        );
    });
}

#[test]
fn many_blocking_calls_run_concurrently_not_serialized() {
    let rt = Runtime::builder().max_blocking_threads(16).build().unwrap();
    rt.block_on(async {
        let start = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..16 {
            handles.push(rusty_tokio::spawn_blocking(|| {
                std::thread::sleep(Duration::from_millis(150));
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // If these ran serialized on one thread, 16 * 150ms = 2.4s.
        // Run concurrently, it should be close to one 150ms sleep.
        assert!(
            start.elapsed() < Duration::from_millis(900),
            "16 blocking sleeps of 150ms took {:?}, expected them to run concurrently",
            start.elapsed()
        );
    });
}

#[test]
fn blocking_pool_respects_its_thread_cap() {
    let rt = Runtime::builder().max_blocking_threads(4).build().unwrap();
    rt.block_on(async {
        let concurrent = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..20 {
            let concurrent = concurrent.clone();
            let max_seen = max_seen.clone();
            handles.push(rusty_tokio::spawn_blocking(move || {
                let now = concurrent.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(30));
                concurrent.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        assert!(
            max_seen.load(Ordering::SeqCst) <= 4,
            "saw {} blocking closures running at once, expected at most the 4-thread cap",
            max_seen.load(Ordering::SeqCst)
        );
    });
}

#[test]
fn a_panicking_blocking_closure_reports_a_join_error() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let result = rusty_tokio::spawn_blocking(|| {
            panic!("deliberate test panic in a blocking closure");
        })
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_panic());

        // The runtime must still be usable afterward.
        let still_works = rusty_tokio::spawn_blocking(|| 1 + 1).await.unwrap();
        assert_eq!(still_works, 2);
    });
}
