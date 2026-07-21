use rusty_tokio::sync::Barrier;
use rusty_tokio::Runtime;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[test]
fn no_task_proceeds_past_wait_until_every_arrival_has_happened() {
    const N: usize = 8;
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    rt.block_on(async {
        let barrier = Arc::new(Barrier::new(N));
        let arrivals = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..N {
            let barrier = barrier.clone();
            let arrivals = arrivals.clone();
            handles.push(rusty_tokio::spawn(async move {
                arrivals.fetch_add(1, Ordering::SeqCst);
                barrier.wait().await;
                // By the time *any* wait() call resolves, every one of
                // the N arrivals must already have happened -- a broken
                // barrier that released early would see fewer than N
                // here at least some of the time.
                arrivals.load(Ordering::SeqCst)
            }));
        }

        for handle in handles {
            assert_eq!(handle.await.unwrap(), N);
        }
    });
}

#[test]
fn exactly_one_call_per_round_is_the_leader() {
    const N: usize = 8;
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    rt.block_on(async {
        let barrier = Arc::new(Barrier::new(N));

        let mut handles = Vec::new();
        for _ in 0..N {
            let barrier = barrier.clone();
            handles.push(rusty_tokio::spawn(async move {
                barrier.wait().await.is_leader()
            }));
        }

        let mut leaders = 0;
        for handle in handles {
            if handle.await.unwrap() {
                leaders += 1;
            }
        }
        assert_eq!(leaders, 1, "expected exactly one leader per round");
    });
}

#[test]
fn barrier_of_one_resolves_immediately_as_leader() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let barrier = Barrier::new(1);
        let result = barrier.wait().await;
        assert!(result.is_leader());
    });
}

#[test]
fn barrier_is_reusable_across_multiple_rounds() {
    const N: usize = 4;
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    rt.block_on(async {
        let barrier = Arc::new(Barrier::new(N));
        let round1_arrivals = Arc::new(AtomicUsize::new(0));
        let round2_arrivals = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..N {
            let barrier = barrier.clone();
            let round1_arrivals = round1_arrivals.clone();
            let round2_arrivals = round2_arrivals.clone();
            handles.push(rusty_tokio::spawn(async move {
                round1_arrivals.fetch_add(1, Ordering::SeqCst);
                barrier.wait().await;
                let seen_round1 = round1_arrivals.load(Ordering::SeqCst);

                round2_arrivals.fetch_add(1, Ordering::SeqCst);
                barrier.wait().await;
                let seen_round2 = round2_arrivals.load(Ordering::SeqCst);

                (seen_round1, seen_round2)
            }));
        }

        for handle in handles {
            assert_eq!(handle.await.unwrap(), (N, N));
        }
    });
}

#[test]
#[should_panic(expected = "Barrier::new(0)")]
fn barrier_of_zero_panics() {
    Barrier::new(0);
}
