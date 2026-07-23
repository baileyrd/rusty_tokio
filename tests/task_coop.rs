use rusty_tokio::task::coop::{consume_budget, has_budget_remaining, unconstrained};
use rusty_tokio::Runtime;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn has_budget_remaining_is_true_outside_of_any_task() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // `block_on`'s own future is never polled through `Task::run`,
        // so no accounting is ever in effect here.
        assert!(has_budget_remaining());
    });
}

#[test]
fn consume_budget_forces_a_tight_loop_to_yield_to_a_sibling_task() {
    // A single worker, so `other` below only ever runs if `hog`'s tight
    // loop actually yields the worker back to the scheduler on its own.
    let rt = Runtime::builder().worker_threads(1).build().unwrap();

    let other_ran = Arc::new(AtomicBool::new(false));
    let other_ran_for_hog = other_ran.clone();

    rt.block_on(async move {
        let driver = rusty_tokio::spawn(async move {
            let hog = rusty_tokio::spawn(async move {
                loop {
                    consume_budget().await;
                    if other_ran_for_hog.load(Ordering::SeqCst) {
                        break;
                    }
                }
            });

            rusty_tokio::spawn(async move {
                other_ran.store(true, Ordering::SeqCst);
            });

            hog.await.unwrap();
        });

        let result = rusty_tokio::time::timeout(Duration::from_secs(5), driver).await;
        assert!(
            result.is_ok(),
            "consume_budget should force a yield within a bounded number \
             of iterations, letting the sibling task run and flip the \
             flag that ends the loop"
        );
        result.unwrap().unwrap();
    });
}

#[test]
fn has_budget_remaining_becomes_false_once_a_task_exhausts_its_budget() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        rusty_tokio::spawn(async {
            assert!(has_budget_remaining());
            // One more than the crate's own fixed per-poll-turn budget,
            // so the last `consume_budget` call is guaranteed to observe
            // (and report via its own yield) an exhausted budget.
            for _ in 0..128 {
                assert!(has_budget_remaining());
                consume_budget().await;
            }
            assert!(
                !has_budget_remaining(),
                "budget should be exactly exhausted after consuming it \
                 128 times in the same poll turn"
            );
        })
        .await
        .unwrap();
    });
}

#[test]
fn unconstrained_exempts_a_future_from_the_ambient_budget() {
    // Same single-worker setup as the other starvation tests: `other`
    // only runs if something yields the worker. Here `hog` wraps its
    // whole tight loop in `unconstrained`, so it should run to
    // completion (all 1000 iterations) *without* ever yielding -- proven
    // by `other` still not having run by the time `hog` finishes.
    let rt = Runtime::builder().worker_threads(1).build().unwrap();

    let other_ran = Arc::new(AtomicBool::new(false));
    let other_ran_for_hog = other_ran.clone();

    rt.block_on(async move {
        let driver = rusty_tokio::spawn(async move {
            let hog = rusty_tokio::spawn(unconstrained(async move {
                for _ in 0..1000 {
                    consume_budget().await;
                }
                other_ran_for_hog.load(Ordering::SeqCst)
            }));

            rusty_tokio::spawn(async move {
                other_ran.store(true, Ordering::SeqCst);
            });

            hog.await.unwrap()
        });

        let saw_other_ran_before_finishing = driver.await.unwrap();
        assert!(
            !saw_other_ran_before_finishing,
            "unconstrained should let the loop finish all 1000 iterations \
             in one poll turn, before the sibling task ever gets a chance \
             to run and flip the flag"
        );
    });
}
