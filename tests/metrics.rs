use rusty_tokio::sync::oneshot;
use rusty_tokio::{Builder, Runtime};
use std::time::Duration;

#[test]
fn num_workers_matches_configured_worker_threads() {
    let rt = Runtime::builder().worker_threads(3).build().unwrap();
    assert_eq!(rt.metrics().num_workers(), 3);
}

#[test]
fn current_thread_runtime_reports_one_worker() {
    let rt = Builder::new_current_thread().build().unwrap();
    assert_eq!(rt.metrics().num_workers(), 1);
}

#[test]
fn num_alive_tasks_reflects_spawned_and_finished_tasks() {
    let rt = Runtime::new().unwrap();
    let metrics = rt.metrics();
    rt.block_on(async move {
        assert_eq!(metrics.num_alive_tasks(), 0);

        let (tx, rx) = oneshot::channel::<()>();
        let handle = rusty_tokio::spawn(async move {
            rx.await.ok();
        });
        // `task_spawned` is called synchronously before the task is
        // even scheduled, so this is never racy.
        assert!(metrics.num_alive_tasks() >= 1);

        tx.send(()).unwrap();
        handle.await.unwrap();

        // The task's own JoinHandle is notified *before*
        // `active_tasks` is decremented (see `task::Task::mark_finished`),
        // so there's a brief window right after `.await` resolves where
        // the count hasn't dropped back to zero yet -- poll for it
        // instead of asserting immediately.
        for _ in 0..200 {
            if metrics.num_alive_tasks() == 0 {
                break;
            }
            rusty_tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(metrics.num_alive_tasks(), 0);
    });
}

#[test]
fn worker_local_queue_depth_reflects_tasks_queued_behind_the_running_one() {
    // A single worker, currently busy running the outer task below --
    // nested spawns from *inside* it land on that same worker's local
    // queue (per `Shared::schedule`'s rule) and sit there, unrun, for as
    // long as the outer task keeps running without yielding.
    let rt = Runtime::builder().worker_threads(1).build().unwrap();
    let metrics = rt.metrics();
    rt.block_on(async move {
        rusty_tokio::spawn(async move {
            for _ in 0..5 {
                rusty_tokio::spawn(async {});
            }
            assert_eq!(metrics.worker_local_queue_depth(0), 5);
        })
        .await
        .unwrap();
    });
}

#[test]
fn global_queue_depth_reflects_tasks_waiting_in_the_injector() {
    let rt = Runtime::builder().worker_threads(1).build().unwrap();
    let metrics = rt.metrics();
    rt.block_on(async move {
        // Occupies the sole worker thread with a synchronous (not
        // `.await`-ing) sleep, so it can't pick anything else up for as
        // long as it runs -- unlike an async task that hits a `Pending`
        // await point and immediately frees the worker back up.
        let occupy = rusty_tokio::spawn(async {
            std::thread::sleep(Duration::from_millis(250));
        });
        // Give the worker a moment to actually pick up and start
        // running `occupy` before spawning more -- otherwise `occupy`
        // itself might still be the one sitting in the queue below.
        rusty_tokio::time::sleep(Duration::from_millis(50)).await;

        for _ in 0..3 {
            rusty_tokio::spawn(async {});
        }
        // The sole worker is synchronously busy inside `occupy`'s sleep
        // right now, so these three are guaranteed to still be waiting
        // in the injector, not yet picked up by anyone.
        assert_eq!(metrics.global_queue_depth(), 3);

        occupy.await.unwrap();
        for _ in 0..200 {
            if metrics.global_queue_depth() == 0 {
                break;
            }
            rusty_tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(metrics.global_queue_depth(), 0);
    });
}

#[test]
fn worker_steal_count_increments_when_a_sibling_steals_work() {
    let rt = Runtime::builder().worker_threads(2).build().unwrap();
    let metrics = rt.metrics();
    rt.block_on(async move {
        // Whichever worker runs this outer task ends up with a pile of
        // nested spawns on its own local queue (see
        // `worker_local_queue_depth`'s test above) -- the other worker
        // has nothing of its own to run and can only get work by
        // stealing from that pile.
        //
        // A large pile, deliberately -- the busy worker's own pop and a
        // thief's steal are lock-free (see issue #8), fast enough that a
        // *small* pile can be fully drained by its owner before the idle
        // sibling's OS thread is even scheduled to attempt a steal at
        // all (confirmed by instrumenting this exact scenario: with a
        // few dozen nested tasks, the sibling's very first steal attempt
        // sometimes already sees an empty queue). Tens of thousands of
        // tasks keeps the busy worker occupied long enough in absolute
        // wall-clock time that the sibling is essentially guaranteed a
        // real window, regardless of OS scheduling latency -- verified
        // stable (0 failures) across 30 repeated runs even under
        // artificial heavy CPU pressure (12 busy-loop processes
        // saturating this 4-core sandbox).
        rusty_tokio::spawn(async {
            for _ in 0..20_000 {
                rusty_tokio::spawn(async {
                    rusty_tokio::task::yield_now().await;
                });
            }
        })
        .await
        .unwrap();

        for _ in 0..200 {
            if metrics.worker_steal_count(0) + metrics.worker_steal_count(1) > 0 {
                break;
            }
            rusty_tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            metrics.worker_steal_count(0) + metrics.worker_steal_count(1) > 0,
            "expected at least one of the two workers to have stolen work from the other"
        );
    });
}

#[test]
fn worker_park_count_increases_while_a_worker_sits_idle() {
    let rt = Runtime::builder().worker_threads(1).build().unwrap();
    let metrics = rt.metrics();
    rt.block_on(async move {
        let before = metrics.worker_park_count(0);
        // `park` times out and re-checks every 50ms, so waiting well
        // past that guarantees at least one more park cycle -- nothing
        // else is scheduled on this runtime to keep the worker busy in
        // the meantime.
        rusty_tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(metrics.worker_park_count(0) > before);
    });
}

#[test]
fn worker_park_unpark_count_alternates_parity_and_increases_while_idle() {
    // `block_on`'s own top-level future runs inline on the *calling*
    // (test) thread, entirely separate from worker 0's own OS thread --
    // reading the counter from there would race worker 0's independent
    // park/unpark cycle. Reading it from *inside* a task actually
    // spawned onto worker 0 instead guarantees the read lands while
    // that worker is confirmably active (even), not mid-`park`.
    let rt = Runtime::builder().worker_threads(1).build().unwrap();
    let metrics = rt.metrics();
    rt.block_on(async move {
        let m = metrics.clone();
        let before = rusty_tokio::spawn(async move { m.worker_park_unpark_count(0) })
            .await
            .unwrap();
        assert_eq!(
            before % 2,
            0,
            "expected an even (active) count while this task is running"
        );

        // Nothing else is scheduled on this runtime -- the worker parks
        // (and re-parks every 50ms) while this sleeps, guaranteeing at
        // least one full park/unpark cycle before the next spawn below.
        rusty_tokio::time::sleep(Duration::from_millis(120)).await;

        let m = metrics.clone();
        let after = rusty_tokio::spawn(async move { m.worker_park_unpark_count(0) })
            .await
            .unwrap();
        assert!(after > before);
        assert_eq!(
            after % 2,
            0,
            "expected an even (active) count again, having unparked to run this task"
        );
    });
}

#[test]
fn worker_total_busy_duration_increases_while_running_real_work() {
    let rt = Runtime::builder().worker_threads(1).build().unwrap();
    let metrics = rt.metrics();
    rt.block_on(async move {
        let before = metrics.worker_total_busy_duration(0);

        // A synchronous (not `.await`-ing) sleep inside the task body
        // itself, so the ~100ms is genuinely spent inside this single
        // `task.run()` call rather than yielded back to the scheduler --
        // exactly the time `worker_total_busy_duration` is meant to
        // attribute to the worker as busy.
        rusty_tokio::spawn(async {
            std::thread::sleep(Duration::from_millis(100));
        })
        .await
        .unwrap();

        let after = metrics.worker_total_busy_duration(0);
        assert!(
            after - before >= Duration::from_millis(80),
            "expected at least ~100ms of newly-busy time, got {:?}",
            after - before
        );
    });
}

#[test]
fn num_blocking_threads_reflects_the_pools_live_threads() {
    let rt = Runtime::new().unwrap();
    let metrics = rt.metrics();
    rt.block_on(async move {
        assert_eq!(metrics.num_blocking_threads(), 0);

        // `spawn_blocking` grows the pool (and increments its thread
        // count) synchronously, before it returns -- no need to wait or
        // poll for this to become visible.
        let handle = rusty_tokio::spawn_blocking(|| {
            std::thread::sleep(Duration::from_millis(150));
        });
        assert_eq!(metrics.num_blocking_threads(), 1);

        handle.await.unwrap();
        // The pool doesn't shrink back down until a thread has sat idle
        // for a while (see `blocking`'s module docs) -- this crate makes
        // no promises about exactly when, only that it grew to 1 while
        // the closure was running, so there's nothing further to assert
        // here.
    });
}
