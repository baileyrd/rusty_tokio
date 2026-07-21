use rusty_tokio::sync::oneshot;
use rusty_tokio::{join, try_join, Runtime};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn join_two_branches_returns_both_outputs_as_a_tuple() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (a, b) = join!(async { 1 }, async { "two" });
        assert_eq!(a, 1);
        assert_eq!(b, "two");
    });
}

#[test]
fn join_three_four_five_branches_all_resolve() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (a, b, c) = join!(async { 1 }, async { 2 }, async { 3 });
        assert_eq!((a, b, c), (1, 2, 3));

        let (a, b, c, d) = join!(async { 1 }, async { 2 }, async { 3 }, async { 4 });
        assert_eq!((a, b, c, d), (1, 2, 3, 4));

        let (a, b, c, d, e) = join!(async { 1 }, async { 2 }, async { 3 }, async { 4 }, async {
            5
        });
        assert_eq!((a, b, c, d, e), (1, 2, 3, 4, 5));
    });
}

#[test]
fn join_waits_for_the_slowest_branch() {
    let rt = Runtime::builder().worker_threads(2).build().unwrap();
    rt.block_on(async {
        let (tx, rx) = oneshot::channel::<&str>();
        rusty_tokio::spawn(async move {
            rusty_tokio::time::sleep(Duration::from_millis(20)).await;
            let _ = tx.send("late");
        });

        let (fast, slow) = join!(async { "fast" }, async { rx.await.unwrap() });
        assert_eq!(fast, "fast");
        assert_eq!(slow, "late");
    });
}

#[test]
fn join_does_not_repoll_a_branch_after_it_resolves() {
    let rt = Runtime::builder().worker_threads(2).build().unwrap();
    rt.block_on(async {
        let polls_after_ready = Arc::new(AtomicUsize::new(0));
        let counter = polls_after_ready.clone();

        // Resolves on its very first poll; if `join!` ever polled it
        // again, this closure would panic on the second call.
        let mut already_done = false;
        let once = std::future::poll_fn(move |_cx| {
            if already_done {
                counter.fetch_add(1, Ordering::SeqCst);
                panic!("polled again after already resolving");
            }
            already_done = true;
            std::task::Poll::Ready(1)
        });

        let (tx, rx) = oneshot::channel::<i32>();
        rusty_tokio::spawn(async move {
            rusty_tokio::time::sleep(Duration::from_millis(20)).await;
            let _ = tx.send(2);
        });

        let (a, b) = join!(once, async { rx.await.unwrap() });
        assert_eq!((a, b), (1, 2));
        assert_eq!(polls_after_ready.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn try_join_returns_ok_tuple_when_every_branch_succeeds() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let result: Result<(i32, &str), &str> = try_join!(async { Ok(1) }, async { Ok("two") });
        assert_eq!(result, Ok((1, "two")));
    });
}

#[test]
fn try_join_short_circuits_on_the_first_error() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let result: Result<(i32, i32), &str> =
            try_join!(async { Err("boom") }, std::future::pending());
        assert_eq!(result, Err("boom"));
    });
}

#[test]
fn try_join_three_four_five_branches_short_circuit_on_any_error() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let r3: Result<(i32, i32, i32), &str> =
            try_join!(async { Ok(1) }, async { Err("bad") }, async { Ok(3) });
        assert_eq!(r3, Err("bad"));

        let r4: Result<(i32, i32, i32, i32), &str> =
            try_join!(async { Ok(1) }, async { Ok(2) }, async { Ok(3) }, async {
                Ok(4)
            });
        assert_eq!(r4, Ok((1, 2, 3, 4)));

        let r5: Result<(i32, i32, i32, i32, i32), &str> = try_join!(
            async { Ok(1) },
            async { Ok(2) },
            async { Ok(3) },
            async { Ok(4) },
            async { Err("last") }
        );
        assert_eq!(r5, Err("last"));
    });
}
