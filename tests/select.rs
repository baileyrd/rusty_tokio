use rusty_tokio::sync::oneshot;
use rusty_tokio::{select, Runtime};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn two_branches_returns_the_first_ready_value() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let winner = select! {
            a = async { 1 } => a,
            b = std::future::pending::<i32>() => b,
        };
        assert_eq!(winner, 1);
    });
}

#[test]
fn three_branches_picks_whichever_actually_resolves() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let winner = select! {
            a = std::future::pending::<&str>() => a,
            b = async { "b" } => b,
            c = std::future::pending::<&str>() => c,
        };
        assert_eq!(winner, "b");
    });
}

#[test]
fn four_branches_picks_whichever_actually_resolves() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let winner = select! {
            a = std::future::pending::<&str>() => a,
            b = std::future::pending::<&str>() => b,
            c = async { "c" } => c,
            d = std::future::pending::<&str>() => d,
        };
        assert_eq!(winner, "c");
    });
}

#[test]
fn five_branches_picks_whichever_actually_resolves() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let winner = select! {
            a = std::future::pending::<&str>() => a,
            b = std::future::pending::<&str>() => b,
            c = std::future::pending::<&str>() => c,
            d = async { "d" } => d,
            e = std::future::pending::<&str>() => e,
        };
        assert_eq!(winner, "d");
    });
}

#[test]
fn earlier_branch_wins_when_both_are_immediately_ready() {
    // Branches are polled in the order written on every poll, so when
    // two are simultaneously ready on the very first poll, the earlier
    // one wins -- documented, deliberate (non-randomized) behavior.
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let winner = select! {
            a = async { "first" } => a,
            b = async { "second" } => b,
        };
        assert_eq!(winner, "first");
    });
}

#[test]
fn losing_branch_future_is_dropped_not_left_running() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let dropped = Arc::new(AtomicBool::new(false));
        struct MarkOnDrop(Arc<AtomicBool>);
        impl Drop for MarkOnDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let marker = MarkOnDrop(dropped.clone());
        let loser = async move {
            let _marker = marker;
            std::future::pending::<()>().await
        };

        select! {
            a = async {} => a,
            b = loser => b,
        };

        assert!(dropped.load(Ordering::SeqCst));
    });
}

#[test]
fn select_waits_for_whichever_branch_wakes_later() {
    let rt = Runtime::builder().worker_threads(2).build().unwrap();
    rt.block_on(async {
        let (tx, rx) = oneshot::channel::<&str>();
        rusty_tokio::spawn(async move {
            rusty_tokio::time::sleep(Duration::from_millis(20)).await;
            let _ = tx.send("late");
        });

        let winner = select! {
            slow = rx => slow.unwrap(),
            fast = std::future::pending::<&str>() => fast,
        };
        assert_eq!(winner, "late");
    });
}

#[test]
fn wildcard_pattern_discards_the_value() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let winner: i32 = select! {
            _ = async { 42 } => 7,
            _ = std::future::pending::<i32>() => 8,
        };
        assert_eq!(winner, 7);
    });
}
