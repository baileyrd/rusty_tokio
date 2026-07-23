use rusty_tokio::Runtime;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn abort_handle_aborts_the_same_task_as_the_join_handle() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let ran = Arc::new(AtomicUsize::new(0));
        let ran2 = ran.clone();
        let handle = rusty_tokio::spawn(async move {
            rusty_tokio::time::sleep(Duration::from_secs(60)).await;
            ran2.fetch_add(1, Ordering::SeqCst);
        });
        let abort_handle = handle.abort_handle();
        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        abort_handle.abort();

        let result = handle.await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_cancelled());
        assert_eq!(ran.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn abort_handle_id_matches_the_join_handle_id() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let handle = rusty_tokio::spawn(async { 1 });
        let abort_handle = handle.abort_handle();
        assert_eq!(abort_handle.id(), handle.id());
        handle.await.unwrap();
    });
}

#[test]
fn abort_handle_stays_usable_after_the_join_handle_is_dropped() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let ran = Arc::new(AtomicUsize::new(0));
        let ran2 = ran.clone();
        let handle = rusty_tokio::spawn(async move {
            rusty_tokio::time::sleep(Duration::from_secs(60)).await;
            ran2.fetch_add(1, Ordering::SeqCst);
        });
        let abort_handle = handle.abort_handle();
        drop(handle);

        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        abort_handle.abort();
        // Give the aborted task a chance to actually be dropped by a
        // worker before checking it never ran its body.
        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(ran.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn abort_handle_clone_shares_the_same_abort_target() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let ran = Arc::new(AtomicUsize::new(0));
        let ran2 = ran.clone();
        let handle = rusty_tokio::spawn(async move {
            rusty_tokio::time::sleep(Duration::from_secs(60)).await;
            ran2.fetch_add(1, Ordering::SeqCst);
        });
        let a = handle.abort_handle();
        let b = a.clone();
        assert_eq!(a.id(), b.id());

        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        b.abort();

        let result = handle.await;
        assert!(result.unwrap_err().is_cancelled());
        assert_eq!(ran.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn is_finished_reflects_completion_on_both_handle_kinds() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let handle = rusty_tokio::spawn(async { 42 });
        let abort_handle = handle.abort_handle();

        // Give the task a chance to actually run to completion.
        while !handle.is_finished() {
            rusty_tokio::task::yield_now().await;
        }
        assert!(handle.is_finished());
        assert!(abort_handle.is_finished());

        assert_eq!(handle.await.unwrap(), 42);
    });
}

#[test]
fn is_finished_is_false_while_the_task_is_still_running() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let handle = rusty_tokio::spawn(async {
            rusty_tokio::time::sleep(Duration::from_secs(60)).await;
        });
        let abort_handle = handle.abort_handle();
        assert!(!handle.is_finished());
        assert!(!abort_handle.is_finished());
        handle.abort();
    });
}

#[test]
fn is_finished_becomes_true_after_abort() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let handle = rusty_tokio::spawn(async {
            rusty_tokio::time::sleep(Duration::from_secs(60)).await;
        });
        let abort_handle = handle.abort_handle();
        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        handle.abort();
        let _ = handle.await;
        assert!(abort_handle.is_finished());
    });
}
