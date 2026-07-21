use rusty_tokio::sync::oneshot;
use rusty_tokio::task::JoinSet;
use std::time::Duration;

// No `#[test]` written here -- `#[rusty_tokio::test]` emits it itself.

#[rusty_tokio::test]
async fn basic_body_runs_on_the_runtime() {
    let (tx, rx) = oneshot::channel::<i32>();
    rusty_tokio::spawn(async move {
        let _ = tx.send(42);
    });
    assert_eq!(rx.await.unwrap(), 42);
}

#[rusty_tokio::test]
async fn can_return_a_result() -> Result<(), String> {
    rusty_tokio::time::sleep(Duration::from_millis(1)).await;
    Ok(())
}

#[rusty_tokio::test(worker_threads = 1)]
async fn accepts_a_worker_threads_argument() {
    let (tx, rx) = oneshot::channel::<&str>();
    rusty_tokio::spawn(async move {
        let _ = tx.send("done");
    });
    assert_eq!(rx.await.unwrap(), "done");
}

#[rusty_tokio::test(worker_threads = 4)]
async fn many_concurrent_spawns_all_complete() {
    let mut set = JoinSet::new();
    for i in 0..20 {
        set.spawn(async move { i });
    }
    let mut sum = 0;
    while let Some(r) = set.join_next().await {
        sum += r.unwrap();
    }
    assert_eq!(sum, (0..20).sum());
}
