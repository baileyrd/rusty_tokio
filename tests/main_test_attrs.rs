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

// The macro re-emits the caller's own attributes ahead of the generated
// body, so an attribute written below the `#[rusty_tokio::test]` line has
// to survive the rewrite. `should_panic` is a load-bearing choice here:
// it only works if the attribute actually lands on the generated `fn`, so
// this fails loudly rather than silently if attributes get dropped.
#[rusty_tokio::test]
#[should_panic(expected = "boom")]
async fn attributes_below_the_macro_are_preserved() {
    panic!("boom");
}

// A trailing comma in the argument list parses the same as none.
#[rusty_tokio::test(worker_threads = 2)]
async fn accepts_a_trailing_comma_in_the_argument_list() {
    let (tx, rx) = oneshot::channel::<u8>();
    rusty_tokio::spawn(async move {
        let _ = tx.send(7);
    });
    assert_eq!(rx.await.unwrap(), 7);
}

// Integer literals keep their Rust spelling, so a suffixed or
// underscore-separated count has to parse the same as a bare one.
#[rusty_tokio::test(worker_threads = 2usize)]
async fn accepts_a_suffixed_worker_threads_literal() {
    assert_eq!(rusty_tokio::spawn(async { 1 + 1 }).await.unwrap(), 2);
}

// Not `fn main`, but still a return type: the macro has to carry `-> T`
// across from the original signature rather than assume `()`.
#[rusty_tokio::test]
async fn propagates_an_error_return_type() -> Result<(), std::num::ParseIntError> {
    let parsed: i32 = "41".parse()?;
    assert_eq!(parsed + 1, 42);
    Ok(())
}
