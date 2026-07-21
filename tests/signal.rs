// All scenarios below share a single `Runtime` and run inside one
// `block_on`, deliberately -- `signal`'s own module docs note that its
// process-wide state (the self-pipe, the reader task, the installed
// `sigaction`s) is driven by whichever `Runtime` first calls into it, and
// dropping a `Runtime` tears down its reactor and scheduler. Since
// `cargo test` runs each `#[test]` fn's own `Runtime` in the same process,
// spreading these across separate `#[test]` fns would let an early test's
// `Runtime` shut down mid-suite and silently break signal delivery for
// every test after it.
use rusty_tokio::signal::{self, SignalKind};
use rusty_tokio::time::timeout;
use rusty_tokio::Runtime;
use std::time::Duration;

fn raise(signum: i32) {
    unsafe {
        libc::raise(signum);
    }
}

#[test]
fn signal_handling() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        ctrl_c_resolves_after_sigint_is_raised().await;
        signal_fires_once_per_occurrence_and_coalesces_bursts().await;
        multiple_listeners_for_the_same_kind_are_all_woken().await;
        dropping_a_listener_does_not_affect_other_listeners_for_the_same_kind().await;
        from_raw_accepts_an_arbitrary_in_range_signal_number().await;
    });
}

async fn ctrl_c_resolves_after_sigint_is_raised() {
    // `ctrl_c()`'s body -- installing the handler and registering its
    // listener -- doesn't run until this future is actually polled, so
    // spawning it separately and then raising from here would race
    // against a worker thread picking it up for its first poll.
    // `timeout` polls its wrapped future immediately on every poll of
    // itself (including the first), so awaiting it directly guarantees
    // registration has already happened by the time the raiser task's
    // sleep elapses -- comfortably true, since registration is a handful
    // of local, non-blocking operations finishing in microseconds.
    rusty_tokio::spawn(async {
        rusty_tokio::time::sleep(Duration::from_millis(50)).await;
        raise(libc::SIGINT);
    });
    timeout(Duration::from_secs(5), rusty_tokio::signal::ctrl_c())
        .await
        .expect("ctrl_c should resolve promptly")
        .unwrap();
}

async fn signal_fires_once_per_occurrence_and_coalesces_bursts() {
    let mut sig = signal::signal(SignalKind::user_defined1()).unwrap();

    raise(libc::SIGUSR1);
    raise(libc::SIGUSR1);
    raise(libc::SIGUSR1);

    // Three raises before any poll coalesce into a single pending
    // notification -- not a queue of three.
    timeout(Duration::from_secs(5), sig.recv())
        .await
        .expect("first recv should resolve promptly")
        .unwrap();

    // No further occurrence yet: the next recv must not resolve
    // immediately.
    assert!(timeout(Duration::from_millis(200), sig.recv())
        .await
        .is_err());

    raise(libc::SIGUSR1);
    timeout(Duration::from_secs(5), sig.recv())
        .await
        .expect("recv after a fresh raise should resolve promptly")
        .unwrap();
}

async fn multiple_listeners_for_the_same_kind_are_all_woken() {
    let mut first = signal::signal(SignalKind::user_defined2()).unwrap();
    let mut second = signal::signal(SignalKind::user_defined2()).unwrap();

    raise(libc::SIGUSR2);

    timeout(Duration::from_secs(5), first.recv())
        .await
        .expect("first listener should resolve promptly")
        .unwrap();
    timeout(Duration::from_secs(5), second.recv())
        .await
        .expect("second listener should resolve promptly")
        .unwrap();
}

async fn dropping_a_listener_does_not_affect_other_listeners_for_the_same_kind() {
    let dropped = signal::signal(SignalKind::window_change()).unwrap();
    let mut kept = signal::signal(SignalKind::window_change()).unwrap();
    drop(dropped);

    raise(libc::SIGWINCH);

    timeout(Duration::from_secs(5), kept.recv())
        .await
        .expect("surviving listener should still resolve promptly")
        .unwrap();
}

async fn from_raw_accepts_an_arbitrary_in_range_signal_number() {
    let kind = SignalKind::from_raw(libc::SIGHUP);
    assert_eq!(kind.as_raw_value(), libc::SIGHUP);
    let mut sig = signal::signal(kind).unwrap();

    raise(libc::SIGHUP);

    timeout(Duration::from_secs(5), sig.recv())
        .await
        .expect("recv should resolve promptly")
        .unwrap();
}
