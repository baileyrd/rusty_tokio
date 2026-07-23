use rusty_tokio::task::{spawn_local, LocalSet};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn run_until_returns_a_plain_future_with_no_spawns() {
    let local = LocalSet::new();
    let value = local.run_until(async { 1 + 1 });
    assert_eq!(value, 2);
}

#[test]
fn spawn_local_accepts_an_rc_holding_future() {
    let local = LocalSet::new();
    let value = local.run_until(async {
        let shared = Rc::new(RefCell::new(0));
        let handle = {
            let shared = shared.clone();
            local_spawn_in_scope(&shared)
        };
        handle.await.unwrap();
        let value = *shared.borrow();
        value
    });
    assert_eq!(value, 1);

    fn local_spawn_in_scope(shared: &Rc<RefCell<i32>>) -> rusty_tokio::task::JoinHandle<()> {
        let shared = shared.clone();
        spawn_local(async move {
            *shared.borrow_mut() += 1;
        })
    }
}

#[test]
fn local_spawn_via_the_set_method_directly() {
    let local = LocalSet::new();
    let counter = Rc::new(RefCell::new(0));
    let handle = {
        let counter = counter.clone();
        local.spawn_local(async move {
            *counter.borrow_mut() += 1;
        })
    };
    local.run_until(async move {
        handle.await.unwrap();
    });
    assert_eq!(*counter.borrow(), 1);
}

#[test]
fn timers_and_the_reactor_work_inside_a_local_set() {
    // `time::sleep` needs an ambient `Runtime` (for the reactor/timer
    // driver it reaches via `Handle::current()`) -- a bare `LocalSet`
    // has no I/O/timer driver of its own, only task scheduling. See
    // this module's own docs: pairing a `LocalSet` with a `Runtime` is
    // the supported way to get timers/I/O inside `spawn_local`'d work.
    let rt = rusty_tokio::Runtime::new().unwrap();
    rt.block_on(async {
        let local = LocalSet::new();
        local.run_until(async {
            let started = std::time::Instant::now();
            rusty_tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(started.elapsed() >= Duration::from_millis(20));
        });
    });
}

#[test]
fn many_local_tasks_all_complete_and_interleave() {
    let local = LocalSet::new();
    let sum = Rc::new(RefCell::new(0));
    local.run_until(async {
        let mut handles = Vec::new();
        for i in 0..20 {
            let sum = sum.clone();
            handles.push(spawn_local(async move {
                *sum.borrow_mut() += i;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    });
    assert_eq!(*sum.borrow(), (0..20).sum());
}

#[test]
fn abort_cancels_a_local_task_before_it_finishes() {
    let rt = rusty_tokio::Runtime::new().unwrap();
    let ran = Rc::new(RefCell::new(false));
    rt.block_on(async {
        let local = LocalSet::new();
        local.run_until(async {
            let ran = ran.clone();
            let handle = spawn_local(async move {
                rusty_tokio::time::sleep(Duration::from_secs(60)).await;
                *ran.borrow_mut() = true;
            });
            rusty_tokio::task::yield_now().await;
            handle.abort();
            let result = handle.await;
            assert!(result.unwrap_err().is_cancelled());
        });
    });
    assert!(!*ran.borrow());
}

#[test]
fn a_panicking_local_task_reports_a_join_error() {
    let local = LocalSet::new();
    local.run_until(async {
        let handle = spawn_local(async {
            panic!("boom");
        });
        let result = handle.await;
        assert!(result.unwrap_err().is_panic());
    });
}

#[test]
#[should_panic(expected = "outside of a `LocalSet::run_until` call")]
fn spawn_local_outside_run_until_panics() {
    drop(spawn_local(async {}));
}

#[test]
fn using_a_local_set_from_a_second_thread_panics() {
    let local = Arc::new(std::sync::Mutex::new(LocalSet::new()));
    local.lock().unwrap().run_until(async {});

    let local2 = local.clone();
    let ok_count = Arc::new(AtomicUsize::new(0));
    let ok_count2 = ok_count.clone();
    // The panic happens on this spawned thread, not the test's own --
    // `#[should_panic]` only catches the latter, so assert on the
    // propagated payload directly instead.
    let result = std::thread::spawn(move || {
        local2.lock().unwrap().run_until(async {});
        ok_count2.fetch_add(1, Ordering::SeqCst);
    })
    .join();

    let payload = result.expect_err("expected the second thread to panic");
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
        .expect("panic payload was not a string");
    assert!(
        message.contains("single thread that first used it"),
        "unexpected panic message: {message}"
    );
    assert_eq!(ok_count.load(Ordering::SeqCst), 0);
}

#[test]
fn run_until_can_be_called_more_than_once_on_the_same_set() {
    let local = LocalSet::new();
    let a = local.run_until(async { 1 });
    let b = local.run_until(async { 2 });
    assert_eq!((a, b), (1, 2));
}

#[test]
fn enter_makes_spawn_local_callable_without_driving_the_set() {
    let local = LocalSet::new();
    let guard = local.enter();
    // Doesn't panic -- `enter` alone is enough to make `spawn_local`
    // (the free function) find this set as the ambient one.
    let handle = spawn_local(async { 1 + 1 });
    drop(guard);

    // Nothing runs it yet -- `enter` only makes the set ambient, it
    // doesn't drive its queue the way `run_until` does.
    assert!(!handle.is_finished());

    let value = local.run_until(async move { handle.await.unwrap() });
    assert_eq!(value, 2);
}

#[test]
fn enter_guard_dropping_restores_the_previously_ambient_set() {
    let outer = LocalSet::new();
    let inner = LocalSet::new();

    // Spawn onto `inner` while it's entered, then (after that guard
    // drops) spawn again -- that second spawn should land back on
    // `outer`, which is still ambient from the enclosing `run_until`.
    let (inner_handle, outer_handle) = outer.run_until(async {
        let inner_handle = {
            let _inner_guard = inner.enter();
            spawn_local(async { "inner" })
        };
        let outer_handle = spawn_local(async { "outer" });
        (inner_handle, outer_handle)
    });

    // Driving `outer` alone resolves the outer-targeted handle...
    let outer_value = outer.run_until(async move { outer_handle.await.unwrap() });
    assert_eq!(outer_value, "outer");

    // ...while the inner-targeted one only resolves once `inner`
    // itself is separately driven -- proving it never landed on
    // `outer`'s queue in the first place.
    let inner_value = inner.run_until(async move { inner_handle.await.unwrap() });
    assert_eq!(inner_value, "inner");
}

#[test]
fn enter_from_a_second_thread_panics() {
    let local = Arc::new(std::sync::Mutex::new(LocalSet::new()));
    local.lock().unwrap().run_until(async {});

    // The panic happens on this spawned thread, not the test's own --
    // `#[should_panic]` only catches the latter, so assert on the
    // propagated payload directly, same as
    // `using_a_local_set_from_a_second_thread_panics` above.
    let local2 = local.clone();
    let result = std::thread::spawn(move || {
        let _guard = local2.lock().unwrap().enter();
    })
    .join();

    let payload = result.expect_err("expected the second thread to panic");
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
        .expect("panic payload was not a string");
    assert!(
        message.contains("single thread that first used it"),
        "unexpected panic message: {message}"
    );
}
