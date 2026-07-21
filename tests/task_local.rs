use rusty_tokio::task;
use rusty_tokio::Runtime;
use std::time::Duration;

rusty_tokio::task_local! {
    static REQUEST_ID: u32;
    static LABEL: String;
}

#[test]
fn scope_makes_the_value_visible_inside_but_not_outside() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        assert!(REQUEST_ID.try_with(|_| ()).is_err());
        REQUEST_ID
            .scope(7, async {
                assert_eq!(REQUEST_ID.with(|id| *id), 7);
            })
            .await;
        assert!(REQUEST_ID.try_with(|_| ()).is_err());
    });
}

#[test]
#[should_panic(expected = "cannot access a task-local value")]
fn with_panics_outside_of_any_scope() {
    REQUEST_ID.with(|id| *id);
}

#[test]
fn nested_scopes_shadow_and_then_restore() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        REQUEST_ID
            .scope(1, async {
                assert_eq!(REQUEST_ID.with(|id| *id), 1);
                REQUEST_ID
                    .scope(2, async {
                        assert_eq!(REQUEST_ID.with(|id| *id), 2);
                    })
                    .await;
                assert_eq!(REQUEST_ID.with(|id| *id), 1);
            })
            .await;
    });
}

#[test]
fn value_is_visible_across_await_points_inside_the_scope() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        REQUEST_ID
            .scope(42, async {
                assert_eq!(REQUEST_ID.with(|id| *id), 42);
                rusty_tokio::time::sleep(Duration::from_millis(5)).await;
                assert_eq!(REQUEST_ID.with(|id| *id), 42);
            })
            .await;
    });
}

#[test]
fn different_interleaved_tasks_on_the_same_thread_do_not_see_each_others_value() {
    // A current-thread runtime so both tasks' polls really do
    // interleave on one OS thread, the exact scenario a plain
    // thread_local! would get wrong.
    let rt = rusty_tokio::Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        let a = rusty_tokio::spawn(REQUEST_ID.scope(1, async {
            task::yield_now().await;
            task::yield_now().await;
            REQUEST_ID.with(|id| *id)
        }));
        let b = rusty_tokio::spawn(REQUEST_ID.scope(2, async {
            task::yield_now().await;
            REQUEST_ID.with(|id| *id)
        }));

        assert_eq!(a.await.unwrap(), 1);
        assert_eq!(b.await.unwrap(), 2);
    });
}

#[test]
fn sync_scope_sets_the_value_for_a_plain_closure() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let result = LABEL.sync_scope(String::from("hello"), || LABEL.with(|s| s.clone()));
        assert_eq!(result, "hello");
        assert!(LABEL.try_with(|_| ()).is_err());
    });
}

#[test]
fn value_is_restored_even_if_the_scoped_future_panics() {
    let rt = Runtime::builder().worker_threads(2).build().unwrap();
    rt.block_on(async {
        REQUEST_ID
            .scope(99, async {
                // Nothing panics in the outer scope; just establishes a
                // baseline the inner spawned task's panic must not
                // disturb.
            })
            .await;

        let handle = rusty_tokio::spawn(REQUEST_ID.scope(5, async {
            panic!("boom");
        }));
        assert!(handle.await.unwrap_err().is_panic());

        // A fresh scope on this thread afterward should see its own
        // value cleanly, not anything left over from the panic.
        REQUEST_ID
            .scope(11, async {
                assert_eq!(REQUEST_ID.with(|id| *id), 11);
            })
            .await;
    });
}
