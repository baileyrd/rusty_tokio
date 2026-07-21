use rusty_tokio::task::{self, Builder};
use rusty_tokio::Runtime;
use std::collections::HashSet;

#[test]
fn spawned_tasks_get_distinct_ids() {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    rt.block_on(async {
        let mut handles = Vec::new();
        for _ in 0..50 {
            handles.push(rusty_tokio::spawn(async {}));
        }
        let mut ids = HashSet::new();
        for h in &handles {
            ids.insert(h.id());
        }
        assert_eq!(ids.len(), 50, "every task should have a distinct id");
        for h in handles {
            h.await.unwrap();
        }
    });
}

#[test]
fn join_handle_id_matches_the_task_own_try_id() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let handle = rusty_tokio::spawn(async { task::try_id() });
        let expected_id = handle.id();
        let observed_id = handle.await.unwrap();
        assert_eq!(observed_id, Some(expected_id));
    });
}

#[test]
fn try_id_returns_none_outside_of_a_task() {
    // `block_on`'s own top-level future isn't itself a spawned `Task`.
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        assert_eq!(task::try_id(), None);
    });
}

#[test]
fn builder_name_is_observable_via_try_name_inside_the_task() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let handle = Builder::new()
            .name("my-named-task")
            .spawn(async { task::try_name() });
        let name = handle.await.unwrap();
        assert_eq!(name.as_deref(), Some("my-named-task"));
    });
}

#[test]
fn plain_spawn_has_no_name() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let handle = rusty_tokio::spawn(async { task::try_name() });
        assert_eq!(handle.await.unwrap(), None);
    });
}

#[test]
fn try_name_returns_none_when_the_builder_was_not_given_one() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let handle = Builder::new().spawn(async { task::try_name() });
        assert_eq!(handle.await.unwrap(), None);
    });
}

#[test]
fn local_set_spawned_tasks_also_get_a_distinct_id() {
    let local = rusty_tokio::task::LocalSet::new();
    let (id_a, id_b) = local.run_until(async {
        let a = task::spawn_local(async { task::try_id() });
        let b = task::spawn_local(async { task::try_id() });
        (a.await.unwrap(), b.await.unwrap())
    });
    assert!(id_a.is_some());
    assert!(id_b.is_some());
    assert_ne!(id_a, id_b);
}

#[test]
fn task_ids_are_unique_across_multiple_runtimes() {
    let rt1 = Runtime::new().unwrap();
    let rt2 = Runtime::new().unwrap();

    let id1 = rt1.block_on(async {
        rusty_tokio::spawn(async { task::try_id().unwrap() })
            .await
            .unwrap()
    });
    let id2 = rt2.block_on(async {
        rusty_tokio::spawn(async { task::try_id().unwrap() })
            .await
            .unwrap()
    });

    assert_ne!(id1, id2);
}
