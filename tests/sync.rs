use rusty_tokio::sync::{mpsc, oneshot, Mutex, MutexGuard, Notify, OnceCell, RwLock, Semaphore};
use rusty_tokio::Runtime;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn mutex_serializes_increments() {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    rt.block_on(async {
        let mutex = Arc::new(Mutex::new(0u64));
        let mut handles = Vec::new();
        for _ in 0..500 {
            let mutex = mutex.clone();
            handles.push(rusty_tokio::spawn(async move {
                let mut guard = mutex.lock().await;
                let cur = *guard;
                // Yield while holding the lock, so a broken mutex that
                // allows concurrent access would actually get a chance
                // to race here instead of getting lucky.
                rusty_tokio::time::sleep(Duration::from_micros(1)).await;
                *guard = cur + 1;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(*mutex.lock().await, 500);
    });
}

#[test]
fn mutex_lock_owned_serializes_increments_across_spawned_tasks() {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    rt.block_on(async {
        let mutex = Arc::new(Mutex::new(0u64));
        let mut handles = Vec::new();
        for _ in 0..500 {
            let mutex = mutex.clone();
            handles.push(rusty_tokio::spawn(async move {
                let mut guard = mutex.lock_owned().await;
                let cur = *guard;
                rusty_tokio::time::sleep(Duration::from_micros(1)).await;
                *guard = cur + 1;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(*mutex.lock().await, 500);
    });
}

#[test]
fn mutex_try_lock_owned_fails_while_held_then_succeeds_after_release() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mutex = Arc::new(Mutex::new(1));
        let guard = mutex.try_lock_owned().unwrap();
        assert!(mutex.try_lock_owned().is_none());
        drop(guard);
        assert!(mutex.try_lock_owned().is_some());
    });
}

#[test]
fn mutex_try_lock_failing_does_not_corrupt_the_real_holders_lock() {
    // Regression test: `try_lock`'s failure path must not construct
    // (and then implicitly drop-release) a `MutexGuard` it never
    // actually acquired -- doing so would erroneously mark the mutex
    // unlocked while the real holder below still thinks it holds it,
    // letting a second `try_lock` wrongly succeed at the same time.
    let mutex = Mutex::new(1);
    let held = mutex.try_lock().unwrap();
    assert!(mutex.try_lock().is_none(), "first failed attempt");
    assert!(mutex.try_lock().is_none(), "second failed attempt");
    drop(held);
    assert!(mutex.try_lock().is_some());
}

#[test]
fn mutex_guard_map_projects_a_field_and_still_releases_on_drop() {
    struct Pair {
        a: u32,
        b: u32,
    }
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mutex = Mutex::new(Pair { a: 1, b: 2 });

        {
            let guard = mutex.lock().await;
            let mut mapped = MutexGuard::map(guard, |pair| &mut pair.a);
            *mapped += 10;
        }

        // The mapped guard's `Drop` must have released the lock --
        // otherwise this would hang forever.
        let guard = mutex.lock().await;
        assert_eq!(guard.a, 11);
        assert_eq!(guard.b, 2);
    });
}

#[test]
fn owned_mutex_guard_map_projects_a_field_and_still_releases_on_drop() {
    struct Pair {
        a: u32,
        b: u32,
    }
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mutex = Arc::new(Mutex::new(Pair { a: 1, b: 2 }));

        {
            let guard = mutex.lock_owned().await;
            let mut mapped = rusty_tokio::sync::OwnedMutexGuard::map(guard, |pair| &mut pair.b);
            *mapped += 10;
        }

        let guard = mutex.lock_owned().await;
        assert_eq!(guard.a, 1);
        assert_eq!(guard.b, 12);
    });
}

#[test]
fn rwlock_allows_concurrent_readers() {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    let lock = Arc::new(RwLock::new(0));
    let concurrent = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));

    rt.block_on(async {
        let mut handles = Vec::new();
        for _ in 0..8 {
            let lock = lock.clone();
            let concurrent = concurrent.clone();
            let max_concurrent = max_concurrent.clone();
            handles.push(rusty_tokio::spawn(async move {
                let _guard = lock.read().await;
                let now = concurrent.fetch_add(1, Ordering::SeqCst) + 1;
                max_concurrent.fetch_max(now, Ordering::SeqCst);
                rusty_tokio::time::sleep(Duration::from_millis(20)).await;
                concurrent.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    });

    assert!(
        max_concurrent.load(Ordering::SeqCst) > 1,
        "readers should be able to run concurrently, not serialize"
    );
}

#[test]
fn rwlock_writers_are_mutually_exclusive_and_exclude_readers() {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    let lock = Arc::new(RwLock::new(0u64));

    rt.block_on(async {
        let mut handles = Vec::new();
        for _ in 0..500 {
            let lock = lock.clone();
            handles.push(rusty_tokio::spawn(async move {
                let mut guard = lock.write().await;
                let cur = *guard;
                // Yield while holding the write lock -- a broken
                // implementation that let a reader or another writer in
                // concurrently would get a real chance to race here.
                rusty_tokio::time::sleep(Duration::from_micros(1)).await;
                *guard = cur + 1;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(*lock.read().await, 500);
    });
}

#[test]
fn rwlock_is_write_preferring() {
    // A writer that starts waiting should get in before a reader that
    // arrives afterward, even though nothing but other readers hold the
    // lock at the moment that later reader actually calls `read()`.
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    let lock = Arc::new(RwLock::new(()));
    let order: Arc<std::sync::Mutex<Vec<&'static str>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    rt.block_on(async {
        let first_reader_guard = lock.read().await;

        let lock_w = lock.clone();
        let order_w = order.clone();
        let writer = rusty_tokio::spawn(async move {
            let _guard = lock_w.write().await;
            order_w.lock().unwrap().push("writer");
        });

        // Give the writer a real chance to actually start waiting
        // (register itself in the queue) before the second reader shows
        // up.
        rusty_tokio::time::sleep(Duration::from_millis(20)).await;

        let lock_r2 = lock.clone();
        let order_r2 = order.clone();
        let second_reader = rusty_tokio::spawn(async move {
            let _guard = lock_r2.read().await;
            order_r2.lock().unwrap().push("second_reader");
        });

        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        drop(first_reader_guard); // let the writer proceed

        writer.await.unwrap();
        second_reader.await.unwrap();
    });

    assert_eq!(*order.lock().unwrap(), vec!["writer", "second_reader"]);
}

#[test]
fn rwlock_try_read_and_try_write() {
    let lock = RwLock::new(5);
    let r1 = lock.try_read().unwrap();
    let r2 = lock.try_read().unwrap(); // multiple readers via try_read too
    assert_eq!(*r1, 5);
    assert_eq!(*r2, 5);
    assert!(
        lock.try_write().is_none(),
        "try_write should fail while readers are held"
    );
    drop(r1);
    drop(r2);

    let mut w = lock.try_write().unwrap();
    *w = 10;
    assert!(
        lock.try_read().is_none(),
        "try_read should fail while the writer is held"
    );
}

#[test]
fn rwlock_read_owned_allows_concurrent_readers_across_spawned_tasks() {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    let lock = Arc::new(RwLock::new(5));
    rt.block_on(async {
        let (tx1, rx1) = oneshot::channel::<()>();
        let (tx2, rx2) = oneshot::channel::<()>();

        let l1 = lock.clone();
        let h1 = rusty_tokio::spawn(async move {
            let _g = l1.read_owned().await;
            let _ = tx1.send(());
            rusty_tokio::time::sleep(Duration::from_millis(50)).await;
        });
        let l2 = lock.clone();
        let h2 = rusty_tokio::spawn(async move {
            let _g = l2.read_owned().await;
            let _ = tx2.send(());
            rusty_tokio::time::sleep(Duration::from_millis(50)).await;
        });

        // Both readers should be able to acquire concurrently -- if
        // read_owned actually excluded, one of these would never fire
        // until the other's sleep finished.
        rx1.await.unwrap();
        rx2.await.unwrap();
        h1.await.unwrap();
        h2.await.unwrap();
    });
}

#[test]
fn rwlock_write_owned_serializes_increments_across_spawned_tasks() {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    rt.block_on(async {
        let lock = Arc::new(RwLock::new(0u64));
        let mut handles = Vec::new();
        for _ in 0..200 {
            let lock = lock.clone();
            handles.push(rusty_tokio::spawn(async move {
                let mut guard = lock.write_owned().await;
                let cur = *guard;
                rusty_tokio::time::sleep(Duration::from_micros(1)).await;
                *guard = cur + 1;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(*lock.read().await, 200);
    });
}

#[test]
fn rwlock_try_read_owned_and_try_write_owned() {
    let lock = Arc::new(RwLock::new(5));
    let r = lock.try_read_owned().unwrap();
    assert_eq!(*r, 5);
    assert!(lock.try_write_owned().is_none());
    drop(r);

    let mut w = lock.try_write_owned().unwrap();
    *w = 10;
    assert!(lock.try_read_owned().is_none());
}

#[test]
fn rwlock_write_guard_map_projects_a_field_and_still_releases_on_drop() {
    struct Pair {
        a: u32,
        b: u32,
    }
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let lock = RwLock::new(Pair { a: 1, b: 2 });

        {
            let guard = lock.write().await;
            let mut mapped = rusty_tokio::sync::RwLockWriteGuard::map(guard, |pair| &mut pair.a);
            *mapped += 10;
        }

        let guard = lock.read().await;
        assert_eq!(guard.a, 11);
        assert_eq!(guard.b, 2);
    });
}

#[test]
fn rwlock_owned_write_guard_map_projects_a_field_and_still_releases_on_drop() {
    struct Pair {
        a: u32,
        b: u32,
    }
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let lock = Arc::new(RwLock::new(Pair { a: 1, b: 2 }));

        {
            let guard = lock.write_owned().await;
            let mut mapped =
                rusty_tokio::sync::OwnedRwLockWriteGuard::map(guard, |pair| &mut pair.b);
            *mapped += 10;
        }

        let guard = lock.read_owned().await;
        assert_eq!(guard.a, 1);
        assert_eq!(guard.b, 12);
    });
}

#[test]
fn rwlock_write_guard_downgrade_lets_other_readers_in_but_not_a_queued_writer() {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    let lock = Arc::new(RwLock::new(1));
    rt.block_on(async {
        let write_guard = lock.write().await;
        let read_guard = write_guard.downgrade();
        assert_eq!(*read_guard, 1);

        // Another reader should be able to join immediately.
        let lock2 = lock.clone();
        let other_reader = rusty_tokio::spawn(async move {
            let g = lock2.read().await;
            *g
        });
        assert_eq!(other_reader.await.unwrap(), 1);

        // A writer queued behind the still-held read guard must not
        // proceed until it drops.
        let lock3 = lock.clone();
        let writer_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer_ran2 = writer_ran.clone();
        let writer = rusty_tokio::spawn(async move {
            let mut g = lock3.write().await;
            writer_ran2.store(true, Ordering::SeqCst);
            *g = 2;
        });
        rusty_tokio::task::yield_now().await;
        assert!(!writer_ran.load(Ordering::SeqCst));

        drop(read_guard);
        writer.await.unwrap();
        assert!(writer_ran.load(Ordering::SeqCst));
        assert_eq!(*lock.read().await, 2);
    });
}

#[test]
fn rwlock_owned_write_guard_downgrade_produces_a_working_owned_read_guard() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let lock = Arc::new(RwLock::new(7));
        let write_guard = lock.write_owned().await;
        let read_guard = write_guard.downgrade();
        assert_eq!(*read_guard, 7);
        drop(read_guard);

        // The lock must be fully released -- a fresh write should
        // succeed without hanging.
        let mut w = lock.write_owned().await;
        *w = 8;
        drop(w);
        assert_eq!(*lock.read_owned().await, 8);
    });
}

#[test]
fn semaphore_caps_concurrency_at_the_configured_permit_count() {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    let semaphore = Arc::new(Semaphore::new(3));
    let concurrent = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));

    rt.block_on(async {
        let mut handles = Vec::new();
        for _ in 0..12 {
            let semaphore = semaphore.clone();
            let concurrent = concurrent.clone();
            let max_concurrent = max_concurrent.clone();
            handles.push(rusty_tokio::spawn(async move {
                let _permit = semaphore.acquire().await;
                let now = concurrent.fetch_add(1, Ordering::SeqCst) + 1;
                max_concurrent.fetch_max(now, Ordering::SeqCst);
                rusty_tokio::time::sleep(Duration::from_millis(20)).await;
                concurrent.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    });

    assert_eq!(
        max_concurrent.load(Ordering::SeqCst),
        3,
        "concurrency should reach but never exceed the permit count"
    );
    assert_eq!(semaphore.available_permits(), 3);
}

#[test]
fn semaphore_grants_queued_waiters_in_fifo_order() {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    let semaphore = Arc::new(Semaphore::new(1));
    let order: Arc<std::sync::Mutex<Vec<u32>>> = Arc::new(std::sync::Mutex::new(Vec::new()));

    rt.block_on(async {
        let held = semaphore.acquire().await; // exhaust the single permit

        let mut handles = Vec::new();
        for i in 0..5u32 {
            let semaphore = semaphore.clone();
            let order = order.clone();
            handles.push(rusty_tokio::spawn(async move {
                let _permit = semaphore.acquire().await;
                order.lock().unwrap().push(i);
            }));
            // Give each task a real chance to actually queue, in order,
            // before the next one is spawned.
            rusty_tokio::time::sleep(Duration::from_millis(10)).await;
        }

        drop(held); // let the queue start draining
        for h in handles {
            h.await.unwrap();
        }
    });

    assert_eq!(*order.lock().unwrap(), vec![0, 1, 2, 3, 4]);
}

#[test]
fn semaphore_acquire_many_reserves_all_requested_permits_at_once() {
    let rt = Runtime::new().unwrap();
    let semaphore = Semaphore::new(3);
    rt.block_on(async {
        let permit = semaphore.acquire_many(2).await;
        assert_eq!(semaphore.available_permits(), 1);
        drop(permit);
        assert_eq!(semaphore.available_permits(), 3);
    });
}

#[test]
fn semaphore_try_acquire_fails_when_not_enough_permits_are_free() {
    let semaphore = Semaphore::new(1);
    let _held = semaphore.try_acquire().unwrap();
    assert!(semaphore.try_acquire().is_none());
}

#[test]
fn semaphore_owned_permit_moves_into_a_spawned_task() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = semaphore.clone().acquire_owned().await;
        let handle = rusty_tokio::spawn(async move {
            // Holding the only permit here, moved in from outside --
            // the point of `acquire_owned` over the borrowed form.
            drop(permit);
        });
        handle.await.unwrap();
        assert_eq!(semaphore.available_permits(), 1);
    });
}

#[test]
fn owned_semaphore_permit_num_permits_and_semaphore_accessors() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let semaphore = Arc::new(Semaphore::new(5));
        let permit = semaphore.clone().acquire_many_owned(3).await;
        assert_eq!(permit.num_permits(), 3);
        assert!(Arc::ptr_eq(permit.semaphore(), &semaphore));
        assert_eq!(semaphore.available_permits(), 2);
        drop(permit);
        assert_eq!(semaphore.available_permits(), 5);
    });
}

#[test]
fn owned_semaphore_permit_merge_combines_permits_and_releases_both_together() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let semaphore = Arc::new(Semaphore::new(5));
        let mut a = semaphore.clone().acquire_many_owned(2).await;
        let b = semaphore.clone().acquire_many_owned(1).await;
        assert_eq!(semaphore.available_permits(), 2);

        a.merge(b);
        assert_eq!(a.num_permits(), 3);
        // Merging doesn't itself release anything.
        assert_eq!(semaphore.available_permits(), 2);

        drop(a);
        assert_eq!(semaphore.available_permits(), 5);
    });
}

#[test]
#[should_panic(expected = "different Semaphores")]
fn owned_semaphore_permit_merge_panics_across_different_semaphores() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let sem_a = Arc::new(Semaphore::new(2));
        let sem_b = Arc::new(Semaphore::new(2));
        let mut a = sem_a.acquire_owned().await;
        let b = sem_b.acquire_owned().await;
        a.merge(b);
    });
}

#[test]
fn semaphore_const_new_is_usable_in_a_static() {
    static SEM: Semaphore = Semaphore::const_new(2);
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        assert_eq!(SEM.available_permits(), 2);
        let _permit = SEM.acquire().await;
        assert_eq!(SEM.available_permits(), 1);
    });
}

#[test]
fn semaphore_max_permits_is_usize_max_shifted_by_three() {
    assert_eq!(Semaphore::MAX_PERMITS, usize::MAX >> 3);
}

#[test]
#[should_panic(expected = "MAX_PERMITS")]
fn semaphore_new_panics_past_max_permits() {
    let _ = Semaphore::new(Semaphore::MAX_PERMITS + 1);
}

#[test]
fn forget_permits_permanently_reduces_availability_without_needing_a_release() {
    let semaphore = Semaphore::new(5);
    let forgotten = semaphore.forget_permits(2);
    assert_eq!(forgotten, 2);
    assert_eq!(semaphore.available_permits(), 3);
}

#[test]
fn forget_permits_saturates_at_whatever_is_actually_available() {
    let semaphore = Semaphore::new(2);
    let forgotten = semaphore.forget_permits(10);
    assert_eq!(forgotten, 2);
    assert_eq!(semaphore.available_permits(), 0);
}

#[test]
fn forget_permits_does_not_wake_or_grant_any_queued_waiter() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let semaphore = Arc::new(Semaphore::new(1));
        let _held = semaphore.acquire().await; // exhaust the single permit

        let semaphore2 = semaphore.clone();
        let waiter = rusty_tokio::spawn(async move {
            let _permit = semaphore2.acquire().await;
        });

        // Give the waiter a chance to actually queue.
        rusty_tokio::task::yield_now().await;
        // Forgetting from an already-exhausted semaphore has nothing to
        // reclaim (0 available), so nothing is forgotten and the queued
        // waiter is left exactly as it was -- still waiting.
        assert_eq!(semaphore.forget_permits(1), 0);

        let timed_out = rusty_tokio::time::timeout(Duration::from_millis(20), waiter).await;
        assert!(
            timed_out.is_err(),
            "forget_permits must not grant a permit to a queued waiter"
        );
    });
}

#[test]
fn watch_initial_value_is_observable_without_waiting() {
    let (_tx, rx) = rusty_tokio::sync::watch::channel(42);
    assert_eq!(*rx.borrow(), 42);
}

#[test]
fn watch_changed_resolves_once_send_happens() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = rusty_tokio::sync::watch::channel(0);
        let waiter = rusty_tokio::spawn(async move {
            rx.changed().await.unwrap();
            *rx.borrow()
        });
        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        tx.send(7).unwrap();
        assert_eq!(waiter.await.unwrap(), 7);
    });
}

#[test]
fn watch_every_clone_observes_the_same_change() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx1) = rusty_tokio::sync::watch::channel(0);
        let mut rx2 = rx1.clone();

        let w1 = rusty_tokio::spawn(async move {
            rx1.changed().await.unwrap();
            *rx1.borrow()
        });
        let w2 = rusty_tokio::spawn(async move {
            rx2.changed().await.unwrap();
            *rx2.borrow()
        });

        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        tx.send(99).unwrap();

        assert_eq!(w1.await.unwrap(), 99);
        assert_eq!(w2.await.unwrap(), 99);
    });
}

#[test]
fn watch_changed_reports_closed_once_the_sender_drops() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = rusty_tokio::sync::watch::channel(0);
        let waiter = rusty_tokio::spawn(async move { rx.changed().await });
        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        drop(tx);
        assert!(waiter.await.unwrap().is_err());
    });
}

#[test]
fn watch_send_reports_no_receivers_left_once_every_receiver_drops() {
    let (tx, rx) = rusty_tokio::sync::watch::channel(0);
    drop(rx);
    assert!(tx.send(1).is_err());
}

#[test]
fn watch_send_modify_updates_in_place_and_counts_as_a_change() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = rusty_tokio::sync::watch::channel(vec![1, 2, 3]);
        let waiter = rusty_tokio::spawn(async move {
            rx.changed().await.unwrap();
            rx.borrow().clone()
        });
        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        tx.send_modify(|v| v.push(4));
        assert_eq!(waiter.await.unwrap(), vec![1, 2, 3, 4]);
    });
}

#[test]
fn watch_borrow_and_update_marks_the_current_version_seen() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = rusty_tokio::sync::watch::channel(1);
        tx.send(2).unwrap();
        // Marks version 2 as already seen -- a subsequent `changed()`
        // should wait for a *third* value, not resolve immediately for
        // this same one again.
        assert_eq!(*rx.borrow_and_update(), 2);

        let waiter = rusty_tokio::spawn(async move {
            rx.changed().await.unwrap();
            *rx.borrow()
        });
        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        tx.send(3).unwrap();
        assert_eq!(waiter.await.unwrap(), 3);
    });
}

#[test]
fn oneshot_delivers_the_value() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, rx) = oneshot::channel();
        rusty_tokio::spawn(async move {
            tx.send(42).unwrap();
        });
        assert_eq!(rx.await.unwrap(), 42);
    });
}

#[test]
fn oneshot_reports_dropped_sender() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, rx) = oneshot::channel::<i32>();
        drop(tx);
        assert!(rx.await.is_err());
    });
}

#[test]
fn mpsc_delivers_in_order_and_closes_on_drop() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = mpsc::channel(4);
        rusty_tokio::spawn(async move {
            for i in 0..20 {
                tx.send(i).await.unwrap();
            }
        });
        let mut received = Vec::new();
        while let Some(v) = rx.recv().await {
            received.push(v);
        }
        assert_eq!(received, (0..20).collect::<Vec<_>>());
    });
}

#[test]
fn mpsc_send_blocks_until_capacity_frees_up() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(1).await.unwrap();

        let sender = rusty_tokio::spawn(async move {
            // This must wait for `rx.recv()` below to free up the one
            // slot before it can proceed.
            tx.send(2).await.unwrap();
        });

        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, Some(2));
        sender.await.unwrap();
    });
}

#[test]
fn reserve_then_send_delivers_the_value() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = mpsc::channel(4);
        let permit = tx.reserve().await.unwrap();
        permit.send(42);
        assert_eq!(rx.recv().await, Some(42));
    });
}

#[test]
fn dropping_a_permit_unused_frees_its_slot_back_up() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = mpsc::channel::<i32>(1);
        let permit = tx.reserve().await.unwrap();
        drop(permit);
        // The dropped permit's slot must be free again -- a plain send
        // should succeed without blocking.
        tx.send(7).await.unwrap();
        assert_eq!(rx.recv().await, Some(7));
    });
}

#[test]
fn reserve_blocks_until_capacity_frees_up() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(1).await.unwrap();

        let reserver = rusty_tokio::spawn(async move {
            let permit = tx.reserve().await.unwrap();
            permit.send(2);
        });

        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, Some(2));
        reserver.await.unwrap();
    });
}

#[test]
fn reserve_fails_once_every_receiver_drops() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, rx) = mpsc::channel::<i32>(4);
        drop(rx);
        assert!(tx.reserve().await.is_err());
    });
}

#[test]
fn reserve_many_reserves_all_n_slots_up_front() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = mpsc::channel(4);
        let mut permits = tx.reserve_many(3).await.unwrap();

        // All 3 slots are reserved as soon as `reserve_many` resolves --
        // a 4th, independent reservation only has room for the one slot
        // still free (held here so it isn't immediately released again).
        let fourth = tx.try_reserve().unwrap();
        assert!(matches!(
            tx.try_reserve(),
            Err(rusty_tokio::sync::mpsc::TrySendError::Full(()))
        ));
        drop(fourth);

        permits.next().unwrap().send(1);
        permits.next().unwrap().send(2);
        permits.next().unwrap().send(3);
        assert!(permits.next().is_none());

        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(rx.recv().await, Some(3));
    });
}

#[test]
fn dropping_a_partially_consumed_permit_iterator_frees_the_rest() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = mpsc::channel(3);
        let mut permits = tx.reserve_many(3).await.unwrap();
        permits.next().unwrap().send(1);
        // The other 2 reserved-but-unused permits are dropped here,
        // along with the iterator itself.
        drop(permits);

        // Both slots the iterator was still holding must be free again.
        tx.send(2).await.unwrap();
        tx.send(3).await.unwrap();

        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(rx.recv().await, Some(3));
    });
}

#[test]
fn reserve_owned_moves_the_sender_into_the_permit_and_back_out_on_send() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = mpsc::channel(4);
        let permit = tx.reserve_owned().await.unwrap();
        let tx = permit.send(9);
        tx.send(10).await.unwrap();

        assert_eq!(rx.recv().await, Some(9));
        assert_eq!(rx.recv().await, Some(10));
    });
}

#[test]
fn owned_permit_release_gives_the_sender_back_without_sending() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = mpsc::channel::<i32>(1);
        let permit = tx.reserve_owned().await.unwrap();
        let tx = permit.release();

        // The released slot is free again.
        tx.send(5).await.unwrap();
        assert_eq!(rx.recv().await, Some(5));
    });
}

#[test]
fn try_reserve_fails_full_without_waiting_then_succeeds_after_a_release() {
    let (tx, mut rx) = mpsc::channel::<i32>(1);
    let permit = tx.try_reserve().unwrap();
    assert!(matches!(
        tx.try_reserve(),
        Err(rusty_tokio::sync::mpsc::TrySendError::Full(()))
    ));
    permit.send(1);

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        assert_eq!(rx.recv().await, Some(1));
        assert!(tx.try_reserve().is_ok());
    });
}

#[test]
fn try_reserve_fails_closed_once_the_receiver_drops() {
    let (tx, rx) = mpsc::channel::<i32>(4);
    drop(rx);
    assert!(matches!(
        tx.try_reserve(),
        Err(rusty_tokio::sync::mpsc::TrySendError::Closed(()))
    ));
}

#[test]
fn try_reserve_owned_hands_the_sender_back_on_failure() {
    let (tx, rx) = mpsc::channel::<i32>(4);
    drop(rx);
    match tx.try_reserve_owned() {
        Err(rusty_tokio::sync::mpsc::TrySendError::Closed(returned_tx)) => {
            // Got the exact same Sender back, not lost.
            drop(returned_tx);
        }
        _ => panic!("expected TrySendError::Closed"),
    }
}

#[test]
fn try_send_succeeds_while_there_is_room_then_reports_full() {
    let (tx, mut rx) = mpsc::channel(1);
    tx.try_send(1).unwrap();
    match tx.try_send(2) {
        Err(rusty_tokio::sync::mpsc::TrySendError::Full(2)) => {}
        other => panic!("expected TrySendError::Full(2), got {other:?}"),
    }

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        assert_eq!(rx.recv().await, Some(1));
    });
}

#[test]
fn try_send_reports_closed_once_the_receiver_drops() {
    let (tx, rx) = mpsc::channel::<i32>(4);
    drop(rx);
    match tx.try_send(9) {
        Err(rusty_tokio::sync::mpsc::TrySendError::Closed(9)) => {}
        other => panic!("expected TrySendError::Closed(9), got {other:?}"),
    }
}

#[test]
fn send_timeout_succeeds_once_capacity_frees_up_in_time() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(1).await.unwrap();

        let sender = rusty_tokio::spawn(async move {
            tx.send_timeout(2, Duration::from_secs(5)).await.unwrap();
        });

        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, Some(2));
        sender.await.unwrap();
    });
}

#[test]
fn send_timeout_times_out_and_hands_the_value_back() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, _rx) = mpsc::channel::<i32>(1);
        tx.send(1).await.unwrap(); // fill the one slot; nobody ever reads it

        match tx.send_timeout(2, Duration::from_millis(20)).await {
            Err(rusty_tokio::sync::mpsc::SendTimeoutError::Timeout(2)) => {}
            other => panic!("expected SendTimeoutError::Timeout(2), got {other:?}"),
        }
    });
}

#[test]
fn send_timeout_reports_closed_once_the_receiver_drops() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, rx) = mpsc::channel::<i32>(4);
        drop(rx);
        match tx.send_timeout(9, Duration::from_secs(5)).await {
            Err(rusty_tokio::sync::mpsc::SendTimeoutError::Closed(9)) => {}
            other => panic!("expected SendTimeoutError::Closed(9), got {other:?}"),
        }
    });
}

#[test]
fn unbounded_send_never_blocks_even_far_past_any_bounded_capacity() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = mpsc::unbounded_channel();
        // A bounded channel with any finite capacity would eventually
        // make one of these `.await`, since `send` here isn't even
        // `async fn` -- there's nothing to await, and nothing here
        // should ever suspend the task.
        for i in 0..10_000 {
            tx.send(i).unwrap();
        }
        drop(tx);

        let mut received = Vec::new();
        while let Some(v) = rx.recv().await {
            received.push(v);
        }
        assert_eq!(received, (0..10_000).collect::<Vec<_>>());
    });
}

#[test]
fn unbounded_send_reports_a_closed_channel_once_the_receiver_drops() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, rx) = mpsc::unbounded_channel::<u32>();
        drop(rx);
        assert!(tx.send(1).is_err());
    });
}

#[test]
fn unbounded_recv_returns_none_once_every_sender_drops() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = mpsc::unbounded_channel::<u32>();
        let tx2 = tx.clone();
        drop(tx);
        drop(tx2);
        assert_eq!(rx.recv().await, None);
    });
}

#[test]
fn notify_wakes_a_waiter() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let notify = Arc::new(Notify::new());
        let notify2 = notify.clone();
        let waiter = rusty_tokio::spawn(async move {
            notify2.notified().await;
            "woken"
        });
        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        notify.notify_one();
        assert_eq!(waiter.await.unwrap(), "woken");
    });
}

#[test]
fn notify_permit_is_banked_for_an_early_notify() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let notify = Notify::new();
        notify.notify_one(); // nobody waiting yet -- banks a permit
                             // Must resolve immediately without ever needing an external wake.
        rusty_tokio::time::timeout(Duration::from_millis(100), notify.notified())
            .await
            .expect("banked permit should let this resolve immediately");
    });
}

#[test]
fn notify_waiters_wakes_and_resolves_every_current_waiter() {
    // Regression test: a naive `notify_waiters` that wakes a waiter's
    // stored `Waker` without also leaving it something to see on the
    // next poll (unlike `notify_one`'s banked permit) lets that woken
    // future register nothing new (it's already `registered`) and
    // return `Pending` forever -- the wakeup fires but is then silently
    // lost. Exercises two concurrent waiters so a bug that only wakes
    // one of them (or wakes both but neither ever actually resolves)
    // shows up as a hang caught by the timeout below.
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    rt.block_on(async {
        let notify = Arc::new(Notify::new());
        let waiters: Vec<_> = (0..2)
            .map(|_| {
                let notify = notify.clone();
                rusty_tokio::spawn(async move { notify.notified().await })
            })
            .collect();

        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        notify.notify_waiters();

        for w in waiters {
            rusty_tokio::time::timeout(Duration::from_millis(200), w)
                .await
                .expect("notify_waiters should wake and resolve every current waiter")
                .unwrap();
        }
    });
}

#[test]
fn notify_waiters_does_not_bank_anything_for_a_later_waiter() {
    // The other half of `notify_waiters`'s documented contract: it only
    // wakes tasks waiting *at the time it's called* -- a `notified()`
    // registered afterward should still have to wait for its own
    // notification, not spuriously resolve from a broadcast that
    // already happened.
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let notify = Notify::new();
        notify.notify_waiters(); // nobody waiting yet -- should be a no-op
        let result =
            rusty_tokio::time::timeout(Duration::from_millis(50), notify.notified()).await;
        assert!(
            result.is_err(),
            "a notified() registered after notify_waiters() already fired should not resolve on its own"
        );
    });
}

#[test]
fn once_cell_get_or_init_runs_the_initializer_exactly_once() {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    rt.block_on(async {
        let cell = Arc::new(OnceCell::new());
        let calls = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..20 {
            let cell = cell.clone();
            let calls = calls.clone();
            handles.push(rusty_tokio::spawn(async move {
                *cell
                    .get_or_init(|| async {
                        calls.fetch_add(1, Ordering::SeqCst);
                        rusty_tokio::time::sleep(Duration::from_millis(10)).await;
                        42
                    })
                    .await
            }));
        }

        for h in handles {
            assert_eq!(h.await.unwrap(), 42);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn once_cell_get_returns_none_before_init_and_some_after() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let cell = OnceCell::new();
        assert_eq!(cell.get(), None);
        assert!(!cell.initialized());

        let value = cell.get_or_init(|| async { "hello" }).await;
        assert_eq!(*value, "hello");
        assert_eq!(cell.get(), Some(&"hello"));
        assert!(cell.initialized());
    });
}

#[test]
fn once_cell_set_succeeds_once_then_fails() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let cell = OnceCell::new();
        assert!(cell.set(1).is_ok());
        match cell.set(2) {
            Err(rusty_tokio::sync::SetError::AlreadyInitialized(v)) => assert_eq!(v, 2),
            _ => panic!("expected AlreadyInitialized"),
        }
        assert_eq!(cell.get(), Some(&1));
    });
}

#[test]
fn once_cell_new_with_is_already_initialized() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let cell = OnceCell::new_with(7);
        assert!(cell.initialized());
        assert_eq!(*cell.get_or_init(|| async { unreachable!() }).await, 7);
    });
}

#[test]
fn once_cell_into_inner_returns_the_value_when_initialized() {
    let cell: OnceCell<String> = OnceCell::new();
    assert_eq!(cell.into_inner(), None);

    let cell = OnceCell::new_with(String::from("owned"));
    assert_eq!(cell.into_inner(), Some(String::from("owned")));
}

#[test]
fn once_cell_recovers_after_the_initializer_panics() {
    let rt = Runtime::builder().worker_threads(2).build().unwrap();
    rt.block_on(async {
        let cell = Arc::new(OnceCell::new());

        // First caller's initializer panics -- the panic must not wedge
        // the cell in "initializing" forever.
        let cell1 = cell.clone();
        let first = rusty_tokio::spawn(async move {
            cell1.get_or_init(|| async { panic!("boom") }).await;
        });
        assert!(first.await.unwrap_err().is_panic());

        assert!(!cell.initialized());
        let value = cell.get_or_init(|| async { 99 }).await;
        assert_eq!(*value, 99);
    });
}

#[test]
fn once_cell_recovers_after_the_initializer_is_cancelled() {
    let rt = Runtime::builder().worker_threads(2).build().unwrap();
    rt.block_on(async {
        let cell = Arc::new(OnceCell::new());

        let cell1 = cell.clone();
        let handle = rusty_tokio::spawn(async move {
            cell1
                .get_or_init(|| async {
                    rusty_tokio::time::sleep(Duration::from_secs(60)).await;
                    1
                })
                .await;
        });
        // Give the task a chance to actually start (register the sleep,
        // moving the cell to `Initializing`) before aborting it.
        rusty_tokio::time::sleep(Duration::from_millis(20)).await;
        handle.abort();
        assert!(handle.await.unwrap_err().is_cancelled());

        assert!(!cell.initialized());
        let value = cell.get_or_init(|| async { 2 }).await;
        assert_eq!(*value, 2);
    });
}

#[test]
fn once_cell_concurrent_waiters_see_the_first_successful_result_after_a_panic() {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    rt.block_on(async {
        let cell = Arc::new(OnceCell::new());
        let attempt = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..10 {
            let cell = cell.clone();
            let attempt = attempt.clone();
            handles.push(rusty_tokio::spawn(async move {
                cell.get_or_init(|| async {
                    // Only the very first attempt across all callers
                    // panics; whichever caller (or callers, since a
                    // panic wakes every parked waiter to re-race) picks
                    // it up next succeeds.
                    if attempt.fetch_add(1, Ordering::SeqCst) == 0 {
                        rusty_tokio::time::sleep(Duration::from_millis(5)).await;
                        panic!("first attempt always fails");
                    }
                    123
                })
                .await;
            }));
        }

        let mut panicked = 0;
        let mut succeeded = 0;
        for h in handles {
            match h.await {
                Ok(()) => succeeded += 1,
                Err(e) if e.is_panic() => panicked += 1,
                Err(e) => panic!("unexpected error: {e:?}"),
            }
        }
        assert_eq!(panicked, 1);
        assert_eq!(succeeded, 9);
        assert_eq!(cell.get(), Some(&123));
    });
}

#[test]
fn broadcast_every_receiver_gets_every_message() {
    let rt = Runtime::builder().worker_threads(2).build().unwrap();
    rt.block_on(async {
        let (tx, mut rx1) = rusty_tokio::sync::broadcast::channel::<i32>(8);
        let mut rx2 = tx.subscribe();

        tx.send(1).unwrap();
        tx.send(2).unwrap();

        assert_eq!(rx1.recv().await.unwrap(), 1);
        assert_eq!(rx1.recv().await.unwrap(), 2);
        assert_eq!(rx2.recv().await.unwrap(), 1);
        assert_eq!(rx2.recv().await.unwrap(), 2);
    });
}

#[test]
fn broadcast_subscribe_only_sees_messages_sent_afterward() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, _rx1) = rusty_tokio::sync::broadcast::channel::<i32>(8);
        tx.send(1).unwrap();

        let mut late = tx.subscribe();
        tx.send(2).unwrap();

        assert_eq!(late.recv().await.unwrap(), 2);
    });
}

#[test]
fn broadcast_recv_waits_for_a_message_sent_later() {
    let rt = Runtime::builder().worker_threads(2).build().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = rusty_tokio::sync::broadcast::channel::<&str>(4);
        rusty_tokio::spawn(async move {
            rusty_tokio::time::sleep(Duration::from_millis(20)).await;
            tx.send("hello").unwrap();
        });
        assert_eq!(rx.recv().await.unwrap(), "hello");
    });
}

#[test]
fn broadcast_lagging_receiver_reports_how_many_it_missed() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = rusty_tokio::sync::broadcast::channel::<i32>(2);
        for i in 0..5 {
            tx.send(i).unwrap();
        }
        // Capacity 2, five sent -- this receiver is now three behind
        // the oldest still-buffered message (values 2 and 3).
        match rx.recv().await {
            Err(rusty_tokio::sync::broadcast::RecvError::Lagged(n)) => assert_eq!(n, 3),
            other => panic!("expected Lagged(3), got {other:?}"),
        }
        // Resumes from the oldest still-available message afterward,
        // not stuck reporting Lagged again.
        assert_eq!(rx.recv().await.unwrap(), 3);
        assert_eq!(rx.recv().await.unwrap(), 4);
    });
}

#[test]
fn broadcast_recv_reports_closed_once_every_sender_drops() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = rusty_tokio::sync::broadcast::channel::<i32>(4);
        tx.send(1).unwrap();
        drop(tx);

        assert_eq!(rx.recv().await.unwrap(), 1);
        assert_eq!(
            rx.recv().await.unwrap_err(),
            rusty_tokio::sync::broadcast::RecvError::Closed
        );
    });
}

#[test]
fn broadcast_send_fails_once_every_receiver_drops() {
    let (tx, rx) = rusty_tokio::sync::broadcast::channel::<i32>(4);
    drop(rx);
    assert!(tx.send(1).is_err());
}

#[test]
fn broadcast_send_returns_the_current_receiver_count() {
    let (tx, _rx1) = rusty_tokio::sync::broadcast::channel::<i32>(4);
    let _rx2 = tx.subscribe();
    let _rx3 = tx.subscribe();
    assert_eq!(tx.send(1).unwrap(), 3);
    assert_eq!(tx.receiver_count(), 3);
}

#[test]
fn broadcast_try_recv_reports_empty_lagged_and_closed() {
    let (tx, mut rx) = rusty_tokio::sync::broadcast::channel::<i32>(2);
    assert_eq!(
        rx.try_recv().unwrap_err(),
        rusty_tokio::sync::broadcast::TryRecvError::Empty
    );

    for i in 0..4 {
        tx.send(i).unwrap();
    }
    assert_eq!(
        rx.try_recv().unwrap_err(),
        rusty_tokio::sync::broadcast::TryRecvError::Lagged(2)
    );
    assert_eq!(rx.try_recv().unwrap(), 2);
    assert_eq!(rx.try_recv().unwrap(), 3);
    assert_eq!(
        rx.try_recv().unwrap_err(),
        rusty_tokio::sync::broadcast::TryRecvError::Empty
    );

    drop(tx);
    assert_eq!(
        rx.try_recv().unwrap_err(),
        rusty_tokio::sync::broadcast::TryRecvError::Closed
    );
}

#[test]
fn broadcast_ring_buffer_overwrites_the_oldest_slot_once_full() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = rusty_tokio::sync::broadcast::channel::<i32>(3);
        for i in 0..3 {
            tx.send(i).unwrap();
        }
        // Fully caught up -- no lag yet.
        assert_eq!(rx.recv().await.unwrap(), 0);
        tx.send(3).unwrap(); // evicts `0`, but rx already read it
        tx.send(4).unwrap(); // evicts `1`
        match rx.recv().await {
            Err(rusty_tokio::sync::broadcast::RecvError::Lagged(n)) => assert_eq!(n, 1),
            other => panic!("expected Lagged(1), got {other:?}"),
        }
        assert_eq!(rx.recv().await.unwrap(), 2);
        assert_eq!(rx.recv().await.unwrap(), 3);
        assert_eq!(rx.recv().await.unwrap(), 4);
    });
}
