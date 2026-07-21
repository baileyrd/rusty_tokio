use rusty_tokio::sync::{mpsc, oneshot, Mutex, Notify, RwLock, Semaphore};
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
