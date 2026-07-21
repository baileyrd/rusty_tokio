use rusty_tokio::sync::{mpsc, oneshot, Mutex, Notify};
use rusty_tokio::Runtime;
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
