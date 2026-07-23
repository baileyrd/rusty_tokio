use rusty_tokio::time;
use rusty_tokio::Builder;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn advance_fires_a_sleep_without_any_real_waiting() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        time::pause();
        let started = std::time::Instant::now();

        let (tx, rx) = rusty_tokio::sync::oneshot::channel::<()>();
        rusty_tokio::spawn(async move {
            time::sleep(Duration::from_secs(3600)).await;
            let _ = tx.send(());
        });
        // Give the spawned task a chance to actually run and register
        // its sleep with the timer driver before advancing.
        rusty_tokio::task::yield_now().await;

        time::advance(Duration::from_secs(3600)).await;
        rx.await.unwrap();

        // No real waiting happened -- the whole test should take
        // milliseconds, not anywhere near an hour.
        assert!(started.elapsed() < Duration::from_secs(5));
    });
}

#[test]
fn advance_fires_several_sleeps_in_deadline_order() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        time::pause();
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));

        for (label, delay) in [("c", 30), ("a", 10), ("b", 20)] {
            let order = order.clone();
            rusty_tokio::spawn(async move {
                time::sleep(Duration::from_secs(delay)).await;
                order.lock().unwrap().push(label);
            });
        }
        // Give every spawned task a chance to actually run once and
        // register its sleep with the timer driver before advancing --
        // otherwise `advance` would find an empty heap and have nothing
        // to fire.
        rusty_tokio::task::yield_now().await;

        time::advance(Duration::from_secs(30)).await;
        assert_eq!(*order.lock().unwrap(), vec!["a", "b", "c"]);
    });
}

#[test]
fn advance_lets_a_chained_sleep_fire_within_the_same_call() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        time::pause();
        let count = Arc::new(AtomicUsize::new(0));
        let count2 = count.clone();

        rusty_tokio::spawn(async move {
            time::sleep(Duration::from_secs(10)).await;
            count2.fetch_add(1, Ordering::SeqCst);
            time::sleep(Duration::from_secs(10)).await;
            count2.fetch_add(1, Ordering::SeqCst);
        });
        rusty_tokio::task::yield_now().await;

        time::advance(Duration::from_secs(25)).await;
        assert_eq!(count.load(Ordering::SeqCst), 2);
    });
}

#[test]
fn resume_makes_sleep_track_real_time_again() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        time::pause();
        time::advance(Duration::from_secs(100)).await;
        time::resume();

        let started = std::time::Instant::now();
        time::sleep(Duration::from_millis(20)).await;
        assert!(started.elapsed() >= Duration::from_millis(20));
    });
}

#[test]
#[should_panic(expected = "current-thread runtime flavor")]
fn pause_panics_on_a_multi_threaded_runtime() {
    let rt = rusty_tokio::Runtime::new().unwrap();
    rt.block_on(async {
        time::pause();
    });
}

#[test]
#[should_panic(expected = "already paused")]
fn pause_twice_panics() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        time::pause();
        time::pause();
    });
}

#[test]
#[should_panic(expected = "time is not paused")]
fn resume_without_pause_panics() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        time::resume();
    });
}

#[test]
#[should_panic(expected = "requires time::pause() first")]
fn advance_without_pause_panics() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        time::advance(Duration::from_secs(1)).await;
    });
}

#[test]
fn advance_drives_an_interval_through_several_ticks_deterministically() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        time::pause();
        let mut interval = time::interval(Duration::from_secs(5));

        time::advance(Duration::from_secs(5)).await;
        let first = interval.tick().await;

        time::advance(Duration::from_secs(15)).await;
        let second = interval.tick().await;
        let third = interval.tick().await;
        let fourth = interval.tick().await;

        assert_eq!(second, first + Duration::from_secs(5));
        assert_eq!(third, first + Duration::from_secs(10));
        assert_eq!(fourth, first + Duration::from_secs(15));
    });
}

#[test]
fn interval_period_reports_the_configured_period() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        let interval = time::interval(Duration::from_secs(5));
        assert_eq!(interval.period(), Duration::from_secs(5));
    });
}

#[test]
fn interval_reset_reschedules_one_period_from_now() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        time::pause();
        let mut interval = time::interval(Duration::from_secs(5));
        time::advance(Duration::from_secs(5)).await;
        let first = interval.tick().await;

        // Let some of the *next* scheduled period elapse, then reset --
        // the next tick should measure a fresh period from the reset
        // point, not from `first`'s original schedule.
        time::advance(Duration::from_secs(2)).await;
        interval.reset();
        time::advance(Duration::from_secs(5)).await;
        let second = interval.tick().await;

        assert_eq!(second, first + Duration::from_secs(7));
    });
}

#[test]
fn interval_reset_immediately_fires_the_next_tick_right_away() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        time::pause();
        let mut interval = time::interval(Duration::from_secs(5));
        time::advance(Duration::from_secs(5)).await;
        let first = interval.tick().await;

        time::advance(Duration::from_secs(1)).await;
        interval.reset_immediately();
        let second = interval.tick().await;
        assert_eq!(second, first + Duration::from_secs(1));
    });
}

#[test]
fn interval_reset_after_reschedules_from_now_by_the_given_duration() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        time::pause();
        let mut interval = time::interval(Duration::from_secs(5));
        time::advance(Duration::from_secs(5)).await;
        let first = interval.tick().await;

        time::advance(Duration::from_secs(1)).await;
        interval.reset_after(Duration::from_secs(3));
        time::advance(Duration::from_secs(3)).await;
        let second = interval.tick().await;
        assert_eq!(second, first + Duration::from_secs(4));
    });
}

#[test]
fn interval_reset_at_reschedules_to_the_given_absolute_deadline() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        time::pause();
        let mut interval = time::interval(Duration::from_secs(5));
        time::advance(Duration::from_secs(5)).await;
        let first = interval.tick().await;

        // Derived from `first` (the crate's own clock) rather than a
        // fresh `std::time::Instant::now()` -- while paused, the crate's
        // virtual clock only moves via `advance`, so reading real wall
        // time here would drift from it.
        let deadline = first + Duration::from_secs(10);
        interval.reset_at(deadline);
        time::advance(Duration::from_secs(10)).await;
        let second = interval.tick().await;
        assert_eq!(second, deadline);
    });
}
