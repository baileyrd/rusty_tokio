use rusty_tokio::time::{interval_at, timeout_at, MissedTickBehavior};
use rusty_tokio::Runtime;
use std::time::{Duration, Instant};

#[test]
fn interval_at_fires_its_first_tick_at_the_given_start() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let start = Instant::now() + Duration::from_millis(30);
        let mut ticker = interval_at(start, Duration::from_millis(10));
        let first = ticker.tick().await;
        assert_eq!(first, start);
        assert!(Instant::now() >= start);
    });
}

#[test]
fn interval_at_second_tick_is_exactly_one_period_after_start() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let start = Instant::now();
        let period = Duration::from_millis(15);
        let mut ticker = interval_at(start, period);
        let first = ticker.tick().await;
        let second = ticker.tick().await;
        assert_eq!(first, start);
        assert_eq!(second, start + period);
    });
}

#[test]
fn poll_tick_is_pending_before_the_deadline_then_ready_at_it() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let start = Instant::now() + Duration::from_millis(30);
        let period = Duration::from_millis(10);
        let mut ticker = interval_at(start, period);

        // Polling manually, well before `start`, rather than `.await`ing
        // `tick()` -- exercises the same poll-based entry point a manual
        // `Future`/`Stream` impl would drive directly.
        std::future::poll_fn(|cx| match ticker.poll_tick(cx) {
            std::task::Poll::Pending => std::task::Poll::Ready(()),
            std::task::Poll::Ready(_) => panic!("expected Pending before the deadline"),
        })
        .await;

        let first = ticker.tick().await;
        assert_eq!(first, start);

        // A second poll_tick, before the next period elapses, is
        // Pending again rather than immediately Ready.
        std::future::poll_fn(|cx| match ticker.poll_tick(cx) {
            std::task::Poll::Pending => std::task::Poll::Ready(()),
            std::task::Poll::Ready(_) => panic!("expected Pending right after the first tick"),
        })
        .await;

        let second = ticker.tick().await;
        assert_eq!(second, start + period);
    });
}

#[test]
fn missed_tick_default_is_burst() {
    let ticker = interval_at(Instant::now(), Duration::from_millis(10));
    assert_eq!(ticker.missed_tick_behavior(), MissedTickBehavior::Burst);
}

#[test]
fn missed_tick_burst_fires_every_missed_deadline_back_to_back() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let period = Duration::from_millis(20);
        let start = Instant::now();
        let mut ticker = interval_at(start, period);
        let first = ticker.tick().await;
        assert_eq!(first, start);

        // Let several periods elapse without calling tick() again.
        rusty_tokio::time::sleep(period * 4).await;

        let before = Instant::now();
        let second = ticker.tick().await;
        let third = ticker.tick().await;
        assert_eq!(second, start + period);
        assert_eq!(third, start + period * 2);
        // Both already-overdue deadlines should fire back-to-back with
        // no real waiting in between -- the whole point of "burst".
        assert!(before.elapsed() < period);
    });
}

#[test]
fn missed_tick_skip_jumps_to_the_next_future_deadline_without_bursting() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let period = Duration::from_millis(20);
        let start = Instant::now();
        let mut ticker = interval_at(start, period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        assert_eq!(ticker.missed_tick_behavior(), MissedTickBehavior::Skip);
        let first = ticker.tick().await;
        assert_eq!(first, start);

        rusty_tokio::time::sleep(period * 4).await;

        // The first post-gap tick still reports the originally-missed
        // deadline -- Skip only changes what's scheduled *next*.
        let second = ticker.tick().await;
        assert_eq!(second, start + period);

        // The next deadline should have jumped straight to a
        // still-in-the-future, period-aligned instant rather than the
        // very next tick in the original grid (`start + 2 * period`,
        // already in the past by now) -- so this call actually has to
        // wait, unlike Burst's back-to-back catch-up above.
        let before = Instant::now();
        let third = ticker.tick().await;
        assert!(third > start + period * 2);
        assert!(before.elapsed() > Duration::from_millis(1));
    });
}

#[test]
fn missed_tick_delay_resets_the_schedule_from_the_actual_fire_time() {
    // Unlike the Burst/Skip tests above, this never asserts a tick's
    // deadline equals `start + n * period` exactly: Delay recomputes
    // the *next* deadline from whenever each tick actually fires, not
    // from the original grid -- including the very first tick, whose
    // completion happens a few microseconds after `start`, not exactly
    // at it. What Delay actually guarantees is checked here instead:
    // after a long gap, the next tick waits close to one fresh period,
    // not "already overdue" (Burst) or "several periods skipped" (Skip).
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let period = Duration::from_millis(20);
        let mut ticker = interval_at(Instant::now(), period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        assert_eq!(ticker.missed_tick_behavior(), MissedTickBehavior::Delay);
        ticker.tick().await;

        rusty_tokio::time::sleep(period * 4).await;

        let before_second = Instant::now();
        ticker.tick().await; // still fires immediately -- already overdue

        let before_third = Instant::now();
        let third = ticker.tick().await;
        assert!(third >= before_second + period);
        assert!(third < before_second + period * 2);
        assert!(before_third.elapsed() < period * 2);
    });
}

#[test]
fn timeout_at_resolves_ok_when_the_future_finishes_before_the_deadline() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let deadline = Instant::now() + Duration::from_millis(50);
        let result = timeout_at(deadline, async { 42 }).await;
        assert_eq!(result, Ok(42));
    });
}

#[test]
fn timeout_at_resolves_elapsed_once_the_deadline_passes() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let deadline = Instant::now() + Duration::from_millis(10);
        let result = timeout_at(
            deadline,
            rusty_tokio::time::sleep(Duration::from_secs(3600)),
        )
        .await;
        assert!(result.is_err());
    });
}

#[test]
fn timeout_at_with_an_already_passed_deadline_resolves_elapsed_immediately() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let deadline = Instant::now() - Duration::from_millis(10);
        let before = Instant::now();
        let result = timeout_at(
            deadline,
            rusty_tokio::time::sleep(Duration::from_secs(3600)),
        )
        .await;
        assert!(result.is_err());
        assert!(before.elapsed() < Duration::from_millis(200));
    });
}
