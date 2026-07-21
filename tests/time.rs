use rusty_tokio::time::interval_at;
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
