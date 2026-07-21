//! `sleep`, `timeout`, and `interval` running concurrently across
//! multiple spawned tasks.

use rusty_tokio::time::{interval, sleep, timeout};
use rusty_tokio::Runtime;
use std::time::Duration;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let sleeper = rusty_tokio::spawn(async {
            let start = std::time::Instant::now();
            sleep(Duration::from_millis(50)).await;
            println!("slept for {:?}", start.elapsed());
        });

        let ticker = rusty_tokio::spawn(async {
            let mut ticks = interval(Duration::from_millis(20));
            for i in 0..3 {
                ticks.tick().await;
                println!("tick {i}");
            }
        });

        let timed_out = timeout(Duration::from_millis(10), sleep(Duration::from_millis(200))).await;
        println!("short timeout over a long sleep: {timed_out:?}");

        let finished_in_time =
            timeout(Duration::from_millis(200), sleep(Duration::from_millis(10))).await;
        println!("long timeout over a short sleep: {finished_in_time:?}");

        sleeper.await.unwrap();
        ticker.await.unwrap();
    });
}
