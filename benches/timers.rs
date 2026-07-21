//! Hand-rolled timer benchmarks -- issue #13 asked for actual
//! measurements of `TimerDriver`'s single-background-thread,
//! `BinaryHeap`-of-deadlines design rather than assumptions. No
//! `criterion` (or any other new dependency) here, matching this
//! project's "no unnecessary dependencies" posture: this is a plain
//! `harness = false` binary (see `Cargo.toml`'s `[[bench]]` entry) that
//! times things by hand with `Instant` and prints a report. Run with
//! `cargo bench` (or `cargo run --release --bench timers -- --bench`,
//! equivalent) -- always in `--release`; timer skew measured in debug
//! builds mostly reflects unoptimized-build overhead, not the driver.
//!
//! Only measures through the crate's public API (`sleep`/`interval`),
//! since `TimerDriver` itself is `pub(crate)` -- this also happens to be
//! the more meaningful measurement anyway: end-to-end wall-clock skew as
//! a real caller would observe it, not internal method latency.

use rusty_tokio::time::{interval, sleep};
use rusty_tokio::Runtime;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, Instant};

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

fn report(label: &str, mut samples: Vec<Duration>) {
    samples.sort();
    let sum: Duration = samples.iter().sum();
    let avg = sum / samples.len() as u32;
    println!(
        "{label}: n={} avg={:?} p50={:?} p99={:?} max={:?}",
        samples.len(),
        avg,
        percentile(&samples, 0.50),
        percentile(&samples, 0.99),
        percentile(&samples, 1.0),
    );
}

/// How far a single, uncontended `sleep(duration)` overshoots its
/// requested duration -- the baseline number every other measurement
/// here is compared against.
fn bench_single_sleep_skew(rt: &Runtime) {
    const N: usize = 500;
    const DURATION: Duration = Duration::from_millis(5);

    let skews = rt.block_on(async {
        let mut skews = Vec::with_capacity(N);
        for _ in 0..N {
            let start = Instant::now();
            sleep(DURATION).await;
            skews.push(start.elapsed().saturating_sub(DURATION));
        }
        skews
    });
    report("single sleep skew (5ms requested)", skews);
}

/// Registration + cancellation throughput: poll a long-lived `Sleep`
/// exactly once (triggering `TimerDriver::register`) then drop it
/// (triggering `TimerDriver::cancel`) without ever letting it fire --
/// the churn pattern a lot of short-lived `timeout()` wrappers around
/// fast-completing futures produce in practice, since `Timeout::poll`
/// creates its `Sleep` up front but usually only ever cancels it.
fn bench_register_cancel_churn(rt: &Runtime) {
    const N: usize = 50_000;

    let elapsed = rt.block_on(async {
        let start = Instant::now();
        for _ in 0..N {
            let mut pending = Some(sleep(Duration::from_secs(30)));
            std::future::poll_fn(|cx| {
                if let Some(s) = pending.as_mut() {
                    let _ = Pin::new(s).poll(cx);
                }
                Poll::Ready(())
            })
            .await;
            drop(pending.take());
        }
        start.elapsed()
    });
    let per_op = elapsed / N as u32;
    println!(
        "register+cancel churn: {N} ops in {elapsed:?} ({:.0} ops/sec, {per_op:?}/op)",
        N as f64 / elapsed.as_secs_f64()
    );
}

/// The head-of-line-blocking question `fire_due`'s docs raise directly:
/// does a canary sleep due *just after* a burst of `BURST` simultaneous
/// sleeps get delayed by however long it takes `fire_due` to pop and
/// wake the whole burst first? Compares the canary's skew here against
/// the uncontended baseline from `bench_single_sleep_skew` above.
fn bench_burst_then_canary(rt: &Runtime) {
    const BURST: usize = 2_000;
    const N: usize = 50;
    const DURATION: Duration = Duration::from_millis(5);

    let skews = rt.block_on(async {
        let mut skews = Vec::with_capacity(N);
        for _ in 0..N {
            let burst: Vec<_> = (0..BURST)
                .map(|_| rusty_tokio::spawn(sleep(DURATION)))
                .collect();
            // Due a fixed 1ms after the burst -- if the burst delays it,
            // this is where that shows up.
            let start = Instant::now();
            sleep(DURATION + Duration::from_millis(1)).await;
            skews.push(
                start
                    .elapsed()
                    .saturating_sub(DURATION + Duration::from_millis(1)),
            );
            for b in burst {
                let _ = b.await;
            }
        }
        skews
    });
    report(
        &format!("canary skew right behind a {BURST}-sleep burst"),
        skews,
    );
}

/// `Interval` claims to correct for drift by scheduling from the
/// *previous* deadline rather than `Instant::now() + period` on every
/// tick -- this actually walks many ticks and checks whether the
/// schedule holds (each tick's ideal instant is `start + period * i`,
/// regardless of how late any earlier tick actually fired).
fn bench_interval_drift(rt: &Runtime) {
    const TICKS: usize = 500;
    const PERIOD: Duration = Duration::from_millis(2);

    let deviations = rt.block_on(async {
        let mut ticker = interval(PERIOD);
        let mut deviations = Vec::with_capacity(TICKS);
        let mut next_ideal: Option<Instant> = None;
        for _ in 0..TICKS {
            let fired_at = ticker.tick().await;
            let ideal = *next_ideal.get_or_insert(fired_at);
            deviations.push(fired_at.saturating_duration_since(ideal));
            next_ideal = Some(ideal + PERIOD);
        }
        deviations
    });
    // Report the deviation of the *last* tick from its ideal schedule
    // instant -- cumulative drift, not per-tick skew (a driver that
    // corrects for drift keeps this bounded no matter how many ticks
    // run; one that doesn't grows it linearly with `TICKS`).
    println!(
        "interval cumulative drift after {TICKS} ticks @ {PERIOD:?}: {:?}",
        deviations.last().unwrap()
    );
}

fn main() {
    let rt = Runtime::new().unwrap();
    println!("--- rusty_tokio timer benchmarks (issue #13) ---");
    bench_single_sleep_skew(&rt);
    bench_register_cancel_churn(&rt);
    bench_burst_then_canary(&rt);
    bench_interval_drift(&rt);
}
