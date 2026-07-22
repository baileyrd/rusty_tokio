//! Hand-rolled scheduler benchmarks -- issue #8 asked whether the
//! work-stealing queues' original `Mutex<VecDeque<_>>` design (correct
//! and simple, but every push/pop/steal took a lock) was actually a
//! bottleneck before reaching for a lock-free replacement, rather than
//! "optimizing blind" -- and, once `crossbeam_deque::{Worker, Stealer,
//! Injector}` replaced it (see `Runtime`'s own crate-doc bullet), this
//! same benchmark measures whether the swap actually helped. No
//! `criterion` here either, matching `benches/timers.rs`'s approach: a
//! plain `harness = false` binary, run with `cargo bench`, always in
//! `--release`.
//!
//! `Shared`'s local/injector queues are `pub(crate)`, so -- like the
//! timer benchmarks -- this can only measure end-to-end throughput
//! through the public API, not queue-internal contention directly. That
//! still answers the question that matters: does the current design's
//! throughput scale with available cores, or flatten out (or worse,
//! regress) as more worker threads start contending for the same
//! queues?

use rusty_tokio::Runtime;
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

/// Many trivial, independently-spawned tasks -- exercises the injector
/// queue (every task here is spawned from the non-worker `block_on`
/// thread) and ordinary steal-driven distribution across workers, but
/// with no single worker ever holding a large pile other workers must
/// fight over.
fn bench_many_independent_tasks(worker_threads: usize) {
    const N: usize = 200_000;
    let rt = Runtime::builder()
        .worker_threads(worker_threads)
        .build()
        .unwrap();
    let elapsed = rt.block_on(async {
        let start = Instant::now();
        let mut handles = Vec::with_capacity(N);
        for _ in 0..N {
            handles.push(rusty_tokio::spawn(async {}));
        }
        for h in handles {
            let _ = h.await;
        }
        start.elapsed()
    });
    println!(
        "  {worker_threads} worker(s): {N} independent tasks in {elapsed:?} ({:.0} tasks/sec)",
        N as f64 / elapsed.as_secs_f64()
    );
}

/// A binary tree of nested `spawn` calls: every task's two children are
/// spawned *from within it*, so (per `Shared::schedule`'s "spawned from
/// a worker thread goes on that worker's own local queue" rule) they
/// initially pile up on whichever single worker happened to be running
/// the parent -- exactly the "busy local queue, everyone else fighting
/// to steal from it" scenario issue #8's contention concern is about,
/// unlike `bench_many_independent_tasks` above.
fn fan_out(remaining: u32) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move {
        if remaining == 0 {
            return;
        }
        let a = rusty_tokio::spawn(fan_out(remaining - 1));
        let b = rusty_tokio::spawn(fan_out(remaining - 1));
        let _ = a.await;
        let _ = b.await;
    })
}

fn bench_steal_heavy_fan_out(worker_threads: usize) {
    const DEPTH: u32 = 18; // 2^18 - 1 = 262,143 tasks
    let total_tasks = (1u64 << DEPTH) - 1;
    let rt = Runtime::builder()
        .worker_threads(worker_threads)
        .build()
        .unwrap();
    let start = Instant::now();
    rt.block_on(fan_out(DEPTH));
    let elapsed = start.elapsed();
    println!(
        "  {worker_threads} worker(s): {total_tasks} steal-heavy tasks in {elapsed:?} ({:.0} tasks/sec)",
        total_tasks as f64 / elapsed.as_secs_f64()
    );
}

fn main() {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let worker_counts: Vec<usize> = [1, 2, cores]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();

    println!("--- rusty_tokio scheduler benchmarks (issue #8) -- {cores} cores available ---");
    println!("many independent tasks (injector + ordinary steal distribution):");
    for &w in &worker_counts {
        bench_many_independent_tasks(w);
    }
    println!("steal-heavy fan-out (one worker's local queue piles up, others must steal):");
    for &w in &worker_counts {
        bench_steal_heavy_fan_out(w);
    }
}
