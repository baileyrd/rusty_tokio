# rusty_tokio

A hand-rolled async runtime for Rust, built from scratch on `std` and raw
`libc` syscalls only -- no `tokio`, no `mio`, no `crossbeam`. It exists to
actually understand how an async runtime works, not to replace tokio.

## What's here

- **Task system** (`task`): a heap-allocated future plus a small atomic
  state machine that decides, on every wake, whether to (re-)enqueue it.
  The obvious "channel of `Arc<Task>`" design (the one most "build your
  own executor" blog posts use) has a real lost-wakeup bug once you're
  actually multi-threaded: a wake that lands *while* a task is mid-poll
  finds the future temporarily missing from its slot and silently drops
  the wakeup. `task`'s module docs walk through the fix.
- **Runtime** (`Runtime`, `Handle`): a fixed pool of worker threads, each
  with its own run queue, backed by a shared injector queue for tasks
  spawned from outside the pool, with work-stealing between workers when
  one goes idle.
- **I/O** (`io`): an `epoll`-backed reactor thread plus non-blocking
  `TcpStream`, `TcpListener`, and `UdpSocket`. Level-triggered by choice
  -- edge-triggered epoll requires every reader to drain a fd to
  `EWOULDBLOCK` or risk missing events forever, an easy invariant to get
  subtly wrong for one extra syscall's worth of savings.
- **Timers** (`time`): `sleep`, `sleep_until`, `timeout`, and `interval`,
  backed by a single background thread holding a min-heap of deadlines.
- **Sync primitives** (`sync`): `Notify` (an async condition variable),
  an async-aware `Mutex`, a `oneshot` channel, and a bounded `mpsc`
  channel.

## Example

```rust
use rusty_tokio::Runtime;
use rusty_tokio::io::TcpListener;

fn main() -> std::io::Result<()> {
    let rt = Runtime::new()?;
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:7878".parse().unwrap())?;
        loop {
            let (stream, _peer) = listener.accept().await?;
            rusty_tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                while let Ok(n) = stream.read(&mut buf).await {
                    if n == 0 || stream.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            });
        }
    })
}
```

More in `examples/`: `tcp_echo.rs`, `timers.rs`, `channels.rs`. Run with
`cargo run --example <name>`.

## What's deliberately not here (yet)

This is a real, working runtime, not a toy -- but it's honest about its
edges instead of papering over them:

- **Linux only.** The reactor is built directly on `epoll`, `eventfd`,
  and `accept4`. A `kqueue` (macOS/BSD) or IOCP (Windows) backend behind
  the same `ScheduledIo` interface is doable, just not done.
- **No `AsyncRead`/`AsyncWrite` trait interop.** `TcpStream` exposes
  plain inherent `async fn read`/`write` rather than the trait pair the
  wider ecosystem (codec/framing crates, etc.) expects.
- **No `spawn_blocking` / blocking thread pool.** A task that calls a
  genuinely blocking syscall stalls the worker thread it's running on.
- **Work-stealing queues are `Mutex<VecDeque<_>>`, not lock-free.**
  Correct and simple; a real lock-free Chase-Lev deque (what tokio
  itself uses) would scale better under heavy contention.
- **No `io_uring`.**

## Testing

```
cargo test      # unit tests for the task state machine, plus integration
                # tests covering multi-threaded scheduling, abort, panics,
                # TCP/UDP over the real reactor, and every sync primitive
cargo clippy --all-targets
```

The integration tests deliberately exercise real concurrency (many
spawned tasks, real timer-driven yields, actual socket I/O) rather than
mocking the scheduler, since the bugs this kind of code has are races
that only show up under real multi-threaded execution.
