# rusty_tokio

A hand-rolled async runtime for Rust, built from scratch on `std` -- no
`tokio`, no `mio`, no `crossbeam`. It exists to actually understand how an
async runtime works, not to replace tokio.

The scheduler, reactor, timers, and sync primitives are all original code
here. Socket lifecycle (bind/connect/accept/addressing) is built on top of
[`rustils`](https://github.com/baileyrd/rustils)' `platform`/`platform-linux`
crates rather than reimplemented a second time -- see "Built on rustils"
below for exactly which seam that is and which two syscalls stayed
hand-rolled because rustils' API can't support them yet.

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
  subtly wrong for one extra syscall's worth of savings. Socket setup
  goes through `rustils`; see "Built on rustils" below. Also includes an
  `AsyncRead`/`AsyncWrite` trait pair (shaped like tokio's/`futures-io`'s
  -- `Pin<&mut Self>`, `poll_*` methods -- but this crate's own
  definitions, not a re-export) plus a generic `copy`, so code doesn't
  need to be written against the concrete `TcpStream` type.
- **Timers** (`time`): `sleep`, `sleep_until`, `timeout`, and `interval`,
  backed by a single background thread holding a min-heap of deadlines.
- **Sync primitives** (`sync`): `Notify` (an async condition variable),
  an async-aware `Mutex`, a `oneshot` channel, and a bounded `mpsc`
  channel.
- **`spawn_blocking`**: offloads a genuinely blocking closure onto a
  separate thread pool that grows on demand (up to a configurable cap)
  and shrinks back down when idle, instead of stalling an async worker
  thread. Implemented as a `oneshot` channel plus an ordinary spawned
  task awaiting it -- not a parallel handle type -- so panics, abort,
  and `.await` on the returned `JoinHandle` all reuse the same task
  machinery every other spawned task does.

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

## Built on rustils

[`rustils`](https://github.com/baileyrd/rustils) is a cross-platform
syscall-wrapper library (portable traits + per-OS backends) with a
blocking-only, object-safe (`Box<dyn TcpStream>`-style) net layer -- not
built with async in mind. Getting a reactor to sit on top of it needed a
raw fd and a way to flip `O_NONBLOCK`, neither of which the net layer
exposed, so that gap was filed as
[rustils#41](https://github.com/baileyrd/rustils/issues/41) and landed in
[rustils#42](https://github.com/baileyrd/rustils/pull/42): `AsFd`/`AsRawFd`
and `set_nonblocking` on the five concrete Linux socket types, plus
concrete (non-`Box<dyn Trait>`) `connect`/`bind`/`accept` constructors --
without which the new methods would've been unreachable, since the
object-safe trait methods only ever hand back a type-erased `Box`.

With that in place, `io/tcp.rs` and `io/udp.rs` build directly on
`platform_linux::{LinuxTcpListener, LinuxTcpStream, LinuxUdpSocket}` for
bind/listen/accept, addressing (`local_addr`/`peer_addr`), `set_nodelay`,
and (for UDP) `send_to`/`recv_from` -- all of rustils' own sockaddr
packing/unpacking, `SO_REUSEADDR`, and stale-socket handling, not
reimplemented a second time here.

Two things stayed hand-rolled in `io/socket.rs`, both because of a real
mismatch rather than taste:

- **Non-blocking `connect`.** `LinuxTcpStream::connect` creates a
  *blocking* socket and calls a blocking `connect(2)` -- correct for
  rustils' own callers, but it would stall an entire worker thread for a
  connection's RTT if used here directly. An async connect needs the
  socket non-blocking *before* `connect(2)`, so it's created and
  connected by hand, then the resulting fd is adopted into a
  `LinuxTcpStream` via `From<OwnedFd>` for everything after.
- **`read`/`write`.** `platform::net::TcpStream::read`/`write` take
  `&mut self` -- fine for rustils' blocking callers, wrong for this
  runtime's `TcpStream`, which deliberately exposes `&self` methods so
  one task can read while another writes the same stream. Bypassing the
  trait for these two (a raw `read`/`write` on an fd already in hand via
  `AsRawFd`) keeps that API instead of hiding a mutex behind it.

## What's deliberately not here (yet)

This is a real, working runtime, not a toy -- but it's honest about its
edges instead of papering over them:

- **Linux only.** The reactor is built directly on `epoll`, `eventfd`,
  and `accept4`. A `kqueue` (macOS/BSD) or IOCP (Windows) backend behind
  the same `ScheduledIo` interface is doable, just not done.
- **`AsyncRead`/`AsyncWrite` are this crate's own traits, not tokio's or
  `futures-io`'s.** Same shape, so generic code within this project works
  the same way, but a third-party codec/framing crate built against
  tokio's actual trait won't accept this crate's `TcpStream` without a
  shim.
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
