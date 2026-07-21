# rusty_tokio

A hand-rolled async runtime for Rust, built from scratch on `std` -- no
`tokio`, no `mio`, no `crossbeam`. It exists to actually understand how an
async runtime works, not to replace tokio.

The scheduler, reactor, timers, and sync primitives are all original code
here. Socket lifecycle (bind/connect/accept/addressing) is built on top of
[`rustils`](https://github.com/baileyrd/rustils)'
`platform`/`platform-linux`/`platform-macos` crates rather than
reimplemented a second time -- see "Built on rustils" below for exactly
which seam that is and which two syscalls stayed hand-rolled because
rustils' API can't support them yet.

## What's here

- **Task system** (`task`): a heap-allocated future plus a small atomic
  state machine that decides, on every wake, whether to (re-)enqueue it.
  The obvious "channel of `Arc<Task>`" design (the one most "build your
  own executor" blog posts use) has a real lost-wakeup bug once you're
  actually multi-threaded: a wake that lands *while* a task is mid-poll
  finds the future temporarily missing from its slot and silently drops
  the wakeup. `task`'s module docs walk through the fix. Also
  `task::yield_now()`, for a task that wants to cooperate with others
  without splitting itself across multiple spawns, and `task::JoinSet`:
  a dynamic collection of spawned tasks -- `join_next().await` resolves
  as soon as *any* member finishes (not spawn order, unlike joining a
  plain `Vec<JoinHandle<T>>` one at a time), `abort_all`/`shutdown` to
  cancel everything at once, and -- unlike a bare `JoinHandle`, which
  never aborts on drop -- dropping the whole set aborts every task still
  in it.
- **`LocalSet`/`spawn_local`**: a place to spawn `!Send` futures --
  holding an `Rc`, a `RefCell`-guarded value, or any other non-thread-safe
  handle -- which `crate::spawn` can never accept, since every task
  spawned there is an `Arc<Task>` any worker thread may poll.
  `LocalSet::run_until(future)` drives every task spawned onto the set
  (via `LocalSet::spawn_local` or the ambient `task::spawn_local`)
  interleaved with `future`, synchronously, on whichever thread calls
  it -- the same "blocks until done" contract as `Runtime::block_on`.
  A `!Send` future still needs a thread-safe `Waker` (the I/O
  reactor/timer driver wake it from their own background threads, same
  as any other task), so the local task type is `Arc`-counted with an
  `unsafe impl Send + Sync` justified by a genuinely enforced invariant,
  not just a comment: `LocalSet` binds itself to whichever thread first
  calls `spawn_local`/`run_until` on it and panics if used from a
  different one afterward, so the actual `!Send` future inside is never
  touched except on that one thread. Pair a `LocalSet` with a `Runtime`
  for `time::sleep`/I/O to work inside `spawn_local`'d work (a bare
  `LocalSet` has no reactor/timer driver of its own, only scheduling);
  there's no "run this `LocalSet` pinned to one worker of an existing
  multi-threaded `Runtime`" integration the way tokio's `LocalSet: Future`
  impl offers, and no graceful-shutdown draining -- a `LocalSet` going
  out of scope just drops whatever's left queued, same as this crate's
  `Runtime` already does for tasks abandoned mid-shutdown.
- **Runtime** (`Runtime`, `Handle`): a fixed pool of worker threads, each
  with its own run queue, backed by a shared injector queue for tasks
  spawned from outside the pool, with work-stealing between workers when
  one goes idle. `benches/scheduler.rs` (`cargo bench`, same hand-rolled
  approach as the timer benchmarks) measures issue #8's contention
  question rather than assuming an answer: on the Linux dev box this was
  built on, throughput for many independently-spawned tasks (which all
  serialize through the single injector-queue `Mutex`) measurably
  *regresses* going from 1 to 4 worker threads (roughly 1M &rarr; 300K
  tasks/sec), while a steal-heavy nested-spawn workload's scaling across
  1/2/4 workers was too noisy in this shared sandbox to draw a confident
  conclusion either way. That's real evidence the injector path can
  bottleneck under contention, but not clean enough evidence about the
  per-worker local queues specifically (issue #8's actual ask) to justify
  a hand-rolled lock-free rewrite without more rigorous measurement (a
  dedicated, non-shared multi-core machine, larger sample sizes) and,
  more importantly, without `loom`-based concurrency testing this project
  doesn't currently have set up -- a correctness bar this specific piece
  of code needs and the scheduler/reactor/timer logic elsewhere in this
  crate is held to via ordinary multi-threaded integration tests instead.
  If a lock-free swap does happen, `crossbeam-deque` (exactly what tokio
  itself uses, well-audited) is the recommended starting point over
  hand-rolling a Chase-Lev deque from scratch -- see #8 for the ongoing
  discussion.
- **Current-thread runtime** (`Builder::new_current_thread()`): the
  above is the default (also `Builder::new_multi_thread()`, spelled out
  for symmetry), but a runtime built this way has no worker-thread pool
  at all -- spawned tasks run interleaved with polls of `block_on`'s own
  future, entirely on whichever thread calls it, instead of on a
  separate pool in the background. Useful for embedding in an
  environment that doesn't want extra OS threads, or lower overhead for
  single-threaded workloads that don't need work-stealing at all.
  Spawned futures still need to be `Send` -- same as the multi-threaded
  flavor; this alone doesn't enable `!Send` futures, which needs a
  `LocalSet` (tracked separately). The I/O reactor and timer driver still
  run on their own dedicated background threads regardless of flavor --
  unlike real tokio's current-thread runtime, which drives its I/O
  reactor inline on the single scheduling thread; collapsing this
  crate's already-dedicated `Reactor`/`TimerDriver` threads into the
  scheduling thread too would be a materially bigger redesign than a
  worker-pool-free scheduler alone, and isn't attempted here.
- **Graceful shutdown**: plain `drop(runtime)` still tears down
  immediately (abandoning anything mid-poll, unchanged from before), but
  `Runtime::shutdown_background()`/`shutdown_timeout(duration)` and
  `Handle::shutdown_notified()`/`is_shutting_down()` give spawned tasks a
  real chance to notice shutdown and clean up first: `shutdown_notified()`
  resolves once shutdown begins (immediately, if it already has), and a
  task awaiting it directly -- e.g. a dedicated cleanup task that does
  nothing until then -- is guaranteed to actually get scheduled and run
  before teardown proceeds, not just notified a moment before being cut
  off. `shutdown_timeout` additionally waits (bounded) for every
  outstanding task and the blocking pool to finish naturally before
  falling back to the same abrupt teardown `drop`/`shutdown_background`
  give; `shutdown_background` never waits at all. Racing
  `shutdown_notified()` against a task's own ongoing work (rather than
  awaiting it as the task's entire body) can now be done with `select!`
  -- see below.
- **I/O** (`io`): a reactor thread plus non-blocking `TcpStream`,
  `TcpListener`, `UdpSocket`, `UnixStream`, and `UnixListener`. Two
  backends behind the same interface
  -- `epoll` on Linux, `kevent` on macOS -- both level-triggered by
  choice, since edge-triggered epoll/kqueue demands every reader drain a
  fd to `EWOULDBLOCK` or risk missing events forever, an easy invariant
  to get subtly wrong for one extra syscall's worth of savings. Socket
  setup goes through `rustils` on both -- see "Built on rustils" below.
  Also includes an `AsyncRead`/`AsyncWrite` trait pair (shaped like
  tokio's/`futures-io`'s -- `Pin<&mut Self>`, `poll_*` methods -- but
  this crate's own definitions, not a re-export) plus a generic `copy`,
  so code doesn't need to be written against the concrete `TcpStream`
  type. `AsyncReadExt` also has `read_to_end`/`read_to_string`
  (accumulate until EOF, the latter checking UTF-8 validity once at the
  end), and `AsyncWrite` has a `poll_write_vectored`/`is_write_vectored`
  pair (default: writes just the first non-empty buffer via `poll_write`
  and ignores the rest -- correct, just not the syscall-count win a real
  `writev`-backed override would be). `TcpStream::split`/`into_split` (and, identically,
  `UnixStream::split`/`into_split`) give borrowed (`ReadHalf`/
  `WriteHalf`, or `UnixReadHalf`/`UnixWriteHalf`) or owned, independently
  `'static` (`OwnedReadHalf`/`OwnedWriteHalf`, or `OwnedUnixReadHalf`/
  `OwnedUnixWriteHalf`) read/write halves for the two-tasks-one-stream
  pattern, without callers having to reach for `Arc` themselves the way
  the shared `&TcpStream`/`&UnixStream` impls otherwise require.

  **This crate's macOS integration has never run on real hardware.**
  It's developed and tested on Linux only; the kqueue reactor and the
  `TcpStream`/`TcpListener`/`UdpSocket` wiring on top of rustils'
  `platform-macos` are verified with `cargo check --target
  x86_64-apple-darwin` (real macOS `libc` bindings, real type-checking)
  but nothing beyond that. `platform-macos` itself is better off: it has
  real `macos-latest` CI upstream, which already caught a genuine
  `AF_UNIX` bug the cross-check alone couldn't -- so the socket layer is
  solid, but this crate's own reactor integration on top of it is still
  reviewed-but-unverified until someone actually runs *this* crate's
  `cargo test` on a Mac.
- **Timers** (`time`): `sleep`, `sleep_until`, `timeout`, `interval`, and
  `interval_at` (like `interval`, but the first tick fires at a given
  `Instant` instead of always `now + period`), backed by a single
  background thread holding a min-heap of deadlines. `Interval` also has
  `MissedTickBehavior` (`Burst` -- the default, and this crate's
  original unconditional behavior -- `Delay`, or `Skip`), for choosing
  what happens when a tick isn't collected before more than one period
  has already elapsed.
  `benches/timers.rs` (`cargo bench`, no `criterion` -- a plain
  `harness = false` binary that times things by hand) measures this
  rather than assuming it: on the Linux dev box this was built on, a
  5ms `sleep` typically overshoots by ~150-200µs (p99 well under 1ms,
  with an occasional outlier into the tens of ms attributable to host
  scheduling noise, not the driver), a 2,000-timer simultaneous burst
  doesn't measurably delay a canary sleep queued just behind it (no
  head-of-line blocking at that scale), `Interval`'s drift correction
  holds exactly (0ns cumulative drift after 500 ticks), and
  register+cancel churn sustains roughly 2M ops/sec. Numbers will vary
  by machine; re-run the benchmark rather than trusting these if it
  matters for your use case.
- **Sync primitives** (`sync`): `Notify` (an async condition variable),
  an async-aware `Mutex`, a `oneshot` channel, a bounded `mpsc` channel,
  and `mpsc::unbounded_channel` -- same one-lock-covers-queue-and-wakers
  design as the bounded channel (see `mpsc`'s module docs for the real
  lost-wakeup bug that shape avoids), just without a capacity check or
  anywhere for a sender to wait, so `UnboundedSender::send` is a plain
  synchronous method, not `async fn`. Also `RwLock`: many concurrent
  readers or one exclusive writer, write-preferring like tokio's own
  (once a writer is queued, later readers queue behind it too, rather
  than jumping ahead just because the write lock itself isn't held
  yet -- otherwise constant read traffic could starve a waiting writer
  indefinitely). Also `Semaphore`: caps concurrency at N permits, fair
  (FIFO) like tokio's own -- both borrowed (`SemaphorePermit`) and
  `Arc`-owned (`OwnedSemaphorePermit`, for holding a permit across a
  spawned task boundary) permit flavors, plus `acquire_many` for
  reserving more than one permit at once. Also `watch`: a single-
  latest-value broadcast (`watch::channel`/`Sender`/`Receiver`) --
  `changed().await` resolves once the value's been updated since this
  receiver last saw it, no queue and no lagging (a receiver that misses
  several updates in a row just sees the latest one, not every
  intermediate value). Useful for "the current configuration" or "has
  shutdown been requested"-shaped state -- the same shape
  `Handle::shutdown_notified`/`is_shutting_down` hand-rolled as a
  one-off special case before this existed.
- **`select!`**: race two to five futures, running whichever resolves
  first and dropping (cancelling) the rest -- `rusty_tokio::select! { pat
  = future => body, ... }`. Deliberately scoped rather than a full
  reimplementation of tokio's macro: exactly 2 through 5 branches
  (`macro_rules!` has no clean way to generate a fresh binding per
  repetition on stable Rust without either a `paste!`-style proc-macro
  dependency or a recursive tt-muncher, and explicit enumeration is more
  legible for a macro this central); every branch's pattern must be
  irrefutable (a plain binding or `_`, not `Some(x)`/`Ok(v)` -- tokio's
  own macro lets a non-matching value fall through to re-poll just that
  branch, which needs more machinery than a first pass takes on); and
  branches are always polled in the order written, not tokio's
  randomized order, so if two are simultaneously and permanently ready
  the earlier one always wins (no starvation protection for that case).
  No `else` branch, no `,if <condition>` guards, no biased mode. See the
  macro's own doc comment for the full scope statement.
- **`join!`/`try_join!`**: run two to five futures concurrently *within
  the calling task* (no extra `spawn`, no extra scheduler `Task`) and
  resolve once every one of them has, returning a tuple of their
  outputs -- `rusty_tokio::join!(fut1, fut2, fut3)`. Shares `select!`'s
  "poll every branch each wake" shape and its 2-5-branch scope limit, but
  waits for all branches instead of stopping at the first; a branch that
  finishes early isn't re-polled while its slower siblings catch up.
  `try_join!` is the `Result`-aware sibling: every branch must resolve to
  a `Result` with the *same* error type, and it short-circuits (drops the
  remaining branches without polling them again) on the first `Err`
  rather than waiting for the rest to finish pointlessly.
- **`spawn_blocking`**: offloads a genuinely blocking closure onto a
  separate thread pool that grows on demand (up to a configurable cap)
  and shrinks back down when idle, instead of stalling an async worker
  thread. Implemented as a `oneshot` channel plus an ordinary spawned
  task awaiting it -- not a parallel handle type -- so panics, abort,
  and `.await` on the returned `JoinHandle` all reuse the same task
  machinery every other spawned task does.
- **`#[rusty_tokio::main]`/`#[rusty_tokio::test]`**: attribute macros
  rewriting an `async fn` into the `Runtime::new().unwrap().block_on(
  async { .. })` boilerplate every example and test in this crate used
  to spell out by hand. `#[rusty_tokio::test]` also emits `#[test]`
  itself. Both accept an optional `worker_threads = N` argument
  (`#[rusty_tokio::main(worker_threads = 4)]`); no other arguments --
  tokio's own `flavor`/`start_paused`/etc. don't apply here, since this
  crate has exactly one runtime flavor and no pausable clock (issue
  #56). Defined in a separate `rusty_tokio-macros` proc-macro crate
  (this project's first `syn`/`quote`/`proc-macro2` dependency --
  `proc-macro = true` crates can't export anything but proc-macros, so
  it can't live inside `rusty_tokio` itself) and re-exported from the
  main crate, mirroring tokio's own `tokio`/`tokio-macros` split.

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
reimplemented a second time here. `io/unix.rs` does the same for
`platform_linux::{LinuxUnixListener, LinuxUnixStream}` -- including the
mode-`0600` bind and stale-socket-file reclaim rustils' own
`unix_listen` already does internally (a throwaway probe connect tells a
dead listener's leftover socket file apart from a live one's, so a
crashed-and-restarted listener can rebind at the same path without
manual cleanup).

Two things stayed hand-rolled in `io/socket/mod.rs`, both because of a
real mismatch rather than taste, and both apply equally to `UnixStream`
as to `TcpStream`:

- **Non-blocking `connect`.** `LinuxTcpStream::connect`/
  `LinuxUnixStream::connect` create a *blocking* socket and call a
  blocking `connect(2)` -- correct for rustils' own callers, but it would
  stall an entire worker thread for a connection's RTT if used here
  directly. An async connect needs the socket non-blocking *before*
  `connect(2)`, so it's created and connected by hand (`new_tcp_socket`/
  `connect` for TCP, `new_unix_socket`/`unix_connect` for `AF_UNIX` --
  the latter packing a `sockaddr_un` from a `Path` the same way the
  former packs a `sockaddr_in`/`sockaddr_in6` from a `SocketAddr`), then
  the resulting fd is adopted into the concrete stream type via
  `From<OwnedFd>` for everything after.
- **`read`/`write`.** `platform::net::TcpStream::read`/`write` and
  `UnixStream::read`/`write` take `&mut self` -- fine for rustils'
  blocking callers, wrong for this runtime's stream types, which
  deliberately expose `&self` methods so one task can read while another
  writes the same stream. Bypassing the trait for these two (a raw
  `read`/`write` on an fd already in hand via `AsRawFd`) keeps that API
  instead of hiding a mutex behind it.

`rustils` had no macOS backend at all for a while, so this crate first
had to hand-roll one (`io/socket/macos.rs`, since deleted). That gap was
filed as [rustils#48](https://github.com/baileyrd/rustils/issues/48) --
pointing at the hand-rolled shim as reference material -- and landed as
`platform-macos` in
[rustils#52](https://github.com/baileyrd/rustils/pull/52), shaped to
match `platform_linux` closely enough (same method names, same
`platform::error::Result`, the rustils#41/#42 `AsFd`/`set_nonblocking`
surface included from day one -- and, it turned out, `UnixStream`/
`UnixListener` included from day one too, not just TCP/UDP) that
`io/tcp.rs`/`io/udp.rs`/`io/unix.rs` need only a `#[cfg]`-gated type
alias between `platform_linux`/`platform_macos`, not their own branching.
The same two hand-rolled exceptions above apply on macOS too, for the
same reasons.
[rustils#53](https://github.com/baileyrd/rustils/pull/53) then added a
real `macos-latest` CI leg for `platform-macos`, which caught a genuine
`AF_UNIX` behavioral bug the cross-target `cargo check` this crate still
relies on could not have found -- see the macOS caveat above for what
that does and doesn't cover on *this* crate's side of the boundary.

## What's deliberately not here (yet)

This is a real, working runtime, not a toy -- but it's honest about its
edges instead of papering over them:

- **No Windows or generic BSD.** Linux (`epoll`) and macOS (`kevent`,
  see the caveat above) both have a reactor backend; an IOCP backend
  behind the same `ScheduledIo` interface would need a Windows socket
  layer too (no rustils backend there yet, unlike macOS now), doable
  but not done. Generic BSD (FreeBSD/OpenBSD/etc.) could likely reuse
  the `kevent` reactor as-is, but `platform-macos` itself only claims
  `target_os = "macos"`, not BSD generally, so there's no socket layer
  to pair it with yet either.
- **`AsyncRead`/`AsyncWrite` are this crate's own traits, not tokio's or
  `futures-io`'s.** Same shape, so generic code within this project works
  the same way, but a third-party codec/framing crate built against
  tokio's actual trait won't accept this crate's `TcpStream` without a
  shim. The optional `futures-io-compat` feature (off by default) adds
  one for `futures-io` specifically -- `rusty_tokio::io::Compat::new(x)`
  wraps anything implementing this crate's own `AsyncRead`/`AsyncWrite`
  so it also implements `futures_io`'s traits of the same name, for
  codec/framing crates that target `futures-io` directly or
  transitively. No equivalent exists for tokio's own traits: doing that
  properly would mean depending on tokio itself, which this crate
  otherwise avoids entirely (see the top of this README).
- **Work-stealing queues are `Mutex<VecDeque<_>>`, not lock-free.**
  Correct and simple; a real lock-free Chase-Lev deque (what tokio
  itself uses) would scale better under heavy contention.
- **io_uring is readiness-only.** The optional `io-uring-reactor`
  feature (off by default, Linux only) swaps `epoll_wait` for
  `IORING_OP_POLL_ADD` behind the same `Reactor`/`ScheduledIo` interface
  every other backend uses -- transparent to `TcpStream`/`UdpSocket`/
  `UnixStream`/`AsyncRead`/`AsyncWrite`, no code changes needed to opt
  in, and every existing integration test passes unchanged against it
  (stable across 5 repeated runs on this dev box). It does *not* route
  actual `read`/`write` syscalls through io_uring's own read/write
  opcodes -- those still go through `socket/mod.rs` exactly as before.
  That's a deliberate safety boundary, not an oversight: io_uring's
  read/write opcodes hand the kernel a pointer into a buffer for the
  whole duration of an async operation, but this crate's `AsyncRead`/
  `AsyncWrite` pass borrowed `&mut [u8]`, and a `Future` holding one can
  be dropped (cancelled) at any `Pending` point under ordinary Rust
  semantics -- do that with a real io_uring read still in flight and the
  kernel can write into memory that's since been freed, a genuine
  use-after-free. `tokio-uring`/`monoio` solve this with an owned,
  passed-by-value buffer API instead of a borrowed one; adopting that
  shape here is a bigger, different-shaped change than issue #9 asked
  for and isn't attempted. Built on the `io-uring` crate (what
  `tokio-uring`/`glommio`/`monoio` all use) for the same reason the
  lock-free deque discussion above recommends it over hand-rolling: ring
  setup/mmap/SQE/CQE layout is real unsafe code this project has no
  `loom`-style verification for.

## Testing

```
cargo test      # unit tests for the task state machine, plus integration
                # tests covering multi-threaded scheduling, abort, panics,
                # TCP/UDP over the real reactor, and every sync primitive
cargo clippy --all-targets
cargo bench     # timer skew/drift/churn and scheduler contention
                # measurements -- see "Timers"/"Runtime" above

# The futures-io compat shim (off by default -- see "What's deliberately
# not here" below) has its own feature-gated test target:
cargo test --features futures-io-compat

# The io_uring reactor backend (off by default, Linux only -- see
# "What's deliberately not here" below) is transparent to every existing
# test, so there's no separate test target for it -- just re-run
# everything against it:
cargo test --features io-uring-reactor

# This crate's macOS reactor integration: compiles and type-checks, but
# nothing beyond that -- see the macOS caveat above.
rustup target add x86_64-apple-darwin
cargo check --target x86_64-apple-darwin --all-targets
```

The integration tests deliberately exercise real concurrency (many
spawned tasks, real timer-driven yields, actual socket I/O) rather than
mocking the scheduler, since the bugs this kind of code has are races
that only show up under real multi-threaded execution.
