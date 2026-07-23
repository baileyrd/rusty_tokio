//! `rusty_tokio` -- a hand-rolled async runtime, built from scratch on
//! `std` (no `mio`, no `tokio`). The scheduler, reactor, timers, and
//! sync primitives are all original code, with one deliberate exception:
//! the scheduler's per-worker work-stealing queues depend on
//! `crossbeam-deque` (see [`Runtime`]'s own docs) rather than
//! hand-rolling a Chase-Lev deque -- real unsafe concurrent code this
//! project has no `loom`-based verification to trust a new
//! implementation of, unlike the scheduler/reactor/timer logic
//! elsewhere in this crate, which the multi-threaded integration tests
//! already hold to that bar. Not a dependency on the general-purpose
//! `crossbeam` suite (channels, epoch GC, etc.) as a shortcut around any
//! of that -- just this one narrowly-scoped sub-crate for the one piece
//! this project already decided isn't its point to hand-roll unverified.
//! "No `mio`" means no dependency on it, not no awareness of it: the
//! Windows reactor (`io::reactor::windows`) implements the same
//! undocumented AFD-poll protocol mio's own Windows backend uses --
//! there's no vendored crate for that trick, only mio's real source as a
//! reference point, cited directly in that module's own docs.
//! Socket setup in [`io`] builds on
//! [`rustils`](https://github.com/baileyrd/rustils)'
//! `platform`/`platform-linux`/`platform-macos` crates on Linux/macOS,
//! and directly on `windows-sys` on Windows (see `io::socket::windows`'s
//! docs for why there's no rustils backend to lean on there) rather than
//! reimplementing sockaddr packing and syscall error mapping a second
//! time -- see the crate README's "Built on rustils" section for
//! exactly which seam that is. It has seven pieces, one module each:
//!
//! - [`task`]: a heap-allocated future plus an atomic state machine
//!   that decides, on every wake, whether to (re-)enqueue it -- see
//!   that module's docs for why a naive "channel of `Arc<Task>`"
//!   design has a real lost-wakeup bug under multi-threaded execution.
//!   Also [`task::yield_now`], for a task that wants to cooperate with
//!   others without splitting itself across multiple spawns,
//!   [`task::JoinSet`], a dynamic collection of spawned tasks joined as
//!   they finish rather than in spawn order, [`task::LocalSet`] /
//!   [`task::spawn_local`] for `!Send` futures (holding an `Rc`, a
//!   `RefCell`-guarded value, etc.) that `crate::spawn` can never
//!   accept -- see [`task::LocalSet`]'s own docs for how a `!Send`
//!   future still gets a thread-safe `Waker` -- and
//!   [`task::Builder`]/[`task::TaskId`]/[`task::try_id`]/
//!   [`task::try_name`]: every spawned task gets a stable, process-wide
//!   unique ID (`JoinHandle::id()`, or `task::try_id()` from inside the
//!   task itself), and `task::Builder::new().name("...").spawn(future)`
//!   lets it carry a name retrievable the same way via
//!   `task::try_name()`. Also [`task_local!`]/[`task::LocalKey`]:
//!   implicit per-task context (a request ID, say) that inner async
//!   calls read via `KEY.with(...)` without it being threaded through
//!   every function signature -- scoped to a task's execution rather
//!   than an OS thread, so it isolates correctly even when many tasks'
//!   polls interleave on one worker thread, unlike a plain
//!   `std::thread_local!`. Also [`task::block_in_place`], for a blocking
//!   call that needs to interleave with non-`Send` local state that
//!   can't cross into a `spawn_blocking` closure -- it runs inline, on
//!   the calling worker thread, first handing that thread's other queued
//!   work off to a freshly spawned replacement so the rest of the pool
//!   doesn't stall waiting on it. Also, behind the off-by-default
//!   `tracing` Cargo feature: every spawned task gets a `tracing::Span`
//!   shaped exactly the way real (unstable) tokio's own instrumentation
//!   shapes it, so the real `console-subscriber`/`tokio-console` tool --
//!   built against that wire format, not this crate specifically --
//!   works against this runtime with zero changes on its end. See
//!   `task::trace`'s module docs for exactly which parts of tokio's full
//!   console support this covers (task registration, name, spawn
//!   location, poll count, busy/idle time) and which it deliberately
//!   doesn't (waker clone/drop/self-wake stats, resource/async-op
//!   instrumentation for `sync` primitives).
//! - [`Runtime`] / [`Handle`]: two flavors. The default
//!   (`Builder::new`/`new_multi_thread`) is a fixed pool of worker
//!   threads, each with its own run queue, backed by a shared injector
//!   queue and able to steal from one another. Both the per-worker
//!   queues and the injector are lock-free (`crossbeam_deque::
//!   Worker`/`Stealer`/`Injector` -- issue #8; see this module's own
//!   opening tagline for why that one dependency doesn't contradict
//!   this crate's hand-rolled-everything-else ethos), each worker
//!   thread owning its own `Worker` through a thread-local (`Worker`
//!   itself is `!Sync`, so it can't live centrally the way a `Mutex`-
//!   guarded queue could) rather than a shared, centrally-indexed
//!   structure. `benches/scheduler.rs` (`cargo bench`) measured this
//!   swap rather than assuming it helped -- see the crate README's
//!   "Runtime" bullet for the before/after numbers. `Builder::
//!   new_current_thread` has no worker-thread pool at all -- spawned
//!   tasks run interleaved with polls of `block_on`'s own future,
//!   entirely on whichever thread calls it (spawned futures still need
//!   to be `Send`, same as the multi-threaded flavor -- `!Send` support
//!   needs a `LocalSet`, tracked separately). `Runtime::
//!   shutdown_background`/`shutdown_timeout` and `Handle::
//!   shutdown_notified`/`is_shutting_down` give spawned tasks a real
//!   chance to observe shutdown and clean up (flush a buffer, close a
//!   file) before teardown, rather than just being abandoned mid-poll
//!   the way plain `drop(runtime)` still does. Also [`RuntimeMetrics`]
//!   (`Runtime::metrics`/`Handle::metrics`): a live, read-only view into
//!   queue depths, per-worker steal/park counts, and blocking-pool
//!   thread count, so answering "how busy is this worker" or "is the
//!   pool starved" no longer means inferring it indirectly through
//!   wall-clock timing of the public API the way `benches/scheduler.rs`/
//!   `benches/timers.rs` (issues #8/#13) had to. Every task also gets a
//!   cooperative scheduling budget, reset at the top of each poll turn
//!   and spent by this crate's own reactor and channel poll points --
//!   without it, a task whose one `poll` call loops internally forever
//!   (a tight `while let Some(x) = rx.recv().await { .. }` over a
//!   channel that's always ready, say) can starve every other task on
//!   its worker indefinitely, since nothing about any individual
//!   `.await` in that loop looks like a bug from inside that one task.
//!   See `coop`'s (crate-private) module docs for exactly which
//!   operations are charged and why the budget check has to run
//!   *before* an operation's own readiness check, not after.
//! - [`io`]: a reactor (`epoll` on Linux, `kevent` on macOS, IOCP + the
//!   AFD-poll trick on Windows) plus non-blocking `TcpStream` /
//!   `TcpListener` / `UdpSocket` (all three cross-platform) and
//!   `UnixStream` / `UnixListener` / [`io::UnixDatagram`] (`AF_UNIX`,
//!   Unix-only -- see the platform-support note below; the last of these
//!   built directly on `std::os::unix::net::UnixDatagram`
//!   rather than a rustils concrete type -- rustils has no `AF_UNIX`
//!   datagram support at all, see `io::unix_datagram`'s own module docs
//!   for why wrapping `std`'s own implementation beat hand-rolling a
//!   third copy of `AF_UNIX` sockaddr packing in this crate), an
//!   `AsyncRead`/`AsyncWrite` trait pair for generic code (`copy`, codecs,
//!   adapters), and
//!   `AsyncBufRead`/`io::BufReader`/`io::BufWriter` for buffering on top
//!   of any of the above -- this crate's own sockets are unbuffered by
//!   design (see `AsyncWrite::poll_flush`'s docs), so these are how a
//!   protocol that wants to read a line at a time or batch small writes
//!   adds that itself. Also [`io::copy_bidirectional`], for a
//!   proxy/relay use case that needs both directions of an `AsyncRead +
//!   AsyncWrite` pair copied concurrently from one future, each shutting
//!   its writer down independently as soon as its own reader hits EOF.
//!   `TcpListener`/`TcpStream`/`UdpSocket` each have `from_std`/
//!   `into_std` too, for adopting an already-created `std` socket (from
//!   a supervisor process, a `socket2`-configured option this crate has
//!   no wrapper for, ...) or handing one back out as a plain blocking
//!   socket. Also [`io::AsyncSeek`], seeking within a stream -- only
//!   meaningful for a file, not a socket, so nothing in this module
//!   implements it; [`fs::File`] (below) does. And
//!   [`io::stdin`]/[`io::stdout`]/[`io::stderr`]: like `fs::File`, these
//!   are a [`spawn_blocking`] abstraction rather than reactor-driven
//!   (stdio generally can't be registered with a reactor either), and
//!   every `Stdout`/`Stderr` write is serialized through a process-wide
//!   lock (one each, independent of each other) so concurrent writers
//!   from different tasks can't interleave mid-message -- see
//!   `io::stdio`'s own module docs for the two-part fix (an internal
//!   `write_all`, never a partial write, plus the lock held for each
//!   call's entire duration) and why the fix needs both halves together.
//!   And [`io::duplex`]: an in-memory, connected pair of streams -- no
//!   socket, fd, or reactor involved at all, just two mutex-guarded byte
//!   buffers with backpressure (a write blocks once the peer's read side
//!   is full) -- for testing anything generic over `AsyncRead`/
//!   `AsyncWrite` without standing up a real loopback `TcpListener`/
//!   `TcpStream` pair. And [`io::TcpSocket`]: `TcpListener::bind`/
//!   `TcpStream::connect` go straight from nothing to bound-and-
//!   listening/connected in one call, with no opportunity to set a
//!   socket option (`SO_REUSEADDR`, `SO_REUSEPORT`, send/receive buffer
//!   sizes) in between -- `TcpSocket::new_v4`/`new_v6` is a bare,
//!   unbound, unconnected staging point for exactly that, `bind`/
//!   `listen`/`connect` turning it into an ordinary `TcpListener`/
//!   `TcpStream` once configured. None of those four options are in
//!   rustils' own `TcpStream`/`TcpListener` traits, so each is a
//!   hand-rolled `setsockopt`/`getsockopt` call, the same treatment
//!   `socket/mod.rs`'s other slivers of raw `libc` already get. And
//!   [`io::lookup_host`]: resolves a hostname (`"example.com:443"`, or
//!   anything else implementing `std::net::ToSocketAddrs`) to its
//!   [`std::net::SocketAddr`]s without blocking a worker thread -- there's
//!   no portable non-blocking `getaddrinfo`, so this is another
//!   [`spawn_blocking`] round trip under the hood, the same shape
//!   `fs::File`/`io::stdio`/`process::Child::wait` already use.
//! - [`fs`]: [`fs::File`], the only type here so far. A regular file
//!   can't be registered with a reactor's readiness model the way a
//!   socket can -- the kernel considers it always "ready", and the real
//!   latency happens synchronously inside the `read`/`write`/`lseek`
//!   syscall itself -- so unlike `io::TcpStream`, `File` is entirely a
//!   [`spawn_blocking`] abstraction: every operation (including
//!   `open`/`create` themselves) moves the underlying `std::fs::File`
//!   onto a blocking-pool thread and hands it back once the syscall
//!   returns. See `fs`'s own module docs for how that reconciles
//!   `std::fs::File`'s `&mut self`-based API with this crate's
//!   `AsyncRead`/`AsyncWrite`/`AsyncSeek` traits, and what happens to an
//!   in-flight operation if its future is dropped before completing.
//! - [`process`]: [`process::Command`], mirroring `std::process::
//!   Command`'s builder API, but `spawn()`'s [`process::Child`] gives
//!   async access to piped `stdin`/`stdout`/`stderr`
//!   ([`process::ChildStdin`]/[`process::ChildStdout`]/
//!   [`process::ChildStderr`]) and its `wait()` doesn't block a worker
//!   thread. Built directly on `std::process`, not rustils -- rustils'
//!   own process abstraction hands piped stdio back as an object-safe,
//!   deliberately fd-hiding `File` trait (for Windows portability),
//!   which can't be reactor-registered; `process`'s own module docs
//!   have the full reasoning, the same call [`io::UnixDatagram`] already
//!   made for an analogous reason. A piped child's stdio is a genuine
//!   pipe (readiness-driven through the reactor, unlike `fs::File`'s
//!   spawn_blocking-per-operation shape); `wait()` itself still runs on
//!   [`spawn_blocking`], since neither `std::process::Child` nor
//!   rustils exposes anything pollable for exit (a `pidfd` on Linux, or
//!   `kevent`'s `EVFILT_PROC` on macOS, would each need their own
//!   from-scratch reactor integration).
//! - [`signal`]: [`signal::ctrl_c`] resolves once on the next `SIGINT`;
//!   [`signal::signal`] returns a [`signal::Signal`] that fires every
//!   time a given [`signal::SignalKind`] arrives, for as long as it's
//!   held. Built on the self-pipe trick -- the actual OS signal handler
//!   only ever does an async-signal-safe `write(2)` to a pre-created
//!   pipe, with everything else (tracking which listeners care, waking
//!   them) happening later in an ordinary spawned task reading that pipe
//!   through the same reactor every socket in this crate uses -- see
//!   that module's own docs for the full reasoning, including why its
//!   state is process-wide rather than per-`Runtime`.
//! - [`time`]: a timer-wheel-ish background thread for `sleep`,
//!   `timeout`, and `interval`. On a [`Builder::new_current_thread`]
//!   runtime, [`time::pause`]/[`time::resume`]/[`time::advance`] swap in
//!   a manually-driven virtual clock for deterministic timer tests that
//!   don't want to wait on real wall time.
//! - [`sync`]: `Notify`, an async `Mutex`/`RwLock`, `Semaphore`,
//!   `OnceCell`, `oneshot`, `watch`, bounded/unbounded `mpsc`, and
//!   `broadcast` (every receiver gets every message, reporting `Lagged`
//!   if one falls behind) -- the primitives above are usually enough to
//!   build everything else on top of. Also [`sync::Barrier`]: a
//!   rendezvous point for a fixed number of tasks, reusable across many
//!   rounds -- every `wait()` call blocks until `n` of them have all
//!   called it, then all resolve together, one arbitrarily marked the
//!   round's "leader". Hand-rolls its own waiter list behind one lock
//!   (rather than building on `Notify`, whose own waiters queue lives
//!   behind a *separate* lock from whatever a caller checks first) so
//!   there's no window for a round to complete between a waiter
//!   checking it hasn't yet and registering to be woken -- see that
//!   module's own docs for the two-lock race this sidesteps.
//! - [`select!`]: race two to five futures, running whichever resolves
//!   first and dropping the rest -- see that macro's own docs for
//!   exactly what's (and isn't) supported.
//! - [`join!`]/[`try_join!`]: run two to five futures concurrently
//!   within the calling task (no extra `spawn`) and resolve once every
//!   one of them has, returning a tuple of their outputs; `try_join!` is
//!   the `Result`-aware sibling, short-circuiting on the first `Err`.
//! - [`macro@main`]/[`macro@test`]: attribute macros rewriting an
//!   `async fn` into the `Runtime::new().unwrap().block_on(async { .. })`
//!   boilerplate every example and test used to spell out by hand.
//!   Defined in the separate `rusty_tokio-macros` proc-macro crate and
//!   re-exported here -- see that crate's own docs for why it has to be
//!   separate, and for the (small) scope this doesn't cover.
//!
//! # Deliberately out of scope (for now)
//!
//! This is a real, working runtime, not a toy -- but it's also honest
//! about its edges rather than papering over them:
//!
//! - **Linux, macOS, and Windows -- not generic BSD.** The reactor has
//!   three backends behind the same `ScheduledIo` interface --
//!   `epoll`+`eventfd` on Linux, `kevent`+`EVFILT_USER` on macOS,
//!   IOCP+the AFD-poll trick on Windows (`io::reactor::windows`'s own
//!   module docs have the full protocol and this crate's deliberate
//!   simplifications versus mio's reference implementation) -- with
//!   socket setup on macOS coming from rustils' `platform-macos` crate
//!   (added in response to rustils#48, filed from this crate's own
//!   experience hand-rolling that layer the first time), and on Windows
//!   entirely hand-rolled against `windows-sys` (`io::socket::windows`'s
//!   docs explain why: rustils' own Windows backend predates the
//!   non-blocking/`AsRawSocket`/`From<OwnedSocket>` surface this crate
//!   needs). **This crate's own integration on top of both the macOS and
//!   Windows backends -- the reactor, `TcpStream`/`TcpListener`/
//!   `UdpSocket` wrapping each platform's socket layer -- is compile-checked
//!   only** (`cargo check --target x86_64-apple-darwin` /
//!   `--target x86_64-pc-windows-gnu`), **never run on real hardware**,
//!   even though `platform-macos` itself now has real `macos-latest` CI
//!   upstream (which already caught a genuine `AF_UNIX` bug the
//!   cross-check alone couldn't). This crate has only ever been
//!   developed and tested on Linux -- treat the macOS and Windows
//!   reactor paths as reviewed-but-unverified until someone runs *this*
//!   crate's test suite on the real OS, not just rustils' or mio's own.
//!   `AF_UNIX` (`unix.rs`/`unix_datagram.rs`) and [`process`]/[`signal`]
//!   are Unix-only and simply absent from the crate on Windows (no
//!   `#[cfg]`-gated stub methods that would panic at runtime) -- there's
//!   no portable equivalent to fall back to, matching how tokio itself
//!   draws this exact same line.
//! - **`AsyncRead`/`AsyncWrite` are this crate's own trait definitions,
//!   not tokio's or `futures-io`'s.** Shaped the same way (`Pin<&mut
//!   Self>`, `poll_*` methods) so generic code here works the same way,
//!   but a third-party codec/framing crate built against tokio's actual
//!   trait won't accept this crate's `TcpStream` without a shim. The
//!   optional `futures-io-compat` feature adds one for `futures-io`
//!   specifically (`io::Compat`, only present when that feature is
//!   enabled, hence not linked directly from these crate-level docs,
//!   which build regardless) -- a small, stable crate several
//!   codec/framing crates target directly or transitively, chosen over
//!   pulling in all of tokio just for its I/O trait definitions. No
//!   equivalent shim for tokio's own traits exists (or is planned) --
//!   that really would mean depending on tokio.
//! - **io_uring is readiness-only, not a full completion-based
//!   redesign.** The optional `io-uring-reactor` feature (off by
//!   default; Linux only) swaps `epoll_wait` for `IORING_OP_POLL_ADD`
//!   behind the exact same `ScheduledIo` interface every other backend
//!   uses -- the actual `read`/`write` syscalls this crate's sockets
//!   make are unchanged. Routing those through io_uring's own
//!   read/write opcodes too (what would actually remove a syscall per
//!   I/O operation, the real point of a "materially different reactor
//!   design") needs an owned-buffer-passed-by-value API shape --
//!   `tokio-uring`/`monoio`'s approach -- because the kernel holds a
//!   pointer into the buffer for the operation's whole duration; this
//!   crate's `AsyncRead`/`AsyncWrite` pass borrowed `&mut [u8]`, and a
//!   `Future` holding one can be dropped mid-operation by ordinary Rust
//!   cancellation, which would be a real use-after-free with a
//!   buffer-touching opcode. See `io::reactor::io_uring`'s module docs
//!   for the full reasoning.
//! - **No `pin!` macro of this crate's own.** `std::pin::pin!` (stable
//!   since Rust 1.68, independently of tokio's own `pin!`, which mostly
//!   exists today for pre-1.68 compatibility and re-export convenience)
//!   already does the exact same stack-pinning job and is what this
//!   crate's own `block_on` uses internally
//!   (`std::pin::pin!(future)` in `runtime::block_on_inner`) -- writing a
//!   second macro that does the same thing would be redundant, not a
//!   real gap. [`pin`] below is just a re-export of `std`'s own, for
//!   surface parity with tokio's own top-level `tokio::pin!`.

pub mod fs;
pub mod io;
#[cfg(unix)]
pub mod process;
#[cfg(unix)]
pub mod signal;
pub mod sync;
pub mod task;
pub mod time;

mod coop;
mod macros;
mod runtime;

pub use runtime::{Builder, EnterGuard, Handle, Runtime, RuntimeMetrics};
pub use rusty_tokio_macros::{main, test};
pub use std::pin::pin;
pub use task::{JoinError, JoinHandle};

use std::future::Future;

/// Spawn a future onto the currently running runtime's worker pool.
///
/// # Panics
/// Panics if called from a thread with no ambient runtime -- i.e.
/// outside a `Runtime::block_on` call or a task already running on one.
#[track_caller]
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    Handle::current().spawn(future)
}

/// Run a genuinely blocking closure (a blocking syscall, heavy CPU work,
/// a synchronous library call with no async equivalent) on a dedicated
/// blocking-task thread pool instead of stalling one of the runtime's
/// async worker threads.
///
/// The returned [`JoinHandle`] behaves like any other: `.await` it for
/// the closure's return value, `Err(JoinError)` if it panicked. Calling
/// [`JoinHandle::abort`] on it detaches from the result but does **not**
/// stop the closure -- there is no way to preempt a thread stuck in a
/// blocking syscall, only to stop waiting for it.
///
/// # Panics
/// Panics if called from a thread with no ambient runtime.
#[track_caller]
pub fn spawn_blocking<F, T>(f: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    Handle::current().spawn_blocking(f)
}
