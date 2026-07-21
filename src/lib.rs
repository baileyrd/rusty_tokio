//! `rusty_tokio` -- a hand-rolled async runtime, built from scratch on
//! `std` (no `mio`, no `tokio`, no `crossbeam`). The scheduler, reactor,
//! timers, and sync primitives are all original code; socket setup in
//! [`io`] builds on [`rustils`](https://github.com/baileyrd/rustils)'
//! `platform`/`platform-linux`/`platform-macos` crates rather than
//! reimplementing sockaddr packing and syscall error mapping a second
//! time -- see the crate README's "Built on rustils" section for
//! exactly which seam that is. It has four pieces, one module each:
//!
//! - [`task`]: a heap-allocated future plus an atomic state machine
//!   that decides, on every wake, whether to (re-)enqueue it -- see
//!   that module's docs for why a naive "channel of `Arc<Task>`"
//!   design has a real lost-wakeup bug under multi-threaded execution.
//! - [`Runtime`] / [`Handle`]: a fixed pool of worker threads, each
//!   with its own run queue, backed by a shared injector queue and
//!   able to steal from one another. `Runtime::shutdown_background`/
//!   `shutdown_timeout` and `Handle::shutdown_notified`/
//!   `is_shutting_down` give spawned tasks a real chance to observe
//!   shutdown and clean up (flush a buffer, close a file) before
//!   teardown, rather than just being abandoned mid-poll the way plain
//!   `drop(runtime)` still does.
//! - [`io`]: a reactor (`epoll` on Linux, `kevent` on macOS) plus
//!   non-blocking `TcpStream` / `TcpListener` / `UdpSocket` /
//!   `UnixStream` / `UnixListener`, and an `AsyncRead`/`AsyncWrite` trait
//!   pair for generic code (`copy`, codecs, adapters).
//! - [`time`]: a timer-wheel-ish background thread for `sleep`,
//!   `timeout`, and `interval`.
//! - [`sync`]: `Notify`, an async `Mutex`, `oneshot`, and bounded `mpsc`
//!   -- the primitives above are usually enough to build everything
//!   else on top of.
//!
//! # Deliberately out of scope (for now)
//!
//! This is a real, working runtime, not a toy -- but it's also honest
//! about its edges rather than papering over them:
//!
//! - **Linux and macOS, not Windows or generic BSD.** The reactor has
//!   two backends behind the same `ScheduledIo` interface --
//!   `epoll`+`eventfd` on Linux, `kevent`+`EVFILT_USER` on macOS -- with
//!   socket setup on macOS now coming from rustils' `platform-macos`
//!   crate (added in response to rustils#48, filed from this crate's
//!   own experience hand-rolling that layer the first time; the old
//!   hand-rolled shim is gone). A Windows (IOCP) backend would need a
//!   third, doable but not done. **This crate's own integration on top
//!   of the macOS backend -- the kqueue reactor, `TcpStream`/
//!   `TcpListener`/`UdpSocket` wrapping `platform-macos`'s types -- is
//!   still compile-checked only (`cargo check --target
//!   x86_64-apple-darwin`), never run on real hardware**, even though
//!   `platform-macos` itself now has real `macos-latest` CI upstream
//!   (which already caught a genuine `AF_UNIX` bug the cross-check
//!   alone couldn't). This crate has only ever been developed and
//!   tested on Linux -- treat the macOS reactor path as
//!   reviewed-but-unverified until someone runs *this* crate's test
//!   suite on an actual Mac, not just rustils'.
//! - **`AsyncRead`/`AsyncWrite` are this crate's own trait definitions,
//!   not tokio's or `futures-io`'s.** Shaped the same way (`Pin<&mut
//!   Self>`, `poll_*` methods) so generic code here works the same way,
//!   but a third-party codec/framing crate built against tokio's actual
//!   trait won't accept this crate's `TcpStream` without a shim.
//! - **Work-stealing queues are `Mutex<VecDeque<_>>`, not lock-free.**
//!   Correct and simple; a real lock-free Chase-Lev deque (what tokio
//!   actually uses) would scale better under heavy contention.
//! - **No `io_uring`.** Would remove a syscall per I/O operation but is
//!   a materially different reactor design.

pub mod io;
pub mod sync;
pub mod task;
pub mod time;

mod runtime;

pub use runtime::{Builder, Handle, Runtime};
pub use task::{JoinError, JoinHandle};

use std::future::Future;

/// Spawn a future onto the currently running runtime's worker pool.
///
/// # Panics
/// Panics if called from a thread with no ambient runtime -- i.e.
/// outside a `Runtime::block_on` call or a task already running on one.
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
pub fn spawn_blocking<F, T>(f: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    Handle::current().spawn_blocking(f)
}
