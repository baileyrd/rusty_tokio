//! Async wrappers around the process's standard streams: [`stdin`],
//! [`stdout`], [`stderr`].
//!
//! Like [`crate::fs::File`], stdio handles generally can't be made
//! non-blocking or registered with `epoll`/`kevent` the way a socket
//! can (this varies by platform and by what stdio is actually connected
//! to -- a terminal, a pipe, a redirected file -- so this crate doesn't
//! try to special-case any of it), so every read/write here is a
//! [`crate::spawn_blocking`] round trip onto the blocking-pool thread
//! that actually calls `std::io::Stdin`/`Stdout`/`Stderr`, rather than
//! reactor-driven.
//!
//! **Ordering.** A socket write doesn't need to worry about a
//! *different* task's write landing in the middle of its own bytes --
//! each `TcpStream` owns its own connection. Every [`Stdout`] (or
//! [`Stderr`]) instance, though, is writing to the exact same process-
//! wide stream as every other one, so two tasks' writes interleaving
//! mid-buffer would be a real, visible bug (garbled output), not just a
//! theoretical one. Fixed two ways together: [`poll_write`
//! ](crate::io::AsyncWrite::poll_write) always calls `std::io::Write::
//! write_all` internally rather than a single (possibly partial) `write`
//! -- so one `poll_write` call is always all-or-nothing, never a partial
//! count a caller's own `write_all` loop might otherwise interleave with
//! someone else's between chunks -- and each call holds a process-wide
//! [`crate::sync::Mutex`] (one per stream: stdin/stdout/stderr each get
//! their own, so a task writing to `stdout` never waits on someone
//! reading `stdin`) for its *entire* duration, including the blocking
//! call itself, not just the syscall. Together, no two concurrent
//! logical writes to the same stream can ever interleave, regardless of
//! how many underlying syscalls `write_all` itself needs.
//!
//! Unlike `File`, there's no persistent OS resource to lose if a
//! blocking closure panics -- `std::io::stdin()`/`stdout()`/`stderr()`
//! are obtained fresh on every call, not moved in and out the way
//! `fs::File`'s `std::fs::File` is -- so a panicked operation here just
//! reports an ordinary `io::Error` and leaves the handle otherwise
//! reusable, no permanent "poisoned" state needed.

use crate::io::{AsyncRead, AsyncWrite, ReadBuf};
use crate::sync::Mutex;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::OnceLock;
use std::task::{Context, Poll};

fn blocking_pool_panicked() -> io::Error {
    io::Error::other("the blocking-pool task running this stdio operation panicked")
}

/// One in-flight operation at a time per handle -- `Idle` between calls,
/// `Busy` holding the boxed future (lock acquisition, the
/// `spawn_blocking` round trip, and the actual blocking call, all
/// bundled into one opaque future rather than modeled as separate
/// poll-driven states, since nothing else needs to observe the
/// in-between steps).
enum State<T> {
    Idle,
    Busy(Pin<Box<dyn Future<Output = io::Result<T>> + Send>>),
}

/// An async handle to the process's standard input. See this module's
/// own docs for why every read is a [`crate::spawn_blocking`] round
/// trip.
pub struct Stdin {
    state: State<Vec<u8>>,
}

/// Returns a new handle to the process's standard input. Cheap to call
/// more than once -- every `Stdin` (like every `Stdout`/`Stderr`) reads
/// from the same underlying process-wide stream, serialized through a
/// shared lock (see this module's own docs).
///
/// # Panics
/// Reading from the returned handle panics if there's no running
/// [`crate::Runtime`] -- construction itself doesn't need one.
pub fn stdin() -> Stdin {
    Stdin { state: State::Idle }
}

impl AsyncRead for Stdin {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            match &mut self.state {
                State::Idle => {
                    let want = buf.remaining();
                    self.state = State::Busy(Box::pin(async move {
                        let _guard = stdin_lock().lock().await;
                        crate::spawn_blocking(move || {
                            let mut chunk = vec![0u8; want];
                            let n = std::io::Read::read(&mut std::io::stdin(), &mut chunk)?;
                            chunk.truncate(n);
                            Ok(chunk)
                        })
                        .await
                        .unwrap_or_else(|_| Err(blocking_pool_panicked()))
                    }));
                }
                State::Busy(fut) => {
                    let result = std::task::ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle;
                    return Poll::Ready(result.map(|chunk| {
                        buf.unfilled_mut()[..chunk.len()].copy_from_slice(&chunk);
                        buf.advance(chunk.len());
                    }));
                }
            }
        }
    }
}

/// An async handle to the process's standard output. See this module's
/// own docs for how concurrent writes from multiple tasks avoid
/// interleaving.
pub struct Stdout {
    state: State<usize>,
}

/// Returns a new handle to the process's standard output. See
/// [`stdin`]'s docs for why creating more than one is cheap and safe.
///
/// # Panics
/// Writing to the returned handle panics if there's no running
/// [`crate::Runtime`] -- construction itself doesn't need one.
pub fn stdout() -> Stdout {
    Stdout { state: State::Idle }
}

impl AsyncWrite for Stdout {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        poll_write_locked(&mut self.state, cx, buf, stdout_lock, std::io::stdout)
    }

    /// A no-op: every `poll_write` call above already writes the whole
    /// buffer (or fails) before returning `Ready`, so there's never
    /// anything left in flight for a later `flush` to wait on.
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let _ = self;
        Poll::Ready(Ok(()))
    }

    /// No OS-level "half-close" concept applies to stdio -- just flushes
    /// (a no-op, per [`poll_flush`](Self::poll_flush)).
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

/// An async handle to the process's standard error. See this module's
/// own docs for how concurrent writes from multiple tasks avoid
/// interleaving.
pub struct Stderr {
    state: State<usize>,
}

/// Returns a new handle to the process's standard error. See
/// [`stdin`]'s docs for why creating more than one is cheap and safe.
///
/// # Panics
/// Writing to the returned handle panics if there's no running
/// [`crate::Runtime`] -- construction itself doesn't need one.
pub fn stderr() -> Stderr {
    Stderr { state: State::Idle }
}

impl AsyncWrite for Stderr {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        poll_write_locked(&mut self.state, cx, buf, stderr_lock, std::io::stderr)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let _ = self;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

/// Shared by [`Stdout::poll_write`] and [`Stderr::poll_write`] -- the
/// same drive-the-boxed-future loop, parameterized only by which
/// process-wide lock and which `std::io` stream accessor to use.
fn poll_write_locked<W>(
    state: &mut State<usize>,
    cx: &mut Context<'_>,
    buf: &[u8],
    lock: fn() -> &'static Mutex<()>,
    stream: fn() -> W,
) -> Poll<io::Result<usize>>
where
    W: io::Write + Send + 'static,
{
    loop {
        match state {
            State::Idle => {
                let data = buf.to_vec();
                let len = data.len();
                *state = State::Busy(Box::pin(async move {
                    let _guard = lock().lock().await;
                    crate::spawn_blocking(move || {
                        std::io::Write::write_all(&mut stream(), &data)?;
                        Ok(len)
                    })
                    .await
                    .unwrap_or_else(|_| Err(blocking_pool_panicked()))
                }));
            }
            State::Busy(fut) => {
                let result = std::task::ready!(fut.as_mut().poll(cx));
                *state = State::Idle;
                return Poll::Ready(result);
            }
        }
    }
}

fn stdin_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn stdout_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn stderr_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}
