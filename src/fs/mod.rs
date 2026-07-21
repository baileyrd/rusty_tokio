//! Async filesystem I/O: [`File`], the only type here so far.
//!
//! A regular file can't be registered with `epoll`/`kevent`'s readiness
//! model the way a socket can -- from the kernel's point of view a file
//! is always "ready"; the actual disk latency happens synchronously
//! inside the `read`/`write`/`lseek` syscall itself, not as something a
//! reactor can wait on separately. So unlike [`crate::io::TcpStream`]
//! (a thin non-blocking wrapper plus reactor readiness), [`File`] is
//! entirely a [`crate::spawn_blocking`] abstraction: every operation
//! moves the underlying `std::fs::File` onto a blocking-pool thread,
//! runs the real syscall there, and hands the file back once it's done.
//! `open`/`create` themselves go through the same path (opening a file
//! can block too -- a network filesystem mount, say).

use crate::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};
use crate::task::JoinHandle;
use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

/// The result of whichever operation was in flight, carried back
/// alongside the `std::fs::File` itself once the blocking closure
/// finishes -- see [`State`]'s docs for why every operation shares one
/// enum instead of three separate ones.
enum Op {
    Read(io::Result<Vec<u8>>),
    Write(io::Result<usize>),
    Seek(io::Result<u64>),
}

/// `std::fs::File`'s `read`/`write`/`seek` all take `&mut self` (there's
/// only one file cursor, so genuinely concurrent operations on the same
/// file make no sense the way full-duplex socket reads/writes do) --
/// [`File`] mirrors that by requiring exclusive access at every
/// `poll_*` call, rather than reusing `TcpStream`'s `&self`-based shared
/// design (see that type's own docs for why *its* split works and this
/// one doesn't apply here).
enum State {
    /// Holds the real file when nothing's in flight.
    Idle(std::fs::File),
    /// A blocking closure holding the file is running on the pool right
    /// now. If the poll that started it gets dropped before this
    /// resolves (a `select!`/timeout cancelling the read/write/seek
    /// future, say), this state persists on `File` itself regardless --
    /// only the *caller's* future was dropped, not the blocking
    /// operation already dispatched to the pool, which keeps running in
    /// the background the same way an abandoned `spawn_blocking` call
    /// always does. The next call to *any* `poll_read`/`poll_write`/
    /// `poll_seek` drains this leftover operation (discarding its result
    /// if it doesn't match what's being asked for now) before starting
    /// the new one -- see each method's shared `drain-then-start` loop.
    Busy(JoinHandle<(std::fs::File, Op)>),
    /// A previous operation's blocking closure panicked, taking the only
    /// copy of the underlying `std::fs::File` down with it -- there's no
    /// way to recover it, so every further operation fails with the same
    /// cached error instead of panicking the calling task in turn.
    Poisoned,
}

fn poisoned_error() -> io::Error {
    io::Error::other(
        "a previous operation on this File panicked inside the blocking pool, \
         taking the underlying std::fs::File down with it -- this File can no \
         longer be used",
    )
}

/// An async handle to an open file -- see this module's own docs for why
/// every operation is a [`crate::spawn_blocking`] round trip rather than
/// reactor-driven the way [`crate::io::TcpStream`] is.
pub struct File {
    state: State,
}

impl File {
    /// Opens an existing file for reading. See `std::fs::File::open`.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn open(path: impl AsRef<Path>) -> io::Result<File> {
        let path = path.as_ref().to_path_buf();
        Self::spawn_open(move || std::fs::File::open(path)).await
    }

    /// Opens a file for writing, creating it if it doesn't exist and
    /// truncating it if it does. See `std::fs::File::create`.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn create(path: impl AsRef<Path>) -> io::Result<File> {
        let path = path.as_ref().to_path_buf();
        Self::spawn_open(move || std::fs::File::create(path)).await
    }

    async fn spawn_open(
        open: impl FnOnce() -> io::Result<std::fs::File> + Send + 'static,
    ) -> io::Result<File> {
        crate::spawn_blocking(open)
            .await
            .unwrap_or_else(|_| Err(poisoned_error()))
            .map(|std_file| File {
                state: State::Idle(std_file),
            })
    }

    /// Takes the underlying file out of `state`, leaving `Poisoned`
    /// behind as a placeholder for the moment in between -- always
    /// immediately overwritten with a fresh `Busy(..)` by the caller.
    /// Only ever called from the `State::Idle` match arm, so the
    /// `unreachable!()` never actually fires.
    fn take_idle(state: &mut State) -> std::fs::File {
        match std::mem::replace(state, State::Poisoned) {
            State::Idle(file) => file,
            State::Busy(_) | State::Poisoned => unreachable!(),
        }
    }
}

impl AsyncRead for File {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            match &mut self.state {
                State::Idle(_) => {
                    let mut std_file = Self::take_idle(&mut self.state);
                    let want = buf.remaining();
                    self.state = State::Busy(crate::spawn_blocking(move || {
                        let mut chunk = vec![0u8; want];
                        let result = std::io::Read::read(&mut std_file, &mut chunk).map(|n| {
                            chunk.truncate(n);
                            chunk
                        });
                        (std_file, Op::Read(result))
                    }));
                }
                State::Busy(handle) => match Pin::new(handle).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(_join_err)) => {
                        self.state = State::Poisoned;
                        return Poll::Ready(Err(poisoned_error()));
                    }
                    Poll::Ready(Ok((std_file, op))) => {
                        self.state = State::Idle(std_file);
                        match op {
                            Op::Read(Ok(chunk)) => {
                                buf.unfilled_mut()[..chunk.len()].copy_from_slice(&chunk);
                                buf.advance(chunk.len());
                                return Poll::Ready(Ok(()));
                            }
                            Op::Read(Err(e)) => return Poll::Ready(Err(e)),
                            // A leftover write/seek from a previously
                            // cancelled future -- already drained by the
                            // `Idle` transition above; loop around to
                            // actually start the read now that the file
                            // is free again.
                            Op::Write(_) | Op::Seek(_) => continue,
                        }
                    }
                },
                State::Poisoned => return Poll::Ready(Err(poisoned_error())),
            }
        }
    }
}

impl AsyncWrite for File {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            match &mut self.state {
                State::Idle(_) => {
                    let mut std_file = Self::take_idle(&mut self.state);
                    // Copied into an owned buffer -- `spawn_blocking`'s
                    // closure needs `'static` data, and `buf` only lives
                    // as long as this one `poll_write` call.
                    let data = buf.to_vec();
                    self.state = State::Busy(crate::spawn_blocking(move || {
                        let result = std::io::Write::write(&mut std_file, &data);
                        (std_file, Op::Write(result))
                    }));
                }
                State::Busy(handle) => match Pin::new(handle).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(_join_err)) => {
                        self.state = State::Poisoned;
                        return Poll::Ready(Err(poisoned_error()));
                    }
                    Poll::Ready(Ok((std_file, op))) => {
                        self.state = State::Idle(std_file);
                        match op {
                            Op::Write(result) => return Poll::Ready(result),
                            Op::Read(_) | Op::Seek(_) => continue,
                        }
                    }
                },
                State::Poisoned => return Poll::Ready(Err(poisoned_error())),
            }
        }
    }

    /// A no-op, like `TcpStream`'s: every `poll_write` call above is
    /// already awaited to completion (`Ready`) before returning, so by
    /// the time a caller gets around to calling `flush`, there's never
    /// anything still in flight left to wait for.
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let _ = self;
        Poll::Ready(Ok(()))
    }

    /// There's no OS-level "half-close" for a plain file the way there
    /// is for a socket's write direction -- this just flushes (a no-op,
    /// per [`poll_flush`](Self::poll_flush)) and nothing else.
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

impl AsyncSeek for File {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: io::SeekFrom,
    ) -> Poll<io::Result<u64>> {
        loop {
            match &mut self.state {
                State::Idle(_) => {
                    let mut std_file = Self::take_idle(&mut self.state);
                    self.state = State::Busy(crate::spawn_blocking(move || {
                        let result = std::io::Seek::seek(&mut std_file, pos);
                        (std_file, Op::Seek(result))
                    }));
                }
                State::Busy(handle) => match Pin::new(handle).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(_join_err)) => {
                        self.state = State::Poisoned;
                        return Poll::Ready(Err(poisoned_error()));
                    }
                    Poll::Ready(Ok((std_file, op))) => {
                        self.state = State::Idle(std_file);
                        match op {
                            Op::Seek(result) => return Poll::Ready(result),
                            Op::Read(_) | Op::Write(_) => continue,
                        }
                    }
                },
                State::Poisoned => return Poll::Ready(Err(poisoned_error())),
            }
        }
    }
}
