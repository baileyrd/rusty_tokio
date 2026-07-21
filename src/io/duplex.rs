//! [`duplex`]: an in-memory, connected pair of [`DuplexStream`]s -- no
//! socket, fd, or reactor registration involved at all. Useful for
//! testing anything generic over `AsyncRead`/`AsyncWrite` without
//! standing up a real loopback `TcpListener`/`TcpStream` pair the way
//! `tests/net.rs`/`tests/async_io.rs` otherwise have to.
//!
//! Closer in shape to [`crate::sync::mpsc`] than to
//! [`crate::io::TcpStream`]: each direction is a plain, mutex-guarded
//! byte buffer with a capacity, and a write blocks (returns `Pending`)
//! once the peer's read-side buffer is full, the same way a bounded
//! `mpsc::Sender::send` blocks once the channel is full -- a purpose-
//! built pair of waker slots per direction rather than reaching for
//! [`crate::sync::Notify`], since there's only ever at most one reader
//! and one writer waiting on a given direction at a time (a single
//! `DuplexStream` half is `&mut self`-exclusive at the `poll_*` level,
//! same as [`crate::fs::File`]), so a full waiter list isn't needed.

use crate::io::{AsyncRead, AsyncWrite, ReadBuf};
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// One direction's buffer -- `duplex`'s pair share two of these, "A
/// writes / B reads" and "B writes / A reads".
struct Pipe {
    buf: VecDeque<u8>,
    capacity: usize,
    /// Set once the writing side's `DuplexStream` has been dropped (or
    /// its writer explicitly shut down) -- a read against an empty
    /// buffer with this set is EOF, not `Pending`.
    write_half_closed: bool,
    /// Set once the reading side's `DuplexStream` has been dropped --
    /// further writes fail immediately instead of endlessly buffering
    /// into a pipe nobody will ever drain.
    read_half_dropped: bool,
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
}

impl Pipe {
    fn new(capacity: usize) -> Self {
        Pipe {
            buf: VecDeque::new(),
            capacity,
            write_half_closed: false,
            read_half_dropped: false,
            read_waker: None,
            write_waker: None,
        }
    }
}

struct Inner {
    /// Written by the `A` half's `poll_write`, read by `B`'s `poll_read`.
    a_to_b: Mutex<Pipe>,
    /// Written by the `B` half's `poll_write`, read by `A`'s `poll_read`.
    b_to_a: Mutex<Pipe>,
}

/// One end of an in-memory duplex pipe, returned in pairs by [`duplex`].
/// Implements `AsyncRead`/`AsyncWrite`, backed entirely by an in-process
/// buffer -- see this module's own docs for the shape.
pub struct DuplexStream {
    inner: Arc<Inner>,
    is_a: bool,
}

/// Returns a connected pair of in-memory streams: whatever's written to
/// one is readable from the other, in both directions independently.
/// Each direction's internal buffer holds at most `max_buf_size` bytes
/// before a write blocks (returns `Pending`) waiting for the peer to
/// read some of it back out -- the same backpressure shape as a bounded
/// [`crate::sync::mpsc::channel`].
///
/// # Panics
/// Panics if `max_buf_size` is zero -- a write could then never
/// complete (there's never any room), the same restriction
/// [`crate::sync::mpsc::channel`] places on its own capacity.
pub fn duplex(max_buf_size: usize) -> (DuplexStream, DuplexStream) {
    assert!(max_buf_size > 0, "duplex buffer capacity must be positive");
    let inner = Arc::new(Inner {
        a_to_b: Mutex::new(Pipe::new(max_buf_size)),
        b_to_a: Mutex::new(Pipe::new(max_buf_size)),
    });
    (
        DuplexStream {
            inner: inner.clone(),
            is_a: true,
        },
        DuplexStream { inner, is_a: false },
    )
}

impl DuplexStream {
    /// The pipe this half writes into.
    fn write_pipe(&self) -> &Mutex<Pipe> {
        if self.is_a {
            &self.inner.a_to_b
        } else {
            &self.inner.b_to_a
        }
    }

    /// The pipe this half reads from.
    fn read_pipe(&self) -> &Mutex<Pipe> {
        if self.is_a {
            &self.inner.b_to_a
        } else {
            &self.inner.a_to_b
        }
    }
}

impl AsyncRead for DuplexStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut pipe = self.read_pipe().lock().unwrap();
        if pipe.buf.is_empty() {
            if pipe.write_half_closed {
                // EOF: report a successful zero-byte read.
                return Poll::Ready(Ok(()));
            }
            pipe.read_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let n = buf.remaining().min(pipe.buf.len());
        for byte in buf.unfilled_mut()[..n].iter_mut() {
            *byte = pipe.buf.pop_front().expect("checked non-empty above");
        }
        buf.advance(n);
        if let Some(waker) = pipe.write_waker.take() {
            waker.wake();
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for DuplexStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut pipe = self.write_pipe().lock().unwrap();
        if pipe.read_half_dropped {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "the reading half of this duplex pair was dropped",
            )));
        }
        if pipe.buf.len() >= pipe.capacity {
            pipe.write_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let n = (pipe.capacity - pipe.buf.len()).min(buf.len());
        pipe.buf.extend(&buf[..n]);
        if let Some(waker) = pipe.read_waker.take() {
            waker.wake();
        }
        Poll::Ready(Ok(n))
    }

    /// A no-op: writes land directly in the shared buffer with nothing
    /// further to flush, the same as every other unbuffered writer in
    /// this crate (see `AsyncWrite::poll_flush`'s own docs).
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let _ = self;
        Poll::Ready(Ok(()))
    }

    /// Half-closes this stream's write direction without dropping it
    /// entirely -- the peer's reads see EOF from here on, but this side
    /// can still read whatever the peer sends back, the same "each
    /// direction closes independently" shape
    /// [`TcpStream::shutdown`](crate::io::AsyncWriteExt::shutdown) has.
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut pipe = self.write_pipe().lock().unwrap();
        pipe.write_half_closed = true;
        if let Some(waker) = pipe.read_waker.take() {
            waker.wake();
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for DuplexStream {
    fn drop(&mut self) {
        // The pipe this half writes into: mark write-closed (peer's
        // reads see EOF), same as an explicit `shutdown`.
        {
            let mut write_pipe = self.write_pipe().lock().unwrap();
            write_pipe.write_half_closed = true;
            if let Some(waker) = write_pipe.read_waker.take() {
                waker.wake();
            }
        }
        // The pipe this half reads from: mark read-dropped, so the
        // peer's writes fail fast instead of endlessly buffering into a
        // pipe nobody's left to drain.
        {
            let mut read_pipe = self.read_pipe().lock().unwrap();
            read_pipe.read_half_dropped = true;
            if let Some(waker) = read_pipe.write_waker.take() {
                waker.wake();
            }
        }
    }
}
