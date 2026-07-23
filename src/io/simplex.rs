//! [`simplex`]: a one-directional in-memory pipe. Shares exactly one
//! buffer between both returned halves -- unlike [`super::duplex`]'s two
//! coupled pipes (one per direction), there's only ever one queue here,
//! and either handle can push into or pop from it. In the common idiom
//! one handle is used only to write and the other only to read (the
//! same "lite version of `duplex`, one direction only" shape tokio's own
//! `SimplexStream` documents), but nothing about the type itself
//! enforces that split.

use crate::io::{AsyncRead, AsyncWrite, ReadBuf};
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct Pipe {
    buf: VecDeque<u8>,
    capacity: usize,
    /// Set once *either* handle has dropped (or explicitly shut down) --
    /// with only one shared pipe and exactly two handles, once one is
    /// gone the other is the sole owner, so a read against an empty
    /// buffer past this point is EOF (nothing else is ever going to
    /// write more).
    write_half_closed: bool,
    /// Set once *either* handle has dropped -- symmetric reasoning to
    /// `write_half_closed`: with the other handle gone, a further write
    /// into this pipe would only ever accumulate for a reader that no
    /// longer exists, so it fails fast instead.
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

/// One end of an in-memory, one-directional pipe, returned in pairs by
/// [`simplex`]. See this module's own docs for the shared-single-buffer
/// shape.
pub struct SimplexStream {
    inner: Arc<Mutex<Pipe>>,
}

/// Returns a connected pair of one-directional in-memory streams: bytes
/// written to either handle are what the other (or, in principle, the
/// same handle) reads back out, up to `max_buf_size` bytes buffered
/// before a write blocks (returns `Pending`) waiting for a read to free
/// up room -- see this module's docs for the single-shared-buffer shape.
///
/// # Panics
/// Panics if `max_buf_size` is zero -- a write could then never
/// complete, the same restriction [`super::duplex`] places on its own
/// capacity.
pub fn simplex(max_buf_size: usize) -> (SimplexStream, SimplexStream) {
    assert!(max_buf_size > 0, "simplex buffer capacity must be positive");
    let inner = Arc::new(Mutex::new(Pipe::new(max_buf_size)));
    (
        SimplexStream {
            inner: inner.clone(),
        },
        SimplexStream { inner },
    )
}

impl AsyncRead for SimplexStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut pipe = self.inner.lock().unwrap();
        if pipe.buf.is_empty() {
            if pipe.write_half_closed {
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

impl AsyncWrite for SimplexStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut pipe = self.inner.lock().unwrap();
        if pipe.read_half_dropped {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "the other half of this simplex pair was dropped",
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

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let _ = self;
        Poll::Ready(Ok(()))
    }

    /// Marks the pipe write-closed, same as this handle dropping -- the
    /// other handle sees EOF (once it's drained whatever's left) on its
    /// next read, but can't be written to as a shortcut for
    /// half-closing just the write direction, since there's only one
    /// direction here in the first place.
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut pipe = self.inner.lock().unwrap();
        pipe.write_half_closed = true;
        if let Some(waker) = pipe.read_waker.take() {
            waker.wake();
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for SimplexStream {
    fn drop(&mut self) {
        let mut pipe = self.inner.lock().unwrap();
        pipe.write_half_closed = true;
        pipe.read_half_dropped = true;
        if let Some(waker) = pipe.read_waker.take() {
            waker.wake();
        }
        if let Some(waker) = pipe.write_waker.take() {
            waker.wake();
        }
    }
}
