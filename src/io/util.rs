//! Trivial no-op [`AsyncRead`]/[`AsyncWrite`] endpoints: [`empty`]
//! (always reports EOF), [`repeat`] (an endless stream of one repeated
//! byte), and [`sink`] (discards everything written to it) -- useful as
//! placeholder or benchmark endpoints without needing a real source or
//! destination.

use super::{AsyncRead, AsyncWrite, ReadBuf};
use std::fmt;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

/// An [`AsyncRead`] that's always at EOF -- every [`poll_read`](AsyncRead::poll_read)
/// call succeeds immediately without filling any of the buffer.
pub fn empty() -> Empty {
    Empty { _priv: () }
}

/// Handle returned by [`empty`].
pub struct Empty {
    _priv: (),
}

impl AsyncRead for Empty {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl fmt::Debug for Empty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Empty").finish()
    }
}

/// An [`AsyncRead`] that endlessly yields `byte` -- every
/// [`poll_read`](AsyncRead::poll_read) call fills the caller's entire
/// buffer with it and never reports EOF.
pub fn repeat(byte: u8) -> Repeat {
    Repeat { byte }
}

/// Handle returned by [`repeat`].
pub struct Repeat {
    byte: u8,
}

impl AsyncRead for Repeat {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let n = buf.remaining();
        buf.unfilled_mut().fill(self.byte);
        buf.advance(n);
        Poll::Ready(Ok(()))
    }
}

impl fmt::Debug for Repeat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Repeat").finish()
    }
}

/// An [`AsyncWrite`] that discards everything written to it, always
/// succeeding immediately -- useful as a benchmark or placeholder
/// destination when the written bytes themselves don't matter.
pub fn sink() -> Sink {
    Sink { _priv: () }
}

/// Handle returned by [`sink`].
pub struct Sink {
    _priv: (),
}

impl AsyncWrite for Sink {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl fmt::Debug for Sink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sink").finish()
    }
}
