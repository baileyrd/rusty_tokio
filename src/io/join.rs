//! [`join`]: combines an independent reader and writer into a single
//! value implementing both [`AsyncRead`] and [`AsyncWrite`] -- the
//! reverse direction of [`super::split`], which pulls one value apart
//! into two. `reader`/`writer` don't need any relationship to each
//! other at all (unlike [`super::SplitReadHalf`]/[`super::SplitWriteHalf`],
//! which must come from the same original value to be reunited).

use super::{AsyncRead, AsyncWrite, ReadBuf};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Combines `reader` and `writer` into a single [`AsyncRead`] +
/// [`AsyncWrite`] value -- reads delegate to `reader`, writes to
/// `writer`, entirely independently of each other.
pub fn join<R, W>(reader: R, writer: W) -> Join<R, W> {
    Join { reader, writer }
}

/// Return value of [`join`].
pub struct Join<R, W> {
    reader: R,
    writer: W,
}

impl<R, W> Join<R, W> {
    pub fn get_ref(&self) -> (&R, &W) {
        (&self.reader, &self.writer)
    }

    pub fn get_mut(&mut self) -> (&mut R, &mut W) {
        (&mut self.reader, &mut self.writer)
    }

    pub fn into_inner(self) -> (R, W) {
        (self.reader, self.writer)
    }
}

impl<R: AsyncRead + Unpin, W: Unpin> AsyncRead for Join<R, W> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().reader).poll_read(cx, buf)
    }
}

impl<R: Unpin, W: AsyncWrite + Unpin> AsyncWrite for Join<R, W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().writer).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().writer).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().writer).poll_shutdown(cx)
    }
}
