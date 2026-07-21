//! An optional `futures_io::{AsyncRead, AsyncWrite}` shim (issue #7),
//! enabled by the `futures-io-compat` Cargo feature -- off by default,
//! so `futures-io` (a small, stable crate, but still a new dependency)
//! is never pulled in unless asked for.
//!
//! This crate's own [`super::AsyncRead`]/[`super::AsyncWrite`] are
//! shaped like the wider ecosystem's (same `Pin<&mut Self>` receiver,
//! same `poll_*` split) but are a distinct, unrelated trait as far as
//! the compiler is concerned -- a third-party codec/framing crate
//! written against tokio's or `futures-io`'s actual traits can't accept
//! this crate's `TcpStream` directly, even though the shape matches.
//! [`Compat`] closes that gap for `futures-io` specifically (not
//! tokio's own traits -- pulling in all of tokio just for its I/O trait
//! definitions was the option this crate's README explicitly rejected
//! in favor of this one): wrap anything implementing this crate's
//! `AsyncRead`/`AsyncWrite` in `Compat::new(..)`, and the result also
//! implements `futures_io`'s traits of the same name, by delegating.
//!
//! `futures_io::AsyncRead::poll_read` takes a plain `&mut [u8]` and
//! returns the byte count directly, unlike this crate's own
//! `poll_read(.., &mut ReadBuf<'_>) -> Poll<io::Result<()>>` -- the
//! `ReadBuf` wrapping is undone here via the same `ReadBuf::new` +
//! `filled().len()` pattern [`super::AsyncReadExt::read`] itself uses.
//! `futures_io::AsyncWrite::poll_close` is `futures-io`'s name for what
//! this crate (and tokio) call `poll_shutdown` -- same operation.

use super::async_io::{AsyncRead, AsyncWrite, ReadBuf};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Wraps a `T: AsyncRead`/`AsyncWrite` (this crate's own traits) so it
/// additionally implements `futures_io`'s traits of the same name -- see
/// this module's docs.
pub struct Compat<T>(T);

impl<T> Compat<T> {
    pub fn new(inner: T) -> Self {
        Compat(inner)
    }

    pub fn into_inner(self) -> T {
        self.0
    }

    pub fn get_ref(&self) -> &T {
        &self.0
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T: AsyncRead + Unpin> futures_io::AsyncRead for Compat<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut read_buf = ReadBuf::new(buf);
        match Pin::new(&mut self.0).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(read_buf.filled().len())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: AsyncWrite + Unpin> futures_io::AsyncWrite for Compat<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}
