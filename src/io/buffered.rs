//! [`BufReader`]/[`BufWriter`]: buffering wrappers around an arbitrary
//! [`AsyncRead`]/[`AsyncWrite`], for protocols that want to read a line
//! at a time or batch small writes into fewer syscalls. This crate's own
//! sockets are unbuffered by design (see `AsyncWrite::poll_flush`'s
//! docs) -- these types are how to add buffering on top when a
//! particular protocol actually wants it, rather than baking it into
//! every socket type unconditionally.
//!
//! Both require `R`/`W: Unpin` -- a deliberate simplification versus
//! tokio's own `BufReader`/`BufWriter`, which pin-project through to a
//! possibly-`!Unpin` inner reader/writer. Every concrete reader/writer
//! this crate actually has (`TcpStream`, `UnixStream`, ...) is already
//! `Unpin` (plain structs over `Arc`/fd handles, no self-referential or
//! generator state), so this doesn't cost any real usage -- it just
//! avoids hand-written unsafe pin projection for a case that would never
//! actually exercise it.

use super::async_io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, ReadBuf};
use std::io;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

const DEFAULT_BUF_SIZE: usize = 8192;

/// Wraps a reader in an internal buffer so small reads (a line, a
/// protocol header a few bytes at a time) don't each cost their own
/// syscall -- see [`AsyncBufRead`]/[`super::AsyncBufReadExt`] for what
/// that buffer is actually used for.
pub struct BufReader<R> {
    inner: R,
    buf: Box<[u8]>,
    pos: usize,
    cap: usize,
}

impl<R> BufReader<R> {
    pub fn new(inner: R) -> Self {
        Self::with_capacity(DEFAULT_BUF_SIZE, inner)
    }

    pub fn with_capacity(capacity: usize, inner: R) -> Self {
        BufReader {
            inner,
            buf: vec![0u8; capacity].into_boxed_slice(),
            pos: 0,
            cap: 0,
        }
    }

    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    pub fn into_inner(self) -> R {
        self.inner
    }

    fn discard_buffer(&mut self) {
        self.pos = 0;
        self.cap = 0;
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for BufReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // A large enough read (at least as big as our own buffer) with
        // nothing currently buffered bypasses the buffer entirely --
        // reading through it first would just be an extra copy for no
        // benefit, the same shortcut `std::io::BufReader` uses.
        if self.pos == self.cap && buf.remaining() >= self.buf.len() {
            let result = Pin::new(&mut self.inner).poll_read(cx, buf);
            self.discard_buffer();
            return result;
        }
        let available = ready!(self.as_mut().poll_fill_buf(cx))?;
        let n = std::cmp::min(available.len(), buf.remaining());
        buf.unfilled_mut()[..n].copy_from_slice(&available[..n]);
        buf.advance(n);
        self.consume(n);
        Poll::Ready(Ok(()))
    }
}

impl<R: AsyncRead + Unpin> AsyncBufRead for BufReader<R> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        if this.pos >= this.cap {
            debug_assert_eq!(this.pos, this.cap);
            let mut read_buf = ReadBuf::new(&mut this.buf);
            match Pin::new(&mut this.inner).poll_read(cx, &mut read_buf) {
                Poll::Ready(Ok(())) => {
                    this.cap = read_buf.filled().len();
                    this.pos = 0;
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(Ok(&this.buf[this.pos..this.cap]))
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.get_mut();
        this.pos = std::cmp::min(this.pos + amt, this.cap);
    }
}

/// Pass-through, unbuffered `AsyncWrite` for a `BufReader` wrapping
/// something that's both readable and writable (e.g. a `TcpStream`) --
/// `BufReader` only ever buffers reads.
impl<R: AsyncWrite + Unpin> AsyncWrite for BufReader<R> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// Wraps a writer in an internal buffer, batching small writes into
/// fewer, larger ones -- flushed automatically once the buffer would
/// overflow, or explicitly via `AsyncWriteExt::flush`/`shutdown`
/// (**important**: unlike a real file handle, dropping a `BufWriter`
/// does *not* flush it -- any not-yet-flushed bytes are silently lost,
/// the same caveat tokio's own `BufWriter` carries).
pub struct BufWriter<W> {
    inner: W,
    buf: Vec<u8>,
    written: usize,
}

impl<W> BufWriter<W> {
    pub fn new(inner: W) -> Self {
        Self::with_capacity(DEFAULT_BUF_SIZE, inner)
    }

    pub fn with_capacity(capacity: usize, inner: W) -> Self {
        BufWriter {
            inner,
            buf: Vec::with_capacity(capacity),
            written: 0,
        }
    }

    pub fn get_ref(&self) -> &W {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut W {
        &mut self.inner
    }

    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: AsyncWrite + Unpin> BufWriter<W> {
    /// Drains as much of `buf` into `inner` as `inner`'s own
    /// `poll_write` accepts per call, tracking partial progress in
    /// `written` across however many polls that takes -- a single
    /// `poll_write_vectored`-sized write on `inner` is not guaranteed to
    /// accept everything in one call.
    fn poll_flush_buf(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;
        let mut result = Ok(());
        while this.written < this.buf.len() {
            match Pin::new(&mut this.inner).poll_write(cx, &this.buf[this.written..]) {
                Poll::Ready(Ok(0)) => {
                    result = Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write the buffered data",
                    ));
                    break;
                }
                Poll::Ready(Ok(n)) => this.written += n,
                Poll::Ready(Err(e)) => {
                    result = Err(e);
                    break;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        if this.written >= this.buf.len() {
            this.buf.clear();
        } else if this.written > 0 {
            this.buf.drain(..this.written);
        }
        this.written = 0;
        Poll::Ready(result)
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for BufWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.buf.len() + buf.len() > self.buf.capacity() {
            ready!(self.as_mut().poll_flush_buf(cx))?;
        }
        if buf.len() >= self.buf.capacity() {
            // Bypass our buffer for a write at least as big as it --
            // buffering it first would just be an extra copy.
            Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
        } else {
            self.buf.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.as_mut().poll_flush_buf(cx))?;
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.as_mut().poll_flush_buf(cx))?;
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// Pass-through `AsyncRead`/`AsyncBufRead` for a `BufWriter` wrapping
/// something that's both readable and writable -- `BufWriter` only ever
/// buffers writes.
impl<W: AsyncRead + Unpin> AsyncRead for BufWriter<W> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<W: AsyncBufRead + Unpin> AsyncBufRead for BufWriter<W> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        Pin::new(&mut self.get_mut().inner).poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        Pin::new(&mut self.get_mut().inner).consume(amt);
    }
}

/// A combined buffered reader and writer around a single stream that's
/// both -- just [`BufWriter`] wrapped around a [`BufReader`], since both
/// already exist and buffer their own direction independently. Useful
/// for a protocol that reads and writes the same connection and wants
/// both directions buffered without wrapping (and re-wrapping) the
/// stream in two separate types by hand.
pub struct BufStream<T> {
    inner: BufWriter<BufReader<T>>,
}

impl<T> BufStream<T> {
    pub fn new(stream: T) -> Self {
        BufStream {
            inner: BufWriter::new(BufReader::new(stream)),
        }
    }

    /// Like [`new`](Self::new), but with independent initial capacities
    /// for the read side and the write side.
    pub fn with_capacity(reader_capacity: usize, writer_capacity: usize, stream: T) -> Self {
        BufStream {
            inner: BufWriter::with_capacity(
                writer_capacity,
                BufReader::with_capacity(reader_capacity, stream),
            ),
        }
    }

    pub fn get_ref(&self) -> &T {
        self.inner.get_ref().get_ref()
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut().get_mut()
    }

    pub fn into_inner(self) -> T {
        self.inner.into_inner().into_inner()
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for BufStream<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<T: AsyncRead + Unpin> AsyncBufRead for BufStream<T> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        Pin::new(&mut self.get_mut().inner).poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        Pin::new(&mut self.get_mut().inner).consume(amt);
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for BufStream<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// Yields one line at a time from an [`AsyncBufRead`] -- see
/// [`super::AsyncBufReadExt::lines`].
pub struct Lines<R> {
    reader: R,
}

impl<R> Lines<R> {
    pub(super) fn new(reader: R) -> Self {
        Lines { reader }
    }
}

impl<R: AsyncBufRead + Unpin + Send> Lines<R> {
    /// The next line, with its trailing `\n` (and `\r`, if present)
    /// stripped -- `Ok(None)` at EOF.
    pub async fn next_line(&mut self) -> io::Result<Option<String>> {
        let mut buf = String::new();
        let n = self.reader.read_line(&mut buf).await?;
        if n == 0 {
            return Ok(None);
        }
        if buf.ends_with('\n') {
            buf.pop();
            if buf.ends_with('\r') {
                buf.pop();
            }
        }
        Ok(Some(buf))
    }
}
