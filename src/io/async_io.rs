//! `AsyncRead`/`AsyncWrite`: a poll-based I/O trait pair shaped like the
//! ones the wider async ecosystem (tokio, `futures-io`) standardizes on
//! -- `Pin<&mut Self>` receiver, a `poll_*` method per operation -- so
//! generic code (`fn copy<R: AsyncRead, W: AsyncWrite>(...)`) works the
//! same way here. These are this crate's own trait definitions, not a
//! re-export of tokio's or `futures-io`'s -- see the crate-level docs
//! for what that does and doesn't buy you.
//!
//! `TcpStream`'s existing `&self` `read`/`write` methods (used directly,
//! or via two `Arc<TcpStream>` clones) remain the way to read from one
//! task while writing from another. These traits are implemented for
//! *both* `TcpStream` and `&TcpStream`, the same split tokio's own
//! `TcpStream` uses: the `&TcpStream` impl holds the real logic (it only
//! ever needed shared access, since the reactor's readiness state and
//! the fd are both already behind `Arc`/kernel-owned handles), and
//! `TcpStream`'s own impl just delegates to it -- so borrowing two
//! `&TcpStream`s to read with one and write with the other while using
//! the trait works exactly like using the inherent methods does.
//!
//! **A method-resolution gotcha worth knowing:** `TcpStream` has its own
//! inherent `read`/`write`/`write_all`/`read_exact` methods (that's what
//! backs the paragraph above), and inherent methods always win over
//! trait methods of the same name for plain dot-call syntax. So
//! `stream.read(buf).await` on a concrete `TcpStream`/`&TcpStream` value
//! calls the *inherent* method, not `AsyncReadExt::read` -- harmless
//! (they do the same thing), but if you specifically mean to invoke the
//! trait, either call it generically (a function parameterized over
//! `T: AsyncRead`, which is how [`copy`] uses it, and has no inherent
//! method to prefer since the concrete type isn't known there) or via
//! UFCS (`AsyncReadExt::read(&mut stream, buf)`).

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A caller-provided buffer for [`AsyncRead::poll_read`] to fill.
///
/// Simpler than tokio's own `ReadBuf`: that one tracks an uninitialized
/// tail with `MaybeUninit<u8>` as a performance optimization for callers
/// that can hand over stack buffers without zeroing them first. Nothing
/// in this crate needs that -- callers already have a valid, fully
/// initialized `&mut [u8]` the ordinary way -- so this is just that
/// slice plus how much of it has been filled so far.
pub struct ReadBuf<'a> {
    buf: &'a mut [u8],
    filled: usize,
}

impl<'a> ReadBuf<'a> {
    pub fn new(buf: &'a mut [u8]) -> Self {
        ReadBuf { buf, filled: 0 }
    }

    /// The portion filled so far.
    pub fn filled(&self) -> &[u8] {
        &self.buf[..self.filled]
    }

    /// The unfilled portion a `poll_read` implementation should write
    /// into and then report via [`advance`](Self::advance).
    pub fn unfilled_mut(&mut self) -> &mut [u8] {
        &mut self.buf[self.filled..]
    }

    /// Record that `n` more bytes, already written into
    /// [`unfilled_mut`](Self::unfilled_mut), are now filled.
    pub fn advance(&mut self, n: usize) {
        assert!(
            self.filled + n <= self.buf.len(),
            "ReadBuf::advance past the end of the buffer"
        );
        self.filled += n;
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.filled
    }

    pub fn capacity(&self) -> usize {
        self.buf.len()
    }
}

pub trait AsyncRead {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>>;
}

pub trait AsyncWrite {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>>;

    /// For this runtime's sockets, writes are unbuffered (every
    /// `poll_write` is already its own `write(2)`), so the default
    /// implementation is a no-op `Ready(Ok(()))`; only a genuinely
    /// buffered writer would need to override this.
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let _ = self;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    /// Like `poll_write`, but from multiple buffers at once -- writes
    /// as many bytes as it can starting from `bufs[0]`, in principle in
    /// one syscall (`writev(2)`) rather than one call per buffer.
    ///
    /// The default implementation doesn't actually do that: it just
    /// writes the first non-empty buffer via `poll_write` and ignores
    /// the rest, the same fallback tokio's own `AsyncWrite` uses for any
    /// writer that hasn't overridden this -- correct (every byte handed
    /// to it does get written, eventually, over repeated calls), just
    /// not the syscall-count win a real vectored write would be. Only
    /// worth overriding for a writer where batching several buffers
    /// into one real `writev` call actually matters.
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let buf = bufs
            .iter()
            .find(|b| !b.is_empty())
            .map_or(&[][..], |b| &**b);
        self.poll_write(cx, buf)
    }

    /// Whether this writer's `poll_write_vectored` actually batches
    /// multiple buffers into fewer syscalls, rather than falling back to
    /// the default single-buffer behavior. `false` unless overridden.
    fn is_write_vectored(&self) -> bool {
        false
    }
}

/// Provided (non-overridable) conveniences over [`AsyncRead::poll_read`].
/// Blanket-implemented for every `AsyncRead`, the same way tokio's
/// `AsyncReadExt` is -- you never impl this yourself, just `poll_read`.
///
/// Written as `-> impl Future + Send` rather than plain `async fn`:
/// `async fn` in a public trait can't pin down whether the returned
/// future is `Send`, and this runtime's `spawn` requires `Send` futures
/// -- generic code calling `.read(buf).await` inside a spawned task
/// needs that guarantee, not just a lint's suggestion to consider it.
pub trait AsyncReadExt: AsyncRead {
    fn read(&mut self, buf: &mut [u8]) -> impl Future<Output = io::Result<usize>> + Send
    where
        Self: Unpin + Send,
    {
        async move {
            std::future::poll_fn(|cx| {
                let mut read_buf = ReadBuf::new(buf);
                match Pin::new(&mut *self).poll_read(cx, &mut read_buf) {
                    Poll::Ready(Ok(())) => Poll::Ready(Ok(read_buf.filled().len())),
                    Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                    Poll::Pending => Poll::Pending,
                }
            })
            .await
        }
    }

    /// Reads until `buf` is completely filled, or fails with
    /// `UnexpectedEof` if the peer closes first.
    fn read_exact(&mut self, mut buf: &mut [u8]) -> impl Future<Output = io::Result<()>> + Send
    where
        Self: Unpin + Send,
    {
        async move {
            while !buf.is_empty() {
                let n = self.read(buf).await?;
                if n == 0 {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "early eof"));
                }
                buf = &mut buf[n..];
            }
            Ok(())
        }
    }

    /// Reads until EOF, appending everything read to `buf`. Returns the
    /// number of bytes appended (not `buf`'s new total length).
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> impl Future<Output = io::Result<usize>> + Send
    where
        Self: Unpin + Send,
    {
        async move {
            let start_len = buf.len();
            let mut chunk = [0u8; 8192];
            loop {
                let n = self.read(&mut chunk).await?;
                if n == 0 {
                    return Ok(buf.len() - start_len);
                }
                buf.extend_from_slice(&chunk[..n]);
            }
        }
    }

    /// Reads until EOF, appending everything read to `buf` as UTF-8.
    /// Fails with `InvalidData` (without modifying `buf`) if the bytes
    /// read aren't valid UTF-8 -- checked only once EOF is reached,
    /// same as tokio's own `read_to_string`, not incrementally per read.
    fn read_to_string(&mut self, buf: &mut String) -> impl Future<Output = io::Result<usize>> + Send
    where
        Self: Unpin + Send,
    {
        async move {
            let mut bytes = Vec::new();
            let n = self.read_to_end(&mut bytes).await?;
            let text = String::from_utf8(bytes).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "stream did not contain valid UTF-8",
                )
            })?;
            buf.push_str(&text);
            Ok(n)
        }
    }
}

impl<T: AsyncRead + ?Sized> AsyncReadExt for T {}

/// Provided conveniences over [`AsyncWrite`]'s `poll_*` methods.
/// Blanket-implemented for every `AsyncWrite`. See [`AsyncReadExt`]'s
/// docs for why these are `-> impl Future + Send` instead of `async fn`.
pub trait AsyncWriteExt: AsyncWrite {
    fn write(&mut self, buf: &[u8]) -> impl Future<Output = io::Result<usize>> + Send
    where
        Self: Unpin + Send,
    {
        async move { std::future::poll_fn(|cx| Pin::new(&mut *self).poll_write(cx, buf)).await }
    }

    /// Like `write`, but from multiple buffers at once -- see
    /// `AsyncWrite::poll_write_vectored`.
    fn write_vectored(
        &mut self,
        bufs: &[io::IoSlice<'_>],
    ) -> impl Future<Output = io::Result<usize>> + Send
    where
        Self: Unpin + Send,
    {
        async move {
            std::future::poll_fn(|cx| Pin::new(&mut *self).poll_write_vectored(cx, bufs)).await
        }
    }

    fn write_all(&mut self, mut buf: &[u8]) -> impl Future<Output = io::Result<()>> + Send
    where
        Self: Unpin + Send,
    {
        async move {
            while !buf.is_empty() {
                let n = self.write(buf).await?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ));
                }
                buf = &buf[n..];
            }
            Ok(())
        }
    }

    fn flush(&mut self) -> impl Future<Output = io::Result<()>> + Send
    where
        Self: Unpin + Send,
    {
        async move { std::future::poll_fn(|cx| Pin::new(&mut *self).poll_flush(cx)).await }
    }

    fn shutdown(&mut self) -> impl Future<Output = io::Result<()>> + Send
    where
        Self: Unpin + Send,
    {
        async move { std::future::poll_fn(|cx| Pin::new(&mut *self).poll_shutdown(cx)).await }
    }
}

impl<T: AsyncWrite + ?Sized> AsyncWriteExt for T {}

/// A reader with an internal buffer, letting callers ask for "whatever's
/// available right now" (`poll_fill_buf`) and consume it incrementally
/// (`consume`) instead of only ever reading into a caller-provided
/// buffer -- what [`AsyncBufReadExt::read_line`]/[`read_until`]/[`lines`]
/// are built on. Implemented by [`super::buffered::BufReader`] for any
/// [`AsyncRead`]; this crate's sockets don't implement it directly, the
/// same way they're unbuffered `AsyncWrite`rs (see that trait's
/// `poll_flush` docs).
///
/// [`read_line`]: AsyncBufReadExt::read_line
/// [`read_until`]: AsyncBufReadExt::read_until
/// [`lines`]: AsyncBufReadExt::lines
pub trait AsyncBufRead: AsyncRead {
    /// Returns the reader's internal buffer, filling it first with at
    /// least one more byte if it's currently empty (unless already at
    /// EOF, in which case an empty slice is returned). The returned
    /// bytes aren't actually removed until [`consume`](Self::consume)
    /// says how many of them were used.
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>>;

    /// Marks `amt` bytes, previously returned by
    /// [`poll_fill_buf`](Self::poll_fill_buf), as consumed -- they won't
    /// be returned by a later `poll_fill_buf` call again.
    fn consume(self: Pin<&mut Self>, amt: usize);
}

/// Provided conveniences over [`AsyncBufRead`]'s `poll_*` methods.
/// Blanket-implemented for every `AsyncBufRead`. See [`AsyncReadExt`]'s
/// docs for why these are `-> impl Future + Send` instead of `async fn`.
pub trait AsyncBufReadExt: AsyncBufRead {
    /// Reads bytes into `buf` until `byte` is seen (inclusive) or EOF,
    /// returning the number of bytes appended. `buf` is *not* cleared
    /// first -- bytes are appended to whatever's already there, the
    /// same as `std::io::BufRead::read_until`.
    fn read_until<'a>(
        &'a mut self,
        byte: u8,
        buf: &'a mut Vec<u8>,
    ) -> impl Future<Output = io::Result<usize>> + Send
    where
        Self: Unpin + Send,
    {
        async move {
            let mut total = 0;
            loop {
                // Does the scan-and-extend work *inside* the `poll_fn`
                // closure itself, returning only owned `(bool, usize)`
                // data -- `poll_fill_buf`'s returned `&[u8]` borrows
                // from `self`, and a plain `FnMut` closure passed to
                // `poll_fn` can't return a reference borrowed from its
                // own captures (the closure's signature has no way to
                // tie that borrow's lifetime to one specific call).
                let (done, used) = std::future::poll_fn(|cx| {
                    let available = match Pin::new(&mut *self).poll_fill_buf(cx) {
                        Poll::Ready(Ok(available)) => available,
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Pending => return Poll::Pending,
                    };
                    let outcome = match available.iter().position(|&b| b == byte) {
                        Some(i) => {
                            buf.extend_from_slice(&available[..=i]);
                            (true, i + 1)
                        }
                        None => {
                            buf.extend_from_slice(available);
                            (false, available.len())
                        }
                    };
                    Poll::Ready(Ok(outcome))
                })
                .await?;
                Pin::new(&mut *self).consume(used);
                total += used;
                if done || used == 0 {
                    return Ok(total);
                }
            }
        }
    }

    /// Reads one line (including the trailing `\n`, if any) into `buf`,
    /// appended -- not cleared first, same as [`read_until`](Self::read_until).
    /// Fails with `InvalidData` (without modifying `buf`) if the bytes
    /// read aren't valid UTF-8.
    fn read_line(&mut self, buf: &mut String) -> impl Future<Output = io::Result<usize>> + Send
    where
        Self: Unpin + Send,
    {
        async move {
            let mut bytes = Vec::new();
            let n = self.read_until(b'\n', &mut bytes).await?;
            let text = String::from_utf8(bytes).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "stream did not contain valid UTF-8",
                )
            })?;
            buf.push_str(&text);
            Ok(n)
        }
    }

    /// An iterator-like helper yielding one line at a time (via
    /// `lines.next_line().await`), with the trailing `\n`/`\r\n`
    /// stripped -- not a `Stream` impl, since this crate has no
    /// `futures-core` dependency to implement that trait against.
    fn lines(self) -> super::buffered::Lines<Self>
    where
        Self: Sized,
    {
        super::buffered::Lines::new(self)
    }
}

impl<T: AsyncBufRead + ?Sized> AsyncBufReadExt for T {}

/// An in-memory sink -- never blocks, so every operation is trivially
/// `Ready`. Handy as a `copy`/`AsyncWriteExt` target in tests, or any
/// code that wants to build a byte stream without a real socket.
impl AsyncWrite for Vec<u8> {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Copies from `reader` to `writer` until EOF, returning the total byte
/// count. Generic over the traits above, the same shape as
/// `tokio::io::copy` -- the point of having the traits at all.
pub async fn copy<R, W>(reader: &mut R, writer: &mut W) -> io::Result<u64>
where
    R: AsyncRead + Unpin + Send + ?Sized,
    W: AsyncWrite + Unpin + Send + ?Sized,
{
    let mut buf = [0u8; 8192];
    let mut total = 0u64;
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            writer.flush().await?;
            return Ok(total);
        }
        writer.write_all(&buf[..n]).await?;
        total += n as u64;
    }
}
