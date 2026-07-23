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

/// Generates a `read_$be_name`/`read_$le_name` pair of big-/little-endian
/// integer or float methods for [`AsyncReadExt`], each reading
/// `size_of::<$ty>()` bytes via `read_exact` and decoding them with
/// `$ty`'s own `from_be_bytes`/`from_le_bytes` -- the exact same
/// hand-rolled approach tokio's own byte-order methods use (no
/// `byteorder` crate dependency needed on either side).
macro_rules! read_int_method {
    ($be_name:ident, $le_name:ident, $ty:ty) => {
        fn $be_name(&mut self) -> impl Future<Output = io::Result<$ty>> + Send
        where
            Self: Unpin + Send,
        {
            async move {
                let mut buf = [0u8; std::mem::size_of::<$ty>()];
                self.read_exact(&mut buf).await?;
                Ok(<$ty>::from_be_bytes(buf))
            }
        }

        fn $le_name(&mut self) -> impl Future<Output = io::Result<$ty>> + Send
        where
            Self: Unpin + Send,
        {
            async move {
                let mut buf = [0u8; std::mem::size_of::<$ty>()];
                self.read_exact(&mut buf).await?;
                Ok(<$ty>::from_le_bytes(buf))
            }
        }
    };
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

    fn read_u8(&mut self) -> impl Future<Output = io::Result<u8>> + Send
    where
        Self: Unpin + Send,
    {
        async move {
            let mut buf = [0u8; 1];
            self.read_exact(&mut buf).await?;
            Ok(buf[0])
        }
    }

    fn read_i8(&mut self) -> impl Future<Output = io::Result<i8>> + Send
    where
        Self: Unpin + Send,
    {
        async move {
            let mut buf = [0u8; 1];
            self.read_exact(&mut buf).await?;
            Ok(buf[0] as i8)
        }
    }

    read_int_method!(read_u16, read_u16_le, u16);
    read_int_method!(read_i16, read_i16_le, i16);
    read_int_method!(read_u32, read_u32_le, u32);
    read_int_method!(read_i32, read_i32_le, i32);
    read_int_method!(read_u64, read_u64_le, u64);
    read_int_method!(read_i64, read_i64_le, i64);
    read_int_method!(read_u128, read_u128_le, u128);
    read_int_method!(read_i128, read_i128_le, i128);
    read_int_method!(read_f32, read_f32_le, f32);
    read_int_method!(read_f64, read_f64_le, f64);

    /// Reads into whatever spare capacity `buf` currently has, advancing
    /// it by however many bytes actually landed. Returns `0` (without
    /// reading at all) once `buf` has no capacity left, the same
    /// EOF-shaped signal `read` itself gives on a zero-length buffer.
    ///
    /// Unlike tokio's own `read_buf`, which reads directly into `B`'s
    /// spare capacity via `BufMut::chunk_mut` (possibly-uninitialized
    /// memory), this crate's own [`ReadBuf`] deliberately doesn't track
    /// an uninitialized tail (see that type's own docs) -- so this reads
    /// into an ordinary zeroed stack buffer first and copies the result
    /// into `buf` via `put_slice`. Costs one extra copy versus tokio's
    /// zero-copy path; not worth the unsafe `MaybeUninit` plumbing this
    /// crate's `ReadBuf` deliberately avoids elsewhere for how rarely
    /// `bytes::Buf` integration is the hot path.
    fn read_buf<B: bytes::BufMut + Send>(
        &mut self,
        buf: &mut B,
    ) -> impl Future<Output = io::Result<usize>> + Send
    where
        Self: Unpin + Send,
    {
        async move {
            if !buf.has_remaining_mut() {
                return Ok(0);
            }
            let mut chunk = [0u8; 8192];
            let want = chunk.len().min(buf.remaining_mut());
            let n = self.read(&mut chunk[..want]).await?;
            buf.put_slice(&chunk[..n]);
            Ok(n)
        }
    }

    /// Reads from `self` until it hits EOF, then reads from `next` --
    /// see [`Chain`].
    fn chain<R: AsyncRead>(self, next: R) -> Chain<Self, R>
    where
        Self: Sized,
    {
        Chain {
            first: self,
            second: next,
            first_done: false,
        }
    }

    /// Limits `self` to at most `limit` bytes total, reporting EOF once
    /// that's been reached even if `self` has more -- see [`Take`].
    fn take(self, limit: u64) -> Take<Self>
    where
        Self: Sized,
    {
        Take { inner: self, limit }
    }
}

impl<T: AsyncRead + ?Sized> AsyncReadExt for T {}

/// Reads from `first` until it hits EOF, then reads from `second` -- see
/// [`AsyncReadExt::chain`]. Requires both `A`/`B: Unpin` -- the same
/// deliberate simplification `BufReader`/`BufWriter` already make (see
/// that module's own docs) rather than hand-written unsafe pin
/// projection for a case none of this crate's own reader types need.
pub struct Chain<A, B> {
    first: A,
    second: B,
    first_done: bool,
}

impl<A, B> Chain<A, B> {
    pub fn get_ref(&self) -> (&A, &B) {
        (&self.first, &self.second)
    }

    pub fn get_mut(&mut self) -> (&mut A, &mut B) {
        (&mut self.first, &mut self.second)
    }

    pub fn into_inner(self) -> (A, B) {
        (self.first, self.second)
    }
}

impl<A: AsyncRead + Unpin, B: AsyncRead + Unpin> AsyncRead for Chain<A, B> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.first_done {
            let before = buf.filled().len();
            match Pin::new(&mut self.first).poll_read(cx, buf) {
                Poll::Ready(Ok(())) => {
                    if buf.filled().len() == before {
                        // Nothing new filled -- `first` hit EOF.
                        self.first_done = true;
                    } else {
                        return Poll::Ready(Ok(()));
                    }
                }
                other => return other,
            }
        }
        Pin::new(&mut self.second).poll_read(cx, buf)
    }
}

/// Limits a reader to at most `limit` bytes total, reporting EOF once
/// that's been reached even if the inner reader has more -- see
/// [`AsyncReadExt::take`]. Requires `R: Unpin`, same reasoning as
/// [`Chain`] above.
pub struct Take<R> {
    inner: R,
    limit: u64,
}

impl<R> Take<R> {
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// Changes the remaining byte limit going forward -- doesn't affect
    /// how much has already been read.
    pub fn set_limit(&mut self, limit: u64) {
        self.limit = limit;
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
}

impl<R: AsyncRead + Unpin> AsyncRead for Take<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.limit == 0 {
            return Poll::Ready(Ok(()));
        }
        let max = self.limit.min(buf.remaining() as u64) as usize;
        let result;
        let n;
        {
            // Reborrows `buf`'s own unfilled memory directly (capped to
            // `max`) rather than reading into a scratch buffer and
            // copying -- the inner reader writes straight into the
            // caller's own buffer, same as an unrestricted read would.
            let mut limited = ReadBuf::new(&mut buf.unfilled_mut()[..max]);
            result = Pin::new(&mut self.inner).poll_read(cx, &mut limited);
            n = limited.filled().len();
        }
        buf.advance(n);
        self.limit -= n as u64;
        result
    }
}

/// Write-side counterpart of [`read_int_method`] -- see that macro's
/// docs.
macro_rules! write_int_method {
    ($be_name:ident, $le_name:ident, $ty:ty) => {
        fn $be_name(&mut self, n: $ty) -> impl Future<Output = io::Result<()>> + Send
        where
            Self: Unpin + Send,
        {
            async move { self.write_all(&n.to_be_bytes()).await }
        }

        fn $le_name(&mut self, n: $ty) -> impl Future<Output = io::Result<()>> + Send
        where
            Self: Unpin + Send,
        {
            async move { self.write_all(&n.to_le_bytes()).await }
        }
    };
}

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

    fn write_u8(&mut self, n: u8) -> impl Future<Output = io::Result<()>> + Send
    where
        Self: Unpin + Send,
    {
        async move { self.write_all(&[n]).await }
    }

    fn write_i8(&mut self, n: i8) -> impl Future<Output = io::Result<()>> + Send
    where
        Self: Unpin + Send,
    {
        async move { self.write_all(&[n as u8]).await }
    }

    write_int_method!(write_u16, write_u16_le, u16);
    write_int_method!(write_i16, write_i16_le, i16);
    write_int_method!(write_u32, write_u32_le, u32);
    write_int_method!(write_i32, write_i32_le, i32);
    write_int_method!(write_u64, write_u64_le, u64);
    write_int_method!(write_i64, write_i64_le, i64);
    write_int_method!(write_u128, write_u128_le, u128);
    write_int_method!(write_i128, write_i128_le, i128);
    write_int_method!(write_f32, write_f32_le, f32);
    write_int_method!(write_f64, write_f64_le, f64);

    /// Writes as much of `buf`'s remaining bytes as one `write` call
    /// takes, advancing `buf` by however much was actually written.
    /// Unlike [`read_buf`](AsyncReadExt::read_buf), this needs no extra
    /// copy: `Buf::chunk` already hands back ordinary initialized
    /// `&[u8]`, so it goes straight to `write`.
    fn write_buf<B: bytes::Buf + Send>(
        &mut self,
        buf: &mut B,
    ) -> impl Future<Output = io::Result<usize>> + Send
    where
        Self: Unpin + Send,
    {
        async move {
            if !buf.has_remaining() {
                return Ok(0);
            }
            let n = self.write(buf.chunk()).await?;
            buf.advance(n);
            Ok(n)
        }
    }

    /// Like [`write_buf`](Self::write_buf), but loops until `buf` is
    /// completely drained -- the `Buf`-based counterpart of `write_all`.
    fn write_all_buf<B: bytes::Buf + Send>(
        &mut self,
        buf: &mut B,
    ) -> impl Future<Output = io::Result<()>> + Send
    where
        Self: Unpin + Send,
    {
        async move {
            while buf.has_remaining() {
                let n = self.write_buf(buf).await?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ));
                }
            }
            Ok(())
        }
    }
}

impl<T: AsyncWrite + ?Sized> AsyncWriteExt for T {}

/// Seeking within a stream -- meaningful for a file (see
/// [`crate::fs::File`]), not for a socket (`TcpStream`/`UdpSocket`/
/// `UnixStream` don't implement this).
///
/// A single `poll_seek(pos)` rather than tokio's own two-phase
/// `start_seek(pos)`/`poll_complete()` split: that split exists so a
/// caller can kick a seek off and poll *something else* while it's
/// pending, useful for tokio's own file implementation where a seek and
/// a read might be interleaved through separate internal buffering
/// state. This crate's [`crate::fs::File`] already funnels every
/// operation -- read, write, or seek -- through the same single
/// in-flight-blocking-call state machine, so there's nothing else
/// meaningful to poll in between; a single combined method is simpler
/// and just as capable for that shape.
pub trait AsyncSeek {
    fn poll_seek(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: io::SeekFrom,
    ) -> Poll<io::Result<u64>>;
}

/// Provided convenience over [`AsyncSeek::poll_seek`]. Blanket-implemented
/// for every `AsyncSeek`. See [`AsyncReadExt`]'s docs for why this is
/// `-> impl Future + Send` instead of `async fn`.
pub trait AsyncSeekExt: AsyncSeek {
    fn seek(&mut self, pos: io::SeekFrom) -> impl Future<Output = io::Result<u64>> + Send
    where
        Self: Unpin + Send,
    {
        async move { std::future::poll_fn(|cx| Pin::new(&mut *self).poll_seek(cx, pos)).await }
    }

    /// The current position, without moving it -- sugar for
    /// `seek(SeekFrom::Current(0))`.
    fn stream_position(&mut self) -> impl Future<Output = io::Result<u64>> + Send
    where
        Self: Unpin + Send,
    {
        self.seek(io::SeekFrom::Current(0))
    }

    /// Seeks back to the start of the stream -- sugar for
    /// `seek(SeekFrom::Start(0))`, discarding the (always-`0`) returned
    /// position.
    fn rewind(&mut self) -> impl Future<Output = io::Result<()>> + Send
    where
        Self: Unpin + Send,
    {
        async move {
            self.seek(io::SeekFrom::Start(0)).await?;
            Ok(())
        }
    }
}

impl<T: AsyncSeek + ?Sized> AsyncSeekExt for T {}

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

    /// Returns the reader's internal buffer, filling it first if it's
    /// currently empty -- a standalone future wrapping
    /// [`poll_fill_buf`](AsyncBufRead::poll_fill_buf) directly, for
    /// callers that want the peeked bytes without also committing to
    /// consuming up to some delimiter the way [`read_until`](Self::read_until)/
    /// [`read_line`](Self::read_line) do.
    fn fill_buf(&mut self) -> FillBuf<'_, Self>
    where
        Self: Unpin,
    {
        FillBuf { reader: Some(self) }
    }
}

impl<T: AsyncBufRead + ?Sized> AsyncBufReadExt for T {}

/// Returned by [`AsyncBufReadExt::fill_buf`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct FillBuf<'a, R: ?Sized> {
    reader: Option<&'a mut R>,
}

impl<'a, R: AsyncBufRead + ?Sized + Unpin> Future for FillBuf<'a, R> {
    type Output = io::Result<&'a [u8]>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&'a [u8]>> {
        let this = self.get_mut();
        let reader = this
            .reader
            .take()
            .expect("polled FillBuf after it already completed");
        match Pin::new(&mut *reader).poll_fill_buf(cx) {
            Poll::Ready(Ok(slice)) => {
                // Safety: `poll_fill_buf`'s signature only ties the
                // returned slice's lifetime to the `&mut R` reborrow
                // passed to this one call, but the bytes it actually
                // points at live as long as the `&'a mut R` this future
                // was constructed from -- extending the lifetime here
                // just makes that already-true fact visible to the type
                // system, the same justification real tokio's own
                // `FillBuf` future gives for this identical transmute.
                let slice: &'a [u8] = unsafe { std::mem::transmute(slice) };
                Poll::Ready(Ok(slice))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => {
                this.reader = Some(reader);
                Poll::Pending
            }
        }
    }
}

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

/// Like [`copy`], but for a `reader` that's already an [`AsyncBufRead`]
/// -- writes straight out of the reader's own internal buffer instead
/// of copying through a second, private one first. A hand-rolled
/// `poll_fn`-driven state machine (rather than plain sequential
/// `.await`s the way [`copy`] itself is written) specifically because a
/// slice borrowed from [`AsyncBufRead::poll_fill_buf`] can't be held
/// across a separate `.await` point safely without extra unsafe code --
/// staying inside one synchronous `poll` call per step sidesteps that
/// entirely, the same reason [`copy_bidirectional`]'s own `CopyBuffer`
/// is written this way instead of as a plain `async fn` loop.
pub async fn copy_buf<R, W>(reader: &mut R, writer: &mut W) -> io::Result<u64>
where
    R: AsyncBufRead + Unpin + ?Sized,
    W: AsyncWrite + Unpin + ?Sized,
{
    let mut pos = 0usize;
    let mut read_done = false;
    let mut amt = 0u64;
    std::future::poll_fn(|cx| loop {
        if !read_done {
            let available = match Pin::new(&mut *reader).poll_fill_buf(cx) {
                Poll::Ready(Ok(buf)) => buf,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };
            if available.is_empty() {
                read_done = true;
            } else {
                match Pin::new(&mut *writer).poll_write(cx, &available[pos..]) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "write zero byte into writer",
                        )));
                    }
                    Poll::Ready(Ok(n)) => {
                        pos += n;
                        amt += n as u64;
                        if pos == available.len() {
                            Pin::new(&mut *reader).consume(pos);
                            pos = 0;
                        }
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
                continue;
            }
        }
        if read_done {
            return match Pin::new(&mut *writer).poll_flush(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(Ok(amt)),
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            };
        }
    })
    .await
}

/// One direction of a [`copy_bidirectional`] relay: reads from `reader`
/// into an internal buffer and writes it out to `writer`, tracking EOF
/// and the running byte count across however many separate polls that
/// takes. Kept as its own state machine (rather than reusing [`copy`]'s
/// simple loop directly) so `copy_bidirectional` can drive *two* of
/// these concurrently from one `poll_fn`, instead of needing two
/// separately spawned tasks.
struct CopyBuffer {
    read_done: bool,
    pos: usize,
    cap: usize,
    amt: u64,
    buf: Box<[u8]>,
}

impl CopyBuffer {
    fn with_capacity(capacity: usize) -> Self {
        CopyBuffer {
            read_done: false,
            pos: 0,
            cap: 0,
            amt: 0,
            buf: vec![0u8; capacity].into_boxed_slice(),
        }
    }

    /// Drives this direction as far as it can go without blocking:
    /// refills the buffer from `reader` when empty, drains it into
    /// `writer`, and -- once `reader` has hit EOF and everything's been
    /// written -- shuts `writer` down and resolves with the total byte
    /// count. `Pending` at any step propagates immediately, so a single
    /// call only ever makes forward progress up to the next thing that
    /// isn't ready yet.
    fn poll_copy<R, W>(
        &mut self,
        cx: &mut Context<'_>,
        mut reader: Pin<&mut R>,
        mut writer: Pin<&mut W>,
    ) -> Poll<io::Result<u64>>
    where
        R: AsyncRead + ?Sized,
        W: AsyncWrite + ?Sized,
    {
        loop {
            if self.pos == self.cap && !self.read_done {
                let mut read_buf = ReadBuf::new(&mut self.buf);
                match reader.as_mut().poll_read(cx, &mut read_buf) {
                    Poll::Ready(Ok(())) => {
                        let n = read_buf.filled().len();
                        if n == 0 {
                            self.read_done = true;
                        } else {
                            self.pos = 0;
                            self.cap = n;
                        }
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }

            while self.pos < self.cap {
                match writer
                    .as_mut()
                    .poll_write(cx, &self.buf[self.pos..self.cap])
                {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "write zero byte into writer",
                        )));
                    }
                    Poll::Ready(Ok(n)) => {
                        self.pos += n;
                        self.amt += n as u64;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }

            if self.pos == self.cap && self.read_done {
                match writer.as_mut().poll_shutdown(cx) {
                    Poll::Ready(Ok(())) => return Poll::Ready(Ok(self.amt)),
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
    }
}

/// Wraps a [`CopyBuffer`] with an explicit terminal state, so that once
/// a direction has resolved `Ready(Ok(amt))` it's never polled again --
/// required here specifically because [`copy_bidirectional`]'s combined
/// `poll_fn` polls *both* directions on every wake regardless of which
/// one woke it, and re-polling a `CopyBuffer` that already finished
/// would re-enter its "reader hit EOF, everything's written" branch and
/// call `poll_shutdown` on the writer a second time -- which, once the
/// peer has since fully closed *its* side too, can itself fail (e.g.
/// `ENOTCONN`), incorrectly turning an already-successful direction into
/// an error. A `Future` isn't supposed to be polled again after
/// returning `Ready` for exactly this kind of reason; this just makes
/// that guarantee explicit instead of relying on it by convention.
enum TransferState {
    Running(CopyBuffer),
    Done(u64),
}

impl TransferState {
    fn poll<R, W>(
        &mut self,
        cx: &mut Context<'_>,
        reader: Pin<&mut R>,
        writer: Pin<&mut W>,
    ) -> Poll<io::Result<u64>>
    where
        R: AsyncRead + ?Sized,
        W: AsyncWrite + ?Sized,
    {
        match self {
            TransferState::Running(buf) => match buf.poll_copy(cx, reader, writer) {
                Poll::Ready(Ok(amt)) => {
                    *self = TransferState::Done(amt);
                    Poll::Ready(Ok(amt))
                }
                other => other,
            },
            TransferState::Done(amt) => Poll::Ready(Ok(*amt)),
        }
    }
}

/// Drives both directions of an `a`<->`b` relay concurrently from one
/// future -- what a proxy/relay use case (forwarding one connection to
/// another) needs, instead of hand-writing two separately `spawn`ed
/// [`copy`] calls plus your own half-close coordination. Each direction
/// shuts down its writer independently, as soon as *that* direction's
/// own reader hits EOF, rather than tearing down the whole relay the
/// instant either side finishes -- e.g. a client that's done sending
/// but still expects a response keeps that response direction alive
/// until the server closes its own end too.
///
/// Resolves once both directions have finished (returning `(a_to_b,
/// b_to_a)` byte counts), or as soon as either direction hits an error
/// (which propagates immediately, without waiting for the other
/// direction to finish first).
pub async fn copy_bidirectional<A, B>(a: &mut A, b: &mut B) -> io::Result<(u64, u64)>
where
    A: AsyncRead + AsyncWrite + Unpin + ?Sized,
    B: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    copy_bidirectional_with_sizes(a, b, 8192, 8192).await
}

/// Like [`copy_bidirectional`], but with an independently chosen buffer
/// size for each direction -- useful when one direction is known to
/// carry meaningfully more traffic than the other (e.g. a bulk download
/// versus its small acknowledgement stream), where one shared 8192-byte
/// default is either wasteful or too small depending on which direction
/// it's sized for.
pub async fn copy_bidirectional_with_sizes<A, B>(
    a: &mut A,
    b: &mut B,
    a_to_b_buf_size: usize,
    b_to_a_buf_size: usize,
) -> io::Result<(u64, u64)>
where
    A: AsyncRead + AsyncWrite + Unpin + ?Sized,
    B: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let mut a_to_b = TransferState::Running(CopyBuffer::with_capacity(a_to_b_buf_size));
    let mut b_to_a = TransferState::Running(CopyBuffer::with_capacity(b_to_a_buf_size));
    std::future::poll_fn(|cx| {
        let a_to_b_result = a_to_b.poll(cx, Pin::new(&mut *a), Pin::new(&mut *b));
        let b_to_a_result = b_to_a.poll(cx, Pin::new(&mut *b), Pin::new(&mut *a));

        match (a_to_b_result, b_to_a_result) {
            (Poll::Ready(Err(e)), _) | (_, Poll::Ready(Err(e))) => Poll::Ready(Err(e)),
            (Poll::Ready(Ok(a_to_b_amt)), Poll::Ready(Ok(b_to_a_amt))) => {
                Poll::Ready(Ok((a_to_b_amt, b_to_a_amt)))
            }
            _ => Poll::Pending,
        }
    })
    .await
}
