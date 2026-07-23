use super::async_io::{AsyncRead, AsyncWrite, ReadBuf};
use super::reactor::{poll_io, ready_io, Interest, Reactor, ScheduledIo, TryCloneIo};
use super::socket::{self, from_platform_err};
use crate::runtime::Handle;
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

// See `tcp.rs`'s equivalent comment: rustils' concrete type either way
// (`platform_linux` on Linux, `platform_macos` on macOS), identical logic
// below regardless of which -- both shaped identically to their TCP
// counterparts, minus `set_nodelay` (no Nagle buffering on `AF_UNIX`) and
// with `Option<PathBuf>` addresses in place of `SocketAddr` (an `AF_UNIX`
// peer that never `bind`-ed has no address to report, unlike TCP).
#[cfg(any(target_os = "linux", target_os = "macos"))]
use platform::net::{UnixListener as _, UnixStream as _};

#[cfg(target_os = "linux")]
use platform_linux::{
    LinuxUnixListener as PlatformUnixListener, LinuxUnixStream as PlatformUnixStream,
};

#[cfg(target_os = "macos")]
use platform_macos::{
    MacosUnixListener as PlatformUnixListener, MacosUnixStream as PlatformUnixStream,
};

/// A non-blocking, epoll-driven Unix domain socket listener.
pub struct UnixListener {
    inner: PlatformUnixListener,
    io: Arc<ScheduledIo>,
    reactor: Arc<Reactor>,
}

impl UnixListener {
    /// Binds and starts listening at `path`, narrowed to owner-only
    /// (mode `0600`) where the OS has that concept. A stale leftover
    /// socket file (left behind by a listener that died without
    /// unlinking it) is reclaimed automatically -- rustils' own
    /// `unix_listen` distinguishes "stale" from "still live" via a
    /// throwaway probe connect; a path a live listener still holds fails
    /// with `AddrInUse` instead, same as `TcpListener::bind` on a port
    /// already in use.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn bind(path: &Path) -> io::Result<UnixListener> {
        let reactor = Handle::current().shared.reactor.clone();
        let inner = PlatformUnixListener::bind(path).map_err(from_platform_err)?;
        inner.set_nonblocking(true).map_err(from_platform_err)?;
        let io = reactor.register(inner.as_raw_fd())?;
        Ok(UnixListener { inner, io, reactor })
    }

    pub async fn accept(&self) -> io::Result<(UnixStream, Option<PathBuf>)> {
        let (stream, peer) = ready_io(&self.io, Interest::Read, || {
            self.inner.accept().map_err(from_platform_err)
        })
        .await?;
        stream.set_nonblocking(true).map_err(from_platform_err)?;
        let stream = UnixStream::from_accepted(stream, self.reactor.clone())?;
        Ok((stream, peer))
    }

    pub fn local_addr(&self) -> io::Result<Option<PathBuf>> {
        self.inner.local_addr().map_err(from_platform_err)
    }
}

impl Drop for UnixListener {
    fn drop(&mut self) {
        self.reactor.deregister(self.inner.as_raw_fd());
    }
}

// Unlike `TcpListener`/`UdpSocket` (`io/tcp.rs`/`io/udp.rs`), there's no
// existing `from_std`/`into_std` to build these on here -- built
// directly on `PlatformUnixListener`'s own `AsFd`/`AsRawFd`/
// `From<OwnedFd>` instead, the same primitives `bind` and `Drop` above
// already use. `IntoRawFd` dup(2)s (`try_clone_io`) rather than
// transferring the exact same fd, for the same reason `TcpListener::
// into_std` does -- see that method's own docs.
impl std::os::fd::AsFd for UnixListener {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

impl std::os::fd::AsRawFd for UnixListener {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.inner.as_raw_fd()
    }
}

impl std::os::fd::FromRawFd for UnixListener {
    unsafe fn from_raw_fd(fd: std::os::fd::RawFd) -> Self {
        let owned = unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) };
        let inner = PlatformUnixListener::from(owned);
        inner
            .set_nonblocking(true)
            .expect("failed to set the adopted fd non-blocking");
        let reactor = Handle::current().shared.reactor.clone();
        let io = reactor
            .register(inner.as_raw_fd())
            .expect("failed to register raw fd with the reactor");
        UnixListener { inner, io, reactor }
    }
}

impl std::os::fd::IntoRawFd for UnixListener {
    fn into_raw_fd(self) -> std::os::fd::RawFd {
        self.inner
            .try_clone_io()
            .expect("failed to duplicate fd")
            .into_raw_fd()
    }
}

/// A non-blocking, epoll-driven Unix domain stream socket.
///
/// Like [`super::TcpStream`], exposes both a plain `&self`
/// `async fn read`/`write` pair and the [`AsyncRead`]/[`AsyncWrite`]
/// trait pair, both implemented for `&UnixStream` so one task can read
/// while another writes the same stream (e.g. via two `Arc<UnixStream>`
/// clones).
pub struct UnixStream {
    inner: PlatformUnixStream,
    io: Arc<ScheduledIo>,
    reactor: Arc<Reactor>,
}

impl UnixStream {
    /// Splits into borrowed read/write halves -- see
    /// [`super::TcpStream::split`], whose reasoning and implementation
    /// this mirrors exactly (just over `&UnixStream` instead of
    /// `&TcpStream`). Named `UnixReadHalf`/`UnixWriteHalf` rather than
    /// plain `ReadHalf`/`WriteHalf` only because both this module and
    /// `tcp.rs` are flattened into `io`'s own namespace (`pub use
    /// tcp::{ReadHalf, ...}` and `pub use unix::{...}` side by side) --
    /// reusing the exact same names here would collide.
    pub fn split(&mut self) -> (UnixReadHalf<'_>, UnixWriteHalf<'_>) {
        (UnixReadHalf(self), UnixWriteHalf(self))
    }

    /// Splits into owned read/write halves -- see
    /// [`super::TcpStream::into_split`], whose reasoning and
    /// implementation this mirrors exactly.
    pub fn into_split(self) -> (OwnedUnixReadHalf, OwnedUnixWriteHalf) {
        let inner = Arc::new(self);
        (OwnedUnixReadHalf(inner.clone()), OwnedUnixWriteHalf(inner))
    }

    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn connect(path: &Path) -> io::Result<UnixStream> {
        let reactor = Handle::current().shared.reactor.clone();
        let fd = socket::new_unix_socket()?;
        socket::unix_connect(fd.as_raw_fd(), path)?;
        let io = reactor.register(fd.as_raw_fd())?;
        let inner = PlatformUnixStream::from(fd);
        // Same non-blocking-connect-completes-asynchronously reasoning
        // as `TcpStream::connect`.
        ready_io(&io, Interest::Write, || {
            socket::take_socket_error(inner.as_raw_fd())
        })
        .await?;
        Ok(UnixStream { inner, io, reactor })
    }

    fn from_accepted(inner: PlatformUnixStream, reactor: Arc<Reactor>) -> io::Result<UnixStream> {
        let io = reactor.register(inner.as_raw_fd())?;
        Ok(UnixStream { inner, io, reactor })
    }

    pub async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        ready_io(&self.io, Interest::Read, || {
            socket::read(self.inner.as_raw_fd(), buf)
        })
        .await
    }

    pub async fn write(&self, buf: &[u8]) -> io::Result<usize> {
        ready_io(&self.io, Interest::Write, || {
            socket::write(self.inner.as_raw_fd(), buf)
        })
        .await
    }

    pub async fn write_all(&self, mut buf: &[u8]) -> io::Result<()> {
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

    /// Reads until `buf` is completely filled, or returns
    /// `UnexpectedEof` if the peer closes first.
    pub async fn read_exact(&self, mut buf: &mut [u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let n = self.read(buf).await?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "early eof"));
            }
            buf = &mut buf[n..];
        }
        Ok(())
    }

    pub fn peer_addr(&self) -> io::Result<Option<PathBuf>> {
        self.inner.peer_addr().map_err(from_platform_err)
    }

    pub fn local_addr(&self) -> io::Result<Option<PathBuf>> {
        self.inner.local_addr().map_err(from_platform_err)
    }

    fn poll_read_priv(&self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        poll_io(&self.io, Interest::Read, cx, || {
            socket::read(self.inner.as_raw_fd(), buf)
        })
    }

    fn poll_write_priv(&self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        poll_io(&self.io, Interest::Write, cx, || {
            socket::write(self.inner.as_raw_fd(), buf)
        })
    }
}

impl Drop for UnixStream {
    fn drop(&mut self) {
        self.reactor.deregister(self.inner.as_raw_fd());
    }
}

// See `UnixListener`'s equivalent impls above.
impl std::os::fd::AsFd for UnixStream {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

impl std::os::fd::AsRawFd for UnixStream {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.inner.as_raw_fd()
    }
}

impl std::os::fd::FromRawFd for UnixStream {
    unsafe fn from_raw_fd(fd: std::os::fd::RawFd) -> Self {
        let owned = unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) };
        let inner = PlatformUnixStream::from(owned);
        inner
            .set_nonblocking(true)
            .expect("failed to set the adopted fd non-blocking");
        let reactor = Handle::current().shared.reactor.clone();
        UnixStream::from_accepted(inner, reactor)
            .expect("failed to register raw fd with the reactor")
    }
}

impl std::os::fd::IntoRawFd for UnixStream {
    fn into_raw_fd(self) -> std::os::fd::RawFd {
        self.inner
            .try_clone_io()
            .expect("failed to duplicate fd")
            .into_raw_fd()
    }
}

impl AsyncRead for &UnixStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.poll_read_priv(cx, buf.unfilled_mut()) {
            Poll::Ready(Ok(n)) => {
                buf.advance(n);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for &UnixStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_priv(cx, buf)
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(socket::shutdown_write(self.inner.as_raw_fd()))
    }
}

/// Delegates to the `&UnixStream` impl above -- see `TcpStream`'s
/// equivalent impl for why `&mut self` here isn't a real exclusivity
/// requirement.
impl AsyncRead for UnixStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut &*self.get_mut()).poll_read(cx, buf)
    }
}

impl AsyncWrite for UnixStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut &*self.get_mut()).poll_write(cx, buf)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut &*self.get_mut()).poll_shutdown(cx)
    }
}

/// Borrowed read half of a [`UnixStream`], created by [`UnixStream::split`].
pub struct UnixReadHalf<'a>(&'a UnixStream);

/// Borrowed write half of a [`UnixStream`], created by [`UnixStream::split`].
pub struct UnixWriteHalf<'a>(&'a UnixStream);

impl AsyncRead for UnixReadHalf<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl AsyncWrite for UnixWriteHalf<'_> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}

/// Owned read half of a [`UnixStream`], created by
/// [`UnixStream::into_split`].
pub struct OwnedUnixReadHalf(Arc<UnixStream>);

/// Owned write half of a [`UnixStream`], created by
/// [`UnixStream::into_split`].
pub struct OwnedUnixWriteHalf(Arc<UnixStream>);

impl AsyncRead for OwnedUnixReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut &*self.get_mut().0).poll_read(cx, buf)
    }
}

impl AsyncWrite for OwnedUnixWriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut &*self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut &*self.get_mut().0).poll_shutdown(cx)
    }
}
