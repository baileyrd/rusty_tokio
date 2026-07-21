use super::async_io::{AsyncRead, AsyncWrite, ReadBuf};
use super::reactor::{poll_io, ready_io, Interest, Reactor, ScheduledIo};
use super::socket::{self, from_platform_err};
use crate::runtime::Handle;
use std::io;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

// `PlatformTcpListener`/`PlatformTcpStream` are the only OS-specific
// names in this file: rustils' concrete types cover bind/accept/
// addressing/`set_nodelay` on both backends it has (`platform_linux` on
// Linux, `platform_macos` on macOS -- see `socket/mod.rs`'s docs for
// what stays hand-rolled on top of either one). Everything below this
// point is identical logic regardless of which one it is.
#[cfg(any(target_os = "linux", target_os = "macos"))]
use platform::net::{TcpListener as _, TcpStream as _};

#[cfg(target_os = "linux")]
use platform_linux::{
    LinuxTcpListener as PlatformTcpListener, LinuxTcpStream as PlatformTcpStream,
};

#[cfg(target_os = "macos")]
use platform_macos::{
    MacosTcpListener as PlatformTcpListener, MacosTcpStream as PlatformTcpStream,
};

/// A non-blocking, epoll-driven TCP listener.
pub struct TcpListener {
    inner: PlatformTcpListener,
    io: Arc<ScheduledIo>,
    reactor: Arc<Reactor>,
}

impl TcpListener {
    /// Binds and starts listening at `addr` (port `0` picks an
    /// ephemeral port -- read it back via [`TcpListener::local_addr`]).
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn bind(addr: SocketAddr) -> io::Result<TcpListener> {
        let reactor = Handle::current().shared.reactor.clone();
        let inner = PlatformTcpListener::bind(addr).map_err(from_platform_err)?;
        // The listener is created blocking; flip it non-blocking before
        // it's ever registered with the reactor or accepted from.
        inner.set_nonblocking(true).map_err(from_platform_err)?;
        let io = reactor.register(inner.as_raw_fd())?;
        Ok(TcpListener { inner, io, reactor })
    }

    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let (stream, peer) = ready_io(&self.io, Interest::Read, || {
            self.inner.accept().map_err(from_platform_err)
        })
        .await?;
        // A freshly accepted fd is born blocking regardless of the
        // listener's own non-blocking state; flip it before it's ever
        // touched.
        stream.set_nonblocking(true).map_err(from_platform_err)?;
        let stream = TcpStream::from_accepted(stream, self.reactor.clone())?;
        Ok((stream, peer))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr().map_err(from_platform_err)
    }
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        self.reactor.deregister(self.inner.as_raw_fd());
    }
}

/// A non-blocking, epoll-driven TCP stream.
///
/// Exposes both a plain `&self` `async fn read`/`write` pair (so one
/// task can read while another writes the same stream, e.g. via two
/// `Arc<TcpStream>` clones) and the [`AsyncRead`]/[`AsyncWrite`] trait
/// pair for generic code -- see `async_io.rs`'s module docs for why both
/// exist and how they share one implementation.
pub struct TcpStream {
    inner: PlatformTcpStream,
    io: Arc<ScheduledIo>,
    reactor: Arc<Reactor>,
}

impl TcpStream {
    /// Splits into borrowed read/write halves, for concurrent read/write
    /// access without needing a full `Arc`-wrapped clone -- e.g. racing
    /// a read against a write from within the same task. For halves that
    /// can be moved into two separate spawned tasks, see
    /// [`TcpStream::into_split`].
    ///
    /// This is purely a borrow-splitting convenience: the underlying
    /// concurrent-access support already exists on `&TcpStream` (see the
    /// `AsyncRead`/`AsyncWrite` impls below), which is all `split` hands
    /// out under two different names.
    pub fn split(&mut self) -> (ReadHalf<'_>, WriteHalf<'_>) {
        (ReadHalf(self), WriteHalf(self))
    }

    /// Splits into owned read/write halves, each independently `'static`
    /// and movable into its own spawned task without the call site
    /// needing to wrap the stream in an `Arc` itself. Internally this
    /// *is* just an `Arc<TcpStream>` behind each half -- the same
    /// pattern [`tests/async_io.rs`'s `shared_ref_impl_*` test]
    /// exercises by hand -- `into_split` only saves callers from doing
    /// that wrapping themselves.
    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        let inner = Arc::new(self);
        (OwnedReadHalf(inner.clone()), OwnedWriteHalf(inner))
    }

    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn connect(addr: SocketAddr) -> io::Result<TcpStream> {
        let reactor = Handle::current().shared.reactor.clone();
        let fd = socket::new_tcp_socket(addr)?;
        socket::connect(fd.as_raw_fd(), addr)?;
        let io = reactor.register(fd.as_raw_fd())?;
        let inner = PlatformTcpStream::from(fd);
        // A non-blocking connect completes asynchronously; the socket
        // becoming writable is the signal to check whether it actually
        // succeeded.
        ready_io(&io, Interest::Write, || {
            socket::take_socket_error(inner.as_raw_fd())
        })
        .await?;
        Ok(TcpStream { inner, io, reactor })
    }

    fn from_accepted(inner: PlatformTcpStream, reactor: Arc<Reactor>) -> io::Result<TcpStream> {
        let io = reactor.register(inner.as_raw_fd())?;
        Ok(TcpStream { inner, io, reactor })
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

    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.inner.set_nodelay(nodelay).map_err(from_platform_err)
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.peer_addr().map_err(from_platform_err)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
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

impl Drop for TcpStream {
    fn drop(&mut self) {
        self.reactor.deregister(self.inner.as_raw_fd());
    }
}

/// The real `AsyncRead` logic: only ever needs shared access, since the
/// reactor readiness state and the fd are both already behind `Arc`/a
/// kernel-owned handle. This is what lets two `&TcpStream`s -- e.g. from
/// [`std::io::copy`]-style code split across two tasks -- read and write
/// concurrently through the trait, the same as the inherent methods.
impl AsyncRead for &TcpStream {
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

impl AsyncWrite for &TcpStream {
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

/// Delegates to the `&TcpStream` impl above -- an owned `TcpStream` only
/// ever needed shared access internally too, so `&mut self` here is
/// purely to match the trait's usual shape, not a real exclusivity
/// requirement.
impl AsyncRead for TcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut &*self.get_mut()).poll_read(cx, buf)
    }
}

impl AsyncWrite for TcpStream {
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

/// Borrowed read half of a [`TcpStream`], created by [`TcpStream::split`].
pub struct ReadHalf<'a>(&'a TcpStream);

/// Borrowed write half of a [`TcpStream`], created by [`TcpStream::split`].
pub struct WriteHalf<'a>(&'a TcpStream);

impl AsyncRead for ReadHalf<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl AsyncWrite for WriteHalf<'_> {
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

/// Owned read half of a [`TcpStream`], created by [`TcpStream::into_split`].
pub struct OwnedReadHalf(Arc<TcpStream>);

/// Owned write half of a [`TcpStream`], created by [`TcpStream::into_split`].
pub struct OwnedWriteHalf(Arc<TcpStream>);

impl AsyncRead for OwnedReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut &*self.get_mut().0).poll_read(cx, buf)
    }
}

impl AsyncWrite for OwnedWriteHalf {
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
