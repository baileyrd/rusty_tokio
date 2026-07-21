use super::async_io::{AsyncRead, AsyncWrite, ReadBuf};
use super::reactor::{poll_io, ready_io, Interest, Reactor, ScheduledIo};
use super::socket::{self, from_platform_err};
use crate::runtime::Handle;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
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

    /// Adopts an already-bound-and-listening `std` listener -- e.g. one
    /// received from a supervisor process, or set up with `socket2` for
    /// an option this crate doesn't expose a wrapper for (`SO_REUSEPORT`
    /// load-balancing, and the like). Flips it non-blocking and
    /// registers it with the reactor without redoing the bind/listen
    /// syscalls.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn from_std(listener: std::net::TcpListener) -> io::Result<TcpListener> {
        let reactor = Handle::current().shared.reactor.clone();
        let inner = PlatformTcpListener::from(OwnedFd::from(listener));
        inner.set_nonblocking(true).map_err(from_platform_err)?;
        let io = reactor.register(inner.as_raw_fd())?;
        Ok(TcpListener { inner, io, reactor })
    }

    /// The reverse of [`from_std`](Self::from_std): hands this listener
    /// back out as a plain blocking `std::net::TcpListener`, flipped back
    /// to blocking first (matching tokio's own documented behavior --
    /// the returned socket is *not* left non-blocking).
    ///
    /// Duplicates the underlying fd (`dup(2)`, via `try_clone_to_owned`)
    /// rather than transferring the exact same one -- a deliberate
    /// simplification: `self` still drops normally at the end of this
    /// call (deregistering from the reactor and closing its own,
    /// original fd, same as ever), and the returned `std` socket is an
    /// independent fd referring to the same underlying open file
    /// description, the same guarantee `TcpStream::try_clone` already
    /// relies on elsewhere in the standard library -- closing one side
    /// doesn't affect the other. Costs one extra syscall versus
    /// transferring ownership of the original fd directly; not worth the
    /// additional unsafe code that would take to do soundly for how
    /// rarely this is called.
    pub fn into_std(self) -> io::Result<std::net::TcpListener> {
        self.inner
            .set_nonblocking(false)
            .map_err(from_platform_err)?;
        let owned = self.inner.as_fd().try_clone_to_owned()?;
        Ok(std::net::TcpListener::from(owned))
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

    /// Adopts an already-connected `std` stream -- e.g. one received
    /// from a supervisor process, or configured with `socket2` for an
    /// option this crate doesn't expose a wrapper for. Flips it
    /// non-blocking and registers it with the reactor without redoing
    /// the connect syscall.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn from_std(stream: std::net::TcpStream) -> io::Result<TcpStream> {
        let reactor = Handle::current().shared.reactor.clone();
        let inner = PlatformTcpStream::from(OwnedFd::from(stream));
        inner.set_nonblocking(true).map_err(from_platform_err)?;
        let io = reactor.register(inner.as_raw_fd())?;
        Ok(TcpStream { inner, io, reactor })
    }

    /// The reverse of [`from_std`](Self::from_std) -- see
    /// [`TcpListener::into_std`] for the flip-to-blocking/`dup(2)`
    /// reasoning, identical here.
    pub fn into_std(self) -> io::Result<std::net::TcpStream> {
        self.inner
            .set_nonblocking(false)
            .map_err(from_platform_err)?;
        let owned = self.inner.as_fd().try_clone_to_owned()?;
        Ok(std::net::TcpStream::from(owned))
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

/// A TCP socket that's neither bound nor connected yet -- a staging
/// point for setting socket options (`SO_REUSEADDR`, `SO_REUSEPORT`,
/// send/receive buffer sizes) before committing to either direction,
/// unlike [`TcpListener::bind`]/[`TcpStream::connect`], which go
/// straight from nothing to bound-and-listening/connected in one call
/// with no such opportunity. Mirrors tokio's own `net::TcpSocket`.
///
/// None of these four options are in rustils' `TcpStream`/`TcpListener`
/// traits at all (only `set_nodelay` is), so every method here is a
/// hand-rolled `setsockopt`/`getsockopt` call in `socket/mod.rs`, the
/// same sliver-of-raw-libc treatment `connect`/`take_socket_error`
/// already get there.
pub struct TcpSocket {
    fd: OwnedFd,
}

impl TcpSocket {
    /// A bare, non-blocking IPv4 socket -- neither bound nor connected.
    pub fn new_v4() -> io::Result<TcpSocket> {
        Self::new(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)))
    }

    /// A bare, non-blocking IPv6 socket -- neither bound nor connected.
    pub fn new_v6() -> io::Result<TcpSocket> {
        Self::new(SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::UNSPECIFIED,
            0,
            0,
            0,
        )))
    }

    /// `domain_addr` is never bound to -- only its `V4`/`V6` variant is
    /// used, to pick `AF_INET`/`AF_INET6` for the underlying `socket(2)`
    /// call (the same reason [`TcpStream::connect`] passes its own
    /// target address through to `socket::new_tcp_socket` before ever
    /// calling `connect(2)` with it).
    fn new(domain_addr: SocketAddr) -> io::Result<TcpSocket> {
        Ok(TcpSocket {
            fd: socket::new_tcp_socket(domain_addr)?,
        })
    }

    /// `SO_REUSEADDR` -- lets a new socket bind to an address still
    /// lingering in `TIME_WAIT` from a previous listener on the same
    /// port, instead of failing with `EADDRINUSE`.
    pub fn set_reuseaddr(&self, reuse: bool) -> io::Result<()> {
        socket::set_reuseaddr(self.fd.as_raw_fd(), reuse)
    }

    pub fn reuseaddr(&self) -> io::Result<bool> {
        socket::reuseaddr(self.fd.as_raw_fd())
    }

    /// `SO_REUSEPORT` -- lets multiple sockets bind to the exact same
    /// address *and* port, with the kernel load-balancing incoming
    /// connections across them (a common multi-process/multi-thread
    /// listener pattern). Supported on both of this crate's targets.
    pub fn set_reuseport(&self, reuse: bool) -> io::Result<()> {
        socket::set_reuseport(self.fd.as_raw_fd(), reuse)
    }

    pub fn reuseport(&self) -> io::Result<bool> {
        socket::reuseport(self.fd.as_raw_fd())
    }

    pub fn set_send_buffer_size(&self, size: u32) -> io::Result<()> {
        socket::set_send_buffer_size(self.fd.as_raw_fd(), size)
    }

    /// The kernel doesn't necessarily use exactly the size last
    /// requested via [`set_send_buffer_size`](Self::set_send_buffer_size)
    /// (Linux, notably, doubles it) -- read this back to see what was
    /// actually applied.
    pub fn send_buffer_size(&self) -> io::Result<u32> {
        socket::send_buffer_size(self.fd.as_raw_fd())
    }

    pub fn set_recv_buffer_size(&self, size: u32) -> io::Result<()> {
        socket::set_recv_buffer_size(self.fd.as_raw_fd(), size)
    }

    pub fn recv_buffer_size(&self) -> io::Result<u32> {
        socket::recv_buffer_size(self.fd.as_raw_fd())
    }

    /// Binds to `addr`. Doesn't start listening yet -- see
    /// [`listen`](Self::listen), a separate step so options can still be
    /// set (or read back) on the bound-but-not-yet-listening socket in
    /// between, matching `bind(2)`/`listen(2)` already being separate
    /// syscalls at the OS level.
    pub fn bind(&self, addr: SocketAddr) -> io::Result<()> {
        socket::bind(self.fd.as_raw_fd(), addr)
    }

    /// Starts listening, turning this into an ordinary [`TcpListener`].
    /// `backlog` is the OS's pending-connection queue length hint (see
    /// `listen(2)`).
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn listen(self, backlog: u32) -> io::Result<TcpListener> {
        socket::listen(self.fd.as_raw_fd(), backlog)?;
        let reactor = Handle::current().shared.reactor.clone();
        let inner = PlatformTcpListener::from(self.fd);
        // Already non-blocking from `socket::new_tcp_socket` -- this is
        // a no-op in practice, kept for the same belt-and-suspenders
        // reason `from_std` sets it explicitly too rather than trusting
        // the fd's existing state.
        inner.set_nonblocking(true).map_err(from_platform_err)?;
        let io = reactor.register(inner.as_raw_fd())?;
        Ok(TcpListener { inner, io, reactor })
    }

    /// Connects, turning this into an ordinary [`TcpStream`].
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn connect(self, addr: SocketAddr) -> io::Result<TcpStream> {
        let reactor = Handle::current().shared.reactor.clone();
        socket::connect(self.fd.as_raw_fd(), addr)?;
        let io = reactor.register(self.fd.as_raw_fd())?;
        let inner = PlatformTcpStream::from(self.fd);
        // A non-blocking connect completes asynchronously; the socket
        // becoming writable is the signal to check whether it actually
        // succeeded -- same as `TcpStream::connect`.
        ready_io(&io, Interest::Write, || {
            socket::take_socket_error(inner.as_raw_fd())
        })
        .await?;
        Ok(TcpStream { inner, io, reactor })
    }
}
