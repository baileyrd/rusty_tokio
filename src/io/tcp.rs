use super::reactor::{ready_io, Interest, Reactor, ScheduledIo};
use super::socket::{self, from_platform_err};
use crate::runtime::Handle;
use platform_linux::{LinuxTcpListener, LinuxTcpStream};
use std::io;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::sync::Arc;

/// A non-blocking, epoll-driven TCP listener, backed by rustils'
/// `LinuxTcpListener` for bind/accept/addressing.
pub struct TcpListener {
    inner: LinuxTcpListener,
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
        let inner = LinuxTcpListener::bind(addr).map_err(from_platform_err)?;
        // rustils creates the listener blocking; flip it non-blocking
        // before it's ever registered with epoll or accepted from.
        inner.set_nonblocking(true).map_err(from_platform_err)?;
        let io = reactor.register(inner.as_raw_fd())?;
        Ok(TcpListener { inner, io, reactor })
    }

    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let (stream, peer) = ready_io(&self.io, Interest::Read, || {
            self.inner.accept().map_err(from_platform_err)
        })
        .await?;
        // accept4 hands back a blocking fd regardless of the listener's
        // own non-blocking state; flip it before it's touched.
        stream.set_nonblocking(true).map_err(from_platform_err)?;
        let stream = TcpStream::from_linux(stream, self.reactor.clone())?;
        Ok((stream, peer))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        platform::net::TcpListener::local_addr(&self.inner).map_err(from_platform_err)
    }
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        self.reactor.deregister(self.inner.as_raw_fd());
    }
}

/// A non-blocking, epoll-driven TCP stream, backed by rustils'
/// `LinuxTcpStream` for addressing/`set_nodelay`. `connect` and the
/// actual `read`/`write` syscalls are hand-rolled -- see `socket.rs`'s
/// module docs for why.
///
/// This exposes plain `async fn read`/`write` rather than the
/// `AsyncRead`/`AsyncWrite` trait pair real ecosystems standardize on --
/// scoped out here to keep the surface small; see the crate-level docs
/// for what's deliberately left for later.
pub struct TcpStream {
    inner: LinuxTcpStream,
    io: Arc<ScheduledIo>,
    reactor: Arc<Reactor>,
}

impl TcpStream {
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn connect(addr: SocketAddr) -> io::Result<TcpStream> {
        let reactor = Handle::current().shared.reactor.clone();
        let fd = socket::new_tcp_socket(addr)?;
        socket::connect(fd.as_raw_fd(), addr)?;
        let io = reactor.register(fd.as_raw_fd())?;
        let inner = LinuxTcpStream::from(fd);
        // A non-blocking connect completes asynchronously; the socket
        // becoming writable is the signal to check whether it actually
        // succeeded.
        ready_io(&io, Interest::Write, || {
            socket::take_socket_error(inner.as_raw_fd())
        })
        .await?;
        Ok(TcpStream { inner, io, reactor })
    }

    fn from_linux(inner: LinuxTcpStream, reactor: Arc<Reactor>) -> io::Result<TcpStream> {
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
        platform::net::TcpStream::set_nodelay(&self.inner, nodelay).map_err(from_platform_err)
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        platform::net::TcpStream::peer_addr(&self.inner).map_err(from_platform_err)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        platform::net::TcpStream::local_addr(&self.inner).map_err(from_platform_err)
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        self.reactor.deregister(self.inner.as_raw_fd());
    }
}
