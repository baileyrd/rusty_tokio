use super::reactor::{ready_io, Interest, Reactor, ScheduledIo};
use super::socket;
use crate::runtime::Handle;
use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::Arc;

/// A non-blocking, epoll-driven TCP listener.
pub struct TcpListener {
    fd: OwnedFd,
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
        let fd = socket::new_tcp_socket(addr)?;
        socket::set_reuseaddr(fd.as_raw_fd())?;
        socket::bind(fd.as_raw_fd(), addr)?;
        socket::listen(fd.as_raw_fd(), 1024)?;
        let io = reactor.register(fd.as_raw_fd())?;
        Ok(TcpListener { fd, io, reactor })
    }

    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let (owned, peer) = ready_io(&self.io, Interest::Read, || {
            socket::accept(self.fd.as_raw_fd())
        })
        .await?;
        let stream = TcpStream::from_owned_fd(owned, self.reactor.clone())?;
        Ok((stream, peer))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        socket::local_addr(self.fd.as_raw_fd())
    }
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        self.reactor.deregister(self.fd.as_raw_fd());
    }
}

/// A non-blocking, epoll-driven TCP stream.
///
/// This exposes plain `async fn read`/`write` rather than the
/// `AsyncRead`/`AsyncWrite` trait pair real ecosystems standardize on --
/// scoped out here to keep the surface small; see the crate-level docs
/// for what's deliberately left for later.
pub struct TcpStream {
    fd: OwnedFd,
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
        // A non-blocking connect completes asynchronously; the socket
        // becoming writable is the signal to check whether it actually
        // succeeded.
        ready_io(&io, Interest::Write, || {
            socket::take_socket_error(fd.as_raw_fd())
        })
        .await?;
        Ok(TcpStream { fd, io, reactor })
    }

    fn from_owned_fd(fd: OwnedFd, reactor: Arc<Reactor>) -> io::Result<TcpStream> {
        let io = reactor.register(fd.as_raw_fd())?;
        Ok(TcpStream { fd, io, reactor })
    }

    pub async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        ready_io(&self.io, Interest::Read, || {
            socket::read(self.fd.as_raw_fd(), buf)
        })
        .await
    }

    pub async fn write(&self, buf: &[u8]) -> io::Result<usize> {
        ready_io(&self.io, Interest::Write, || {
            socket::write(self.fd.as_raw_fd(), buf)
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
        socket::set_nodelay(self.fd.as_raw_fd(), nodelay)
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        socket::peer_addr(self.fd.as_raw_fd())
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        socket::local_addr(self.fd.as_raw_fd())
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        self.reactor.deregister(self.fd.as_raw_fd());
    }
}
