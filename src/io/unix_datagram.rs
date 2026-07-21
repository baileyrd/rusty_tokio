//! [`UnixDatagram`]: the connectionless, `AF_UNIX` counterpart of
//! [`super::UdpSocket`] -- one socket both sends and receives, addressed
//! by filesystem path instead of `SocketAddr`, no listener/stream split
//! the way [`super::UnixStream`]/[`super::UnixListener`] have.
//!
//! Every other socket type in this module (`TcpStream`/`TcpListener`/
//! `UdpSocket`/`UnixStream`/`UnixListener`) is built on rustils'
//! `platform_linux`/`platform_macos` concrete types. `AF_UNIX` datagram
//! sockets aren't: rustils' `Net` trait (`crates/platform/src/net.rs`)
//! only has `unix_connect`/`unix_listen` for connection-oriented `AF_UNIX`
//! sockets, nothing `SOCK_DGRAM`-shaped at all. Rather than hand-rolling
//! a *third* copy of `AF_UNIX` sockaddr packing and `sendto`/`recvfrom`
//! in this crate (`socket/mod.rs` already has one hand-rolled copy for
//! non-blocking `AF_UNIX` stream `connect`, and rustils has its own
//! internal one for `unix_connect`/`unix_listen`), this wraps
//! `std::os::unix::net::UnixDatagram` directly instead: std's own
//! implementation is already complete (`bind`/`send_to`/`recv_from`/
//! `connect`/`send`/`recv`/`local_addr`, addressed via its own
//! `std::os::unix::net::SocketAddr`) and needs zero new unsafe code here
//! -- only a `set_nonblocking(true)` and reactor registration, the same
//! bridge [`super::TcpStream::from_std`] already builds for adopting a
//! `std` socket into this crate's reactor.

use super::reactor::{ready_io, Interest, Reactor, ScheduledIo};
use crate::runtime::Handle;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

/// A non-blocking, epoll-driven `AF_UNIX` datagram socket. See this
/// module's own docs for why it's built directly on
/// `std::os::unix::net::UnixDatagram` rather than a rustils concrete
/// type the way every other socket in this module is.
pub struct UnixDatagram {
    inner: std::os::unix::net::UnixDatagram,
    io: Arc<ScheduledIo>,
    reactor: Arc<Reactor>,
}

impl UnixDatagram {
    /// Binds to `path`, ready to `send_to`/`recv_from` any peer, or
    /// `connect` to fix one default peer for `send`/`recv`.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn bind(path: impl AsRef<Path>) -> io::Result<UnixDatagram> {
        let reactor = Handle::current().shared.reactor.clone();
        let inner = std::os::unix::net::UnixDatagram::bind(path)?;
        inner.set_nonblocking(true)?;
        let io = reactor.register(inner.as_raw_fd())?;
        Ok(UnixDatagram { inner, io, reactor })
    }

    /// An unbound (unnamed) socket -- can send via
    /// [`send_to`](Self::send_to) or, once [`connect`](Self::connect)ed,
    /// `send`/`recv`, but has no path of its own for a peer to reply to
    /// via `send_to` unless it's bound first.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn unbound() -> io::Result<UnixDatagram> {
        let reactor = Handle::current().shared.reactor.clone();
        let inner = std::os::unix::net::UnixDatagram::unbound()?;
        inner.set_nonblocking(true)?;
        let io = reactor.register(inner.as_raw_fd())?;
        Ok(UnixDatagram { inner, io, reactor })
    }

    pub async fn send_to(&self, buf: &[u8], path: impl AsRef<Path>) -> io::Result<usize> {
        let path = path.as_ref();
        ready_io(&self.io, Interest::Write, || self.inner.send_to(buf, path)).await
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        ready_io(&self.io, Interest::Read, || self.inner.recv_from(buf)).await
    }

    /// Fixes `path` as this socket's peer, so [`send`](Self::send)/
    /// [`recv`](Self::recv) can omit it on every call afterward --
    /// synchronous, like [`super::UdpSocket::connect`]: an `AF_UNIX`
    /// `connect(2)` to an already-bound peer completes immediately, no
    /// asynchronous handshake the way a TCP connect needs to wait out.
    pub fn connect(&self, path: impl AsRef<Path>) -> io::Result<()> {
        self.inner.connect(path)
    }

    /// Sends to whichever peer [`connect`](Self::connect) fixed.
    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        ready_io(&self.io, Interest::Write, || self.inner.send(buf)).await
    }

    /// Receives from whichever peer [`connect`](Self::connect) fixed --
    /// datagrams from anyone else are not delivered to a connected
    /// socket.
    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        ready_io(&self.io, Interest::Read, || self.inner.recv(buf)).await
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

impl Drop for UnixDatagram {
    fn drop(&mut self) {
        self.reactor.deregister(self.inner.as_raw_fd());
    }
}
