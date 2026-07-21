use super::reactor::{ready_io, Interest, Reactor, ScheduledIo};
use super::socket::{self, from_platform_err};
use crate::runtime::Handle;
use std::io;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::sync::Arc;

// See `tcp.rs`'s equivalent comment: rustils' concrete type either way
// (`platform_linux` on Linux, `platform_macos` on macOS), identical
// logic below regardless of which.
#[cfg(any(target_os = "linux", target_os = "macos"))]
use platform::net::UdpSocket as _;

#[cfg(target_os = "linux")]
use platform_linux::LinuxUdpSocket as PlatformUdpSocket;

#[cfg(target_os = "macos")]
use platform_macos::MacosUdpSocket as PlatformUdpSocket;

/// A non-blocking, epoll-driven UDP socket. `bind`/`send_to`/
/// `recv_from`/`local_addr` never block on their own (only readiness
/// does), so unlike `TcpStream` there was no need to hand-roll anything
/// beyond socket lifecycle here.
pub struct UdpSocket {
    inner: PlatformUdpSocket,
    io: Arc<ScheduledIo>,
    reactor: Arc<Reactor>,
}

impl UdpSocket {
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn bind(addr: SocketAddr) -> io::Result<UdpSocket> {
        let reactor = Handle::current().shared.reactor.clone();
        let inner = PlatformUdpSocket::bind(addr).map_err(from_platform_err)?;
        inner.set_nonblocking(true).map_err(from_platform_err)?;
        let io = reactor.register(inner.as_raw_fd())?;
        Ok(UdpSocket { inner, io, reactor })
    }

    pub async fn send_to(&self, buf: &[u8], addr: SocketAddr) -> io::Result<usize> {
        ready_io(&self.io, Interest::Write, || {
            self.inner.send_to(buf, addr).map_err(from_platform_err)
        })
        .await
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        ready_io(&self.io, Interest::Read, || {
            self.inner.recv_from(buf).map_err(from_platform_err)
        })
        .await
    }

    /// Fixes `addr` as this socket's peer, so [`send`](Self::send)/
    /// [`recv`](Self::recv) can omit it on every call afterward. Unlike
    /// TCP's `connect`, this is a local, synchronous operation -- UDP's
    /// `connect(2)` doesn't perform a network handshake, it just records
    /// a default peer address (and, for a socket that was never bound,
    /// picks one) -- so there's nothing to `.await` here, and this
    /// crate's usual hand-rolled non-blocking-connect dance (`connect`
    /// in `socket/mod.rs`, needed because *TCP*'s `connect(2)` can
    /// return `EINPROGRESS`) is reused directly: for UDP, its
    /// `EINPROGRESS` branch simply never triggers.
    pub fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        socket::connect(self.inner.as_raw_fd(), addr)
    }

    /// Sends to whichever peer [`connect`](Self::connect) fixed.
    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        ready_io(&self.io, Interest::Write, || {
            socket::write(self.inner.as_raw_fd(), buf)
        })
        .await
    }

    /// Receives from whichever peer [`connect`](Self::connect) fixed --
    /// datagrams from anyone else are not delivered to a connected UDP
    /// socket.
    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        ready_io(&self.io, Interest::Read, || {
            socket::read(self.inner.as_raw_fd(), buf)
        })
        .await
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr().map_err(from_platform_err)
    }
}

impl Drop for UdpSocket {
    fn drop(&mut self) {
        self.reactor.deregister(self.inner.as_raw_fd());
    }
}
