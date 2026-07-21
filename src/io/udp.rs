use super::reactor::{ready_io, Interest, Reactor, ScheduledIo};
use super::socket::{self, from_platform_err};
use crate::runtime::Handle;
use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
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

    /// Adopts an already-bound `std` socket -- e.g. one received from a
    /// supervisor process, or configured with `socket2` for an option
    /// this crate doesn't expose a wrapper for. Flips it non-blocking
    /// and registers it with the reactor without redoing the bind
    /// syscall.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn from_std(socket: std::net::UdpSocket) -> io::Result<UdpSocket> {
        let reactor = Handle::current().shared.reactor.clone();
        let inner = PlatformUdpSocket::from(OwnedFd::from(socket));
        inner.set_nonblocking(true).map_err(from_platform_err)?;
        let io = reactor.register(inner.as_raw_fd())?;
        Ok(UdpSocket { inner, io, reactor })
    }

    /// The reverse of [`from_std`](Self::from_std) -- see
    /// [`crate::io::TcpListener::into_std`] for the
    /// flip-to-blocking/`dup(2)` reasoning, identical here.
    pub fn into_std(self) -> io::Result<std::net::UdpSocket> {
        self.inner
            .set_nonblocking(false)
            .map_err(from_platform_err)?;
        let owned = self.inner.as_fd().try_clone_to_owned()?;
        Ok(std::net::UdpSocket::from(owned))
    }
}

impl Drop for UdpSocket {
    fn drop(&mut self) {
        self.reactor.deregister(self.inner.as_raw_fd());
    }
}
