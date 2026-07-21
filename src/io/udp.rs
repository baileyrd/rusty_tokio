use super::reactor::{ready_io, Interest, Reactor, ScheduledIo};
use super::socket::from_platform_err;
use crate::runtime::Handle;
use std::io;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::sync::Arc;

// See `tcp.rs`'s equivalent comment: rustils' concrete type on Linux,
// this crate's own hand-rolled shim on macOS/BSD, identical logic below
// either way.
#[cfg(target_os = "linux")]
use platform::net::UdpSocket as _;
#[cfg(target_os = "linux")]
use platform_linux::LinuxUdpSocket as PlatformUdpSocket;

#[cfg(any(target_os = "macos", target_os = "ios"))]
use super::socket::MacosUdpSocket as PlatformUdpSocket;

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

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr().map_err(from_platform_err)
    }
}

impl Drop for UdpSocket {
    fn drop(&mut self) {
        self.reactor.deregister(self.inner.as_raw_fd());
    }
}
