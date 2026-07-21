use super::reactor::{ready_io, Interest, Reactor, ScheduledIo};
use super::socket::from_platform_err;
use crate::runtime::Handle;
use platform_linux::LinuxUdpSocket;
use std::io;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::sync::Arc;

/// A non-blocking, epoll-driven UDP socket, backed entirely by rustils'
/// `LinuxUdpSocket` -- `bind`/`send_to`/`recv_from`/`local_addr` never
/// block on their own (only readiness does), so unlike `TcpStream` there
/// was no need to hand-roll anything here.
pub struct UdpSocket {
    inner: LinuxUdpSocket,
    io: Arc<ScheduledIo>,
    reactor: Arc<Reactor>,
}

impl UdpSocket {
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn bind(addr: SocketAddr) -> io::Result<UdpSocket> {
        let reactor = Handle::current().shared.reactor.clone();
        let inner = LinuxUdpSocket::bind(addr).map_err(from_platform_err)?;
        inner.set_nonblocking(true).map_err(from_platform_err)?;
        let io = reactor.register(inner.as_raw_fd())?;
        Ok(UdpSocket { inner, io, reactor })
    }

    pub async fn send_to(&self, buf: &[u8], addr: SocketAddr) -> io::Result<usize> {
        ready_io(&self.io, Interest::Write, || {
            platform::net::UdpSocket::send_to(&self.inner, buf, addr).map_err(from_platform_err)
        })
        .await
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        ready_io(&self.io, Interest::Read, || {
            platform::net::UdpSocket::recv_from(&self.inner, buf).map_err(from_platform_err)
        })
        .await
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        platform::net::UdpSocket::local_addr(&self.inner).map_err(from_platform_err)
    }
}

impl Drop for UdpSocket {
    fn drop(&mut self) {
        self.reactor.deregister(self.inner.as_raw_fd());
    }
}
