use super::reactor::{ready_io, Interest, Reactor, ScheduledIo};
use super::socket;
use crate::runtime::Handle;
use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::Arc;

/// A non-blocking, epoll-driven UDP socket.
pub struct UdpSocket {
    fd: OwnedFd,
    io: Arc<ScheduledIo>,
    reactor: Arc<Reactor>,
}

impl UdpSocket {
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn bind(addr: SocketAddr) -> io::Result<UdpSocket> {
        let reactor = Handle::current().shared.reactor.clone();
        let fd = socket::new_udp_socket(addr)?;
        socket::bind(fd.as_raw_fd(), addr)?;
        let io = reactor.register(fd.as_raw_fd())?;
        Ok(UdpSocket { fd, io, reactor })
    }

    pub async fn send_to(&self, buf: &[u8], addr: SocketAddr) -> io::Result<usize> {
        ready_io(&self.io, Interest::Write, || {
            socket::send_to(self.fd.as_raw_fd(), buf, addr)
        })
        .await
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        ready_io(&self.io, Interest::Read, || {
            socket::recv_from(self.fd.as_raw_fd(), buf)
        })
        .await
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        socket::local_addr(self.fd.as_raw_fd())
    }
}

impl Drop for UdpSocket {
    fn drop(&mut self) {
        self.reactor.deregister(self.fd.as_raw_fd());
    }
}
