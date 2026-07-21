//! Non-blocking, epoll-driven networking. Linux-only (`epoll`, `eventfd`,
//! `accept4` are all Linux syscalls) -- see the crate root docs for what
//! porting to other platforms would take.

pub(crate) mod reactor;
mod socket;
mod tcp;
mod udp;

pub use tcp::{TcpListener, TcpStream};
pub use udp::UdpSocket;
