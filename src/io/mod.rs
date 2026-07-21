//! Non-blocking, epoll-driven networking. Linux-only (`epoll`, `eventfd`,
//! `accept4` are all Linux syscalls) -- see the crate root docs for what
//! porting to other platforms would take.
//!
//! Socket bind/connect/accept/addressing is built on `rustils`' concrete
//! `platform_linux::{LinuxTcpListener, LinuxTcpStream, LinuxUdpSocket}`
//! rather than reimplemented here -- see `socket.rs`'s module docs for
//! the (small) remainder that's still hand-rolled and why.

mod async_io;
pub(crate) mod reactor;
mod socket;
mod tcp;
mod udp;

pub use async_io::{copy, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
pub use tcp::{TcpListener, TcpStream};
pub use udp::UdpSocket;
