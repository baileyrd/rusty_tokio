//! Non-blocking networking: `epoll` on Linux, `kevent` on macOS/BSD --
//! see `reactor/mod.rs` for the shared/per-backend split, and
//! `reactor/kqueue.rs`'s docs for the caveat that the macOS backend is
//! compile-checked (`cargo check --target x86_64-apple-darwin`) but has
//! never been run on real hardware.
//!
//! Socket bind/connect/accept/addressing is built on `rustils`' concrete
//! `platform_linux::{LinuxTcpListener, LinuxTcpStream, LinuxUdpSocket}`
//! on Linux (see `socket/mod.rs`'s docs for the small remainder that's
//! still hand-rolled even there) -- rustils has no macOS backend, so
//! `socket/macos.rs` hand-rolls the equivalent surface directly against
//! `libc` instead, shaped to match closely enough that `tcp.rs`/`udp.rs`
//! need only a `#[cfg]`-gated type alias, not their own OS branching.

mod async_io;
pub(crate) mod reactor;
mod socket;
mod tcp;
mod udp;

pub use async_io::{copy, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
pub use tcp::{TcpListener, TcpStream};
pub use udp::UdpSocket;
