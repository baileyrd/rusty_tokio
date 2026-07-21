//! Non-blocking networking: `epoll` on Linux, `kevent` on macOS -- see
//! `reactor/mod.rs` for the shared/per-backend split, and
//! `reactor/kqueue.rs`'s docs for the caveat that this crate's own
//! integration with the macOS backend is compile-checked (`cargo check
//! --target x86_64-apple-darwin`) but has never been run on real
//! hardware.
//!
//! Socket bind/connect/accept/addressing is built on `rustils`' concrete
//! `platform_linux::{LinuxTcpListener, LinuxTcpStream, LinuxUdpSocket}`
//! on Linux and `platform_macos::{MacosTcpListener, MacosTcpStream,
//! MacosUdpSocket}` on macOS (see `socket/mod.rs`'s docs for the small
//! remainder that's still hand-rolled on both), shaped identically
//! enough between the two backends that `tcp.rs`/`udp.rs` need only a
//! `#[cfg]`-gated type alias, not their own OS branching.

mod async_io;
pub(crate) mod reactor;
mod socket;
mod tcp;
mod udp;

pub use async_io::{copy, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
pub use tcp::{TcpListener, TcpStream};
pub use udp::UdpSocket;
