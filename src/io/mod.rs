//! Non-blocking networking: `epoll` on Linux, `kevent` on macOS -- see
//! `reactor/mod.rs` for the shared/per-backend split, and
//! `reactor/kqueue.rs`'s docs for the caveat that this crate's own
//! integration with the macOS backend is compile-checked (`cargo check
//! --target x86_64-apple-darwin`) but has never been run on real
//! hardware. A fourth backend, `reactor/io_uring.rs`, swaps `epoll` for
//! `IORING_OP_POLL_ADD` on Linux behind the `io-uring-reactor` feature
//! (off by default) -- see that module's docs for scope and why.
//!
//! Socket bind/connect/accept/addressing is built on `rustils`' concrete
//! `platform_linux::{LinuxTcpListener, LinuxTcpStream, LinuxUdpSocket,
//! LinuxUnixListener, LinuxUnixStream}` on Linux and
//! `platform_macos::{MacosTcpListener, MacosTcpStream, MacosUdpSocket,
//! MacosUnixListener, MacosUnixStream}` on macOS (see `socket/mod.rs`'s
//! docs for the small remainder that's still hand-rolled on both), shaped
//! identically enough between the two backends that `tcp.rs`/`udp.rs`/
//! `unix.rs` each need only a `#[cfg]`-gated type alias, not their own OS
//! branching.

mod async_io;
#[cfg(feature = "futures-io-compat")]
mod compat;
pub(crate) mod reactor;
mod socket;
mod tcp;
mod udp;
mod unix;

pub use async_io::{copy, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
#[cfg(feature = "futures-io-compat")]
pub use compat::Compat;
pub use tcp::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, TcpListener, TcpStream, WriteHalf};
pub use udp::UdpSocket;
pub use unix::{UnixListener, UnixStream};
