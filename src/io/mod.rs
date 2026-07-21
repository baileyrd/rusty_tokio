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
//! branching. [`UnixDatagram`] is the one exception -- rustils has no
//! `AF_UNIX` datagram support at all, so `unix_datagram.rs` wraps
//! `std::os::unix::net::UnixDatagram` directly instead; see that
//! module's own docs for why.

mod async_io;
mod buffered;
#[cfg(feature = "futures-io-compat")]
mod compat;
mod duplex;
pub(crate) mod reactor;
mod socket;
mod stdio;
mod tcp;
mod udp;
mod unix;
mod unix_datagram;

pub use async_io::{
    copy, copy_bidirectional, AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncSeek,
    AsyncSeekExt, AsyncWrite, AsyncWriteExt, ReadBuf,
};
pub use buffered::{BufReader, BufWriter, Lines};
#[cfg(feature = "futures-io-compat")]
pub use compat::Compat;
pub use duplex::{duplex, DuplexStream};
pub use stdio::{stderr, stdin, stdout, Stderr, Stdin, Stdout};
pub use tcp::{
    OwnedReadHalf, OwnedWriteHalf, ReadHalf, TcpListener, TcpSocket, TcpStream, WriteHalf,
};
pub use udp::UdpSocket;
pub use unix::{
    OwnedUnixReadHalf, OwnedUnixWriteHalf, UnixListener, UnixReadHalf, UnixStream, UnixWriteHalf,
};
pub use unix_datagram::UnixDatagram;
