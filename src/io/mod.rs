//! Non-blocking networking: `epoll` on Linux, `kevent` on macOS, IOCP +
//! the AFD-poll trick on Windows -- see `reactor/mod.rs` for the
//! shared/per-backend split, `reactor/kqueue.rs`'s docs for the caveat
//! that this crate's own integration with the macOS backend is
//! compile-checked (`cargo check --target x86_64-apple-darwin`) but has
//! never been run on real hardware, and `reactor/windows.rs`'s docs for
//! the identical caveat on Windows (`cargo check --target
//! x86_64-pc-windows-gnu`). A fourth backend, `reactor/io_uring.rs`,
//! swaps `epoll` for `IORING_OP_POLL_ADD` on Linux behind the
//! `io-uring-reactor` feature (off by default) -- see that module's docs
//! for scope and why.
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
//!
//! Windows has no `AF_UNIX`-backed rustils crate at all (nor, for that
//! matter, a `platform-windows` net module at parity with this crate's
//! needs -- see `socket/windows.rs`'s docs), so `unix.rs`/
//! `unix_datagram.rs` are POSIX-only, `#[cfg(unix)]`-gated below; `tcp.rs`/
//! `udp.rs` instead gain a third, hand-rolled `socket::windows` arm.

#[cfg(unix)]
mod async_fd;
mod async_io;
mod buffered;
#[cfg(feature = "futures-io-compat")]
mod compat;
mod duplex;
mod interest;
mod join;
mod lookup;
pub(crate) mod reactor;
mod readiness;
mod simplex;
pub(crate) mod socket;
mod split;
mod stdio;
mod tcp;
mod udp;
#[cfg(unix)]
mod unix;
#[cfg(unix)]
mod unix_datagram;
mod util;

#[cfg(unix)]
pub use async_fd::{AsyncFd, AsyncFdReadyGuard, TryIoError};
pub use async_io::{
    copy, copy_bidirectional, AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncSeek,
    AsyncSeekExt, AsyncWrite, AsyncWriteExt, Chain, ReadBuf, Take,
};
pub use buffered::{BufReader, BufStream, BufWriter, Lines};
#[cfg(feature = "futures-io-compat")]
pub use compat::Compat;
pub use duplex::{duplex, DuplexStream};
pub use interest::{Interest, Ready};
pub use join::{join, Join};
pub use lookup::{lookup_host, LookupHost};
pub use simplex::{simplex, SimplexStream};
pub use split::{split, SplitReadHalf, SplitWriteHalf};
pub use stdio::{stderr, stdin, stdout, Stderr, Stdin, Stdout};
pub use tcp::{
    OwnedReadHalf, OwnedWriteHalf, ReadHalf, TcpListener, TcpSocket, TcpStream, WriteHalf,
};
pub use udp::UdpSocket;
#[cfg(unix)]
pub use unix::{
    OwnedUnixReadHalf, OwnedUnixWriteHalf, UnixListener, UnixReadHalf, UnixStream, UnixWriteHalf,
};
#[cfg(unix)]
pub use unix_datagram::UnixDatagram;
pub use util::{empty, repeat, sink, Empty, Repeat, Sink};
