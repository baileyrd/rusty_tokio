use super::reactor::{
    poll_io, AsRawIo, Interest as ReactorInterest, OwnedIo, Reactor, ScheduledIo, TryCloneIo,
};
use super::socket::{self, from_platform_err};
use super::{readiness, Interest, Ready};
use crate::runtime::Handle;
use std::io;
use std::net::SocketAddr;
#[cfg(unix)]
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use std::task::{Context, Poll};

// See `tcp.rs`'s equivalent comment: rustils' concrete type either way
// (`platform_linux` on Linux, `platform_macos` on macOS), identical
// logic below regardless of which; on Windows, `socket::windows`'s own
// hand-rolled type.
#[cfg(any(target_os = "linux", target_os = "macos"))]
use platform::net::UdpSocket as _;

#[cfg(target_os = "linux")]
use platform_linux::LinuxUdpSocket as PlatformUdpSocket;

#[cfg(target_os = "macos")]
use platform_macos::MacosUdpSocket as PlatformUdpSocket;

#[cfg(target_os = "windows")]
use socket::windows::WindowsUdpSocket as PlatformUdpSocket;

/// The largest possible UDP payload over IPv4 -- a 65535-byte IPv4
/// packet minus the fixed 20-byte IP header and 8-byte UDP header.
/// [`UdpSocket::recv_buf`]/[`recv_buf_from`](UdpSocket::recv_buf_from)
/// cap their temporary read buffer here, so a caller-provided
/// [`bytes::BufMut`] with unbounded `remaining_mut()` (e.g.
/// `bytes::BytesMut`) doesn't trigger an attempt to allocate that much.
pub const MAX_UDP_DATAGRAM_SIZE: usize = 65_507;

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
        let io = reactor.register(inner.as_raw_io())?;
        Ok(UdpSocket { inner, io, reactor })
    }

    pub async fn send_to(&self, buf: &[u8], addr: SocketAddr) -> io::Result<usize> {
        std::future::poll_fn(|cx| self.poll_send_to(cx, buf, addr)).await
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        std::future::poll_fn(|cx| self.poll_recv_from(cx, buf)).await
    }

    /// Like [`recv_from`](Self::recv_from), but into a
    /// [`bytes::BufMut`]'s spare capacity instead of a plain
    /// `&mut [u8]`.
    ///
    /// Sized to `buf`'s remaining capacity capped at
    /// [`MAX_UDP_DATAGRAM_SIZE`] -- not some much smaller fixed chunk
    /// the way [`TcpStream::try_read_buf`](super::TcpStream::try_read_buf)
    /// is, since unlike a stream, a datagram that doesn't fit the
    /// buffer passed to `recvfrom(2)` is truncated and the rest
    /// silently discarded rather than held back for a later read; but
    /// also not the full, uncapped `remaining_mut()` an auto-growing
    /// `BufMut` like `bytes::BytesMut` reports as effectively unbounded
    /// (`isize::MAX`-ish), which would otherwise try to allocate an
    /// impossible temporary buffer. No real datagram exceeds the cap, so
    /// this never truncates one that would otherwise have fit.
    pub async fn recv_buf_from<B: bytes::BufMut>(
        &self,
        buf: &mut B,
    ) -> io::Result<(usize, SocketAddr)> {
        let want = buf.remaining_mut().min(MAX_UDP_DATAGRAM_SIZE);
        let mut chunk = vec![0u8; want];
        let (n, addr) = self.recv_from(&mut chunk).await?;
        buf.put_slice(&chunk[..n]);
        Ok((n, addr))
    }

    /// Non-`async fn` form of [`send_to`](Self::send_to).
    pub fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        addr: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        poll_io(&self.io, ReactorInterest::Write, cx, || {
            self.inner.send_to(buf, addr).map_err(from_platform_err)
        })
    }

    /// Non-`async fn` form of [`recv_from`](Self::recv_from).
    pub fn poll_recv_from(
        &self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<(usize, SocketAddr)>> {
        poll_io(&self.io, ReactorInterest::Read, cx, || {
            self.inner.recv_from(buf).map_err(from_platform_err)
        })
    }

    /// Like [`recv_from`](Self::recv_from), but the returned datagram
    /// stays in the socket's receive queue -- a later `recv`/`recv_from`/
    /// `peek_from` call sees it again from the start. Bypasses rustils'
    /// own `recv_from` entirely (it has no peek variant), going straight
    /// to `recvfrom(2)`/`recvfrom` with `MSG_PEEK` on the raw socket --
    /// see `socket::peek_from`'s own docs.
    pub async fn peek_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        std::future::poll_fn(|cx| self.poll_peek_from(cx, buf)).await
    }

    /// Non-`async fn` form of [`peek_from`](Self::peek_from).
    pub fn poll_peek_from(
        &self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<(usize, SocketAddr)>> {
        poll_io(&self.io, ReactorInterest::Read, cx, || {
            socket::peek_from(self.inner.as_raw_io(), buf)
        })
    }

    /// Peeks without waiting, failing immediately (with `WouldBlock`)
    /// if no datagram is available yet.
    pub fn try_peek_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.try_io(Interest::READABLE, || {
            socket::peek_from(self.inner.as_raw_io(), buf)
        })
    }

    /// Like [`peek_from`](Self::peek_from), but only the next
    /// datagram's sender matters -- doesn't dequeue it (or need a
    /// buffer to receive its data into at all).
    pub async fn peek_sender(&self) -> io::Result<SocketAddr> {
        std::future::poll_fn(|cx| self.poll_peek_sender(cx)).await
    }

    /// Non-`async fn` form of [`peek_sender`](Self::peek_sender).
    pub fn poll_peek_sender(&self, cx: &mut Context<'_>) -> Poll<io::Result<SocketAddr>> {
        poll_io(&self.io, ReactorInterest::Read, cx, || {
            socket::peek_sender(self.inner.as_raw_io())
        })
    }

    /// Peeks the next datagram's sender without waiting, failing
    /// immediately (with `WouldBlock`) if none is available yet.
    pub fn try_peek_sender(&self) -> io::Result<SocketAddr> {
        self.try_io(Interest::READABLE, || {
            socket::peek_sender(self.inner.as_raw_io())
        })
    }

    /// Fixes `addr` as this socket's peer, so [`send`](Self::send)/
    /// [`recv`](Self::recv) can omit it on every call afterward. Unlike
    /// TCP's `connect`, this is a local, synchronous operation -- UDP's
    /// `connect(2)` doesn't perform a network handshake, it just records
    /// a default peer address (and, for a socket that was never bound,
    /// picks one) -- so there's nothing to `.await` here, and this
    /// crate's usual hand-rolled non-blocking-connect dance (`connect`
    /// in `socket/mod.rs`, needed because *TCP*'s `connect(2)` can
    /// return `EINPROGRESS`) is reused directly: for UDP, its
    /// `EINPROGRESS` branch simply never triggers.
    pub fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        socket::connect(self.inner.as_raw_io(), addr)
    }

    /// Sends to whichever peer [`connect`](Self::connect) fixed.
    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        std::future::poll_fn(|cx| self.poll_send(cx, buf)).await
    }

    /// Receives from whichever peer [`connect`](Self::connect) fixed --
    /// datagrams from anyone else are not delivered to a connected UDP
    /// socket.
    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        std::future::poll_fn(|cx| self.poll_recv(cx, buf)).await
    }

    /// Like [`recv`](Self::recv), but into a [`bytes::BufMut`]'s spare
    /// capacity -- see [`recv_buf_from`](Self::recv_buf_from) for why
    /// this is sized to `buf`'s remaining capacity capped at
    /// [`MAX_UDP_DATAGRAM_SIZE`] rather than some smaller fixed chunk
    /// (or `buf`'s own, possibly-unbounded `remaining_mut()` directly).
    pub async fn recv_buf<B: bytes::BufMut>(&self, buf: &mut B) -> io::Result<usize> {
        let want = buf.remaining_mut().min(MAX_UDP_DATAGRAM_SIZE);
        let mut chunk = vec![0u8; want];
        let n = self.recv(&mut chunk).await?;
        buf.put_slice(&chunk[..n]);
        Ok(n)
    }

    /// Non-`async fn` form of [`send`](Self::send).
    pub fn poll_send(&self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        poll_io(&self.io, ReactorInterest::Write, cx, || {
            socket::write(self.inner.as_raw_io(), buf)
        })
    }

    /// Non-`async fn` form of [`recv`](Self::recv).
    pub fn poll_recv(&self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        poll_io(&self.io, ReactorInterest::Read, cx, || {
            socket::read(self.inner.as_raw_io(), buf)
        })
    }

    /// Waits for this socket to become readable -- see
    /// [`ready`](Self::ready) for using this together with your own
    /// non-blocking I/O via [`try_io`](Self::try_io).
    pub async fn readable(&self) -> io::Result<()> {
        self.ready(Interest::READABLE).await.map(|_| ())
    }

    pub async fn writable(&self) -> io::Result<()> {
        self.ready(Interest::WRITABLE).await.map(|_| ())
    }

    /// Resolves once *any* of `interest`'s requested directions is
    /// ready, reporting exactly which one(s) actually are.
    pub async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        std::future::poll_fn(|cx| self.poll_ready(interest, cx)).await
    }

    /// Non-`async fn` form of [`ready`](Self::ready).
    pub fn poll_ready(&self, interest: Interest, cx: &mut Context<'_>) -> Poll<io::Result<Ready>> {
        readiness::poll_ready(&self.io, interest, cx)
    }

    /// Non-`async fn` form of [`readable`](Self::readable) -- also the
    /// method [`recv`](Self::recv)/[`recv_from`](Self::recv_from) wait
    /// on internally, exposed directly for a caller doing its own
    /// non-blocking `recv`/`recv_from` via [`try_io`](Self::try_io).
    pub fn poll_recv_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        super::reactor::poll_ready(&self.io, ReactorInterest::Read, cx).map(Ok)
    }

    /// Non-`async fn` form of [`writable`](Self::writable) -- the send
    /// side's equivalent of [`poll_recv_ready`](Self::poll_recv_ready).
    pub fn poll_send_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        super::reactor::poll_ready(&self.io, ReactorInterest::Write, cx).map(Ok)
    }

    /// Runs `f` (the caller's own non-blocking send/recv against this
    /// socket's fd) once `interest` is ready, clearing that cached
    /// readiness if `f` reports `WouldBlock` -- see
    /// [`TcpStream::try_io`](super::TcpStream::try_io) for the same
    /// pattern, identical reasoning here.
    pub fn try_io<R>(
        &self,
        interest: Interest,
        f: impl FnOnce() -> io::Result<R>,
    ) -> io::Result<R> {
        readiness::try_io(&self.io, interest, f)
    }

    /// Sends to `addr` without waiting, failing immediately (with
    /// `WouldBlock`) if the socket isn't ready to accept more right
    /// now.
    pub fn try_send_to(&self, buf: &[u8], addr: SocketAddr) -> io::Result<usize> {
        self.try_io(Interest::WRITABLE, || {
            self.inner.send_to(buf, addr).map_err(from_platform_err)
        })
    }

    /// Receives without waiting, failing immediately (with
    /// `WouldBlock`) if no datagram is available yet.
    pub fn try_recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.try_io(Interest::READABLE, || {
            self.inner.recv_from(buf).map_err(from_platform_err)
        })
    }

    /// Sends to whichever peer [`connect`](Self::connect) fixed,
    /// without waiting -- see [`try_send_to`](Self::try_send_to) for
    /// the failure shape.
    pub fn try_send(&self, buf: &[u8]) -> io::Result<usize> {
        self.try_io(Interest::WRITABLE, || {
            socket::write(self.inner.as_raw_io(), buf)
        })
    }

    /// Receives from whichever peer [`connect`](Self::connect) fixed,
    /// without waiting -- see [`try_recv_from`](Self::try_recv_from)
    /// for the failure shape.
    pub fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.try_io(Interest::READABLE, || {
            socket::read(self.inner.as_raw_io(), buf)
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr().map_err(from_platform_err)
    }

    /// `SO_BINDTODEVICE` -- see [`crate::io::TcpSocket::bind_device`] for
    /// the full explanation; identical option, UDP side.
    #[cfg(target_os = "linux")]
    pub fn bind_device(&self, interface: Option<&[u8]>) -> io::Result<()> {
        socket::set_bind_device(self.inner.as_raw_io(), interface)
    }

    /// The reverse of [`bind_device`](Self::bind_device).
    #[cfg(target_os = "linux")]
    pub fn device(&self) -> io::Result<Option<Vec<u8>>> {
        socket::bind_device(self.inner.as_raw_io())
    }

    /// Runs `f` against a temporary, non-owning `std::net::UdpSocket`
    /// wrapping this socket's raw fd -- `std::net::UdpSocket` already has
    /// safe wrappers for every multicast/broadcast `setsockopt` this
    /// crate wants (`join_multicast_v4`, `set_broadcast`, etc.), so
    /// there's no need to hand-roll the raw `IP_ADD_MEMBERSHIP`/
    /// `SO_BROADCAST`/etc. plumbing a second time the way
    /// [`bind_device`](Self::bind_device) has to for an option `std`
    /// doesn't cover. `mem::forget`ing the temporary afterward is what
    /// makes this non-owning: without it, the temporary's own `Drop`
    /// would close the fd out from under `self.inner`, which still
    /// thinks it owns it.
    #[cfg(unix)]
    fn with_std<R>(&self, f: impl FnOnce(&std::net::UdpSocket) -> R) -> R {
        use std::os::fd::FromRawFd;
        // SAFETY: `as_raw_io()` is a valid, currently-open fd owned by
        // `self.inner`; `mem::forget` below stops this temporary
        // `std::net::UdpSocket` from double-closing it on drop.
        let borrowed = unsafe { std::net::UdpSocket::from_raw_fd(self.inner.as_raw_io()) };
        let result = f(&borrowed);
        std::mem::forget(borrowed);
        result
    }

    /// Joins an IPv4 multicast group -- see
    /// `std::net::UdpSocket::join_multicast_v4`.
    #[cfg(unix)]
    pub fn join_multicast_v4(&self, multiaddr: Ipv4Addr, interface: Ipv4Addr) -> io::Result<()> {
        self.with_std(|s| s.join_multicast_v4(&multiaddr, &interface))
    }

    /// Leaves an IPv4 multicast group previously joined with
    /// [`join_multicast_v4`](Self::join_multicast_v4) -- see
    /// `std::net::UdpSocket::leave_multicast_v4`.
    #[cfg(unix)]
    pub fn leave_multicast_v4(&self, multiaddr: Ipv4Addr, interface: Ipv4Addr) -> io::Result<()> {
        self.with_std(|s| s.leave_multicast_v4(&multiaddr, &interface))
    }

    /// Joins an IPv6 multicast group -- see
    /// `std::net::UdpSocket::join_multicast_v6`.
    #[cfg(unix)]
    pub fn join_multicast_v6(&self, multiaddr: &Ipv6Addr, interface: u32) -> io::Result<()> {
        self.with_std(|s| s.join_multicast_v6(multiaddr, interface))
    }

    /// Leaves an IPv6 multicast group previously joined with
    /// [`join_multicast_v6`](Self::join_multicast_v6) -- see
    /// `std::net::UdpSocket::leave_multicast_v6`.
    #[cfg(unix)]
    pub fn leave_multicast_v6(&self, multiaddr: &Ipv6Addr, interface: u32) -> io::Result<()> {
        self.with_std(|s| s.leave_multicast_v6(multiaddr, interface))
    }

    /// Whether outgoing IPv4 multicast packets sent from this socket are
    /// looped back to the local host's own listeners -- see
    /// `std::net::UdpSocket::set_multicast_loop_v4`.
    #[cfg(unix)]
    pub fn set_multicast_loop_v4(&self, on: bool) -> io::Result<()> {
        self.with_std(|s| s.set_multicast_loop_v4(on))
    }

    /// The reverse of [`set_multicast_loop_v4`](Self::set_multicast_loop_v4).
    #[cfg(unix)]
    pub fn multicast_loop_v4(&self) -> io::Result<bool> {
        self.with_std(|s| s.multicast_loop_v4())
    }

    /// The IPv6 equivalent of
    /// [`set_multicast_loop_v4`](Self::set_multicast_loop_v4).
    #[cfg(unix)]
    pub fn set_multicast_loop_v6(&self, on: bool) -> io::Result<()> {
        self.with_std(|s| s.set_multicast_loop_v6(on))
    }

    /// The reverse of [`set_multicast_loop_v6`](Self::set_multicast_loop_v6).
    #[cfg(unix)]
    pub fn multicast_loop_v6(&self) -> io::Result<bool> {
        self.with_std(|s| s.multicast_loop_v6())
    }

    /// The TTL outgoing IPv4 multicast packets are sent with, distinct
    /// from unicast traffic's own TTL -- see
    /// `std::net::UdpSocket::set_multicast_ttl_v4`.
    #[cfg(unix)]
    pub fn set_multicast_ttl_v4(&self, ttl: u32) -> io::Result<()> {
        self.with_std(|s| s.set_multicast_ttl_v4(ttl))
    }

    /// The reverse of [`set_multicast_ttl_v4`](Self::set_multicast_ttl_v4).
    #[cfg(unix)]
    pub fn multicast_ttl_v4(&self) -> io::Result<u32> {
        self.with_std(|s| s.multicast_ttl_v4())
    }

    /// Adopts an already-bound `std` socket -- e.g. one received from a
    /// supervisor process, or configured with `socket2` for an option
    /// this crate doesn't expose a wrapper for. Flips it non-blocking
    /// and registers it with the reactor without redoing the bind
    /// syscall.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn from_std(socket: std::net::UdpSocket) -> io::Result<UdpSocket> {
        let reactor = Handle::current().shared.reactor.clone();
        let inner = PlatformUdpSocket::from(OwnedIo::from(socket));
        inner.set_nonblocking(true).map_err(from_platform_err)?;
        let io = reactor.register(inner.as_raw_io())?;
        Ok(UdpSocket { inner, io, reactor })
    }

    /// The reverse of [`from_std`](Self::from_std) -- see
    /// [`crate::io::TcpListener::into_std`] for the
    /// flip-to-blocking/`dup(2)` reasoning, identical here.
    pub fn into_std(self) -> io::Result<std::net::UdpSocket> {
        self.inner
            .set_nonblocking(false)
            .map_err(from_platform_err)?;
        let owned = self.inner.try_clone_io()?;
        Ok(std::net::UdpSocket::from(owned))
    }
}

impl Drop for UdpSocket {
    fn drop(&mut self) {
        self.reactor.deregister(self.inner.as_raw_io());
    }
}

// See `TcpListener`'s equivalent impls (`io/tcp.rs`) for why these are
// plain delegation and why `FromRawFd`/`IntoRawFd` reuse `from_std`/
// `into_std`.
#[cfg(unix)]
impl std::os::fd::AsFd for UdpSocket {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

#[cfg(unix)]
impl std::os::fd::AsRawFd for UdpSocket {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.inner.as_raw_fd()
    }
}

#[cfg(unix)]
impl std::os::fd::FromRawFd for UdpSocket {
    unsafe fn from_raw_fd(fd: std::os::fd::RawFd) -> Self {
        let std_socket = unsafe { std::net::UdpSocket::from_raw_fd(fd) };
        UdpSocket::from_std(std_socket).expect("failed to register raw fd with the reactor")
    }
}

#[cfg(unix)]
impl std::os::fd::IntoRawFd for UdpSocket {
    fn into_raw_fd(self) -> std::os::fd::RawFd {
        self.into_std()
            .expect("failed to convert back to a std socket")
            .into_raw_fd()
    }
}

#[cfg(windows)]
impl std::os::windows::io::AsSocket for UdpSocket {
    fn as_socket(&self) -> std::os::windows::io::BorrowedSocket<'_> {
        self.inner.as_socket()
    }
}

#[cfg(windows)]
impl std::os::windows::io::AsRawSocket for UdpSocket {
    fn as_raw_socket(&self) -> std::os::windows::io::RawSocket {
        self.inner.as_raw_socket()
    }
}

#[cfg(windows)]
impl std::os::windows::io::FromRawSocket for UdpSocket {
    unsafe fn from_raw_socket(socket: std::os::windows::io::RawSocket) -> Self {
        let std_socket = unsafe { std::net::UdpSocket::from_raw_socket(socket) };
        UdpSocket::from_std(std_socket).expect("failed to register raw socket with the reactor")
    }
}

#[cfg(windows)]
impl std::os::windows::io::IntoRawSocket for UdpSocket {
    fn into_raw_socket(self) -> std::os::windows::io::RawSocket {
        self.into_std()
            .expect("failed to convert back to a std socket")
            .into_raw_socket()
    }
}
