use super::async_io::{AsyncRead, AsyncWrite, ReadBuf};
use super::reactor::{
    poll_io, ready_io, Interest as ReactorInterest, Reactor, ScheduledIo, TryCloneIo,
};
use super::socket::{self, from_platform_err};
use super::{readiness, Interest, Ready};
use crate::runtime::Handle;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

// See `tcp.rs`'s equivalent comment: rustils' concrete type either way
// (`platform_linux` on Linux, `platform_macos` on macOS), identical logic
// below regardless of which -- both shaped identically to their TCP
// counterparts, minus `set_nodelay` (no Nagle buffering on `AF_UNIX`) and
// minus `local_addr`/`peer_addr`, which bypass rustils entirely (see
// `UnixSocketAddr`'s own docs for why) rather than using its
// `Option<PathBuf>`-shaped equivalents -- unlike `tcp.rs`, this file
// never actually calls a `platform::net::UnixListener`/`UnixStream`
// trait method by name, only inherent methods on the concrete
// `PlatformUnixListener`/`PlatformUnixStream` types below, so there's no
// blanket `as _` trait import to bring into scope here.
#[cfg(target_os = "linux")]
use platform_linux::{
    LinuxUnixListener as PlatformUnixListener, LinuxUnixStream as PlatformUnixStream,
};

#[cfg(target_os = "macos")]
use platform_macos::{
    MacosUnixListener as PlatformUnixListener, MacosUnixStream as PlatformUnixStream,
};

/// An `AF_UNIX` address: a filesystem pathname, a Linux/Android
/// abstract-namespace name (a kernel-assigned identifier with no
/// filesystem presence at all, unrelated to `/proc`'s notion of
/// "abstract" -- see [`as_abstract_name`](Self::as_abstract_name)), or
/// unnamed (an unbound socket, or the client end of a `connect`-only
/// pair that never itself called `bind`). A plain `Option<PathBuf>`
/// (this crate's original shape for [`UnixListener::local_addr`]/
/// [`UnixStream::local_addr`]/[`UnixStream::peer_addr`]) can't represent
/// the abstract-namespace case at all -- an abstract name is an
/// arbitrary byte string, not a real path -- so this wraps
/// `std::os::unix::net::SocketAddr` instead, mirroring tokio's own
/// `net::unix::UnixSocketAddr` exactly (itself the same wrapper).
#[derive(Clone)]
pub struct UnixSocketAddr(std::os::unix::net::SocketAddr);

impl UnixSocketAddr {
    /// An address for [`UnixListener::bind_addr`]/[`UnixStream::connect_addr`]
    /// naming a real filesystem path -- see `std::os::unix::net::SocketAddr::from_pathname`.
    pub fn from_pathname(path: impl AsRef<Path>) -> io::Result<UnixSocketAddr> {
        std::os::unix::net::SocketAddr::from_pathname(path).map(UnixSocketAddr)
    }

    /// An address naming a Linux/Android abstract-namespace identifier
    /// instead of a real filesystem path -- `name`'s raw bytes (which
    /// may contain anything, including embedded NULs, unlike a
    /// pathname) are matched exactly by a peer naming the same bytes;
    /// nothing is created on the filesystem, and the name stops
    /// existing once every socket bound to it closes. See
    /// `std::os::unix::net::SocketAddr::from_abstract_name`. Linux/
    /// Android-only: no other platform has this concept.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn from_abstract_name(name: impl AsRef<[u8]>) -> io::Result<UnixSocketAddr> {
        use std::os::linux::net::SocketAddrExt;
        std::os::unix::net::SocketAddr::from_abstract_name(name).map(UnixSocketAddr)
    }

    /// This address's filesystem path, if it's a pathname address --
    /// `None` for an abstract-namespace or unnamed address.
    pub fn as_pathname(&self) -> Option<&Path> {
        self.0.as_pathname()
    }

    /// This address's raw abstract-namespace name, if it's one -- see
    /// [`from_abstract_name`](Self::from_abstract_name). `None` for a
    /// pathname or unnamed address. Linux/Android-only.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn as_abstract_name(&self) -> Option<&[u8]> {
        use std::os::linux::net::SocketAddrExt;
        self.0.as_abstract_name()
    }

    /// Whether this is the unnamed address -- an unbound socket, or a
    /// stream socket's end that only ever `connect`-ed, never `bind`-ed
    /// its own address.
    pub fn is_unnamed(&self) -> bool {
        self.0.is_unnamed()
    }
}

impl std::fmt::Debug for UnixSocketAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

/// Borrows `fd` just long enough to ask `std`'s own `getsockname`/
/// `getpeername` wrapper what address it's bound to/connected to --
/// `std::os::unix::net::SocketAddr` already correctly distinguishes
/// pathname/abstract-namespace/unnamed on read-back, so there's no need
/// to hand-roll `sockaddr_un` parsing a second time here (only *packing*
/// one, for `bind_addr`/`connect_addr`, needs that -- see
/// `socket::unix_bind_addr`/`unix_connect_addr`). `mem::forget`ing the
/// temporary afterward keeps this non-owning -- see
/// `UdpSocket::with_std`'s identical reasoning (`std::os::unix::net::
/// UnixStream` rather than `UnixListener`/`UnixDatagram` purely because
/// it alone has both `local_addr` and `peer_addr`; the underlying
/// `getsockname`/`getpeername` calls don't care which of the three a
/// bare fd is treated as).
fn with_borrowed_std_stream<R>(
    fd: std::os::fd::RawFd,
    f: impl FnOnce(&std::os::unix::net::UnixStream) -> R,
) -> R {
    // SAFETY: `fd` is a valid, currently-open fd owned by the caller for
    // the duration of this call; `mem::forget` below stops this
    // temporary from double-closing it.
    let borrowed = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd) };
    let result = f(&borrowed);
    std::mem::forget(borrowed);
    result
}

/// A non-blocking, epoll-driven Unix domain socket listener.
pub struct UnixListener {
    inner: PlatformUnixListener,
    io: Arc<ScheduledIo>,
    reactor: Arc<Reactor>,
}

impl UnixListener {
    /// Binds and starts listening at `path`, narrowed to owner-only
    /// (mode `0600`) where the OS has that concept. A stale leftover
    /// socket file (left behind by a listener that died without
    /// unlinking it) is reclaimed automatically -- rustils' own
    /// `unix_listen` distinguishes "stale" from "still live" via a
    /// throwaway probe connect; a path a live listener still holds fails
    /// with `AddrInUse` instead, same as `TcpListener::bind` on a port
    /// already in use.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn bind(path: &Path) -> io::Result<UnixListener> {
        let reactor = Handle::current().shared.reactor.clone();
        let inner = PlatformUnixListener::bind(path).map_err(from_platform_err)?;
        inner.set_nonblocking(true).map_err(from_platform_err)?;
        let io = reactor.register(inner.as_raw_fd())?;
        Ok(UnixListener { inner, io, reactor })
    }

    /// Binds and starts listening at `addr` -- the [`UnixSocketAddr`]-based
    /// counterpart of [`bind`](Self::bind), the only way to bind at a
    /// Linux/Android abstract-namespace address rather than a real
    /// filesystem path (see [`UnixSocketAddr::from_abstract_name`]). Unlike
    /// `bind`, doesn't reclaim a stale leftover socket file -- there's no
    /// path-based rustils helper to reuse for that, and an
    /// abstract-namespace address has no filesystem presence to leave a
    /// stale file behind in the first place.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn bind_addr(addr: &UnixSocketAddr) -> io::Result<UnixListener> {
        let fd = socket::new_unix_socket()?;
        socket::unix_bind_addr(fd.as_raw_fd(), &addr.0)?;
        // 1024 matches the fixed backlog tokio's own convenience `bind`
        // uses internally, in the absence of a caller-supplied one here
        // (the same reason `UnixListener::bind` above needs none either
        // -- only `UnixSocket::listen`'s own explicit `backlog` parameter
        // exposes this as a choice at all).
        socket::listen(fd.as_raw_fd(), 1024)?;
        let reactor = Handle::current().shared.reactor.clone();
        let inner = PlatformUnixListener::from(fd);
        inner.set_nonblocking(true).map_err(from_platform_err)?;
        let io = reactor.register(inner.as_raw_fd())?;
        Ok(UnixListener { inner, io, reactor })
    }

    pub async fn accept(&self) -> io::Result<(UnixStream, Option<PathBuf>)> {
        std::future::poll_fn(|cx| self.poll_accept(cx)).await
    }

    /// Non-`async fn` form of [`accept`](Self::accept), for a caller
    /// implementing its own `Future`/poll loop.
    pub fn poll_accept(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<(UnixStream, Option<PathBuf>)>> {
        let accepted = match poll_io(&self.io, ReactorInterest::Read, cx, || {
            self.inner.accept().map_err(from_platform_err)
        }) {
            Poll::Ready(result) => result,
            Poll::Pending => return Poll::Pending,
        };
        Poll::Ready(accepted.and_then(|(stream, peer)| {
            stream.set_nonblocking(true).map_err(from_platform_err)?;
            let stream = UnixStream::from_accepted(stream, self.reactor.clone())?;
            Ok((stream, peer))
        }))
    }

    pub fn local_addr(&self) -> io::Result<UnixSocketAddr> {
        with_borrowed_std_stream(self.inner.as_raw_fd(), |s| s.local_addr()).map(UnixSocketAddr)
    }

    /// `SO_ERROR` -- see [`TcpStream::take_error`](super::TcpStream::take_error)
    /// for the full contract, identical here.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        socket::take_error(self.inner.as_raw_fd())
    }
}

impl Drop for UnixListener {
    fn drop(&mut self) {
        self.reactor.deregister(self.inner.as_raw_fd());
    }
}

// Unlike `TcpListener`/`UdpSocket` (`io/tcp.rs`/`io/udp.rs`), there's no
// existing `from_std`/`into_std` to build these on here -- built
// directly on `PlatformUnixListener`'s own `AsFd`/`AsRawFd`/
// `From<OwnedFd>` instead, the same primitives `bind` and `Drop` above
// already use. `IntoRawFd` dup(2)s (`try_clone_io`) rather than
// transferring the exact same fd, for the same reason `TcpListener::
// into_std` does -- see that method's own docs.
impl std::os::fd::AsFd for UnixListener {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

impl std::os::fd::AsRawFd for UnixListener {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.inner.as_raw_fd()
    }
}

impl std::os::fd::FromRawFd for UnixListener {
    unsafe fn from_raw_fd(fd: std::os::fd::RawFd) -> Self {
        let owned = unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) };
        let inner = PlatformUnixListener::from(owned);
        inner
            .set_nonblocking(true)
            .expect("failed to set the adopted fd non-blocking");
        let reactor = Handle::current().shared.reactor.clone();
        let io = reactor
            .register(inner.as_raw_fd())
            .expect("failed to register raw fd with the reactor");
        UnixListener { inner, io, reactor }
    }
}

impl std::os::fd::IntoRawFd for UnixListener {
    fn into_raw_fd(self) -> std::os::fd::RawFd {
        self.inner
            .try_clone_io()
            .expect("failed to duplicate fd")
            .into_raw_fd()
    }
}

/// A bare Unix domain socket, before it's been decided whether to
/// `bind` + [`listen`](Self::listen) (becoming a [`UnixListener`]),
/// [`connect`](Self::connect) (becoming a [`UnixStream`]), or --
/// only for one created via [`new_datagram`](Self::new_datagram) --
/// [`datagram`](Self::datagram) (becoming a [`super::UnixDatagram`]).
/// Mirrors tokio's own `net::UnixSocket`, the `AF_UNIX` counterpart of
/// [`super::TcpSocket`], which already has this "bare socket before
/// commit" shape.
///
/// Unlike `TcpSocket`, a single underlying `socket(2)` call can't be
/// re-purposed between stream and datagram after the fact -- `listen`/
/// `connect`/`datagram` each check `SO_TYPE` up front and reject the
/// wrong kind with an error, rather than tracking which constructor was
/// used as a separate field (which wouldn't survive a socket adopted
/// via [`FromRawFd`](std::os::fd::FromRawFd) anyway).
pub struct UnixSocket {
    fd: std::os::fd::OwnedFd,
}

impl UnixSocket {
    /// A bare, non-blocking `SOCK_STREAM` socket -- see
    /// [`listen`](Self::listen)/[`connect`](Self::connect).
    pub fn new_stream() -> io::Result<UnixSocket> {
        Ok(UnixSocket {
            fd: socket::new_unix_socket()?,
        })
    }

    /// A bare, non-blocking `SOCK_DGRAM` socket -- see
    /// [`datagram`](Self::datagram).
    pub fn new_datagram() -> io::Result<UnixSocket> {
        Ok(UnixSocket {
            fd: socket::new_unix_datagram_socket()?,
        })
    }

    /// Binds to `path`. Doesn't start listening (nor otherwise become
    /// usable) yet -- see [`listen`](Self::listen)/
    /// [`connect`](Self::connect)/[`datagram`](Self::datagram), matching
    /// `bind(2)`/`listen(2)` already being separate syscalls at the OS
    /// level (the same reason [`super::TcpSocket::bind`] is its own
    /// step too).
    pub fn bind(&self, path: impl AsRef<Path>) -> io::Result<()> {
        socket::unix_bind(self.fd.as_raw_fd(), path.as_ref())
    }

    /// Starts listening, turning this into an ordinary [`UnixListener`].
    /// `backlog` is the OS's pending-connection queue length hint (see
    /// `listen(2)`).
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    ///
    /// # Errors
    /// Fails if this socket was created via
    /// [`new_datagram`](Self::new_datagram) instead of
    /// [`new_stream`](Self::new_stream).
    pub fn listen(self, backlog: u32) -> io::Result<UnixListener> {
        if self.socket_type()? == libc::SOCK_DGRAM {
            return Err(io::Error::other(
                "listen cannot be called on a datagram socket",
            ));
        }
        socket::listen(self.fd.as_raw_fd(), backlog)?;
        let reactor = Handle::current().shared.reactor.clone();
        let inner = PlatformUnixListener::from(self.fd);
        // Already non-blocking from `socket::new_unix_socket` -- kept
        // for the same belt-and-suspenders reason `TcpSocket::listen`
        // sets it again too.
        inner.set_nonblocking(true).map_err(from_platform_err)?;
        let io = reactor.register(inner.as_raw_fd())?;
        Ok(UnixListener { inner, io, reactor })
    }

    /// Connects, turning this into an ordinary [`UnixStream`].
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    ///
    /// # Errors
    /// Fails if this socket was created via
    /// [`new_datagram`](Self::new_datagram) instead of
    /// [`new_stream`](Self::new_stream).
    pub async fn connect(self, path: impl AsRef<Path>) -> io::Result<UnixStream> {
        if self.socket_type()? == libc::SOCK_DGRAM {
            return Err(io::Error::other(
                "connect cannot be called on a datagram socket",
            ));
        }
        let reactor = Handle::current().shared.reactor.clone();
        socket::unix_connect(self.fd.as_raw_fd(), path.as_ref())?;
        let io = reactor.register(self.fd.as_raw_fd())?;
        let inner = PlatformUnixStream::from(self.fd);
        // Same non-blocking-connect-completes-asynchronously reasoning
        // as `UnixStream::connect`.
        ready_io(&io, ReactorInterest::Write, || {
            socket::take_socket_error(inner.as_raw_fd())
        })
        .await?;
        Ok(UnixStream { inner, io, reactor })
    }

    /// Converts into an ordinary [`super::UnixDatagram`].
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    ///
    /// # Errors
    /// Fails if this socket was created via
    /// [`new_stream`](Self::new_stream) instead of
    /// [`new_datagram`](Self::new_datagram).
    pub fn datagram(self) -> io::Result<super::UnixDatagram> {
        if self.socket_type()? == libc::SOCK_STREAM {
            return Err(io::Error::other(
                "datagram cannot be called on a stream socket",
            ));
        }
        super::UnixDatagram::from_owned_fd(self.fd)
    }

    fn socket_type(&self) -> io::Result<libc::c_int> {
        socket::unix_socket_type(self.fd.as_raw_fd())
    }
}

// Built directly on `std::os::fd::OwnedFd`'s own `AsFd`/`AsRawFd`/
// `FromRawFd`/`IntoRawFd` -- a bare `UnixSocket` is never registered
// with the reactor (`listen`/`connect`/`datagram` each do that only
// once they've committed to a concrete type), so there's nothing to
// deregister on drop either, unlike `UnixListener`/`UnixStream`.
impl std::os::fd::AsFd for UnixSocket {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl std::os::fd::AsRawFd for UnixSocket {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.fd.as_raw_fd()
    }
}

impl std::os::fd::FromRawFd for UnixSocket {
    unsafe fn from_raw_fd(fd: std::os::fd::RawFd) -> Self {
        UnixSocket {
            fd: unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) },
        }
    }
}

impl std::os::fd::IntoRawFd for UnixSocket {
    fn into_raw_fd(self) -> std::os::fd::RawFd {
        self.fd.into_raw_fd()
    }
}

/// A non-blocking, epoll-driven Unix domain stream socket.
///
/// Like [`super::TcpStream`], exposes both a plain `&self`
/// `async fn read`/`write` pair and the [`AsyncRead`]/[`AsyncWrite`]
/// trait pair, both implemented for `&UnixStream` so one task can read
/// while another writes the same stream (e.g. via two `Arc<UnixStream>`
/// clones).
pub struct UnixStream {
    inner: PlatformUnixStream,
    io: Arc<ScheduledIo>,
    reactor: Arc<Reactor>,
}

impl UnixStream {
    /// Splits into borrowed read/write halves -- see
    /// [`super::TcpStream::split`], whose reasoning and implementation
    /// this mirrors exactly (just over `&UnixStream` instead of
    /// `&TcpStream`). Named `UnixReadHalf`/`UnixWriteHalf` rather than
    /// plain `ReadHalf`/`WriteHalf` only because both this module and
    /// `tcp.rs` are flattened into `io`'s own namespace (`pub use
    /// tcp::{ReadHalf, ...}` and `pub use unix::{...}` side by side) --
    /// reusing the exact same names here would collide.
    pub fn split(&mut self) -> (UnixReadHalf<'_>, UnixWriteHalf<'_>) {
        (UnixReadHalf(self), UnixWriteHalf(self))
    }

    /// Splits into owned read/write halves -- see
    /// [`super::TcpStream::into_split`], whose reasoning and
    /// implementation this mirrors exactly.
    pub fn into_split(self) -> (OwnedUnixReadHalf, OwnedUnixWriteHalf) {
        let inner = Arc::new(self);
        (OwnedUnixReadHalf(inner.clone()), OwnedUnixWriteHalf(inner))
    }

    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn connect(path: &Path) -> io::Result<UnixStream> {
        let reactor = Handle::current().shared.reactor.clone();
        let fd = socket::new_unix_socket()?;
        socket::unix_connect(fd.as_raw_fd(), path)?;
        let io = reactor.register(fd.as_raw_fd())?;
        let inner = PlatformUnixStream::from(fd);
        // Same non-blocking-connect-completes-asynchronously reasoning
        // as `TcpStream::connect`.
        ready_io(&io, ReactorInterest::Write, || {
            socket::take_socket_error(inner.as_raw_fd())
        })
        .await?;
        Ok(UnixStream { inner, io, reactor })
    }

    /// Connects to `addr` -- the [`UnixSocketAddr`]-based counterpart of
    /// [`connect`](Self::connect), the only way to connect to a Linux/
    /// Android abstract-namespace address rather than a real filesystem
    /// path.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn connect_addr(addr: &UnixSocketAddr) -> io::Result<UnixStream> {
        let reactor = Handle::current().shared.reactor.clone();
        let fd = socket::new_unix_socket()?;
        socket::unix_connect_addr(fd.as_raw_fd(), &addr.0)?;
        let io = reactor.register(fd.as_raw_fd())?;
        let inner = PlatformUnixStream::from(fd);
        ready_io(&io, ReactorInterest::Write, || {
            socket::take_socket_error(inner.as_raw_fd())
        })
        .await?;
        Ok(UnixStream { inner, io, reactor })
    }

    /// A pair of `UnixStream`s already connected to each other
    /// (`socketpair(2)`) -- for handing one end to a child process or a
    /// spawned task while keeping the other, with no filesystem path
    /// (nor a listener to `bind`/`accept` through) involved at all.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn pair() -> io::Result<(UnixStream, UnixStream)> {
        let reactor = Handle::current().shared.reactor.clone();
        let (fd_a, fd_b) = socket::unix_socketpair()?;
        let a = UnixStream::from_accepted(PlatformUnixStream::from(fd_a), reactor.clone())?;
        let b = UnixStream::from_accepted(PlatformUnixStream::from(fd_b), reactor)?;
        Ok((a, b))
    }

    fn from_accepted(inner: PlatformUnixStream, reactor: Arc<Reactor>) -> io::Result<UnixStream> {
        let io = reactor.register(inner.as_raw_fd())?;
        Ok(UnixStream { inner, io, reactor })
    }

    pub async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        ready_io(&self.io, ReactorInterest::Read, || {
            socket::read(self.inner.as_raw_fd(), buf)
        })
        .await
    }

    pub async fn write(&self, buf: &[u8]) -> io::Result<usize> {
        ready_io(&self.io, ReactorInterest::Write, || {
            socket::write(self.inner.as_raw_fd(), buf)
        })
        .await
    }

    pub async fn write_all(&self, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let n = self.write(buf).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            buf = &buf[n..];
        }
        Ok(())
    }

    /// Reads until `buf` is completely filled, or returns
    /// `UnexpectedEof` if the peer closes first.
    pub async fn read_exact(&self, mut buf: &mut [u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let n = self.read(buf).await?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "early eof"));
            }
            buf = &mut buf[n..];
        }
        Ok(())
    }

    pub fn peer_addr(&self) -> io::Result<UnixSocketAddr> {
        with_borrowed_std_stream(self.inner.as_raw_fd(), |s| s.peer_addr()).map(UnixSocketAddr)
    }

    pub fn local_addr(&self) -> io::Result<UnixSocketAddr> {
        with_borrowed_std_stream(self.inner.as_raw_fd(), |s| s.local_addr()).map(UnixSocketAddr)
    }

    /// `SO_ERROR` -- see [`TcpStream::take_error`](super::TcpStream::take_error)
    /// for the full contract, identical here.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        socket::take_error(self.inner.as_raw_fd())
    }

    /// The effective credentials (user ID, group ID, and -- where the
    /// platform reports one -- process ID) of whichever process called
    /// `connect` or `pair` to create the *other* end of this socket.
    /// See [`UCred`]'s own docs for how each platform actually obtains
    /// these.
    pub fn peer_cred(&self) -> io::Result<UCred> {
        ucred::get_peer_cred(self.inner.as_raw_fd())
    }

    /// Waits for this stream to become readable -- see
    /// [`super::TcpStream::readable`], identical reasoning here.
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

    /// Non-`async fn` form of [`readable`](Self::readable).
    pub fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        super::reactor::poll_ready(&self.io, ReactorInterest::Read, cx).map(Ok)
    }

    /// Non-`async fn` form of [`writable`](Self::writable).
    pub fn poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        super::reactor::poll_ready(&self.io, ReactorInterest::Write, cx).map(Ok)
    }

    /// Runs `f` (the caller's own non-blocking read/write against this
    /// stream's fd) once `interest` is ready, clearing that cached
    /// readiness if `f` reports `WouldBlock` -- see
    /// [`super::TcpStream::try_io`] for the same pattern, identical
    /// reasoning here.
    pub fn try_io<R>(
        &self,
        interest: Interest,
        f: impl FnOnce() -> io::Result<R>,
    ) -> io::Result<R> {
        readiness::try_io(&self.io, interest, f)
    }

    /// Reads without waiting, failing immediately (with `WouldBlock`)
    /// if nothing's available yet.
    pub fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.try_io(Interest::READABLE, || {
            socket::read(self.inner.as_raw_fd(), buf)
        })
    }

    /// Writes without waiting, failing immediately (with `WouldBlock`)
    /// if the socket isn't ready to accept more right now.
    pub fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        self.try_io(Interest::WRITABLE, || {
            socket::write(self.inner.as_raw_fd(), buf)
        })
    }

    /// Like [`try_read`](Self::try_read), but scatters into every
    /// buffer in `bufs` in one `readv(2)` call, rather than only ever
    /// filling the first one.
    pub fn try_read_vectored(&self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        self.try_io(Interest::READABLE, || {
            socket::readv(self.inner.as_raw_fd(), bufs)
        })
    }

    /// Like [`try_write`](Self::try_write), but gathers from every
    /// buffer in `bufs` in one `writev(2)` call.
    pub fn try_write_vectored(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.try_io(Interest::WRITABLE, || {
            socket::writev(self.inner.as_raw_fd(), bufs)
        })
    }

    fn poll_read_priv(&self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        poll_io(&self.io, ReactorInterest::Read, cx, || {
            socket::read(self.inner.as_raw_fd(), buf)
        })
    }

    fn poll_write_priv(&self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        poll_io(&self.io, ReactorInterest::Write, cx, || {
            socket::write(self.inner.as_raw_fd(), buf)
        })
    }
}

impl Drop for UnixStream {
    fn drop(&mut self) {
        self.reactor.deregister(self.inner.as_raw_fd());
    }
}

// See `UnixListener`'s equivalent impls above.
impl std::os::fd::AsFd for UnixStream {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

impl std::os::fd::AsRawFd for UnixStream {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.inner.as_raw_fd()
    }
}

impl std::os::fd::FromRawFd for UnixStream {
    unsafe fn from_raw_fd(fd: std::os::fd::RawFd) -> Self {
        let owned = unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) };
        let inner = PlatformUnixStream::from(owned);
        inner
            .set_nonblocking(true)
            .expect("failed to set the adopted fd non-blocking");
        let reactor = Handle::current().shared.reactor.clone();
        UnixStream::from_accepted(inner, reactor)
            .expect("failed to register raw fd with the reactor")
    }
}

impl std::os::fd::IntoRawFd for UnixStream {
    fn into_raw_fd(self) -> std::os::fd::RawFd {
        self.inner
            .try_clone_io()
            .expect("failed to duplicate fd")
            .into_raw_fd()
    }
}

impl AsyncRead for &UnixStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.poll_read_priv(cx, buf.unfilled_mut()) {
            Poll::Ready(Ok(n)) => {
                buf.advance(n);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for &UnixStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_priv(cx, buf)
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(socket::shutdown_write(self.inner.as_raw_fd()))
    }
}

/// Delegates to the `&UnixStream` impl above -- see `TcpStream`'s
/// equivalent impl for why `&mut self` here isn't a real exclusivity
/// requirement.
impl AsyncRead for UnixStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut &*self.get_mut()).poll_read(cx, buf)
    }
}

impl AsyncWrite for UnixStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut &*self.get_mut()).poll_write(cx, buf)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut &*self.get_mut()).poll_shutdown(cx)
    }
}

/// Borrowed read half of a [`UnixStream`], created by [`UnixStream::split`].
pub struct UnixReadHalf<'a>(&'a UnixStream);

/// Borrowed write half of a [`UnixStream`], created by [`UnixStream::split`].
pub struct UnixWriteHalf<'a>(&'a UnixStream);

impl UnixReadHalf<'_> {
    pub fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.try_read(buf)
    }

    pub fn try_read_vectored(&self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        self.0.try_read_vectored(bufs)
    }
}

impl UnixWriteHalf<'_> {
    pub fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        self.0.try_write(buf)
    }

    pub fn try_write_vectored(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.0.try_write_vectored(bufs)
    }
}

impl AsyncRead for UnixReadHalf<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl AsyncWrite for UnixWriteHalf<'_> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}

/// Owned read half of a [`UnixStream`], created by
/// [`UnixStream::into_split`].
pub struct OwnedUnixReadHalf(Arc<UnixStream>);

/// Owned write half of a [`UnixStream`], created by
/// [`UnixStream::into_split`].
pub struct OwnedUnixWriteHalf(Arc<UnixStream>);

impl OwnedUnixReadHalf {
    pub fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.try_read(buf)
    }

    pub fn try_read_vectored(&self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        self.0.try_read_vectored(bufs)
    }

    /// Recombines this half with its `other` write half back into a
    /// single [`UnixStream`], if they came from the same
    /// [`UnixStream::into_split`] call -- see [`UnixReuniteError`] for
    /// when they didn't.
    pub fn reunite(self, other: OwnedUnixWriteHalf) -> Result<UnixStream, UnixReuniteError> {
        reunite(self, other)
    }
}

impl OwnedUnixWriteHalf {
    pub fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        self.0.try_write(buf)
    }

    pub fn try_write_vectored(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.0.try_write_vectored(bufs)
    }

    /// Recombines this half with its `other` read half back into a
    /// single [`UnixStream`] -- see [`OwnedUnixReadHalf::reunite`].
    pub fn reunite(self, other: OwnedUnixReadHalf) -> Result<UnixStream, UnixReuniteError> {
        reunite(other, self)
    }
}

impl AsRef<UnixStream> for OwnedUnixReadHalf {
    fn as_ref(&self) -> &UnixStream {
        &self.0
    }
}

impl AsRef<UnixStream> for OwnedUnixWriteHalf {
    fn as_ref(&self) -> &UnixStream {
        &self.0
    }
}

/// Recombines `read`/`write` into the single `UnixStream` they were
/// [`split`](UnixStream::into_split) from, if the two `Arc`s underneath
/// them are the same allocation -- `Err` otherwise, handing both halves
/// straight back rather than dropping them.
fn reunite(
    read: OwnedUnixReadHalf,
    write: OwnedUnixWriteHalf,
) -> Result<UnixStream, UnixReuniteError> {
    if Arc::ptr_eq(&read.0, &write.0) {
        drop(write);
        // `read` was the last of the two clones sharing this `Arc`, now
        // that `write`'s has just been dropped -- this always succeeds.
        Ok(Arc::try_unwrap(read.0).unwrap_or_else(|_| {
            unreachable!(
                "UnixStream: Arc::try_unwrap failed in reunite despite being the last clone"
            )
        }))
    } else {
        Err(UnixReuniteError(read, write))
    }
}

/// The error [`OwnedUnixReadHalf::reunite`]/[`OwnedUnixWriteHalf::reunite`]
/// return when the two halves passed in didn't come from the same
/// [`UnixStream::into_split`] call -- hands both halves straight back
/// rather than dropping them, so the caller isn't forced to discard
/// otherwise-still-usable halves just because they didn't match.
///
/// Named `UnixReuniteError` (rather than colliding with
/// [`super::ReuniteError`], the same shape for [`super::TcpStream`]'s
/// owned halves) since this crate flattens every type straight to the
/// crate root rather than nesting them under per-protocol modules the
/// way tokio's own `tcp::ReuniteError`/`unix::ReuniteError` (identically
/// named, but distinguished by their different module paths) do.
pub struct UnixReuniteError(pub OwnedUnixReadHalf, pub OwnedUnixWriteHalf);

// See `tcp::ReuniteError`'s identical comment: neither owned half nor
// `UnixStream` itself implements `Debug`, so this is hand-written rather
// than derived.
impl std::fmt::Debug for UnixReuniteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("UnixReuniteError").finish()
    }
}

impl std::fmt::Display for UnixReuniteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tried to reunite halves that are not from the same socket"
        )
    }
}

impl std::error::Error for UnixReuniteError {}

impl AsyncRead for OwnedUnixReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut &*self.get_mut().0).poll_read(cx, buf)
    }
}

impl AsyncWrite for OwnedUnixWriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut &*self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut &*self.get_mut().0).poll_shutdown(cx)
    }
}

/// A type representing a Unix user ID -- deliberately a plain `u32`
/// rather than `libc::uid_t` itself (which the exact underlying integer
/// type of varies by platform), matching tokio's own `net::unix::uid_t`.
#[allow(non_camel_case_types)]
pub type uid_t = u32;

/// A type representing a Unix group ID -- see [`uid_t`] for why this
/// isn't `libc::gid_t` directly.
#[allow(non_camel_case_types)]
pub type gid_t = u32;

/// A type representing a Unix process (or process group) ID -- see
/// [`uid_t`] for why this isn't `libc::pid_t` directly.
#[allow(non_camel_case_types)]
pub type pid_t = i32;

/// The effective credentials of the process on the other end of a
/// [`UnixStream`] -- see [`UnixStream::peer_cred`]. Obtained via
/// `SO_PEERCRED` on Linux, or `LOCAL_PEEREPID` (for the pid) plus
/// `getpeereid(2)` (for the uid/gid) on macOS -- the two platforms this
/// crate builds on report a peer's credentials through genuinely
/// different mechanisms, unlike most other socket options here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UCred {
    uid: uid_t,
    gid: gid_t,
    pid: Option<pid_t>,
}

impl UCred {
    /// The peer's effective user ID.
    pub fn uid(&self) -> uid_t {
        self.uid
    }

    /// The peer's effective group ID.
    pub fn gid(&self) -> gid_t {
        self.gid
    }

    /// The peer's process ID -- always `Some` on both platforms this
    /// crate supports (Linux's `SO_PEERCRED` and macOS's
    /// `LOCAL_PEEREPID` both report one), unlike some other Unix
    /// platforms tokio itself runs on but this crate doesn't build for.
    pub fn pid(&self) -> Option<pid_t> {
        self.pid
    }
}

mod ucred {
    use super::UCred;
    use std::io;
    use std::os::fd::RawFd;

    #[cfg(target_os = "linux")]
    pub(super) fn get_peer_cred(fd: RawFd) -> io::Result<UCred> {
        use std::mem;

        // SAFETY: `ucred` is a plain C struct of three integers -- valid
        // for any bit pattern, so a zeroed value is already well-formed
        // to hand `getsockopt` a pointer into.
        let mut cred: libc::ucred = unsafe { mem::zeroed() };
        let mut len = mem::size_of::<libc::ucred>() as libc::socklen_t;

        // SAFETY: `fd` is a valid, currently-open socket (borrowed from
        // `self.inner`, still owned by the caller for the duration of
        // this call); `cred`/`len` are correctly-sized, initialized
        // out-parameters matching what `SO_PEERCRED` expects.
        let ret = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut cred as *mut libc::ucred).cast(),
                &mut len,
            )
        };
        if ret == 0 && len as usize == mem::size_of::<libc::ucred>() {
            Ok(UCred {
                uid: cred.uid,
                gid: cred.gid,
                pid: Some(cred.pid),
            })
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(target_os = "macos")]
    pub(super) fn get_peer_cred(fd: RawFd) -> io::Result<UCred> {
        use std::mem::MaybeUninit;

        // `LOCAL_PEEREPID` (Darwin-specific, unlike Linux's single
        // `SO_PEERCRED` covering all three fields at once) reports only
        // the peer's pid; the uid/gid still come from the separate
        // `getpeereid(2)` call below, matching tokio's own macOS
        // implementation.
        let mut pid: MaybeUninit<libc::pid_t> = MaybeUninit::uninit();
        let mut pid_len: libc::socklen_t = std::mem::size_of::<libc::pid_t>() as libc::socklen_t;
        // SAFETY: `fd` is a valid, currently-open socket; `pid`/`pid_len`
        // are correctly-sized, initialized out-parameters.
        let ret = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_LOCAL,
                libc::LOCAL_PEEREPID,
                pid.as_mut_ptr().cast(),
                &mut pid_len,
            )
        };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
        if pid_len as usize != std::mem::size_of::<libc::pid_t>() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected pid size from LOCAL_PEEREPID",
            ));
        }
        // SAFETY: just confirmed above that `getsockopt` filled in
        // exactly `size_of::<pid_t>()` bytes.
        let pid = unsafe { pid.assume_init() };

        let mut uid = MaybeUninit::uninit();
        let mut gid = MaybeUninit::uninit();
        // SAFETY: `fd` is a valid, currently-open socket; `uid`/`gid`
        // are valid out-parameters for `getpeereid` to initialize.
        let ret = unsafe { libc::getpeereid(fd, uid.as_mut_ptr(), gid.as_mut_ptr()) };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `getpeereid` returned success above, so both
        // out-parameters are now initialized.
        let (uid, gid) = unsafe { (uid.assume_init(), gid.assume_init()) };

        Ok(UCred {
            uid,
            gid,
            pid: Some(pid),
        })
    }
}
