//! The sliver of raw-`libc` socket code this crate still needs after
//! adopting `rustils`' `platform_linux`/`platform_macos` crates for
//! socket lifecycle (bind/listen/accept/UDP), addressing, and socket
//! options -- see `tcp.rs`/`udp.rs`, which use those directly.
//!
//! Two things are deliberately still hand-rolled here on *every* OS
//! instead:
//!
//! - **Non-blocking `connect`.** `{Linux,Macos}TcpStream::connect`
//!   creates a *blocking* socket and calls a blocking `connect(2)`
//!   internally -- correct for rustils' own blocking-I/O consumers, but
//!   exactly wrong for an async runtime: it would stall a whole worker
//!   thread for the connection's RTT. An async connect needs the socket
//!   to already be non-blocking *before* `connect(2)` is called, so it
//!   returns `EINPROGRESS` immediately and the reactor waits for
//!   writability instead. Nothing in rustils' public surface exposes
//!   "create a bare socket, don't connect yet", so that one syscall pair
//!   (`socket` + `connect`) stays here; the resulting fd is then adopted
//!   into the concrete stream type via `From<OwnedFd>` for everything
//!   after.
//! - **`read`/`write`.** `platform::net::TcpStream::read`/`write` take
//!   `&mut self` (a reasonable choice for rustils' own blocking callers,
//!   who never need to read and write concurrently from two tasks
//!   sharing one stream) -- but this runtime's `TcpStream` deliberately
//!   exposes `&self` methods so one task can read while another writes.
//!   Bypassing the trait for the two syscalls that are trivial anyway
//!   (a raw `read`/`write` on an fd we already have via `AsRawFd`) keeps
//!   that API intact instead of hiding a mutex behind it.
//!
//! The same non-blocking-`connect` gap applies to `AF_UNIX` streams too
//! (`unix.rs`), so `new_unix_socket`/`unix_connect`/`sun_path_offset`/
//! `to_sockaddr_un` below mirror `new_tcp_socket`/`connect`/`to_sockaddr`
//! exactly, just packing a `sockaddr_un` from a `Path` instead of a
//! `sockaddr_{in,in6}` from a `SocketAddr` -- rustils' own
//! `{Linux,Macos}UnixStream::connect` is blocking for the same reason its
//! TCP counterpart is, and for the same reason has no "bare socket, don't
//! connect yet" constructor to build an async version on top of.

#[cfg(target_os = "macos")]
use super::from_platform_err;
use libc::{c_int, sockaddr, sockaddr_in, sockaddr_in6, sockaddr_storage, socklen_t};
use std::io;
use std::mem;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

fn cvt(ret: c_int) -> io::Result<c_int> {
    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(ret)
    }
}

fn domain_for(addr: SocketAddr) -> c_int {
    match addr {
        SocketAddr::V4(_) => libc::AF_INET,
        SocketAddr::V6(_) => libc::AF_INET6,
    }
}

fn to_sockaddr(addr: SocketAddr) -> (sockaddr_storage, socklen_t) {
    // SAFETY: an all-zero `sockaddr_storage` is a valid (if inert)
    // value for this plain-old-data type; only the variant selected by
    // `ss_family` (written below) is ever read back by the kernel.
    let mut storage: sockaddr_storage = unsafe { mem::zeroed() };
    let len = match addr {
        SocketAddr::V4(v4) => {
            // Built via `zeroed()` + field assignment rather than a
            // full struct literal so this works unchanged on BSD
            // sockaddr layouts too: `sockaddr_in`/`sockaddr_in6` there
            // carry an extra leading `sin_len`/`sin6_len` byte Linux's
            // don't, and zero is the correct value for it here (the
            // kernel doesn't require callers to fill it in for
            // `bind`/`connect`/etc., only `sockaddr_storage`'s overall
            // buffer size, passed separately as `len` below).
            let mut sin: sockaddr_in = unsafe { mem::zeroed() };
            sin.sin_family = libc::AF_INET as libc::sa_family_t;
            sin.sin_port = v4.port().to_be();
            sin.sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
            // SAFETY: `storage` is large enough and suitably aligned for
            // any sockaddr variant (that's `sockaddr_storage`'s purpose);
            // writing a `sockaddr_in` to its start and reading it back
            // that way is exactly how the kernel itself treats the
            // buffer once `ss_family` says `AF_INET`.
            unsafe {
                std::ptr::write(
                    (&mut storage as *mut sockaddr_storage).cast::<sockaddr_in>(),
                    sin,
                );
            }
            mem::size_of::<sockaddr_in>()
        }
        SocketAddr::V6(v6) => {
            // See the V4 arm above for why this is built field-by-field
            // rather than as a struct literal.
            let mut sin6: sockaddr_in6 = unsafe { mem::zeroed() };
            sin6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            sin6.sin6_port = v6.port().to_be();
            sin6.sin6_flowinfo = v6.flowinfo();
            sin6.sin6_addr.s6_addr = v6.ip().octets();
            sin6.sin6_scope_id = v6.scope_id();
            // SAFETY: see the V4 arm above.
            unsafe {
                std::ptr::write(
                    (&mut storage as *mut sockaddr_storage).cast::<sockaddr_in6>(),
                    sin6,
                );
            }
            mem::size_of::<sockaddr_in6>()
        }
    };
    (storage, len as socklen_t)
}

/// The reverse of [`to_sockaddr`] -- needed only for `peek_from`/
/// `peek_sender` below, which bypass rustils' own `recv_from` (and thus
/// its typed `SocketAddr` result) to reach `recvfrom(2)`'s `MSG_PEEK`
/// flag directly. Every other address-producing call in this crate
/// (`accept`, `local_addr`, `peer_addr`, plain `recv_from`) still goes
/// through rustils' own decoding.
///
/// # Safety
/// `storage.ss_family` must match the variant the kernel actually wrote
/// (always true right after a `recvfrom(2)` call that succeeded).
unsafe fn from_sockaddr(storage: &sockaddr_storage) -> io::Result<SocketAddr> {
    match storage.ss_family as c_int {
        libc::AF_INET => {
            // SAFETY: `storage.ss_family == AF_INET`, the caller's
            // contract for this whole function.
            let sin = unsafe { &*(storage as *const sockaddr_storage).cast::<sockaddr_in>() };
            let port = u16::from_be(sin.sin_port);
            let ip = Ipv4Addr::from(sin.sin_addr.s_addr.to_ne_bytes());
            Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
        }
        libc::AF_INET6 => {
            // SAFETY: `storage.ss_family == AF_INET6`, the caller's
            // contract for this whole function.
            let sin6 = unsafe { &*(storage as *const sockaddr_storage).cast::<sockaddr_in6>() };
            let port = u16::from_be(sin6.sin6_port);
            let ip = Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            Ok(SocketAddr::V6(SocketAddrV6::new(
                ip,
                port,
                sin6.sin6_flowinfo,
                sin6.sin6_scope_id,
            )))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "kernel returned an address family other than AF_INET/AF_INET6",
        )),
    }
}

/// A bare, non-blocking socket -- not yet bound or connected. Nothing in
/// rustils' public surface creates a socket without also connecting
/// (blocking) or binding it, so this one syscall stays hand-rolled; see
/// this module's docs.
pub(crate) fn new_tcp_socket(addr: SocketAddr) -> io::Result<OwnedFd> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: plain integer arguments, no memory referenced.
        let fd = unsafe {
            libc::socket(
                domain_for(addr),
                libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
                0,
            )
        };
        cvt(fd)?;
        // SAFETY: `fd` was just returned by `socket(2)` and is valid,
        // otherwise-unowned, and wrapped exactly once.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
    #[cfg(target_os = "macos")]
    {
        // macOS has no `SOCK_NONBLOCK`/`SOCK_CLOEXEC` socket-type flags
        // -- that's a Linux extension -- so both are set via `fcntl`
        // right after creation instead of atomically at `socket(2)`
        // time (the same reasoning `platform_macos::sys::net`'s own
        // socket-creating calls document). This leaves a window
        // (between `socket()` returning and the `fcntl` calls
        // completing) where a concurrent `fork` elsewhere in the
        // process could inherit this fd; this crate never forks, so
        // that window is never exercised in practice.
        //
        // SAFETY: plain integer arguments, no memory referenced.
        let fd = unsafe { libc::socket(domain_for(addr), libc::SOCK_STREAM, 0) };
        cvt(fd)?;
        // SAFETY: `fd` was just returned by `socket(2)` and is valid,
        // otherwise-unowned, and wrapped exactly once.
        let owned = unsafe { OwnedFd::from_raw_fd(fd) };
        // `set_nonblocking` is `pub` in `platform_macos::sys::net` (it
        // only needs `&OwnedFd`, not one of that crate's own concrete
        // types), so it's reused directly rather than hand-rolled a
        // second time. `set_cloexec` is private there, unlike
        // `set_nonblocking` -- platform-macos's own socket-creating
        // calls need it internally but never expose it standalone, so
        // this one `fcntl` stays hand-rolled.
        platform_macos::sys::net::set_nonblocking(&owned, true).map_err(from_platform_err)?;
        use std::os::fd::AsRawFd;
        // SAFETY: `owned` is caller-owned and open; `FD_CLOEXEC` is the
        // sole variadic argument `F_SETFD` expects.
        if unsafe { libc::fcntl(owned.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(owned)
    }
}

/// A bare, non-blocking `AF_UNIX` stream socket -- the `AF_UNIX`
/// counterpart of [`new_tcp_socket`], same reasoning: nothing in
/// rustils' public surface creates one without also connecting
/// (blocking) or binding it.
pub(crate) fn new_unix_socket() -> io::Result<OwnedFd> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: plain integer arguments, no memory referenced.
        let fd = unsafe {
            libc::socket(
                libc::AF_UNIX,
                libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
                0,
            )
        };
        cvt(fd)?;
        // SAFETY: `fd` was just returned by `socket(2)` and is valid,
        // otherwise-unowned, and wrapped exactly once.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
    #[cfg(target_os = "macos")]
    {
        // See `new_tcp_socket`'s macOS arm for why this is two steps
        // instead of one atomic `socket(2)` call.
        //
        // SAFETY: plain integer arguments, no memory referenced.
        let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        cvt(fd)?;
        // SAFETY: `fd` was just returned by `socket(2)` and is valid,
        // otherwise-unowned, and wrapped exactly once.
        let owned = unsafe { OwnedFd::from_raw_fd(fd) };
        platform_macos::sys::net::set_nonblocking(&owned, true).map_err(from_platform_err)?;
        use std::os::fd::AsRawFd;
        // SAFETY: `owned` is caller-owned and open; `FD_CLOEXEC` is the
        // sole variadic argument `F_SETFD` expects.
        if unsafe { libc::fcntl(owned.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(owned)
    }
}

/// The byte offset of `sockaddr_un::sun_path` within `sockaddr_un` --
/// `sun_path` is a trailing array whose start isn't at a portable
/// constant offset (Linux's `sockaddr_un` has no leading `sun_len` byte,
/// BSD's does), so it's measured once here rather than hard-coded.
fn sun_path_offset() -> usize {
    // SAFETY: an all-zero `sockaddr_un` is a valid (if meaningless)
    // value; nothing here is read before being written, only its
    // fields' addresses are taken.
    let addr: libc::sockaddr_un = unsafe { mem::zeroed() };
    let base = std::ptr::addr_of!(addr) as usize;
    let path = std::ptr::addr_of!(addr.sun_path) as usize;
    path - base
}

/// Pack a filesystem `path` into a kernel-layout `sockaddr_un` and the
/// length of the filled-in prefix -- the `AF_UNIX` counterpart of
/// `to_sockaddr`. Includes the trailing NUL in `len`, matching what a C
/// program passes to `bind`/`connect` for a pathname socket.
fn to_sockaddr_un(path: &Path) -> io::Result<(libc::sockaddr_un, socklen_t)> {
    let bytes = path.as_os_str().as_bytes();
    if bytes.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "AF_UNIX path must not contain a NUL byte",
        ));
    }
    // SAFETY: see `sun_path_offset`.
    let mut addr: libc::sockaddr_un = unsafe { mem::zeroed() };
    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
    if bytes.len() >= addr.sun_path.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "AF_UNIX path too long (must fit in sockaddr_un::sun_path)",
        ));
    }
    for (slot, byte) in addr.sun_path.iter_mut().zip(bytes.iter()) {
        *slot = *byte as libc::c_char;
    }
    let len = sun_path_offset() + bytes.len() + 1;
    // BSD's `sockaddr_un` (unlike Linux's) carries a leading `sun_len`
    // byte -- see `to_sockaddr`'s `sockaddr_in`/`sockaddr_in6` arms above
    // for the same distinction on the TCP side, and rustils'
    // `platform-macos::sys::net::to_sockaddr_un` for confirmation this
    // field is filled in (to the same filled-prefix length `len` computed
    // below) rather than left zero.
    #[cfg(target_os = "macos")]
    {
        addr.sun_len = len as u8;
    }
    Ok((addr, len as socklen_t))
}

/// `connect(2)` to an `AF_UNIX` path on a non-blocking socket -- the
/// `AF_UNIX` counterpart of [`connect`]; see that function's docs for why
/// `EINPROGRESS` is treated as success here too.
pub(crate) fn unix_connect(fd: RawFd, path: &Path) -> io::Result<()> {
    let (addr, len) = to_sockaddr_un(path)?;
    // SAFETY: `addr` holds a valid `sockaddr_un` for exactly `len` bytes
    // (`to_sockaddr_un`'s contract); `fd` is a valid, freshly created,
    // still-unconnected socket.
    let r = unsafe {
        libc::connect(
            fd,
            (&addr as *const libc::sockaddr_un).cast::<sockaddr>(),
            len,
        )
    };
    if r == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::EINPROGRESS) {
        return Ok(());
    }
    Err(err)
}

/// `connect(2)` on a non-blocking socket returns `EINPROGRESS`
/// immediately rather than blocking -- that is success from this
/// function's point of view; the caller waits for the socket to become
/// writable and then calls [`take_socket_error`] to find out whether the
/// connection actually succeeded.
pub(crate) fn connect(fd: RawFd, addr: SocketAddr) -> io::Result<()> {
    let (storage, len) = to_sockaddr(addr);
    // SAFETY: `storage` holds a valid sockaddr for exactly `len` bytes
    // (`to_sockaddr`'s contract); `fd` is a valid, freshly created,
    // still-unconnected socket.
    let r = unsafe {
        libc::connect(
            fd,
            (&storage as *const sockaddr_storage).cast::<sockaddr>(),
            len,
        )
    };
    if r == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::EINPROGRESS) {
        return Ok(());
    }
    Err(err)
}

/// `getsockopt(SOL_SOCKET, SO_ERROR)` -- the standard way to learn
/// whether a non-blocking `connect` that just became writable actually
/// succeeded, or failed asynchronously (e.g. connection refused). Not
/// exposed by rustils, which never needs it (its own `connect` is
/// synchronous and reports failure directly).
pub(crate) fn take_socket_error(fd: RawFd) -> io::Result<()> {
    let mut err: c_int = 0;
    let mut len = mem::size_of::<c_int>() as socklen_t;
    // SAFETY: `&mut err`/`&mut len` are valid, exclusively borrowed
    // out-params the kernel fills; `fd` is caller-owned.
    cvt(unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ERROR,
            (&mut err as *mut c_int).cast(),
            &mut len,
        )
    })?;
    if err == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(err))
    }
}

pub(crate) fn read(fd: RawFd, buf: &mut [u8]) -> io::Result<usize> {
    // SAFETY: `buf` is valid for `buf.len()` bytes for the call's
    // duration; `fd` is caller-owned and open.
    let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

pub(crate) fn write(fd: RawFd, buf: &[u8]) -> io::Result<usize> {
    // SAFETY: `buf` is valid for `buf.len()` bytes for the call's
    // duration; `fd` is caller-owned and open.
    let n = unsafe { libc::write(fd, buf.as_ptr().cast(), buf.len()) };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

/// Genuine scatter-read via `readv(2)` -- fills as many of `bufs` as one
/// kernel call returns data for, rather than the "just the first buffer"
/// shortcut a naive vectored read might take.
pub(crate) fn readv(fd: RawFd, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
    let iovcnt = bufs.len().min(c_int::MAX as usize) as c_int;
    // SAFETY: `IoSliceMut` is guaranteed to have the same memory layout
    // as `iovec` on Unix (the whole reason it exists); `bufs` is valid
    // for `iovcnt` entries for the call's duration; `fd` is caller-owned
    // and open.
    let n = unsafe { libc::readv(fd, bufs.as_ptr().cast(), iovcnt) };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

/// The write-side counterpart of [`readv`], via `writev(2)`.
pub(crate) fn writev(fd: RawFd, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
    let iovcnt = bufs.len().min(c_int::MAX as usize) as c_int;
    // SAFETY: see `readv` above -- `IoSlice` has the identical guarantee.
    let n = unsafe { libc::writev(fd, bufs.as_ptr().cast(), iovcnt) };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

/// Like [`read`], but via `recv(2)` with `MSG_PEEK` -- the bytes stay in
/// the socket's receive queue for the next real `read`/`recv` call.
pub(crate) fn peek(fd: RawFd, buf: &mut [u8]) -> io::Result<usize> {
    // SAFETY: `buf` is valid for `buf.len()` bytes for the call's
    // duration; `fd` is caller-owned and open.
    let n = unsafe { libc::recv(fd, buf.as_mut_ptr().cast(), buf.len(), libc::MSG_PEEK) };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

/// The UDP `recvfrom(2)`-with-`MSG_PEEK` counterpart of [`peek`] --
/// reports the next datagram's length and sender without dequeuing it,
/// via `recvfrom(2)` directly rather than rustils' own `recv_from`
/// (which has no peek variant at all).
pub(crate) fn peek_from(fd: RawFd, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
    // SAFETY: an all-zero `sockaddr_storage` is a valid (if inert) value.
    let mut storage: sockaddr_storage = unsafe { mem::zeroed() };
    let mut fromlen = mem::size_of::<sockaddr_storage>() as socklen_t;
    // SAFETY: `buf` is valid for `buf.len()` bytes for the call's
    // duration; `&mut storage`/`&mut fromlen` are valid, exclusively
    // borrowed out-params; `fd` is caller-owned and open.
    let n = unsafe {
        libc::recvfrom(
            fd,
            buf.as_mut_ptr().cast(),
            buf.len(),
            libc::MSG_PEEK,
            (&mut storage as *mut sockaddr_storage).cast::<sockaddr>(),
            &mut fromlen,
        )
    };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the kernel just filled `storage` in via the call above.
    let peer = unsafe { from_sockaddr(&storage) }?;
    Ok((n as usize, peer))
}

/// Like [`peek_from`], but only the sender's address matters -- a
/// zero-length peek still reports the next datagram's source (UDP
/// datagram boundaries are preserved regardless of how much of it is
/// actually read), without needing any buffer to receive data into.
pub(crate) fn peek_sender(fd: RawFd) -> io::Result<SocketAddr> {
    let (_n, addr) = peek_from(fd, &mut [])?;
    Ok(addr)
}

/// `shutdown(2)` with `SHUT_WR` -- backs `AsyncWrite::poll_shutdown`,
/// signaling EOF to the peer without closing the fd itself (that still
/// happens on `Drop`, same as ever). Not exposed by rustils, which has
/// no async writer half needing a distinct "done writing" signal.
pub(crate) fn shutdown_write(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fd` is caller-owned and open.
    if unsafe { libc::shutdown(fd, libc::SHUT_WR) } < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// `bind(2)` on a bare (not yet bound) socket -- backs [`TcpSocket`
/// ](super::TcpSocket)'s `bind`, which needs bind and listen as two
/// separate steps (so socket options can be set on the still-unbound
/// socket first); rustils' own `{Linux,Macos}TcpListener::bind` only
/// exposes the combined "bind and immediately listen" operation.
pub(crate) fn bind(fd: RawFd, addr: SocketAddr) -> io::Result<()> {
    let (storage, len) = to_sockaddr(addr);
    // SAFETY: `storage` holds a valid sockaddr for exactly `len` bytes
    // (`to_sockaddr`'s contract); `fd` is caller-owned and not yet bound.
    cvt(unsafe {
        libc::bind(
            fd,
            (&storage as *const sockaddr_storage).cast::<sockaddr>(),
            len,
        )
    })?;
    Ok(())
}

/// `listen(2)` -- the other half of [`bind`] above, turning a bound
/// socket into one the kernel will queue incoming connections for.
pub(crate) fn listen(fd: RawFd, backlog: u32) -> io::Result<()> {
    // SAFETY: `fd` is caller-owned and already bound.
    cvt(unsafe { libc::listen(fd, backlog as c_int) })?;
    Ok(())
}

fn setsockopt_int(fd: RawFd, level: c_int, name: c_int, value: c_int) -> io::Result<()> {
    // SAFETY: `&value` is a valid `c_int` the kernel only reads for the
    // call's duration; `fd` is caller-owned.
    cvt(unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            (&value as *const c_int).cast(),
            mem::size_of::<c_int>() as socklen_t,
        )
    })?;
    Ok(())
}

fn getsockopt_int(fd: RawFd, level: c_int, name: c_int) -> io::Result<c_int> {
    let mut value: c_int = 0;
    let mut len = mem::size_of::<c_int>() as socklen_t;
    // SAFETY: `&mut value`/`&mut len` are valid, exclusively borrowed
    // out-params the kernel fills; `fd` is caller-owned.
    cvt(unsafe { libc::getsockopt(fd, level, name, (&mut value as *mut c_int).cast(), &mut len) })?;
    Ok(value)
}

/// `SO_REUSEADDR` -- not in rustils' `TcpStream`/`TcpListener` traits at
/// all (per that crate's `net.rs`, only `set_nodelay` is), so hand-rolled
/// here the same way the rest of this module's slivers are.
pub(crate) fn set_reuseaddr(fd: RawFd, reuse: bool) -> io::Result<()> {
    setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, reuse as c_int)
}

pub(crate) fn reuseaddr(fd: RawFd) -> io::Result<bool> {
    Ok(getsockopt_int(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR)? != 0)
}

/// `SO_REUSEPORT` -- supported by both this crate's targets (Linux since
/// kernel 3.9, macOS/BSD for much longer), unlike some other platforms.
pub(crate) fn set_reuseport(fd: RawFd, reuse: bool) -> io::Result<()> {
    setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT, reuse as c_int)
}

pub(crate) fn reuseport(fd: RawFd) -> io::Result<bool> {
    Ok(getsockopt_int(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT)? != 0)
}

pub(crate) fn set_send_buffer_size(fd: RawFd, size: u32) -> io::Result<()> {
    setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_SNDBUF, size as c_int)
}

/// The kernel doesn't necessarily use exactly the size last requested
/// (Linux, notably, doubles whatever's set to leave room for its own
/// bookkeeping) -- read this back to see what was actually applied,
/// rather than assuming the requested value stuck.
pub(crate) fn send_buffer_size(fd: RawFd) -> io::Result<u32> {
    Ok(getsockopt_int(fd, libc::SOL_SOCKET, libc::SO_SNDBUF)? as u32)
}

pub(crate) fn set_recv_buffer_size(fd: RawFd, size: u32) -> io::Result<()> {
    setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, size as c_int)
}

pub(crate) fn recv_buffer_size(fd: RawFd) -> io::Result<u32> {
    Ok(getsockopt_int(fd, libc::SOL_SOCKET, libc::SO_RCVBUF)? as u32)
}

/// `SO_BINDTODEVICE` -- Linux-only (no macOS/BSD equivalent at all, unlike
/// every other option in this file), binds a socket to a specific network
/// interface by name (e.g. `b"eth0"`) so its traffic only goes over that
/// interface regardless of routing table entries. `interface: None` clears
/// a previous binding. Typically needs `CAP_NET_ADMIN` to set.
#[cfg(target_os = "linux")]
pub(crate) fn set_bind_device(fd: RawFd, interface: Option<&[u8]>) -> io::Result<()> {
    let bytes = interface.unwrap_or(&[]);
    // SAFETY: `bytes` is a valid, exclusively-borrowed byte slice the
    // kernel only reads for the call's duration; `fd` is caller-owned.
    cvt(unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_BINDTODEVICE,
            bytes.as_ptr().cast(),
            bytes.len() as socklen_t,
        )
    })?;
    Ok(())
}

/// The reverse of [`set_bind_device`] -- `None` if the socket isn't
/// currently bound to a specific interface.
#[cfg(target_os = "linux")]
pub(crate) fn bind_device(fd: RawFd) -> io::Result<Option<Vec<u8>>> {
    // IFNAMSIZ (16) is the kernel's own hard cap on interface name
    // length, name included nul terminator.
    let mut buf = [0u8; libc::IFNAMSIZ];
    let mut len = buf.len() as socklen_t;
    // SAFETY: `&mut buf`/`&mut len` are valid, exclusively borrowed
    // out-params the kernel fills; `fd` is caller-owned.
    cvt(unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_BINDTODEVICE,
            buf.as_mut_ptr().cast(),
            &mut len,
        )
    })?;
    if len == 0 || buf[0] == 0 {
        return Ok(None);
    }
    let name_len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    Ok(Some(buf[..name_len].to_vec()))
}

/// Flips `fd`'s `O_NONBLOCK` flag via `fcntl(2)` -- unlike every socket
/// type in this module (each has its own concrete `set_nonblocking`,
/// either from rustils or, on macOS at creation time, hand-rolled), a
/// plain pipe fd (a child process's piped stdin/stdout/stderr, adopted
/// from `std::process::Child`) has no such method of its own. `read(2)`/
/// `write(2)` on a pipe behave exactly like a socket's once non-blocking
/// -- `EWOULDBLOCK` when there's nothing to read or no room to write --
/// so [`read`]/[`write`] above and [`super::reactor::poll_io`]'s retry
/// loop work unmodified on one.
pub(crate) fn set_nonblocking(fd: RawFd, nonblocking: bool) -> io::Result<()> {
    // SAFETY: `fd` is caller-owned and open; `F_GETFL` takes no further
    // argument.
    let flags = cvt(unsafe { libc::fcntl(fd, libc::F_GETFL) })?;
    let flags = if nonblocking {
        flags | libc::O_NONBLOCK
    } else {
        flags & !libc::O_NONBLOCK
    };
    // SAFETY: `fd` is caller-owned and open; `flags` is a valid
    // `O_NONBLOCK`-adjusted copy of what `F_GETFL` just returned.
    cvt(unsafe { libc::fcntl(fd, libc::F_SETFL, flags) })?;
    Ok(())
}
