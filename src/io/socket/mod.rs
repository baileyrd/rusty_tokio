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

use libc::{c_int, sockaddr, sockaddr_in, sockaddr_in6, sockaddr_storage, socklen_t};
use platform::error::{OsCode, PlatformError};
use std::io;
use std::mem;
use std::net::SocketAddr;
use std::os::fd::{FromRawFd, OwnedFd, RawFd};

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

/// Adapts `rustils`' two-axis `PlatformError` to `std::io::Error` so it
/// composes with the rest of this crate's (and every caller's) plain
/// `io::Result`-based API. This is effectively always the `Errno` arm,
/// which round-trips through std's own errno mapping -- so e.g.
/// `EAGAIN` still comes back as `io::ErrorKind::WouldBlock`, exactly
/// what `reactor::ready_io`'s retry loop checks for.
pub(crate) fn from_platform_err(e: PlatformError) -> io::Error {
    if let OsCode::Errno(errno) = e.os {
        return io::Error::from_raw_os_error(errno);
    }
    io::Error::other(e)
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
