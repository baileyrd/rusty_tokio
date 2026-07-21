//! Hand-rolled macOS/BSD socket lifecycle: bind/listen/accept/UDP, plus
//! addressing and socket options. `rustils` has no macOS backend --
//! Linux gets this from `platform_linux` instead (used directly from
//! `tcp.rs`/`udp.rs`) -- so this mirrors that crate's concrete-type
//! shape (same method names, same `platform::error::Result` return
//! type) closely enough that `tcp.rs`/`udp.rs` need only a `#[cfg]`-gated
//! type alias at the top, not OS-specific logic of their own.
//!
//! Untested on real hardware as of this writing -- this sandbox is
//! Linux-only, so this file is verified with `cargo check --target
//! x86_64-apple-darwin` (real macOS `libc` bindings, real type-checking)
//! but has never actually been linked or run on macOS. Treat it as
//! reviewed-but-unverified until someone runs the test suite on real
//! hardware.

use super::{from_sockaddr, new_tcp_socket, new_udp_socket, to_sockaddr};
use platform::error::{ErrorKind, OsCode, PlatformError};
use std::mem;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

type Result<T> = platform::error::Result<T>;

fn errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

fn kind_of(errno: i32) -> ErrorKind {
    match errno {
        libc::ECONNREFUSED => ErrorKind::ConnectionRefused,
        libc::ECONNRESET => ErrorKind::ConnectionReset,
        libc::ECONNABORTED => ErrorKind::ConnectionAborted,
        libc::ENOTCONN => ErrorKind::NotConnected,
        libc::EADDRINUSE => ErrorKind::AddrInUse,
        libc::EADDRNOTAVAIL => ErrorKind::AddrNotAvailable,
        libc::ETIMEDOUT => ErrorKind::TimedOut,
        libc::EACCES | libc::EPERM => ErrorKind::PermissionDenied,
        libc::EINVAL => ErrorKind::InvalidInput,
        libc::EAGAIN => ErrorKind::WouldBlock,
        libc::EINTR => ErrorKind::Interrupted,
        _ => ErrorKind::Other,
    }
}

fn net_err(op: &'static str) -> PlatformError {
    let e = errno();
    PlatformError::new(kind_of(e), OsCode::Errno(e), op)
}

/// For failures that aren't an OS errno at all (`from_sockaddr` failing
/// because the kernel handed back an address family that's neither
/// `AF_INET` nor `AF_INET6` -- a logic error in this code's own
/// assumptions, not a syscall failure) -- unlike `net_err`, which would
/// incorrectly attribute it to whatever `errno` happens to still say
/// from some earlier, unrelated call.
fn other_err(op: &'static str) -> PlatformError {
    PlatformError::new(ErrorKind::Other, OsCode::None, op)
}

/// Converts an `io::Error` from the shared, OS-agnostic helpers in
/// `mod.rs` (`new_tcp_socket`/`new_udp_socket`, which return plain
/// `io::Result` since they're shared with Linux) into this module's
/// `PlatformError`, using the error's own `raw_os_error` rather than
/// re-reading global `errno` state (which could have been clobbered by
/// something else running between the failing call and here).
fn from_io_err(e: std::io::Error, op: &'static str) -> PlatformError {
    match e.raw_os_error() {
        Some(errno) => PlatformError::new(kind_of(errno), OsCode::Errno(errno), op),
        None => PlatformError::new(ErrorKind::Other, OsCode::None, op),
    }
}

/// Toggle `O_NONBLOCK`. Also called from `mod.rs`'s `new_socket` (every
/// socket on this backend is born non-blocking, since macOS has no
/// `SOCK_NONBLOCK` socket-type flag to do that atomically at creation).
pub(super) fn set_nonblocking(fd: RawFd, nonblocking: bool) -> Result<()> {
    // SAFETY: `fd` is caller-owned and open; `fcntl(F_GETFL)` takes no
    // variadic argument.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(net_err("fcntl(F_GETFL)"));
    }
    let new_flags = if nonblocking {
        flags | libc::O_NONBLOCK
    } else {
        flags & !libc::O_NONBLOCK
    };
    // SAFETY: `fd` is caller-owned and open; `new_flags` is a plain
    // integer, the sole variadic argument `F_SETFL` expects.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, new_flags) } < 0 {
        return Err(net_err("fcntl(F_SETFL)"));
    }
    Ok(())
}

/// Set `FD_CLOEXEC`. Also called from `mod.rs`'s `new_socket` (the
/// `SOCK_CLOEXEC` half of what Linux gets atomically at creation).
pub(super) fn set_cloexec(fd: RawFd) -> Result<()> {
    // SAFETY: `fd` is caller-owned and open.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
        return Err(net_err("fcntl(F_SETFD)"));
    }
    Ok(())
}

fn set_reuseaddr(fd: RawFd) -> Result<()> {
    let value: libc::c_int = 1;
    // SAFETY: `&value` is a valid `c_int`-sized buffer outliving the
    // call; `fd` is caller-owned and open.
    let r = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            (&value as *const libc::c_int).cast(),
            mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if r < 0 {
        return Err(net_err("setsockopt(SO_REUSEADDR)"));
    }
    Ok(())
}

fn getsockname(fd: RawFd) -> Result<SocketAddr> {
    // SAFETY: `storage`/`len` are valid, exclusively borrowed out-params
    // the kernel fills; `fd` is caller-owned and open.
    let storage = unsafe {
        let mut storage: libc::sockaddr_storage = mem::zeroed();
        let mut len = mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        let r = libc::getsockname(
            fd,
            (&mut storage as *mut libc::sockaddr_storage).cast(),
            &mut len,
        );
        if r < 0 {
            return Err(net_err("getsockname"));
        }
        storage
    };
    from_sockaddr(&storage).map_err(|_| other_err("getsockname"))
}

fn getpeername(fd: RawFd) -> Result<SocketAddr> {
    // SAFETY: see `getsockname`.
    let storage = unsafe {
        let mut storage: libc::sockaddr_storage = mem::zeroed();
        let mut len = mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        let r = libc::getpeername(
            fd,
            (&mut storage as *mut libc::sockaddr_storage).cast(),
            &mut len,
        );
        if r < 0 {
            return Err(net_err("getpeername"));
        }
        storage
    };
    from_sockaddr(&storage).map_err(|_| other_err("getpeername"))
}

/// A connected TCP stream backed by an `OwnedFd`.
pub struct MacosTcpStream {
    fd: OwnedFd,
}

impl MacosTcpStream {
    pub(crate) fn set_nonblocking(&self, nonblocking: bool) -> Result<()> {
        set_nonblocking(self.fd.as_raw_fd(), nonblocking)
    }

    pub(crate) fn set_nodelay(&self, nodelay: bool) -> Result<()> {
        let value: libc::c_int = nodelay as libc::c_int;
        // SAFETY: `&value` is a valid `c_int`-sized buffer outliving the
        // call; `fd` is caller-owned and open.
        let r = unsafe {
            libc::setsockopt(
                self.fd.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                (&value as *const libc::c_int).cast(),
                mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if r < 0 {
            return Err(net_err("setsockopt(TCP_NODELAY)"));
        }
        Ok(())
    }

    pub(crate) fn peer_addr(&self) -> Result<SocketAddr> {
        getpeername(self.fd.as_raw_fd())
    }

    pub(crate) fn local_addr(&self) -> Result<SocketAddr> {
        getsockname(self.fd.as_raw_fd())
    }
}

impl AsRawFd for MacosTcpStream {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

/// Any already-connected stream socket's fd works as a `MacosTcpStream`
/// -- mirrors `platform_linux::LinuxTcpStream`'s own `From<OwnedFd>`.
impl From<OwnedFd> for MacosTcpStream {
    fn from(fd: OwnedFd) -> Self {
        MacosTcpStream { fd }
    }
}

/// A listening TCP socket backed by an `OwnedFd`.
pub struct MacosTcpListener {
    fd: OwnedFd,
}

impl MacosTcpListener {
    /// `socket` + `SO_REUSEADDR` + `bind` + `listen`.
    pub(crate) fn bind(addr: SocketAddr) -> Result<Self> {
        let fd = new_tcp_socket(addr).map_err(|e| from_io_err(e, "socket"))?;
        set_reuseaddr(fd.as_raw_fd())?;
        let (storage, len) = to_sockaddr(addr);
        // SAFETY: `storage` holds a valid sockaddr for exactly `len`
        // bytes (`to_sockaddr`'s contract); `fd` is a valid, freshly
        // created socket.
        let r = unsafe {
            libc::bind(
                fd.as_raw_fd(),
                (&storage as *const libc::sockaddr_storage).cast(),
                len,
            )
        };
        if r < 0 {
            return Err(net_err("bind"));
        }
        // SAFETY: `fd` is a valid, bound socket.
        if unsafe { libc::listen(fd.as_raw_fd(), libc::SOMAXCONN) } < 0 {
            return Err(net_err("listen"));
        }
        Ok(MacosTcpListener { fd })
    }

    pub(crate) fn set_nonblocking(&self, nonblocking: bool) -> Result<()> {
        set_nonblocking(self.fd.as_raw_fd(), nonblocking)
    }

    /// `accept`, returning the concrete `MacosTcpStream` directly.
    /// Unlike Linux, there's no `accept4` to request `SOCK_CLOEXEC`
    /// atomically -- `FD_CLOEXEC` is set via `fcntl` right after
    /// (`set_nonblocking` is the caller's job, same as Linux's
    /// `LinuxTcpListener::accept`, since a fresh `accept`ed fd doesn't
    /// inherit the listener's own non-blocking state on any backend).
    pub(crate) fn accept(&self) -> Result<(MacosTcpStream, SocketAddr)> {
        // SAFETY: `storage`/`len` are valid, exclusively borrowed
        // out-params the kernel fills; `fd` is a valid, listening
        // socket.
        let (newfd, storage) = unsafe {
            let mut storage: libc::sockaddr_storage = mem::zeroed();
            let mut len = mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
            let newfd = libc::accept(
                self.fd.as_raw_fd(),
                (&mut storage as *mut libc::sockaddr_storage).cast(),
                &mut len,
            );
            (newfd, storage)
        };
        if newfd < 0 {
            return Err(net_err("accept"));
        }
        // SAFETY: `newfd` was just returned by `accept(2)` and is
        // valid, otherwise-unowned, and wrapped exactly once.
        let owned = unsafe { OwnedFd::from_raw_fd(newfd) };
        set_cloexec(owned.as_raw_fd())?;
        let peer = from_sockaddr(&storage).map_err(|_| other_err("accept"))?;
        Ok((MacosTcpStream { fd: owned }, peer))
    }

    pub(crate) fn local_addr(&self) -> Result<SocketAddr> {
        getsockname(self.fd.as_raw_fd())
    }
}

impl AsRawFd for MacosTcpListener {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

/// A UDP datagram socket backed by an `OwnedFd`.
pub struct MacosUdpSocket {
    fd: OwnedFd,
}

impl MacosUdpSocket {
    pub(crate) fn bind(addr: SocketAddr) -> Result<Self> {
        let fd = new_udp_socket(addr).map_err(|e| from_io_err(e, "socket"))?;
        let (storage, len) = to_sockaddr(addr);
        // SAFETY: `storage` holds a valid sockaddr for exactly `len`
        // bytes; `fd` is a valid, freshly created socket.
        let r = unsafe {
            libc::bind(
                fd.as_raw_fd(),
                (&storage as *const libc::sockaddr_storage).cast(),
                len,
            )
        };
        if r < 0 {
            return Err(net_err("bind"));
        }
        Ok(MacosUdpSocket { fd })
    }

    pub(crate) fn set_nonblocking(&self, nonblocking: bool) -> Result<()> {
        set_nonblocking(self.fd.as_raw_fd(), nonblocking)
    }

    pub(crate) fn send_to(&self, buf: &[u8], addr: SocketAddr) -> Result<usize> {
        let (storage, len) = to_sockaddr(addr);
        // SAFETY: `buf` is valid for `buf.len()` bytes; `storage` holds
        // a valid sockaddr for exactly `len` bytes; `fd` is
        // caller-owned.
        let n = unsafe {
            libc::sendto(
                self.fd.as_raw_fd(),
                buf.as_ptr().cast(),
                buf.len(),
                0,
                (&storage as *const libc::sockaddr_storage).cast(),
                len,
            )
        };
        if n < 0 {
            return Err(net_err("sendto"));
        }
        Ok(n as usize)
    }

    pub(crate) fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        // SAFETY: `storage`/`len` are valid, exclusively borrowed
        // out-params the kernel fills; `buf` is valid for `buf.len()`
        // bytes; `fd` is caller-owned.
        let (n, storage) = unsafe {
            let mut storage: libc::sockaddr_storage = mem::zeroed();
            let mut len = mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
            let n = libc::recvfrom(
                self.fd.as_raw_fd(),
                buf.as_mut_ptr().cast(),
                buf.len(),
                0,
                (&mut storage as *mut libc::sockaddr_storage).cast(),
                &mut len,
            );
            (n, storage)
        };
        if n < 0 {
            return Err(net_err("recvfrom"));
        }
        let peer = from_sockaddr(&storage).map_err(|_| other_err("recvfrom"))?;
        Ok((n as usize, peer))
    }

    pub(crate) fn local_addr(&self) -> Result<SocketAddr> {
        getsockname(self.fd.as_raw_fd())
    }
}

impl AsRawFd for MacosUdpSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}
