//! Raw, non-blocking socket syscalls. Every socket this runtime hands
//! out is created here with `SOCK_NONBLOCK` from birth -- there is no
//! "make it non-blocking after the fact" step, which sidesteps a whole
//! class of races where a caller forgets to flip that switch.

use libc::{c_int, sockaddr, sockaddr_in, sockaddr_in6, sockaddr_storage, socklen_t};
use std::io;
use std::mem;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
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
    // `ss_family` (written below) is ever read back by the kernel or by
    // `from_sockaddr`.
    let mut storage: sockaddr_storage = unsafe { mem::zeroed() };
    let len = match addr {
        SocketAddr::V4(v4) => {
            let sin = sockaddr_in {
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: v4.port().to_be(),
                sin_addr: libc::in_addr {
                    s_addr: u32::from_ne_bytes(v4.ip().octets()),
                },
                sin_zero: [0; 8],
            };
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
            let sin6 = sockaddr_in6 {
                sin6_family: libc::AF_INET6 as libc::sa_family_t,
                sin6_port: v6.port().to_be(),
                sin6_flowinfo: v6.flowinfo(),
                sin6_addr: libc::in6_addr {
                    s6_addr: v6.ip().octets(),
                },
                sin6_scope_id: v6.scope_id(),
            };
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

fn from_sockaddr(storage: &sockaddr_storage) -> io::Result<SocketAddr> {
    match storage.ss_family as c_int {
        libc::AF_INET => {
            // SAFETY: `ss_family == AF_INET` means the kernel filled this
            // buffer as a `sockaddr_in`, which fits within
            // `sockaddr_storage`; reading it back that way mirrors what
            // `to_sockaddr` wrote.
            let sin = unsafe { &*(storage as *const sockaddr_storage).cast::<sockaddr_in>() };
            let ip = Ipv4Addr::from(sin.sin_addr.s_addr.to_ne_bytes());
            Ok(SocketAddr::V4(SocketAddrV4::new(
                ip,
                u16::from_be(sin.sin_port),
            )))
        }
        libc::AF_INET6 => {
            // SAFETY: see the V4 arm above, for `sockaddr_in6`.
            let sin6 = unsafe { &*(storage as *const sockaddr_storage).cast::<sockaddr_in6>() };
            let ip = Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            Ok(SocketAddr::V6(SocketAddrV6::new(
                ip,
                u16::from_be(sin6.sin6_port),
                sin6.sin6_flowinfo,
                sin6.sin6_scope_id,
            )))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unrecognized address family",
        )),
    }
}

pub(crate) fn new_tcp_socket(addr: SocketAddr) -> io::Result<OwnedFd> {
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

pub(crate) fn new_udp_socket(addr: SocketAddr) -> io::Result<OwnedFd> {
    // SAFETY: see `new_tcp_socket`.
    let fd = unsafe {
        libc::socket(
            domain_for(addr),
            libc::SOCK_DGRAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
            0,
        )
    };
    cvt(fd)?;
    // SAFETY: see `new_tcp_socket`.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

pub(crate) fn set_reuseaddr(fd: RawFd) -> io::Result<()> {
    let value: c_int = 1;
    // SAFETY: `&value` is a valid `c_int`-sized buffer outliving the
    // call; `fd` is caller-owned and still open.
    cvt(unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            (&value as *const c_int).cast(),
            mem::size_of::<c_int>() as socklen_t,
        )
    })?;
    Ok(())
}

pub(crate) fn bind(fd: RawFd, addr: SocketAddr) -> io::Result<()> {
    let (storage, len) = to_sockaddr(addr);
    // SAFETY: `storage` holds a valid sockaddr for exactly `len` bytes
    // (`to_sockaddr`'s contract); `fd` is a valid, freshly created
    // socket.
    cvt(unsafe {
        libc::bind(
            fd,
            (&storage as *const sockaddr_storage).cast::<sockaddr>(),
            len,
        )
    })?;
    Ok(())
}

pub(crate) fn listen(fd: RawFd, backlog: c_int) -> io::Result<()> {
    // SAFETY: `fd` is a valid, bound socket.
    cvt(unsafe { libc::listen(fd, backlog) })?;
    Ok(())
}

/// `connect(2)` on a non-blocking socket returns `EINPROGRESS`
/// immediately rather than blocking -- that is success from this
/// function's point of view; the caller waits for the socket to become
/// writable and then calls [`take_socket_error`] to find out whether the
/// connection actually succeeded.
pub(crate) fn connect(fd: RawFd, addr: SocketAddr) -> io::Result<()> {
    let (storage, len) = to_sockaddr(addr);
    // SAFETY: see `bind`.
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
/// succeeded, or failed asynchronously (e.g. connection refused).
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

pub(crate) fn accept(fd: RawFd) -> io::Result<(OwnedFd, SocketAddr)> {
    // SAFETY: `storage`/`len` are valid, exclusively borrowed out-params
    // the kernel fills; `fd` is a valid, listening socket.
    let (newfd, storage) = unsafe {
        let mut storage: sockaddr_storage = mem::zeroed();
        let mut len = mem::size_of::<sockaddr_storage>() as socklen_t;
        let newfd = libc::accept4(
            fd,
            (&mut storage as *mut sockaddr_storage).cast::<sockaddr>(),
            &mut len,
            libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
        );
        (newfd, storage)
    };
    cvt(newfd)?;
    // SAFETY: `newfd` was just returned by `accept4(2)` and is valid,
    // otherwise-unowned, and wrapped exactly once.
    let owned = unsafe { OwnedFd::from_raw_fd(newfd) };
    let peer = from_sockaddr(&storage)?;
    Ok((owned, peer))
}

pub(crate) fn local_addr(fd: RawFd) -> io::Result<SocketAddr> {
    // SAFETY: see `accept`.
    let storage = unsafe {
        let mut storage: sockaddr_storage = mem::zeroed();
        let mut len = mem::size_of::<sockaddr_storage>() as socklen_t;
        cvt(libc::getsockname(
            fd,
            (&mut storage as *mut sockaddr_storage).cast::<sockaddr>(),
            &mut len,
        ))?;
        storage
    };
    from_sockaddr(&storage)
}

pub(crate) fn peer_addr(fd: RawFd) -> io::Result<SocketAddr> {
    // SAFETY: see `accept`.
    let storage = unsafe {
        let mut storage: sockaddr_storage = mem::zeroed();
        let mut len = mem::size_of::<sockaddr_storage>() as socklen_t;
        cvt(libc::getpeername(
            fd,
            (&mut storage as *mut sockaddr_storage).cast::<sockaddr>(),
            &mut len,
        ))?;
        storage
    };
    from_sockaddr(&storage)
}

pub(crate) fn set_nodelay(fd: RawFd, nodelay: bool) -> io::Result<()> {
    let value: c_int = nodelay as c_int;
    // SAFETY: see `set_reuseaddr`.
    cvt(unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            (&value as *const c_int).cast(),
            mem::size_of::<c_int>() as socklen_t,
        )
    })?;
    Ok(())
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

pub(crate) fn send_to(fd: RawFd, buf: &[u8], addr: SocketAddr) -> io::Result<usize> {
    let (storage, len) = to_sockaddr(addr);
    // SAFETY: `buf` is valid for `buf.len()` bytes; `storage` holds a
    // valid sockaddr for exactly `len` bytes; `fd` is caller-owned.
    let n = unsafe {
        libc::sendto(
            fd,
            buf.as_ptr().cast(),
            buf.len(),
            0,
            (&storage as *const sockaddr_storage).cast::<sockaddr>(),
            len,
        )
    };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

pub(crate) fn recv_from(fd: RawFd, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
    // SAFETY: `storage`/`len` are valid, exclusively borrowed out-params
    // the kernel fills; `buf` is valid for `buf.len()` bytes; `fd` is
    // caller-owned.
    let (n, storage) = unsafe {
        let mut storage: sockaddr_storage = mem::zeroed();
        let mut len = mem::size_of::<sockaddr_storage>() as socklen_t;
        let n = libc::recvfrom(
            fd,
            buf.as_mut_ptr().cast(),
            buf.len(),
            0,
            (&mut storage as *mut sockaddr_storage).cast::<sockaddr>(),
            &mut len,
        );
        (n, storage)
    };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    let peer = from_sockaddr(&storage)?;
    Ok((n as usize, peer))
}
