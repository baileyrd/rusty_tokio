//! The Windows winsock2 socket layer.
//!
//! Unlike Linux/macOS (`platform_linux`/`platform_macos`, both extended
//! upstream in rustils specifically for this crate's reactor -- the
//! non-blocking toggle, `AsRawSocket`-equivalent access, `From<OwnedSocket>`
//! adoption; see `socket/mod.rs`'s own docs), rustils' `platform-windows`
//! crate predates that work and has no equivalent surface: its net module
//! only ever hands out `Box<dyn TcpStream>`-style trait objects with no
//! non-blocking toggle and no way back to a concrete, `AsRawSocket`-able
//! type. Depending on it here would mean immediately hand-rolling the
//! exact same missing pieces on top of it anyway, so this module skips
//! the extra layer and goes straight to `windows-sys` -- Microsoft's own
//! low-level FFI bindings, the same crate mio itself depends on for its
//! entire Windows backend (including the AFD-poll protocol
//! `io::reactor::windows` implements against the identical dependency).
//!
//! Two things work differently here than on the POSIX side:
//!
//! - **Non-blocking `connect`.** A non-blocking Winsock `connect` returns
//!   `SOCKET_ERROR` with `WSAEWOULDBLOCK` immediately rather than
//!   blocking for the connection's round trip -- Winsock's spelling of
//!   POSIX's `EINPROGRESS` -- and the reactor waits for writability the
//!   same way `socket/mod.rs`'s own `connect` documents.
//! - **`WSAStartup`.** Winsock needs one process-wide `WSAStartup` call
//!   before any other socket function works; done lazily, once, via
//!   [`std::sync::Once`] the first time this crate creates a socket.
//!
//! `SO_REUSEPORT` has no Windows equivalent at all -- `SO_REUSEADDR`
//! there already covers a strict superset of it (including letting two
//! *different*, unrelated processes bind the exact same address
//! simultaneously, not just a `TIME_WAIT` leftover, which POSIX callers
//! would find surprising) -- [`set_reuseport`]/[`reuseport`] below reuse
//! `SO_REUSEADDR` as the closest available primitive, the same pragmatic
//! choice most cross-platform networking libraries make.

use platform::error::{ErrorKind as PKind, OsCode, PlatformError, Result as PResult};
use std::io;
use std::mem;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::windows::io::{
    AsRawSocket, AsSocket, BorrowedSocket, FromRawSocket, OwnedSocket, RawSocket,
};
use std::sync::Once;
use std::time::Duration;

use windows_sys::Win32::Networking::WinSock::{
    self, IN_ADDR_0_0, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_STORAGE, SOCKET, WSADATA,
};

/// One-time, process-wide Winsock init -- every socket-creating function
/// below calls this first.
fn wsa_init() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let mut data: WSADATA = unsafe { mem::zeroed() };
        // 2.2 has been Winsock's only meaningfully deployed version for
        // over two decades; every OS this target actually runs on
        // supports it.
        //
        // SAFETY: `&mut data` is a valid, exclusively borrowed out-param
        // for the call's duration.
        let r = unsafe { WinSock::WSAStartup(0x0202, &mut data) };
        assert_eq!(r, 0, "WSAStartup failed with error {r}");
    });
}

fn domain_for(addr: SocketAddr) -> i32 {
    match addr {
        SocketAddr::V4(_) => WinSock::AF_INET as i32,
        SocketAddr::V6(_) => WinSock::AF_INET6 as i32,
    }
}

fn to_sockaddr(addr: SocketAddr) -> (SOCKADDR_STORAGE, i32) {
    // SAFETY: an all-zero `SOCKADDR_STORAGE` is a valid (if inert) value
    // for this plain-old-data type; only the variant selected by
    // `ss_family` (written below) is ever read back by the kernel.
    let mut storage: SOCKADDR_STORAGE = unsafe { mem::zeroed() };
    let len = match addr {
        SocketAddr::V4(v4) => {
            let mut sin: SOCKADDR_IN = unsafe { mem::zeroed() };
            sin.sin_family = WinSock::AF_INET;
            sin.sin_port = v4.port().to_be();
            let o = v4.ip().octets();
            // Written through the byte-wise union member rather than the
            // `S_addr: u32` one -- this sidesteps any question of which
            // byte order a plain integer assignment would need, the same
            // way `Ipv4Addr::octets()` already hands back memory-order
            // bytes on the POSIX side.
            sin.sin_addr.S_un.S_un_b = IN_ADDR_0_0 {
                s_b1: o[0],
                s_b2: o[1],
                s_b3: o[2],
                s_b4: o[3],
            };
            // SAFETY: `storage` is large enough and suitably aligned for
            // any sockaddr variant (that's `SOCKADDR_STORAGE`'s purpose);
            // writing a `SOCKADDR_IN` to its start and reading it back
            // that way is exactly how the kernel itself treats the
            // buffer once `ss_family` says `AF_INET`.
            unsafe {
                std::ptr::write(
                    (&mut storage as *mut SOCKADDR_STORAGE).cast::<SOCKADDR_IN>(),
                    sin,
                );
            }
            mem::size_of::<SOCKADDR_IN>()
        }
        SocketAddr::V6(v6) => {
            let mut sin6: SOCKADDR_IN6 = unsafe { mem::zeroed() };
            sin6.sin6_family = WinSock::AF_INET6;
            sin6.sin6_port = v6.port().to_be();
            sin6.sin6_flowinfo = v6.flowinfo();
            sin6.sin6_addr.u.Byte = v6.ip().octets();
            sin6.Anonymous.sin6_scope_id = v6.scope_id();
            // SAFETY: see the V4 arm above.
            unsafe {
                std::ptr::write(
                    (&mut storage as *mut SOCKADDR_STORAGE).cast::<SOCKADDR_IN6>(),
                    sin6,
                );
            }
            mem::size_of::<SOCKADDR_IN6>()
        }
    };
    (storage, len as i32)
}

/// The reverse of [`to_sockaddr`] -- used after `accept`/`getsockname`/
/// `getpeername`/`recvfrom`, all of which hand a filled-in
/// `SOCKADDR_STORAGE` back from the kernel.
///
/// # Safety
/// `storage.ss_family` must actually match the variant the kernel wrote
/// (always true for a `SOCKADDR_STORAGE` the kernel itself just filled
/// in, which is the only caller of this function).
unsafe fn from_sockaddr(storage: &SOCKADDR_STORAGE) -> io::Result<SocketAddr> {
    match storage.ss_family {
        WinSock::AF_INET => {
            // SAFETY: `storage.ss_family == AF_INET`, the caller's
            // contract for this whole function.
            let sin = unsafe { &*(storage as *const SOCKADDR_STORAGE).cast::<SOCKADDR_IN>() };
            let port = u16::from_be(sin.sin_port);
            // SAFETY: `S_un_b` was the last (and only) member written by
            // whichever kernel call filled in `storage`.
            let b = unsafe { sin.sin_addr.S_un.S_un_b };
            Ok(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::new(b.s_b1, b.s_b2, b.s_b3, b.s_b4),
                port,
            )))
        }
        WinSock::AF_INET6 => {
            // SAFETY: `storage.ss_family == AF_INET6`, the caller's
            // contract for this whole function.
            let sin6 = unsafe { &*(storage as *const SOCKADDR_STORAGE).cast::<SOCKADDR_IN6>() };
            let port = u16::from_be(sin6.sin6_port);
            // SAFETY: `Byte` and `sin6_scope_id` are plain, always-valid
            // reinterpretations of an address the kernel just filled in.
            let ip = Ipv6Addr::from(unsafe { sin6.sin6_addr.u.Byte });
            let scope_id = unsafe { sin6.Anonymous.sin6_scope_id };
            Ok(SocketAddr::V6(SocketAddrV6::new(
                ip,
                port,
                sin6.sin6_flowinfo,
                scope_id,
            )))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "kernel returned an address family other than AF_INET/AF_INET6",
        )),
    }
}

fn wsa_error() -> io::Error {
    // SAFETY: no arguments; reads the calling thread's last-error slot,
    // which `WSAStartup` guarantees is populated after any failed
    // Winsock call.
    io::Error::from_raw_os_error(unsafe { WinSock::WSAGetLastError() })
}

/// A bare, non-blocking socket -- not yet bound or connected. Split out
/// from `connect`/`bind` (mirroring `socket/mod.rs`'s own
/// `new_tcp_socket`) so an async `connect` can flip the socket
/// non-blocking *before* `connect(2)` -- sorry, `connect` -- ever runs.
pub(crate) fn new_tcp_socket(addr: SocketAddr) -> io::Result<OwnedSocket> {
    wsa_init();
    // SAFETY: plain integer arguments, no memory referenced.
    let sock =
        unsafe { WinSock::socket(domain_for(addr), WinSock::SOCK_STREAM, WinSock::IPPROTO_TCP) };
    if sock == WinSock::INVALID_SOCKET {
        return Err(wsa_error());
    }
    // SAFETY: `sock` was just returned by `socket()` and is valid,
    // otherwise-unowned, and wrapped exactly once.
    let owned = unsafe { OwnedSocket::from_raw_socket(sock as RawSocket) };
    set_nonblocking(owned.as_raw_socket(), true)?;
    Ok(owned)
}

/// The UDP counterpart of [`new_tcp_socket`], already bound (UDP has no
/// separate "bare socket" stage worth exposing -- nothing else needs to
/// run in between, unlike TCP's non-blocking-connect dance).
pub(crate) fn new_udp_socket(addr: SocketAddr) -> io::Result<OwnedSocket> {
    wsa_init();
    // SAFETY: plain integer arguments, no memory referenced.
    let sock =
        unsafe { WinSock::socket(domain_for(addr), WinSock::SOCK_DGRAM, WinSock::IPPROTO_UDP) };
    if sock == WinSock::INVALID_SOCKET {
        return Err(wsa_error());
    }
    // SAFETY: `sock` was just returned by `socket()` and is valid,
    // otherwise-unowned, and wrapped exactly once.
    let owned = unsafe { OwnedSocket::from_raw_socket(sock as RawSocket) };
    set_nonblocking(owned.as_raw_socket(), true)?;
    bind(owned.as_raw_socket(), addr)?;
    Ok(owned)
}

/// `connect` on a non-blocking socket returns `WSAEWOULDBLOCK`
/// immediately rather than blocking -- that is success from this
/// function's point of view, exactly mirroring `socket/mod.rs`'s own
/// `connect` (POSIX's `EINPROGRESS`); the caller waits for the socket to
/// become writable and then calls [`take_socket_error`] to find out
/// whether the connection actually succeeded.
pub(crate) fn connect(sock: RawSocket, addr: SocketAddr) -> io::Result<()> {
    let (storage, len) = to_sockaddr(addr);
    // SAFETY: `storage` holds a valid sockaddr for exactly `len` bytes
    // (`to_sockaddr`'s contract); `sock` is a valid, freshly created,
    // still-unconnected socket.
    let r = unsafe {
        WinSock::connect(
            sock as SOCKET,
            (&storage as *const SOCKADDR_STORAGE).cast::<SOCKADDR>(),
            len,
        )
    };
    if r == 0 {
        return Ok(());
    }
    // SAFETY: reads the calling thread's last-error slot right after a
    // failed Winsock call.
    let code = unsafe { WinSock::WSAGetLastError() };
    if code == WinSock::WSAEWOULDBLOCK {
        return Ok(());
    }
    Err(io::Error::from_raw_os_error(code))
}

/// `SO_ERROR` -- the standard way to learn whether a non-blocking
/// `connect` that just became writable actually succeeded, or failed
/// asynchronously (e.g. connection refused). Collapses [`take_error`]'s
/// `Option` into the pending error itself -- see that function's own
/// docs for the reasoning, identical here.
pub(crate) fn take_socket_error(sock: RawSocket) -> io::Result<()> {
    match take_error(sock)? {
        None => Ok(()),
        Some(err) => Err(err),
    }
}

/// The reverse framing of [`take_socket_error`]: `Ok(None)` if there's
/// no pending socket error, `Ok(Some(err))` if there was one (reading
/// `SO_ERROR` clears it), `Err(..)` only if `getsockopt` itself failed
/// outright.
pub(crate) fn take_error(sock: RawSocket) -> io::Result<Option<io::Error>> {
    let err = getsockopt_int(sock, WinSock::SOL_SOCKET, WinSock::SO_ERROR)?;
    if err == 0 {
        Ok(None)
    } else {
        Ok(Some(io::Error::from_raw_os_error(err)))
    }
}

pub(crate) fn read(sock: RawSocket, buf: &mut [u8]) -> io::Result<usize> {
    let len = buf.len().min(i32::MAX as usize) as i32;
    // SAFETY: `buf` is valid for `buf.len()` bytes for the call's
    // duration; `sock` is caller-owned and open.
    let n = unsafe { WinSock::recv(sock as SOCKET, buf.as_mut_ptr().cast(), len, 0) };
    if n == WinSock::SOCKET_ERROR {
        Err(wsa_error())
    } else {
        Ok(n as usize)
    }
}

pub(crate) fn write(sock: RawSocket, buf: &[u8]) -> io::Result<usize> {
    let len = buf.len().min(i32::MAX as usize) as i32;
    // SAFETY: `buf` is valid for `buf.len()` bytes for the call's
    // duration; `sock` is caller-owned and open.
    let n = unsafe { WinSock::send(sock as SOCKET, buf.as_ptr().cast(), len, 0) };
    if n == WinSock::SOCKET_ERROR {
        Err(wsa_error())
    } else {
        Ok(n as usize)
    }
}

/// Genuine scatter-read via `WSARecv` -- fills as many of `bufs` as one
/// kernel call returns data for, rather than the "just the first
/// buffer" shortcut a naive vectored read might take.
pub(crate) fn readv(sock: RawSocket, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
    let mut received: u32 = 0;
    let mut flags: u32 = 0;
    let count = bufs.len().min(u32::MAX as usize) as u32;
    // SAFETY: `IoSliceMut` is guaranteed to have the same memory layout
    // as `WSABUF` on Windows (the whole reason it exists); `bufs` is
    // valid for `count` entries for the call's duration; `sock` is
    // caller-owned and open. `lpOverlapped`/the completion routine are
    // both null/`None` -- this is a plain synchronous call on an
    // already-non-blocking socket, the same shape `read`/`write` above
    // already use.
    let r = unsafe {
        WinSock::WSARecv(
            sock as SOCKET,
            bufs.as_mut_ptr().cast(),
            count,
            &mut received,
            &mut flags,
            std::ptr::null_mut(),
            None,
        )
    };
    if r == WinSock::SOCKET_ERROR {
        Err(wsa_error())
    } else {
        Ok(received as usize)
    }
}

/// The write-side counterpart of [`readv`], via `WSASend`.
pub(crate) fn writev(sock: RawSocket, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
    let mut sent: u32 = 0;
    let count = bufs.len().min(u32::MAX as usize) as u32;
    // SAFETY: see `readv` above -- `IoSlice` has the identical guarantee.
    // `WSASend` doesn't mutate the buffers, but `WSABUF` has no
    // const/mut distinction, hence the cast.
    let r = unsafe {
        WinSock::WSASend(
            sock as SOCKET,
            bufs.as_ptr().cast_mut().cast(),
            count,
            &mut sent,
            0,
            std::ptr::null_mut(),
            None,
        )
    };
    if r == WinSock::SOCKET_ERROR {
        Err(wsa_error())
    } else {
        Ok(sent as usize)
    }
}

/// Like [`read`], but via `recv` with `MSG_PEEK` -- the bytes stay in
/// the socket's receive queue for the next real `recv` call.
pub(crate) fn peek(sock: RawSocket, buf: &mut [u8]) -> io::Result<usize> {
    let len = buf.len().min(i32::MAX as usize) as i32;
    // SAFETY: `buf` is valid for `buf.len()` bytes for the call's
    // duration; `sock` is caller-owned and open.
    let n = unsafe {
        WinSock::recv(
            sock as SOCKET,
            buf.as_mut_ptr().cast(),
            len,
            WinSock::MSG_PEEK,
        )
    };
    if n == WinSock::SOCKET_ERROR {
        Err(wsa_error())
    } else {
        Ok(n as usize)
    }
}

pub(crate) fn udp_send_to(sock: RawSocket, buf: &[u8], addr: SocketAddr) -> io::Result<usize> {
    let (storage, len) = to_sockaddr(addr);
    let buf_len = buf.len().min(i32::MAX as usize) as i32;
    // SAFETY: `buf` is valid for `buf.len()` bytes for the call's
    // duration; `storage` holds a valid sockaddr for exactly `len` bytes;
    // `sock` is caller-owned and open.
    let n = unsafe {
        WinSock::sendto(
            sock as SOCKET,
            buf.as_ptr().cast(),
            buf_len,
            0,
            (&storage as *const SOCKADDR_STORAGE).cast::<SOCKADDR>(),
            len,
        )
    };
    if n == WinSock::SOCKET_ERROR {
        Err(wsa_error())
    } else {
        Ok(n as usize)
    }
}

pub(crate) fn udp_recv_from(sock: RawSocket, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
    // SAFETY: an all-zero `SOCKADDR_STORAGE` is a valid (if inert) value.
    let mut storage: SOCKADDR_STORAGE = unsafe { mem::zeroed() };
    let mut fromlen = mem::size_of::<SOCKADDR_STORAGE>() as i32;
    let buf_len = buf.len().min(i32::MAX as usize) as i32;
    // SAFETY: `buf` is valid for `buf.len()` bytes for the call's
    // duration; `&mut storage`/`&mut fromlen` are valid, exclusively
    // borrowed out-params; `sock` is caller-owned and open.
    let n = unsafe {
        WinSock::recvfrom(
            sock as SOCKET,
            buf.as_mut_ptr().cast(),
            buf_len,
            0,
            (&mut storage as *mut SOCKADDR_STORAGE).cast::<SOCKADDR>(),
            &mut fromlen,
        )
    };
    if n == WinSock::SOCKET_ERROR {
        return Err(wsa_error());
    }
    // SAFETY: the kernel just filled `storage` in via the call above.
    let peer = unsafe { from_sockaddr(&storage) }?;
    Ok((n as usize, peer))
}

/// Like [`udp_recv_from`], but via `recvfrom` with `MSG_PEEK` -- reports
/// the next datagram's length and sender without dequeuing it.
pub(crate) fn peek_from(sock: RawSocket, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
    // SAFETY: an all-zero `SOCKADDR_STORAGE` is a valid (if inert) value.
    let mut storage: SOCKADDR_STORAGE = unsafe { mem::zeroed() };
    let mut fromlen = mem::size_of::<SOCKADDR_STORAGE>() as i32;
    let buf_len = buf.len().min(i32::MAX as usize) as i32;
    // SAFETY: `buf` is valid for `buf.len()` bytes for the call's
    // duration; `&mut storage`/`&mut fromlen` are valid, exclusively
    // borrowed out-params; `sock` is caller-owned and open.
    let n = unsafe {
        WinSock::recvfrom(
            sock as SOCKET,
            buf.as_mut_ptr().cast(),
            buf_len,
            WinSock::MSG_PEEK,
            (&mut storage as *mut SOCKADDR_STORAGE).cast::<SOCKADDR>(),
            &mut fromlen,
        )
    };
    if n == WinSock::SOCKET_ERROR {
        return Err(wsa_error());
    }
    // SAFETY: the kernel just filled `storage` in via the call above.
    let peer = unsafe { from_sockaddr(&storage) }?;
    Ok((n as usize, peer))
}

/// Like [`peek_from`], but only the sender's address matters -- a
/// zero-length peek still reports the next datagram's source, without
/// needing any buffer to receive data into.
pub(crate) fn peek_sender(sock: RawSocket) -> io::Result<SocketAddr> {
    let (_n, addr) = peek_from(sock, &mut [])?;
    Ok(addr)
}

/// `SD_SEND` -- backs `AsyncWrite::poll_shutdown`, signaling EOF to the
/// peer without closing the socket itself (that still happens on
/// `Drop`).
pub(crate) fn shutdown_write(sock: RawSocket) -> io::Result<()> {
    // SAFETY: `sock` is caller-owned and open.
    if unsafe { WinSock::shutdown(sock as SOCKET, WinSock::SD_SEND) } == WinSock::SOCKET_ERROR {
        Err(wsa_error())
    } else {
        Ok(())
    }
}

pub(crate) fn bind(sock: RawSocket, addr: SocketAddr) -> io::Result<()> {
    let (storage, len) = to_sockaddr(addr);
    // SAFETY: `storage` holds a valid sockaddr for exactly `len` bytes;
    // `sock` is caller-owned and not yet bound.
    let r = unsafe {
        WinSock::bind(
            sock as SOCKET,
            (&storage as *const SOCKADDR_STORAGE).cast::<SOCKADDR>(),
            len,
        )
    };
    if r == WinSock::SOCKET_ERROR {
        Err(wsa_error())
    } else {
        Ok(())
    }
}

pub(crate) fn listen(sock: RawSocket, backlog: u32) -> io::Result<()> {
    let backlog = backlog.min(i32::MAX as u32) as i32;
    // SAFETY: `sock` is caller-owned and already bound.
    if unsafe { WinSock::listen(sock as SOCKET, backlog) } == WinSock::SOCKET_ERROR {
        Err(wsa_error())
    } else {
        Ok(())
    }
}

pub(crate) fn local_addr(sock: RawSocket) -> io::Result<SocketAddr> {
    // SAFETY: an all-zero `SOCKADDR_STORAGE` is a valid (if inert) value.
    let mut storage: SOCKADDR_STORAGE = unsafe { mem::zeroed() };
    let mut len = mem::size_of::<SOCKADDR_STORAGE>() as i32;
    // SAFETY: `&mut storage`/`&mut len` are valid, exclusively borrowed
    // out-params; `sock` is caller-owned.
    let r = unsafe {
        WinSock::getsockname(
            sock as SOCKET,
            (&mut storage as *mut SOCKADDR_STORAGE).cast::<SOCKADDR>(),
            &mut len,
        )
    };
    if r == WinSock::SOCKET_ERROR {
        return Err(wsa_error());
    }
    // SAFETY: the kernel just filled `storage` in via the call above.
    unsafe { from_sockaddr(&storage) }
}

pub(crate) fn peer_addr(sock: RawSocket) -> io::Result<SocketAddr> {
    // SAFETY: an all-zero `SOCKADDR_STORAGE` is a valid (if inert) value.
    let mut storage: SOCKADDR_STORAGE = unsafe { mem::zeroed() };
    let mut len = mem::size_of::<SOCKADDR_STORAGE>() as i32;
    // SAFETY: `&mut storage`/`&mut len` are valid, exclusively borrowed
    // out-params; `sock` is caller-owned and connected.
    let r = unsafe {
        WinSock::getpeername(
            sock as SOCKET,
            (&mut storage as *mut SOCKADDR_STORAGE).cast::<SOCKADDR>(),
            &mut len,
        )
    };
    if r == WinSock::SOCKET_ERROR {
        return Err(wsa_error());
    }
    // SAFETY: the kernel just filled `storage` in via the call above.
    unsafe { from_sockaddr(&storage) }
}

fn setsockopt_int(sock: RawSocket, level: i32, name: i32, value: i32) -> io::Result<()> {
    // SAFETY: `&value` is a valid `i32` the kernel only reads for the
    // call's duration; `sock` is caller-owned.
    let r = unsafe {
        WinSock::setsockopt(
            sock as SOCKET,
            level,
            name,
            (&value as *const i32).cast(),
            mem::size_of::<i32>() as i32,
        )
    };
    if r == WinSock::SOCKET_ERROR {
        Err(wsa_error())
    } else {
        Ok(())
    }
}

fn getsockopt_int(sock: RawSocket, level: i32, name: i32) -> io::Result<i32> {
    let mut value: i32 = 0;
    let mut len = mem::size_of::<i32>() as i32;
    // SAFETY: `&mut value`/`&mut len` are valid, exclusively borrowed
    // out-params; `sock` is caller-owned.
    let r = unsafe {
        WinSock::getsockopt(
            sock as SOCKET,
            level,
            name,
            (&mut value as *mut i32).cast(),
            &mut len,
        )
    };
    if r == WinSock::SOCKET_ERROR {
        Err(wsa_error())
    } else {
        Ok(value)
    }
}

pub(crate) fn set_reuseaddr(sock: RawSocket, reuse: bool) -> io::Result<()> {
    setsockopt_int(
        sock,
        WinSock::SOL_SOCKET,
        WinSock::SO_REUSEADDR,
        reuse as i32,
    )
}

pub(crate) fn reuseaddr(sock: RawSocket) -> io::Result<bool> {
    Ok(getsockopt_int(sock, WinSock::SOL_SOCKET, WinSock::SO_REUSEADDR)? != 0)
}

/// See this module's docs: Windows has no distinct `SO_REUSEPORT`, so
/// this reuses `SO_REUSEADDR` -- a strict superset of the POSIX option's
/// behavior, not an exact match, but the closest available primitive.
pub(crate) fn set_reuseport(sock: RawSocket, reuse: bool) -> io::Result<()> {
    set_reuseaddr(sock, reuse)
}

pub(crate) fn reuseport(sock: RawSocket) -> io::Result<bool> {
    reuseaddr(sock)
}

pub(crate) fn set_send_buffer_size(sock: RawSocket, size: u32) -> io::Result<()> {
    setsockopt_int(
        sock,
        WinSock::SOL_SOCKET,
        WinSock::SO_SNDBUF,
        size.min(i32::MAX as u32) as i32,
    )
}

pub(crate) fn send_buffer_size(sock: RawSocket) -> io::Result<u32> {
    Ok(getsockopt_int(sock, WinSock::SOL_SOCKET, WinSock::SO_SNDBUF)? as u32)
}

pub(crate) fn set_recv_buffer_size(sock: RawSocket, size: u32) -> io::Result<()> {
    setsockopt_int(
        sock,
        WinSock::SOL_SOCKET,
        WinSock::SO_RCVBUF,
        size.min(i32::MAX as u32) as i32,
    )
}

pub(crate) fn recv_buffer_size(sock: RawSocket) -> io::Result<u32> {
    Ok(getsockopt_int(sock, WinSock::SOL_SOCKET, WinSock::SO_RCVBUF)? as u32)
}

pub(crate) fn set_nodelay(sock: RawSocket, nodelay: bool) -> io::Result<()> {
    setsockopt_int(
        sock,
        WinSock::IPPROTO_TCP,
        WinSock::TCP_NODELAY,
        nodelay as i32,
    )
}

pub(crate) fn nodelay(sock: RawSocket) -> io::Result<bool> {
    Ok(getsockopt_int(sock, WinSock::IPPROTO_TCP, WinSock::TCP_NODELAY)? != 0)
}

pub(crate) fn set_keepalive(sock: RawSocket, keepalive: bool) -> io::Result<()> {
    setsockopt_int(
        sock,
        WinSock::SOL_SOCKET,
        WinSock::SO_KEEPALIVE,
        keepalive as i32,
    )
}

pub(crate) fn keepalive(sock: RawSocket) -> io::Result<bool> {
    Ok(getsockopt_int(sock, WinSock::SOL_SOCKET, WinSock::SO_KEEPALIVE)? != 0)
}

/// `SO_LINGER` -- like the POSIX side, the option value is a struct
/// (`WinSock::LINGER { l_onoff, l_linger }`, both `u16` here rather than
/// POSIX's `c_int`), not a plain int, so this bypasses
/// `setsockopt_int`/`getsockopt_int`. `d`'s whole-second count is
/// truncated (not rounded) and capped at `u16::MAX` seconds -- Winsock's
/// own field width, narrower than POSIX's `c_int`.
pub(crate) fn set_linger(sock: RawSocket, linger: Option<Duration>) -> io::Result<()> {
    let value = WinSock::LINGER {
        l_onoff: linger.is_some() as u16,
        l_linger: linger.map_or(0, |d| d.as_secs().min(u16::MAX as u64) as u16),
    };
    // SAFETY: `&value` is a valid `WinSock::LINGER` the kernel only
    // reads for the call's duration; `sock` is caller-owned.
    let r = unsafe {
        WinSock::setsockopt(
            sock as SOCKET,
            WinSock::SOL_SOCKET,
            WinSock::SO_LINGER,
            (&value as *const WinSock::LINGER).cast(),
            mem::size_of::<WinSock::LINGER>() as i32,
        )
    };
    if r == WinSock::SOCKET_ERROR {
        Err(wsa_error())
    } else {
        Ok(())
    }
}

/// The reverse of [`set_linger`] -- `None` if lingering is currently
/// disabled.
pub(crate) fn linger(sock: RawSocket) -> io::Result<Option<Duration>> {
    let mut value = WinSock::LINGER {
        l_onoff: 0,
        l_linger: 0,
    };
    let mut len = mem::size_of::<WinSock::LINGER>() as i32;
    // SAFETY: `&mut value`/`&mut len` are valid, exclusively borrowed
    // out-params the kernel fills; `sock` is caller-owned.
    let r = unsafe {
        WinSock::getsockopt(
            sock as SOCKET,
            WinSock::SOL_SOCKET,
            WinSock::SO_LINGER,
            (&mut value as *mut WinSock::LINGER).cast(),
            &mut len,
        )
    };
    if r == WinSock::SOCKET_ERROR {
        return Err(wsa_error());
    }
    Ok((value.l_onoff != 0).then(|| Duration::from_secs(value.l_linger as u64)))
}

pub(crate) fn set_ttl(sock: RawSocket, ttl: u32) -> io::Result<()> {
    setsockopt_int(sock, WinSock::IPPROTO_IP, WinSock::IP_TTL, ttl as i32)
}

pub(crate) fn ttl(sock: RawSocket) -> io::Result<u32> {
    Ok(getsockopt_int(sock, WinSock::IPPROTO_IP, WinSock::IP_TTL)? as u32)
}

/// `IP_TOS` -- see the POSIX side's identical option for the `u32`-vs-
/// `u8` reasoning.
pub(crate) fn set_tos_v4(sock: RawSocket, tos: u32) -> io::Result<()> {
    setsockopt_int(sock, WinSock::IPPROTO_IP, WinSock::IP_TOS, tos as i32)
}

pub(crate) fn tos_v4(sock: RawSocket) -> io::Result<u32> {
    Ok(getsockopt_int(sock, WinSock::IPPROTO_IP, WinSock::IP_TOS)? as u32)
}

/// The IPv6 equivalent of [`set_tos_v4`]/[`tos_v4`] (`IPV6_TCLASS`).
pub(crate) fn set_tclass_v6(sock: RawSocket, tclass: u32) -> io::Result<()> {
    setsockopt_int(
        sock,
        WinSock::IPPROTO_IPV6,
        WinSock::IPV6_TCLASS,
        tclass as i32,
    )
}

pub(crate) fn tclass_v6(sock: RawSocket) -> io::Result<u32> {
    Ok(getsockopt_int(sock, WinSock::IPPROTO_IPV6, WinSock::IPV6_TCLASS)? as u32)
}

pub(crate) fn set_nonblocking(sock: RawSocket, nonblocking: bool) -> io::Result<()> {
    let mut mode: u32 = if nonblocking { 1 } else { 0 };
    // SAFETY: `&mut mode` is a valid, exclusively borrowed `u32` for the
    // call's duration; `sock` is caller-owned.
    let r = unsafe { WinSock::ioctlsocket(sock as SOCKET, WinSock::FIONBIO, &mut mode) };
    if r == WinSock::SOCKET_ERROR {
        Err(wsa_error())
    } else {
        Ok(())
    }
}

/// Classifies a raw `WSAGetLastError`/`GetLastError` code into this
/// crate's portable [`PKind`] taxonomy -- used only when building a
/// [`PlatformError`] (below), never by the plain `io::Result` functions
/// above, which let `io::Error::from_raw_os_error`'s own platform-aware
/// mapping speak for itself (in particular for `WouldBlock`, which is
/// all [`super::super::reactor::poll_io`]'s retry loop actually checks).
fn kind_for(code: i32) -> PKind {
    match code {
        WinSock::WSAECONNREFUSED => PKind::ConnectionRefused,
        WinSock::WSAECONNRESET => PKind::ConnectionReset,
        WinSock::WSAECONNABORTED => PKind::ConnectionAborted,
        WinSock::WSAENOTCONN => PKind::NotConnected,
        WinSock::WSAEADDRINUSE => PKind::AddrInUse,
        WinSock::WSAEADDRNOTAVAIL => PKind::AddrNotAvailable,
        WinSock::WSAETIMEDOUT => PKind::TimedOut,
        WinSock::WSAEINTR => PKind::Interrupted,
        WinSock::WSAEWOULDBLOCK | WinSock::WSAEINPROGRESS => PKind::WouldBlock,
        _ => PKind::Other,
    }
}

/// Wraps a plain `io::Error` from one of this module's own functions
/// into the [`PlatformError`] shape `platform::net`'s traits (and this
/// crate's `from_platform_err`) expect -- see this module's docs for why
/// `WindowsTcpListener`/`WindowsTcpStream`/`WindowsUdpSocket` need this
/// at all, unlike their own free-function helpers above.
fn to_platform_err(e: io::Error, op: &'static str) -> PlatformError {
    match e.raw_os_error() {
        Some(code) => PlatformError::new(kind_for(code), OsCode::Win32(code as u32), op),
        None => PlatformError::new(PKind::Other, OsCode::None, op),
    }
}

/// A listening TCP socket backed by an owned Winsock socket -- the
/// Windows counterpart of `LinuxTcpListener`/`MacosTcpListener`, see this
/// module's docs for why it's hand-rolled here instead of reused from
/// rustils.
pub struct WindowsTcpListener(OwnedSocket);

impl WindowsTcpListener {
    pub fn bind(addr: SocketAddr) -> PResult<Self> {
        let sock = new_tcp_socket(addr).map_err(|e| to_platform_err(e, "socket"))?;
        bind(sock.as_raw_socket(), addr).map_err(|e| to_platform_err(e, "bind"))?;
        listen(sock.as_raw_socket(), WinSock::SOMAXCONN)
            .map_err(|e| to_platform_err(e, "listen"))?;
        Ok(Self(sock))
    }

    pub fn accept(&self) -> PResult<(WindowsTcpStream, SocketAddr)> {
        // SAFETY: an all-zero `SOCKADDR_STORAGE` is a valid (if inert)
        // value.
        let mut storage: SOCKADDR_STORAGE = unsafe { mem::zeroed() };
        let mut len = mem::size_of::<SOCKADDR_STORAGE>() as i32;
        // SAFETY: `&mut storage`/`&mut len` are valid, exclusively
        // borrowed out-params; `self.0` is caller-owned and listening.
        let sock = unsafe {
            WinSock::accept(
                self.0.as_raw_socket() as SOCKET,
                (&mut storage as *mut SOCKADDR_STORAGE).cast::<SOCKADDR>(),
                &mut len,
            )
        };
        if sock == WinSock::INVALID_SOCKET {
            return Err(to_platform_err(wsa_error(), "accept"));
        }
        // SAFETY: `sock` was just returned by `accept()` and is valid,
        // otherwise-unowned, and wrapped exactly once.
        let owned = unsafe { OwnedSocket::from_raw_socket(sock as RawSocket) };
        // SAFETY: the kernel just filled `storage` in via the call above.
        let peer = unsafe { from_sockaddr(&storage) }.map_err(|e| to_platform_err(e, "accept"))?;
        Ok((WindowsTcpStream(owned), peer))
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> PResult<()> {
        set_nonblocking(self.0.as_raw_socket(), nonblocking)
            .map_err(|e| to_platform_err(e, "ioctlsocket"))
    }

    pub fn local_addr(&self) -> PResult<SocketAddr> {
        local_addr(self.0.as_raw_socket()).map_err(|e| to_platform_err(e, "getsockname"))
    }
}

impl From<OwnedSocket> for WindowsTcpListener {
    fn from(sock: OwnedSocket) -> Self {
        Self(sock)
    }
}

impl AsRawSocket for WindowsTcpListener {
    fn as_raw_socket(&self) -> RawSocket {
        self.0.as_raw_socket()
    }
}

impl AsSocket for WindowsTcpListener {
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.0.as_socket()
    }
}

/// A connected TCP stream backed by an owned Winsock socket.
pub struct WindowsTcpStream(OwnedSocket);

impl WindowsTcpStream {
    pub fn set_nonblocking(&self, nonblocking: bool) -> PResult<()> {
        set_nonblocking(self.0.as_raw_socket(), nonblocking)
            .map_err(|e| to_platform_err(e, "ioctlsocket"))
    }

    pub fn set_nodelay(&self, nodelay: bool) -> PResult<()> {
        set_nodelay(self.0.as_raw_socket(), nodelay).map_err(|e| to_platform_err(e, "setsockopt"))
    }

    pub fn peer_addr(&self) -> PResult<SocketAddr> {
        peer_addr(self.0.as_raw_socket()).map_err(|e| to_platform_err(e, "getpeername"))
    }

    pub fn local_addr(&self) -> PResult<SocketAddr> {
        local_addr(self.0.as_raw_socket()).map_err(|e| to_platform_err(e, "getsockname"))
    }
}

impl From<OwnedSocket> for WindowsTcpStream {
    fn from(sock: OwnedSocket) -> Self {
        Self(sock)
    }
}

impl AsRawSocket for WindowsTcpStream {
    fn as_raw_socket(&self) -> RawSocket {
        self.0.as_raw_socket()
    }
}

impl AsSocket for WindowsTcpStream {
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.0.as_socket()
    }
}

/// A UDP datagram socket backed by an owned Winsock socket.
pub struct WindowsUdpSocket(OwnedSocket);

impl WindowsUdpSocket {
    pub fn bind(addr: SocketAddr) -> PResult<Self> {
        let sock = new_udp_socket(addr).map_err(|e| to_platform_err(e, "socket"))?;
        Ok(Self(sock))
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> PResult<()> {
        set_nonblocking(self.0.as_raw_socket(), nonblocking)
            .map_err(|e| to_platform_err(e, "ioctlsocket"))
    }

    pub fn send_to(&self, buf: &[u8], addr: SocketAddr) -> PResult<usize> {
        udp_send_to(self.0.as_raw_socket(), buf, addr).map_err(|e| to_platform_err(e, "sendto"))
    }

    pub fn recv_from(&self, buf: &mut [u8]) -> PResult<(usize, SocketAddr)> {
        udp_recv_from(self.0.as_raw_socket(), buf).map_err(|e| to_platform_err(e, "recvfrom"))
    }

    pub fn local_addr(&self) -> PResult<SocketAddr> {
        local_addr(self.0.as_raw_socket()).map_err(|e| to_platform_err(e, "getsockname"))
    }
}

impl From<OwnedSocket> for WindowsUdpSocket {
    fn from(sock: OwnedSocket) -> Self {
        Self(sock)
    }
}

impl AsRawSocket for WindowsUdpSocket {
    fn as_raw_socket(&self) -> RawSocket {
        self.0.as_raw_socket()
    }
}

impl AsSocket for WindowsUdpSocket {
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.0.as_socket()
    }
}
