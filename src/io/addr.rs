//! [`ToSocketAddrs`]: what [`TcpStream::connect`](super::TcpStream::connect)/
//! [`TcpListener::bind_addrs`](super::TcpListener::bind_addrs)/
//! [`UdpSocket::bind_addrs`](super::UdpSocket::bind_addrs)/
//! [`UdpSocket::connect_addrs`](super::UdpSocket::connect_addrs) accept
//! -- not just a concrete [`SocketAddr`], but a `"host:port"` string, an
//! `(&str, u16)` pair, or anything else covered below, resolved via the
//! existing [`lookup_host`] (itself a [`crate::spawn_blocking`] round
//! trip -- DNS resolution is genuinely blocking, no portable
//! non-blocking `getaddrinfo` exists) for whichever forms actually need
//! it. Mirrors tokio's own `net::ToSocketAddrs`.
//!
//! Sealed (an implementation detail of exactly which forms are
//! supported, not a trait meant for downstream `impl`s) for the same
//! reason real tokio's own is: its associated `Future`'s exact
//! (unnameable) type is stabilization-pending, the same
//! `AsyncReadExt::read`-style reasoning [`super::async_io`]'s own module
//! docs give for using `-> impl Future + Send` rather than a plain
//! `async fn` here too.
//!
//! [`TcpListener::bind`](super::TcpListener::bind)/[`UdpSocket::bind`
//! ](super::UdpSocket::bind)/[`UdpSocket::connect`
//! ](super::UdpSocket::connect) stay their existing, synchronous,
//! concrete-`SocketAddr`-only selves rather than becoming generic over
//! this trait directly -- unlike [`TcpStream::connect`
//! ](super::TcpStream::connect) (already `async fn`, so widening its
//! parameter type is fully backwards compatible), resolving a hostname
//! genuinely needs to `.await` a blocking-pool round trip, and turning
//! an existing *synchronous* public method `async` out from under
//! existing callers would break every one of them. `bind_addrs`/
//! `connect_addrs` are additive `async fn`s instead, alongside the
//! original methods, not replacements for them.

use super::lookup_host;
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::vec;

mod sealed {
    use super::SocketAddr;
    use std::future::Future;
    use std::io;

    pub trait ToSocketAddrsSealed {
        type Iter: Iterator<Item = SocketAddr> + Send + 'static;

        fn to_socket_addrs(&self) -> impl Future<Output = io::Result<Self::Iter>> + Send;
    }
}

/// See the module docs.
pub trait ToSocketAddrs: sealed::ToSocketAddrsSealed {}

impl<T: sealed::ToSocketAddrsSealed + ?Sized> ToSocketAddrs for T {}

/// A caller passing a `&str`/`&(str, u16)` literal (the overwhelmingly
/// common case -- `TcpStream::connect("example.com:443")`, not
/// `connect("example.com:443".to_string())`) resolves through here,
/// deferring to the unsized [`str`]'s own impl.
impl sealed::ToSocketAddrsSealed for &str {
    type Iter = super::LookupHost;

    fn to_socket_addrs(&self) -> impl Future<Output = io::Result<Self::Iter>> + Send {
        <str as sealed::ToSocketAddrsSealed>::to_socket_addrs(*self)
    }
}

/// A single already-known address, wrapped to implement `Iterator` --
/// backs every impl below that doesn't need [`lookup_host`] at all
/// (nothing to resolve -- it's already a concrete address).
#[doc(hidden)]
pub struct Once(std::option::IntoIter<SocketAddr>);

impl Iterator for Once {
    type Item = SocketAddr;

    fn next(&mut self) -> Option<SocketAddr> {
        self.0.next()
    }
}

fn once(addr: SocketAddr) -> Once {
    Once(Some(addr).into_iter())
}

impl sealed::ToSocketAddrsSealed for SocketAddr {
    type Iter = Once;

    fn to_socket_addrs(&self) -> impl Future<Output = io::Result<Self::Iter>> + Send {
        std::future::ready(Ok(once(*self)))
    }
}

impl sealed::ToSocketAddrsSealed for SocketAddrV4 {
    type Iter = Once;

    fn to_socket_addrs(&self) -> impl Future<Output = io::Result<Self::Iter>> + Send {
        std::future::ready(Ok(once(SocketAddr::V4(*self))))
    }
}

impl sealed::ToSocketAddrsSealed for SocketAddrV6 {
    type Iter = Once;

    fn to_socket_addrs(&self) -> impl Future<Output = io::Result<Self::Iter>> + Send {
        std::future::ready(Ok(once(SocketAddr::V6(*self))))
    }
}

impl sealed::ToSocketAddrsSealed for (IpAddr, u16) {
    type Iter = Once;

    fn to_socket_addrs(&self) -> impl Future<Output = io::Result<Self::Iter>> + Send {
        std::future::ready(Ok(once(SocketAddr::new(self.0, self.1))))
    }
}

impl sealed::ToSocketAddrsSealed for (Ipv4Addr, u16) {
    type Iter = Once;

    fn to_socket_addrs(&self) -> impl Future<Output = io::Result<Self::Iter>> + Send {
        std::future::ready(Ok(once(SocketAddr::from((self.0, self.1)))))
    }
}

impl sealed::ToSocketAddrsSealed for (Ipv6Addr, u16) {
    type Iter = Once;

    fn to_socket_addrs(&self) -> impl Future<Output = io::Result<Self::Iter>> + Send {
        std::future::ready(Ok(once(SocketAddr::from((self.0, self.1)))))
    }
}

impl sealed::ToSocketAddrsSealed for &[SocketAddr] {
    type Iter = vec::IntoIter<SocketAddr>;

    fn to_socket_addrs(&self) -> impl Future<Output = io::Result<Self::Iter>> + Send {
        let addrs = self.to_vec();
        std::future::ready(Ok(addrs.into_iter()))
    }
}

impl sealed::ToSocketAddrsSealed for str {
    type Iter = super::LookupHost;

    fn to_socket_addrs(&self) -> impl Future<Output = io::Result<Self::Iter>> + Send {
        // The fast path real tokio's own `str` impl takes too: a
        // `"host:port"` string that already parses directly as a
        // `SocketAddr` (the overwhelmingly common case -- an IP literal
        // rather than a real hostname) needs no DNS lookup at all, so
        // skip the `spawn_blocking` round trip entirely rather than pay
        // for a needless thread hop.
        let fast = self.parse::<SocketAddr>().ok();
        let owned = self.to_owned();
        async move {
            if let Some(addr) = fast {
                return Ok(super::LookupHost::single(addr));
            }
            lookup_host(owned).await
        }
    }
}

impl sealed::ToSocketAddrsSealed for String {
    type Iter = super::LookupHost;

    fn to_socket_addrs(&self) -> impl Future<Output = io::Result<Self::Iter>> + Send {
        <str as sealed::ToSocketAddrsSealed>::to_socket_addrs(self.as_str())
    }
}

impl sealed::ToSocketAddrsSealed for (&str, u16) {
    type Iter = super::LookupHost;

    fn to_socket_addrs(&self) -> impl Future<Output = io::Result<Self::Iter>> + Send {
        // Same fast path as the plain `str` impl, but the host and port
        // arrive already split apart rather than needing their own
        // `"host:port"` parse first.
        let fast = self
            .0
            .parse::<IpAddr>()
            .ok()
            .map(|ip| SocketAddr::new(ip, self.1));
        let owned = (self.0.to_owned(), self.1);
        async move {
            if let Some(addr) = fast {
                return Ok(super::LookupHost::single(addr));
            }
            lookup_host(owned).await
        }
    }
}

impl sealed::ToSocketAddrsSealed for (String, u16) {
    type Iter = super::LookupHost;

    fn to_socket_addrs(&self) -> impl Future<Output = io::Result<Self::Iter>> + Send {
        // Same fast path as `(&str, u16)` -- not delegated to it
        // directly, since that would borrow through a `(&str, u16)`
        // temporary that doesn't outlive the returned future.
        let fast = self
            .0
            .parse::<IpAddr>()
            .ok()
            .map(|ip| SocketAddr::new(ip, self.1));
        let owned = self.clone();
        async move {
            if let Some(addr) = fast {
                return Ok(super::LookupHost::single(addr));
            }
            lookup_host(owned).await
        }
    }
}
