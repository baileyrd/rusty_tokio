//! [`lookup_host`] resolves a hostname (`"example.com:443"`, or anything
//! else implementing `std::net::ToSocketAddrs`) to one or more
//! [`SocketAddr`]s without blocking a worker thread.
//!
//! There's no portable non-blocking `getaddrinfo` -- DNS resolution (and
//! `/etc/hosts`/NSS lookups more generally) is a genuinely blocking
//! operation everywhere. `lookup_host` runs the real, blocking
//! `ToSocketAddrs::to_socket_addrs` on the [`crate::spawn_blocking`]
//! pool -- the same "looks async, is actually a `spawn_blocking` round
//! trip under the hood" shape [`crate::fs::File`]/`crate::io::stdio`/
//! [`crate::process::Child::wait`] already use for operations with no
//! reactor-driven alternative -- and collects the results eagerly into a
//! [`LookupHost`] before returning, so the borrow on `host` doesn't need
//! to outlive the blocking-pool call.

use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::vec;

fn blocking_pool_panicked() -> io::Error {
    io::Error::other("the blocking-pool task resolving this host panicked")
}

/// Resolves `host` (a `"host:port"` string, an `(&str, u16)` pair, or
/// anything else implementing `std::net::ToSocketAddrs`) to its
/// [`SocketAddr`]s, on the blocking-task pool rather than the calling
/// task's own worker thread.
///
/// # Panics
/// Panics if called from a thread with no ambient runtime.
pub async fn lookup_host<T>(host: T) -> io::Result<LookupHost>
where
    T: ToSocketAddrs + Send + 'static,
{
    let addrs =
        crate::spawn_blocking(move || host.to_socket_addrs().map(|iter| iter.collect::<Vec<_>>()))
            .await
            .map_err(|_| blocking_pool_panicked())??;
    Ok(LookupHost(addrs.into_iter()))
}

/// The resolved [`SocketAddr`]s from a [`lookup_host`] call.
pub struct LookupHost(vec::IntoIter<SocketAddr>);

impl Iterator for LookupHost {
    type Item = SocketAddr;

    fn next(&mut self) -> Option<SocketAddr> {
        self.0.next()
    }
}
