//! [`AsyncFd`]: registers an arbitrary caller-owned raw fd (a custom
//! device, an eventfd, a GPIO line -- anything the kernel reports
//! readiness for via `epoll`/`kevent`) with this crate's reactor, the
//! same one every socket type here already uses internally. Unlike
//! `TcpStream`/`UdpSocket`, `AsyncFd` performs no I/O itself -- it only
//! tracks readiness; the actual `read`/`write`/`ioctl`/whatever syscall
//! is the caller's own, run through [`AsyncFdReadyGuard::try_io`] so a
//! `WouldBlock` result correctly clears the stale readiness signal
//! rather than leaving the next wait falsely reporting "still ready".
//!
//! Unix-only, matching real tokio's own `tokio::io::unix::AsyncFd`
//! gating -- Windows' IOCP has no comparable "hand me an arbitrary fd"
//! concept (see `io::reactor::windows`'s own docs).
//!
//! Deliberately narrower than tokio's own `AsyncFd` in two ways: there's
//! no `_mut` guard variant (`AsyncFdReadyMutGuard`/`readable_mut`/
//! `writable_mut`) -- the shared-reference guard below covers the
//! common case, and adding the mutable-access half doubles this
//! module's surface for a less-common one; and no separate
//! `AsyncFdTryNewError` non-panicking constructor -- every other
//! reactor-registering constructor in this crate already panics outside
//! a running [`crate::Runtime`] (see `TcpStream::connect`, etc.), and
//! `new`/`with_interest` below just follow that same established
//! convention rather than introducing a new one.

use super::reactor::{clear_ready, poll_ready, Reactor, ScheduledIo};
use super::{Interest, Ready};
use crate::runtime::Handle;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::Arc;
use std::task::{Context, Poll};

/// An arbitrary caller-owned I/O object, registered with this crate's
/// reactor for readiness notifications. See the module docs.
pub struct AsyncFd<T: AsRawFd> {
    inner: Option<T>,
    fd: RawFd,
    io: Arc<ScheduledIo>,
    reactor: Arc<Reactor>,
    interest: Interest,
}

impl<T: AsRawFd> AsyncFd<T> {
    /// Registers `inner`, interested in both readability and
    /// writability. See [`with_interest`](Self::with_interest) to
    /// narrow that down.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn new(inner: T) -> io::Result<Self> {
        Self::with_interest(inner, Interest::READABLE | Interest::WRITABLE)
    }

    /// Registers `inner`, interested only in the given `interest`
    /// direction(s) -- [`poll_read_ready`](Self::poll_read_ready)/
    /// [`readable`](Self::readable) panic if `interest` didn't include
    /// [`Interest::READABLE`], and likewise for the write side.
    ///
    /// Purely an API-level contract here: this crate's reactor always
    /// monitors both directions for every registered fd regardless (see
    /// the module docs), so narrowing `interest` costs nothing at the OS
    /// level -- it exists to catch a caller's own logic error (asking
    /// for readiness it never declared wanting), same as real tokio.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn with_interest(inner: T, interest: Interest) -> io::Result<Self> {
        let fd = inner.as_raw_fd();
        let reactor = Handle::current().shared.reactor.clone();
        let io = reactor.register(fd)?;
        Ok(AsyncFd {
            inner: Some(inner),
            fd,
            io,
            reactor,
            interest,
        })
    }

    /// The declared interest this `AsyncFd` was constructed with.
    pub fn interest(&self) -> Interest {
        self.interest
    }

    pub fn get_ref(&self) -> &T {
        self.inner
            .as_ref()
            .expect("inner value only ever taken by into_inner, which consumes self")
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.inner
            .as_mut()
            .expect("inner value only ever taken by into_inner, which consumes self")
    }

    /// Deregisters from the reactor and hands the wrapped value back.
    pub fn into_inner(mut self) -> T {
        self.inner
            .take()
            .expect("inner value only ever taken here, and this consumes self")
    }

    /// Non-`async fn` form of [`readable`](Self::readable), for a caller
    /// implementing its own `Future`/poll loop.
    ///
    /// # Panics
    /// Panics if this `AsyncFd` wasn't constructed with
    /// [`Interest::READABLE`].
    pub fn poll_read_ready<'a>(
        &'a self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<AsyncFdReadyGuard<'a, T>>> {
        assert!(
            self.interest.is_readable(),
            "AsyncFd polled for read readiness without declaring Interest::READABLE"
        );
        poll_ready(&self.io, super::reactor::Interest::Read, cx).map(|()| {
            Ok(AsyncFdReadyGuard {
                async_fd: self,
                ready: Ready::READABLE,
            })
        })
    }

    /// Non-`async fn` form of [`writable`](Self::writable), for a caller
    /// implementing its own `Future`/poll loop.
    ///
    /// # Panics
    /// Panics if this `AsyncFd` wasn't constructed with
    /// [`Interest::WRITABLE`].
    pub fn poll_write_ready<'a>(
        &'a self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<AsyncFdReadyGuard<'a, T>>> {
        assert!(
            self.interest.is_writable(),
            "AsyncFd polled for write readiness without declaring Interest::WRITABLE"
        );
        poll_ready(&self.io, super::reactor::Interest::Write, cx).map(|()| {
            Ok(AsyncFdReadyGuard {
                async_fd: self,
                ready: Ready::WRITABLE,
            })
        })
    }

    /// Resolves once this fd is readable, handing back a guard the
    /// caller runs its own read through via
    /// [`try_io`](AsyncFdReadyGuard::try_io).
    ///
    /// # Panics
    /// Panics if this `AsyncFd` wasn't constructed with
    /// [`Interest::READABLE`], or if called outside a running
    /// [`crate::Runtime`].
    pub async fn readable(&self) -> io::Result<AsyncFdReadyGuard<'_, T>> {
        std::future::poll_fn(|cx| self.poll_read_ready(cx)).await
    }

    /// Resolves once this fd is writable, handing back a guard the
    /// caller runs its own write through via
    /// [`try_io`](AsyncFdReadyGuard::try_io).
    ///
    /// # Panics
    /// Panics if this `AsyncFd` wasn't constructed with
    /// [`Interest::WRITABLE`], or if called outside a running
    /// [`crate::Runtime`].
    pub async fn writable(&self) -> io::Result<AsyncFdReadyGuard<'_, T>> {
        std::future::poll_fn(|cx| self.poll_write_ready(cx)).await
    }
}

impl<T: AsRawFd> Drop for AsyncFd<T> {
    fn drop(&mut self) {
        self.reactor.deregister(self.fd);
    }
}

/// Resolves this `AsyncFd`'s readiness -- see [`AsyncFd::readable`]/
/// [`AsyncFd::writable`].
pub struct AsyncFdReadyGuard<'a, T: AsRawFd> {
    async_fd: &'a AsyncFd<T>,
    ready: Ready,
}

impl<'a, T: AsRawFd> AsyncFdReadyGuard<'a, T> {
    pub fn get_ref(&self) -> &T {
        self.async_fd.get_ref()
    }

    /// The readiness direction(s) this guard represents.
    pub fn ready(&self) -> Ready {
        self.ready
    }

    /// Clears the cached readiness this guard represents, so the next
    /// `readable`/`writable`/`poll_read_ready`/`poll_write_ready` call
    /// waits for a fresh notification instead of immediately reporting
    /// "still ready" off a stale signal. [`try_io`](Self::try_io) already
    /// calls this on `WouldBlock` -- only needed directly if a caller
    /// wants to signal "actually not ready" without going through it.
    pub fn clear_ready(&mut self) {
        if self.ready.is_readable() {
            clear_ready(&self.async_fd.io, super::reactor::Interest::Read);
        }
        if self.ready.is_writable() {
            clear_ready(&self.async_fd.io, super::reactor::Interest::Write);
        }
    }

    /// Runs `f` (the caller's own syscall against
    /// [`get_ref`](Self::get_ref)'s fd), clearing this guard's cached
    /// readiness if it reports `WouldBlock` -- a false-positive
    /// readiness signal, common with level-triggered epoll/kqueue -- so
    /// the next wait doesn't immediately return `Ready` again for a
    /// direction that isn't actually ready. Returns `Err(TryIoError)` on
    /// `WouldBlock` (retry via `readable`/`writable` again), or
    /// `Ok(f`'s own result`)` otherwise.
    pub fn try_io<R>(
        &mut self,
        f: impl FnOnce(&AsyncFd<T>) -> io::Result<R>,
    ) -> Result<io::Result<R>, TryIoError> {
        match f(self.async_fd) {
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                self.clear_ready();
                Err(TryIoError(()))
            }
            other => Ok(other),
        }
    }
}

/// Returned by [`AsyncFdReadyGuard::try_io`] when the caller's closure
/// hit `WouldBlock` -- the readiness signal that led here was stale;
/// wait on [`AsyncFd::readable`]/[`AsyncFd::writable`] again.
#[derive(Debug)]
pub struct TryIoError(());

impl std::fmt::Display for TryIoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "a readiness-based I/O operation would block, and its stale readiness has already been cleared"
        )
    }
}

impl std::error::Error for TryIoError {}
