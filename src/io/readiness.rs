//! Shared "wait for any of `interest`'s directions, on any registered
//! socket" plumbing behind `TcpStream`/`TcpListener`/`UdpSocket`/
//! `UnixStream`/`UnixListener`'s `readable`/`writable`/`ready`/`try_io`
//! methods (issue #134) -- one copy instead of five, since the logic is
//! identical for every socket type that's already registered with the
//! reactor via a `ScheduledIo`.
//!
//! Cross-platform, unlike [`super::AsyncFd`] (Unix-only): every socket
//! type here is already registered with the reactor on every platform
//! this crate supports, so there's no IOCP-specific "arbitrary fd"
//! problem to work around the way a caller-supplied `AsyncFd` would
//! have on Windows.

use super::reactor::{
    clear_ready, poll_ready as reactor_poll_ready, Interest as ReactorInterest, ScheduledIo,
};
use super::{Interest, Ready};
use std::io;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Resolves once *any* of `interest`'s requested directions is ready,
/// reporting exactly which one(s) actually are -- matching real
/// tokio's own "`ready` waits for readable-or-writable, whichever comes
/// first" semantics rather than requiring every requested direction at
/// once.
pub(crate) fn poll_ready(
    io: &Arc<ScheduledIo>,
    interest: Interest,
    cx: &mut Context<'_>,
) -> Poll<io::Result<Ready>> {
    let mut ready = Ready::EMPTY;
    if interest.is_readable() && reactor_poll_ready(io, ReactorInterest::Read, cx).is_ready() {
        ready |= Ready::READABLE;
    }
    if interest.is_writable() && reactor_poll_ready(io, ReactorInterest::Write, cx).is_ready() {
        ready |= Ready::WRITABLE;
    }
    if ready.is_empty() {
        Poll::Pending
    } else {
        Poll::Ready(Ok(ready))
    }
}

/// Runs `f` (the caller's own non-blocking syscall) once; if it hits
/// `WouldBlock`, clears the cached readiness for whichever of
/// `interest`'s directions `f` needed, so the next `readable`/
/// `writable`/`ready` call waits for a fresh notification instead of
/// immediately reporting "still ready" off the same stale signal --
/// then hands `f`'s result (including that same `WouldBlock`, if that's
/// what happened) straight back to the caller.
pub(crate) fn try_io<R>(
    io: &Arc<ScheduledIo>,
    interest: Interest,
    f: impl FnOnce() -> io::Result<R>,
) -> io::Result<R> {
    match f() {
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
            if interest.is_readable() {
                clear_ready(io, ReactorInterest::Read);
            }
            if interest.is_writable() {
                clear_ready(io, ReactorInterest::Write);
            }
            Err(e)
        }
        other => other,
    }
}
