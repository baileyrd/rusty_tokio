//! [`split`]: splits any single `AsyncRead + AsyncWrite` value into an
//! independent read half and write half, usable concurrently from two
//! different tasks -- the generic counterpart of the concrete,
//! borrow-based `split`/`into_split` methods `TcpStream`/`UnixStream`
//! already have. Those exist because a concrete socket type only ever
//! needed *shared* access internally (the reactor state and fd are
//! already behind `Arc`/a kernel-owned handle) -- but an arbitrary
//! `T: AsyncRead + AsyncWrite` has no such guarantee, so the two halves
//! here share ownership of `T` itself behind a `Mutex`, reunited later
//! via [`SplitReadHalf::unsplit`].
//!
//! The `Mutex` is a plain, briefly-held blocking one, not an async-aware
//! cooperative lock -- each `poll_read`/`poll_write`/etc. call locks it
//! only for the duration of one call into the wrapped `T`'s own poll
//! method (itself non-blocking), so contention would only ever briefly
//! stall a worker thread in the genuinely rare case both halves are
//! polled at the exact same instant on two different threads. A real
//! async-aware "at most one side polling at a time" lock would avoid
//! even that, at the cost of meaningfully more code for a case this
//! crate's own tests don't show actually mattering in practice.

use super::{AsyncRead, AsyncWrite, ReadBuf};
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

struct Inner<T> {
    stream: Mutex<T>,
}

/// The read half of a [`split`] pair.
pub struct SplitReadHalf<T> {
    inner: Arc<Inner<T>>,
}

/// The write half of a [`split`] pair.
pub struct SplitWriteHalf<T> {
    inner: Arc<Inner<T>>,
}

/// Splits `stream` into an independent read half and write half, each
/// usable from its own task. Reunite them later via
/// [`SplitReadHalf::unsplit`].
pub fn split<T>(stream: T) -> (SplitReadHalf<T>, SplitWriteHalf<T>)
where
    T: AsyncRead + AsyncWrite,
{
    let inner = Arc::new(Inner {
        stream: Mutex::new(stream),
    });
    (
        SplitReadHalf {
            inner: inner.clone(),
        },
        SplitWriteHalf { inner },
    )
}

impl<T> SplitReadHalf<T> {
    /// Recombines this half with `other` back into the original `T` --
    /// only valid if `other` came from the same [`split`] call as
    /// `self`.
    ///
    /// # Panics
    /// Panics if `other` didn't come from the same `split` call.
    pub fn unsplit(self, other: SplitWriteHalf<T>) -> T {
        if !Arc::ptr_eq(&self.inner, &other.inner) {
            panic!("tried to unsplit a SplitReadHalf/SplitWriteHalf pair that didn't come from the same split() call");
        }
        drop(other);
        let inner = Arc::try_unwrap(self.inner).unwrap_or_else(|_| {
            unreachable!("both halves are accounted for -- self and the just-dropped other")
        });
        inner.stream.into_inner().unwrap()
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for SplitReadHalf<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut guard = self.inner.stream.lock().unwrap();
        Pin::new(&mut *guard).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for SplitWriteHalf<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut guard = self.inner.stream.lock().unwrap();
        Pin::new(&mut *guard).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut guard = self.inner.stream.lock().unwrap();
        Pin::new(&mut *guard).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut guard = self.inner.stream.lock().unwrap();
        Pin::new(&mut *guard).poll_shutdown(cx)
    }
}
