//! Unix named pipes (FIFOs): [`PipeOpenOptions`]/its `open_receiver`/
//! `open_sender` for opening an existing FIFO at a filesystem path,
//! [`PipeSender`]/[`PipeReceiver`] for the two directional halves, and
//! the free function [`pipe`] for a fresh anonymous (unnamed) pipe pair.
//! Mirrors tokio's own `net::unix::pipe` module -- named with a `Pipe`
//! prefix here (`PipeSender`/`PipeReceiver`/`PipeOpenOptions`) rather
//! than tokio's bare `Sender`/`Receiver`/`OpenOptions`, the same
//! disambiguation this crate already applies to `UnixReadHalf`/
//! `UnixSocketAddr`/etc: this crate flattens every type straight into
//! `io`'s own namespace (no nested `io::pipe` submodule the way tokio
//! nests `net::unix::pipe`), where a bare `Sender`/`Receiver`/
//! `OpenOptions` would read ambiguously next to `sync::mpsc::Sender`/
//! `fs::OpenOptions`/etc even without an actual name collision.
//!
//! Unix-only: no other platform has named pipes in this sense (Windows'
//! own named pipes are a very different, IOCP-driven mechanism this
//! crate doesn't cover).
//!
//! A FIFO's fd is genuinely readiness-driven -- `epoll`/`kevent` report
//! it the same way a socket is, unlike a regular file (which the kernel
//! always considers "ready", the reason [`crate::fs::File`] is a
//! [`crate::spawn_blocking`]-per-operation type instead) -- so
//! `PipeSender`/`PipeReceiver` are built directly on the same reactor/
//! `ScheduledIo` primitives every socket type in this module already
//! uses, wrapping a plain `std::fs::File` for the actual `read`/`write`
//! syscalls (rather than a raw fd) since `File` already implements
//! `Read`/`Write` for a shared `&File`, and -- crucially -- this crate's
//! own [`ReadBuf`] is a plain always-initialized `&mut [u8]` (see
//! `async_io.rs`), so reading into it needs no `MaybeUninit` handling
//! the way real tokio's does.

use super::async_io::{AsyncRead, AsyncWrite, ReadBuf};
use super::reactor::{poll_io, Interest as ReactorInterest, Reactor, ScheduledIo};
use super::{readiness, Interest, Ready};
use crate::runtime::Handle;
use libc::c_int;
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

fn is_pipe(fd: BorrowedFd<'_>) -> io::Result<bool> {
    // SAFETY: `stat` is a plain C struct of scalars -- valid for any bit
    // pattern -- and `fstat` only ever reads through `fd`, a borrowed,
    // currently-open descriptor.
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    let r = unsafe { libc::fstat(fd.as_raw_fd(), &mut stat) };
    if r == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok((stat.st_mode as libc::mode_t & libc::S_IFMT) == libc::S_IFIFO)
    }
}

fn file_status_flags(fd: BorrowedFd<'_>) -> io::Result<c_int> {
    // SAFETY: `fd` is a valid, currently-open descriptor; `F_GETFL`
    // takes no further argument.
    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
    if flags < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(flags)
    }
}

fn has_read_access(flags: c_int) -> bool {
    matches!(flags & libc::O_ACCMODE, libc::O_RDONLY | libc::O_RDWR)
}

fn has_write_access(flags: c_int) -> bool {
    matches!(flags & libc::O_ACCMODE, libc::O_WRONLY | libc::O_RDWR)
}

fn set_nonblocking(fd: BorrowedFd<'_>, nonblocking: bool) -> io::Result<()> {
    let previous = file_status_flags(fd)?;
    let new = if nonblocking {
        previous | libc::O_NONBLOCK
    } else {
        previous & !libc::O_NONBLOCK
    };
    // SAFETY: `fd` is a valid, currently-open descriptor; `new` is a
    // valid `F_SETFL` flags value (the same one `F_GETFL` just reported,
    // with only `O_NONBLOCK` toggled).
    if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, new) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// A fresh, non-blocking, `CLOEXEC` anonymous pipe (`pipe2(2)`/`pipe(2)`)
/// -- the same platform split (one atomic call on Linux, two steps on
/// macOS) [`super::socket::new_unix_socket`] uses, for the same reason.
/// Backs the free function [`pipe`].
fn new_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0 as c_int; 2];
    #[cfg(target_os = "linux")]
    // SAFETY: `fds` is a valid, exclusively-borrowed 2-element out-param
    // for the call's duration.
    let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
    #[cfg(target_os = "macos")]
    // SAFETY: same as the Linux arm; macOS has no `pipe2`, so
    // `O_NONBLOCK`/`FD_CLOEXEC` are set via `fcntl` right after instead.
    let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: both fds were just returned by `pipe`/`pipe2` above,
    // valid, otherwise-unowned, and each wrapped exactly once.
    let (read_fd, write_fd) =
        unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
    #[cfg(target_os = "macos")]
    for fd in [&read_fd, &write_fd] {
        set_nonblocking(fd.as_fd(), true)?;
        // SAFETY: `fd` is caller-owned and open; `FD_CLOEXEC` is the
        // sole variadic argument `F_SETFD` expects.
        if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok((read_fd, write_fd))
}

/// A fresh anonymous (unnamed) pipe -- no filesystem path or listener
/// involved at all, just the read/write ends of a single `pipe(2)`
/// call. For a named pipe (FIFO) at a filesystem path instead, see
/// [`PipeOpenOptions`].
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub fn pipe() -> io::Result<(PipeSender, PipeReceiver)> {
    let (read_fd, write_fd) = new_pipe()?;
    let sender = PipeSender::from_owned_fd_unchecked(write_fd)?;
    let receiver = PipeReceiver::from_owned_fd_unchecked(read_fd)?;
    Ok((sender, receiver))
}

/// A builder for opening an existing named pipe (FIFO) at a filesystem
/// path -- see [`open_receiver`](Self::open_receiver)/
/// [`open_sender`](Self::open_sender). Doesn't *create* the FIFO itself
/// (`mkfifo(2)`, out of scope here the same way it is for tokio's own
/// equivalent); the path must already exist as one.
#[derive(Clone, Debug, Default)]
pub struct PipeOpenOptions {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    read_write: bool,
    unchecked: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PipeEnd {
    Sender,
    Receiver,
}

impl PipeOpenOptions {
    /// All options initially `false`.
    pub fn new() -> PipeOpenOptions {
        PipeOpenOptions::default()
    }

    /// Opens the FIFO for both reading and writing rather than just the
    /// one direction [`open_receiver`](Self::open_receiver)/
    /// [`open_sender`](Self::open_sender) would otherwise use alone --
    /// doesn't change which of [`PipeReceiver`]/[`PipeSender`] comes
    /// back, only how the underlying `open(2)` call itself is made.
    ///
    /// The usual reason: opening a FIFO for reading alone blocks (at the
    /// OS level, were it not for the `O_NONBLOCK` this builder always
    /// adds) until a writer opens it too, and a sender opened without
    /// any reader yet present fails outright with `ENXIO` rather than
    /// waiting -- opening read-write sidesteps both, since the same fd
    /// then counts as its own reader. Not defined by POSIX; only
    /// guaranteed to work on Linux (hence Linux/Android-only here,
    /// matching real tokio).
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn read_write(&mut self, value: bool) -> &mut Self {
        self.read_write = value;
        self
    }

    /// Skips verifying that the opened file is actually a FIFO (`fstat`
    /// reporting `S_IFIFO`) -- use only when already certain it is.
    pub fn unchecked(&mut self, value: bool) -> &mut Self {
        self.unchecked = value;
        self
    }

    fn open(&self, path: &Path, end: PipeEnd) -> io::Result<File> {
        let mut options = std::fs::OpenOptions::new();
        options
            .read(end == PipeEnd::Receiver)
            .write(end == PipeEnd::Sender)
            .custom_flags(libc::O_NONBLOCK);
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if self.read_write {
            options.read(true).write(true);
        }
        let file = options.open(path)?;
        if !self.unchecked && !is_pipe(file.as_fd())? {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "not a pipe"));
        }
        Ok(file)
    }

    /// Opens the FIFO at `path` for reading.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn open_receiver(&self, path: impl AsRef<Path>) -> io::Result<PipeReceiver> {
        let file = self.open(path.as_ref(), PipeEnd::Receiver)?;
        PipeReceiver::from_file_unchecked(file)
    }

    /// Opens the FIFO at `path` for writing.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn open_sender(&self, path: impl AsRef<Path>) -> io::Result<PipeSender> {
        let file = self.open(path.as_ref(), PipeEnd::Sender)?;
        PipeSender::from_file_unchecked(file)
    }
}

/// The writing half of a named pipe (FIFO) or anonymous pipe -- see
/// [`PipeOpenOptions::open_sender`] for opening an existing FIFO, or
/// [`pipe`] for a fresh anonymous pair.
pub struct PipeSender {
    file: File,
    io: Arc<ScheduledIo>,
    reactor: Arc<Reactor>,
}

impl PipeSender {
    /// Adopts an already-open file, after confirming it's actually a
    /// pipe open for writing and flipping it non-blocking. See
    /// [`from_file_unchecked`](Self::from_file_unchecked) to skip these
    /// checks (e.g. if the file is already known to satisfy them).
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn from_file(file: File) -> io::Result<PipeSender> {
        if !is_pipe(file.as_fd())? {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "not a pipe"));
        }
        if !has_write_access(file_status_flags(file.as_fd())?) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "not opened for writing",
            ));
        }
        set_nonblocking(file.as_fd(), true)?;
        Self::from_file_unchecked(file)
    }

    /// Adopts an already-open file with none of [`from_file`
    /// ](Self::from_file)'s checks -- the caller vouches it's a pipe,
    /// open for writing, and already non-blocking.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn from_file_unchecked(file: File) -> io::Result<PipeSender> {
        let reactor = Handle::current().shared.reactor.clone();
        let io = reactor.register(file.as_raw_fd())?;
        Ok(PipeSender { file, io, reactor })
    }

    /// Adopts an already-open fd -- see [`from_file`](Self::from_file).
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn from_owned_fd(owned_fd: OwnedFd) -> io::Result<PipeSender> {
        Self::from_file(File::from(owned_fd))
    }

    /// Adopts an already-open fd with none of [`from_file`
    /// ](Self::from_file)'s checks -- see [`from_file_unchecked`
    /// ](Self::from_file_unchecked).
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn from_owned_fd_unchecked(owned_fd: OwnedFd) -> io::Result<PipeSender> {
        Self::from_file_unchecked(File::from(owned_fd))
    }

    /// Resolves once *any* of `interest`'s requested directions is
    /// ready, reporting exactly which one(s) actually are.
    pub async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        std::future::poll_fn(|cx| readiness::poll_ready(&self.io, interest, cx)).await
    }

    pub async fn writable(&self) -> io::Result<()> {
        self.ready(Interest::WRITABLE).await.map(|_| ())
    }

    /// Non-`async fn` form of [`writable`](Self::writable).
    pub fn poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        super::reactor::poll_ready(&self.io, ReactorInterest::Write, cx).map(Ok)
    }

    /// Runs `f` (the caller's own non-blocking syscall against this
    /// pipe), clearing stale cached readiness on `WouldBlock` -- see
    /// `TcpStream::try_io`'s identical contract.
    pub fn try_io<R>(
        &self,
        interest: Interest,
        f: impl FnOnce() -> io::Result<R>,
    ) -> io::Result<R> {
        readiness::try_io(&self.io, interest, f)
    }

    /// Writes without waiting, failing immediately (`WouldBlock`) if the
    /// pipe isn't currently writable. If `buf` is no longer than
    /// `PIPE_BUF` (`4096` on Linux), the write is atomic -- it can't
    /// interleave with another writer's concurrent write to the same
    /// pipe.
    pub fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        self.try_io(Interest::WRITABLE, || (&self.file).write(buf))
    }

    /// Like [`try_write`](Self::try_write), but gathers from every
    /// buffer in `bufs` in one `writev(2)` call.
    pub fn try_write_vectored(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.try_io(Interest::WRITABLE, || (&self.file).write_vectored(bufs))
    }

    /// Hands the underlying fd back, first restoring blocking mode
    /// (undoing the non-blocking flip every constructor here applies).
    /// A duplicate (`dup(2)`) of this `PipeSender`'s own fd, not the
    /// exact same one -- `self` still drops normally (deregistering and
    /// closing its own original fd), the same reason
    /// [`TcpListener::into_std`](super::TcpListener::into_std) dups
    /// rather than transfers ownership directly.
    pub fn into_blocking_fd(self) -> io::Result<OwnedFd> {
        let fd = self.into_nonblocking_fd()?;
        set_nonblocking(fd.as_fd(), false)?;
        Ok(fd)
    }

    /// Like [`into_blocking_fd`](Self::into_blocking_fd), but leaves the
    /// returned fd non-blocking rather than restoring blocking mode.
    pub fn into_nonblocking_fd(self) -> io::Result<OwnedFd> {
        let dup = self.file.try_clone()?;
        Ok(OwnedFd::from(dup))
    }
}

impl AsyncWrite for PipeSender {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        poll_io(&this.io, ReactorInterest::Write, cx, || {
            (&this.file).write(buf)
        })
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        poll_io(&this.io, ReactorInterest::Write, cx, || {
            (&this.file).write_vectored(bufs)
        })
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl Drop for PipeSender {
    fn drop(&mut self) {
        self.reactor.deregister(self.file.as_raw_fd());
    }
}

impl AsFd for PipeSender {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.file.as_fd()
    }
}

impl AsRawFd for PipeSender {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

/// The reading half of a named pipe (FIFO) or anonymous pipe -- see
/// [`PipeOpenOptions::open_receiver`] for opening an existing FIFO, or
/// [`pipe`] for a fresh anonymous pair.
pub struct PipeReceiver {
    file: File,
    io: Arc<ScheduledIo>,
    reactor: Arc<Reactor>,
}

impl PipeReceiver {
    /// Adopts an already-open file, after confirming it's actually a
    /// pipe open for reading and flipping it non-blocking. See
    /// [`from_file_unchecked`](Self::from_file_unchecked) to skip these
    /// checks.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn from_file(file: File) -> io::Result<PipeReceiver> {
        if !is_pipe(file.as_fd())? {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "not a pipe"));
        }
        if !has_read_access(file_status_flags(file.as_fd())?) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "not opened for reading",
            ));
        }
        set_nonblocking(file.as_fd(), true)?;
        Self::from_file_unchecked(file)
    }

    /// Adopts an already-open file with none of [`from_file`
    /// ](Self::from_file)'s checks -- the caller vouches it's a pipe,
    /// open for reading, and already non-blocking.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn from_file_unchecked(file: File) -> io::Result<PipeReceiver> {
        let reactor = Handle::current().shared.reactor.clone();
        let io = reactor.register(file.as_raw_fd())?;
        Ok(PipeReceiver { file, io, reactor })
    }

    /// Adopts an already-open fd -- see [`from_file`](Self::from_file).
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn from_owned_fd(owned_fd: OwnedFd) -> io::Result<PipeReceiver> {
        Self::from_file(File::from(owned_fd))
    }

    /// Adopts an already-open fd with none of [`from_file`
    /// ](Self::from_file)'s checks -- see [`from_file_unchecked`
    /// ](Self::from_file_unchecked).
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub fn from_owned_fd_unchecked(owned_fd: OwnedFd) -> io::Result<PipeReceiver> {
        Self::from_file_unchecked(File::from(owned_fd))
    }

    /// Resolves once *any* of `interest`'s requested directions is
    /// ready, reporting exactly which one(s) actually are.
    pub async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        std::future::poll_fn(|cx| readiness::poll_ready(&self.io, interest, cx)).await
    }

    pub async fn readable(&self) -> io::Result<()> {
        self.ready(Interest::READABLE).await.map(|_| ())
    }

    /// Non-`async fn` form of [`readable`](Self::readable).
    pub fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        super::reactor::poll_ready(&self.io, ReactorInterest::Read, cx).map(Ok)
    }

    /// Runs `f` (the caller's own non-blocking syscall against this
    /// pipe), clearing stale cached readiness on `WouldBlock` -- see
    /// `TcpStream::try_io`'s identical contract.
    pub fn try_io<R>(
        &self,
        interest: Interest,
        f: impl FnOnce() -> io::Result<R>,
    ) -> io::Result<R> {
        readiness::try_io(&self.io, interest, f)
    }

    /// Reads without waiting, failing immediately (`WouldBlock`) if
    /// nothing's available yet.
    pub fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.try_io(Interest::READABLE, || (&self.file).read(buf))
    }

    /// Like [`try_read`](Self::try_read), but scatters into every buffer
    /// in `bufs` in one `readv(2)` call.
    pub fn try_read_vectored(&self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        self.try_io(Interest::READABLE, || (&self.file).read_vectored(bufs))
    }

    /// Hands the underlying fd back, first restoring blocking mode --
    /// see [`PipeSender::into_blocking_fd`] for the identical dup-based
    /// reasoning.
    pub fn into_blocking_fd(self) -> io::Result<OwnedFd> {
        let fd = self.into_nonblocking_fd()?;
        set_nonblocking(fd.as_fd(), false)?;
        Ok(fd)
    }

    /// Like [`into_blocking_fd`](Self::into_blocking_fd), but leaves the
    /// returned fd non-blocking rather than restoring blocking mode.
    pub fn into_nonblocking_fd(self) -> io::Result<OwnedFd> {
        let dup = self.file.try_clone()?;
        Ok(OwnedFd::from(dup))
    }
}

impl AsyncRead for PipeReceiver {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match poll_io(&this.io, ReactorInterest::Read, cx, || {
            (&this.file).read(buf.unfilled_mut())
        }) {
            Poll::Ready(Ok(n)) => {
                buf.advance(n);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for PipeReceiver {
    fn drop(&mut self) {
        self.reactor.deregister(self.file.as_raw_fd());
    }
}

impl AsFd for PipeReceiver {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.file.as_fd()
    }
}

impl AsRawFd for PipeReceiver {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}
