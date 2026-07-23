//! Async filesystem I/O: [`File`], the only type here so far.
//!
//! A regular file can't be registered with `epoll`/`kevent`'s readiness
//! model the way a socket can -- from the kernel's point of view a file
//! is always "ready"; the actual disk latency happens synchronously
//! inside the `read`/`write`/`lseek` syscall itself, not as something a
//! reactor can wait on separately. So unlike [`crate::io::TcpStream`]
//! (a thin non-blocking wrapper plus reactor readiness), [`File`] is
//! entirely a [`crate::spawn_blocking`] abstraction: every operation
//! moves the underlying `std::fs::File` onto a blocking-pool thread,
//! runs the real syscall there, and hands the file back once it's done.
//! `open`/`create` themselves go through the same path (opening a file
//! can block too -- a network filesystem mount, say).

use crate::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};
use crate::task::JoinHandle;
use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

/// The result of whichever operation was in flight, carried back
/// alongside the `std::fs::File` itself once the blocking closure
/// finishes -- see [`State`]'s docs for why every operation shares one
/// enum instead of three separate ones.
enum Op {
    Read(io::Result<Vec<u8>>),
    Write(io::Result<usize>),
    Seek(io::Result<u64>),
    SetLen(io::Result<()>),
    SetPermissions(io::Result<()>),
    SyncAll(io::Result<()>),
    SyncData(io::Result<()>),
    TryClone(io::Result<std::fs::File>),
}

/// The chunk size a single `poll_read`/`poll_write` dispatches to the
/// blocking pool, absent a call to [`File::set_max_buf_size`] -- matches
/// tokio's own default. Keeps one oversized read/write from tying up a
/// blocking-pool thread for an unbounded amount of time; callers reading/
/// writing more than this just get however much fit, same as a real
/// `read(2)`/`write(2)` short read/write, and loop (`AsyncReadExt`/
/// `AsyncWriteExt`'s `_exact`/`_all` helpers already do).
const DEFAULT_MAX_BUF_SIZE: usize = 2 * 1024 * 1024;

/// `std::fs::File`'s `read`/`write`/`seek` all take `&mut self` (there's
/// only one file cursor, so genuinely concurrent operations on the same
/// file make no sense the way full-duplex socket reads/writes do) --
/// [`File`] mirrors that by requiring exclusive access at every
/// `poll_*` call, rather than reusing `TcpStream`'s `&self`-based shared
/// design (see that type's own docs for why *its* split works and this
/// one doesn't apply here).
enum State {
    /// Holds the real file when nothing's in flight.
    Idle(std::fs::File),
    /// A blocking closure holding the file is running on the pool right
    /// now. If the poll that started it gets dropped before this
    /// resolves (a `select!`/timeout cancelling the read/write/seek
    /// future, say), this state persists on `File` itself regardless --
    /// only the *caller's* future was dropped, not the blocking
    /// operation already dispatched to the pool, which keeps running in
    /// the background the same way an abandoned `spawn_blocking` call
    /// always does. The next call to *any* `poll_read`/`poll_write`/
    /// `poll_seek` drains this leftover operation (discarding its result
    /// if it doesn't match what's being asked for now) before starting
    /// the new one -- see each method's shared `drain-then-start` loop.
    Busy(JoinHandle<(std::fs::File, Op)>),
    /// A previous operation's blocking closure panicked, taking the only
    /// copy of the underlying `std::fs::File` down with it -- there's no
    /// way to recover it, so every further operation fails with the same
    /// cached error instead of panicking the calling task in turn.
    Poisoned,
}

fn poisoned_error() -> io::Error {
    io::Error::other(
        "a previous operation on this File panicked inside the blocking pool, \
         taking the underlying std::fs::File down with it -- this File can no \
         longer be used",
    )
}

/// An async handle to an open file -- see this module's own docs for why
/// every operation is a [`crate::spawn_blocking`] round trip rather than
/// reactor-driven the way [`crate::io::TcpStream`] is.
pub struct File {
    state: State,
    max_buf_size: usize,
}

impl File {
    /// Opens an existing file for reading. See `std::fs::File::open`.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn open(path: impl AsRef<Path>) -> io::Result<File> {
        let path = path.as_ref().to_path_buf();
        Self::spawn_open(move || std::fs::File::open(path)).await
    }

    /// Opens a file for writing, creating it if it doesn't exist and
    /// truncating it if it does. See `std::fs::File::create`.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn create(path: impl AsRef<Path>) -> io::Result<File> {
        let path = path.as_ref().to_path_buf();
        Self::spawn_open(move || std::fs::File::create(path)).await
    }

    async fn spawn_open(
        open: impl FnOnce() -> io::Result<std::fs::File> + Send + 'static,
    ) -> io::Result<File> {
        crate::spawn_blocking(open)
            .await
            .unwrap_or_else(|_| Err(poisoned_error()))
            .map(|std_file| File {
                state: State::Idle(std_file),
                max_buf_size: DEFAULT_MAX_BUF_SIZE,
            })
    }

    /// Takes the underlying file out of `state`, leaving `Poisoned`
    /// behind as a placeholder for the moment in between -- always
    /// immediately overwritten with a fresh `Busy(..)` by the caller.
    /// Only ever called from the `State::Idle` match arm, so the
    /// `unreachable!()` never actually fires.
    fn take_idle(state: &mut State) -> std::fs::File {
        match std::mem::replace(state, State::Poisoned) {
            State::Idle(file) => file,
            State::Busy(_) | State::Poisoned => unreachable!(),
        }
    }

    /// The current cap on how many bytes a single `poll_read`/
    /// `poll_write` call dispatches to the blocking pool at once. See
    /// [`DEFAULT_MAX_BUF_SIZE`]'s docs for why this exists at all.
    pub fn max_buf_size(&self) -> usize {
        self.max_buf_size
    }

    /// Changes the cap [`max_buf_size`](Self::max_buf_size) reports and
    /// every subsequent read/write respects. Pure in-memory bookkeeping
    /// -- no I/O, so this doesn't need `spawn_blocking` the way every
    /// other method here does.
    pub fn set_max_buf_size(&mut self, max_buf_size: usize) {
        self.max_buf_size = max_buf_size;
    }

    /// Truncates or extends the file to exactly `size` bytes. See
    /// `std::fs::File::set_len`.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn set_len(&mut self, size: u64) -> io::Result<()> {
        std::future::poll_fn(|cx| {
            self.poll_dispatch(cx, move |f| {
                let result = f.set_len(size);
                (f, Op::SetLen(result))
            })
        })
        .await
    }

    /// Changes the file's permissions. See `std::fs::File::set_permissions`.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn set_permissions(&mut self, perm: std::fs::Permissions) -> io::Result<()> {
        std::future::poll_fn(|cx| {
            // `poll_fn`'s closure is `FnMut` (may run more than once if
            // the operation is `Pending` on an earlier poll), but
            // `poll_dispatch`'s `dispatch` param is `FnOnce` -- cloning
            // here, once per poll, gives each reconstruction of the
            // inner closure its own owned copy instead of trying to
            // move the same outer-captured `perm` more than once.
            let perm = perm.clone();
            self.poll_dispatch(cx, move |f| {
                let result = f.set_permissions(perm);
                (f, Op::SetPermissions(result))
            })
        })
        .await
    }

    /// Flushes both the file's data and metadata to disk. See
    /// `std::fs::File::sync_all`.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn sync_all(&mut self) -> io::Result<()> {
        std::future::poll_fn(|cx| {
            self.poll_dispatch(cx, |f| {
                let result = f.sync_all();
                (f, Op::SyncAll(result))
            })
        })
        .await
    }

    /// Flushes the file's data to disk, but not necessarily metadata that
    /// doesn't affect subsequent reads (e.g. modification time) -- may be
    /// faster than [`sync_all`](Self::sync_all) on platforms where that
    /// distinction exists. See `std::fs::File::sync_data`.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn sync_data(&mut self) -> io::Result<()> {
        std::future::poll_fn(|cx| {
            self.poll_dispatch(cx, |f| {
                let result = f.sync_data();
                (f, Op::SyncData(result))
            })
        })
        .await
    }

    /// Duplicates this file (`dup(2)`-equivalent -- an independent fd
    /// onto the same underlying open file description, same guarantee
    /// `std::fs::File::try_clone` gives), by handing the request to the
    /// blocking pool the same way every other operation here does.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn try_clone(&mut self) -> io::Result<File> {
        let cloned = std::future::poll_fn(|cx| self.poll_try_clone(cx)).await?;
        Ok(File {
            state: State::Idle(cloned),
            max_buf_size: self.max_buf_size,
        })
    }

    /// Hands this file back out as a plain, blocking `std::fs::File` --
    /// only if nothing is currently in flight on it (`Busy`) and it
    /// hasn't been [`Poisoned`](State::Poisoned) by a previous panic;
    /// `Err(self)` otherwise, so the caller can decide whether to wait
    /// (e.g. by awaiting an in-progress operation to completion first)
    /// or give up rather than losing the file. Unlike every other method
    /// here, this needs no `spawn_blocking` round trip at all -- taking
    /// the already-idle `std::fs::File` out is itself non-blocking.
    pub fn try_into_std(mut self) -> Result<std::fs::File, Self> {
        match std::mem::replace(&mut self.state, State::Poisoned) {
            State::Idle(std_file) => Ok(std_file),
            state @ (State::Busy(_) | State::Poisoned) => {
                self.state = state;
                Err(self)
            }
        }
    }

    /// Shared `drain-then-start` dispatch for the `&mut self` metadata
    /// operations above -- same shape as `poll_read`/`poll_write`/
    /// `poll_seek`'s own loops, just generic over which blocking closure
    /// to run and which `Op` variant to unwrap. Only starts a fresh
    /// dispatch from the `Idle` arm (mirroring `poll_read` etc.); a
    /// `Busy` re-poll just re-polls the same in-flight operation, so
    /// `dispatch` -- freshly constructed by the caller on every poll,
    /// same as `poll_fn`'s own closure is -- is silently dropped unused
    /// in that case.
    fn poll_dispatch(
        &mut self,
        cx: &mut Context<'_>,
        dispatch: impl FnOnce(std::fs::File) -> (std::fs::File, Op) + Send + 'static,
    ) -> Poll<io::Result<()>> {
        // `dispatch` only actually starts once state is genuinely idle --
        // which may not be the very first loop iteration, if `state` is
        // `Busy` with a leftover operation from an earlier cancelled
        // future that has to drain first (see `poll_read`'s docs on
        // `Busy`). Wrapped in `Option` so `.take()` can hand it out from
        // inside the loop without the borrow checker treating it as
        // reachable more than once -- it never actually is: state only
        // transitions into `Idle` once per call, after which this branch
        // isn't reached again.
        let mut dispatch = Some(dispatch);
        loop {
            if let State::Idle(_) = &self.state {
                let std_file = Self::take_idle(&mut self.state);
                let dispatch = dispatch.take().expect("Idle reached only once per call");
                self.state = State::Busy(crate::spawn_blocking(move || dispatch(std_file)));
            }
            match &mut self.state {
                State::Idle(_) => unreachable!("just started a dispatch above"),
                State::Busy(handle) => match Pin::new(handle).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(_join_err)) => {
                        self.state = State::Poisoned;
                        return Poll::Ready(Err(poisoned_error()));
                    }
                    Poll::Ready(Ok((std_file, op))) => {
                        self.state = State::Idle(std_file);
                        match op {
                            Op::SetLen(result)
                            | Op::SetPermissions(result)
                            | Op::SyncAll(result)
                            | Op::SyncData(result) => return Poll::Ready(result),
                            Op::Read(_) | Op::Write(_) | Op::Seek(_) | Op::TryClone(_) => continue,
                        }
                    }
                },
                State::Poisoned => return Poll::Ready(Err(poisoned_error())),
            }
        }
    }

    /// [`poll_dispatch`](Self::poll_dispatch)'s sibling for
    /// [`try_clone`](Self::try_clone) -- same `drain-then-start` shape,
    /// just unwrapping `Op::TryClone` instead of the plain-`()` variants.
    fn poll_try_clone(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<std::fs::File>> {
        loop {
            if let State::Idle(_) = &self.state {
                let std_file = Self::take_idle(&mut self.state);
                self.state = State::Busy(crate::spawn_blocking(move || {
                    let cloned = std_file.try_clone();
                    (std_file, Op::TryClone(cloned))
                }));
            }
            match &mut self.state {
                State::Idle(_) => unreachable!("just started a dispatch above"),
                State::Busy(handle) => match Pin::new(handle).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(_join_err)) => {
                        self.state = State::Poisoned;
                        return Poll::Ready(Err(poisoned_error()));
                    }
                    Poll::Ready(Ok((std_file, op))) => {
                        self.state = State::Idle(std_file);
                        match op {
                            Op::TryClone(result) => return Poll::Ready(result),
                            Op::Read(_)
                            | Op::Write(_)
                            | Op::Seek(_)
                            | Op::SetLen(_)
                            | Op::SetPermissions(_)
                            | Op::SyncAll(_)
                            | Op::SyncData(_) => continue,
                        }
                    }
                },
                State::Poisoned => return Poll::Ready(Err(poisoned_error())),
            }
        }
    }
}

impl AsyncRead for File {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            match &mut self.state {
                State::Idle(_) => {
                    let mut std_file = Self::take_idle(&mut self.state);
                    let want = buf.remaining().min(self.max_buf_size);
                    self.state = State::Busy(crate::spawn_blocking(move || {
                        let mut chunk = vec![0u8; want];
                        let result = std::io::Read::read(&mut std_file, &mut chunk).map(|n| {
                            chunk.truncate(n);
                            chunk
                        });
                        (std_file, Op::Read(result))
                    }));
                }
                State::Busy(handle) => match Pin::new(handle).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(_join_err)) => {
                        self.state = State::Poisoned;
                        return Poll::Ready(Err(poisoned_error()));
                    }
                    Poll::Ready(Ok((std_file, op))) => {
                        self.state = State::Idle(std_file);
                        match op {
                            Op::Read(Ok(chunk)) => {
                                buf.unfilled_mut()[..chunk.len()].copy_from_slice(&chunk);
                                buf.advance(chunk.len());
                                return Poll::Ready(Ok(()));
                            }
                            Op::Read(Err(e)) => return Poll::Ready(Err(e)),
                            // A leftover operation from a previously
                            // cancelled future -- already drained by the
                            // `Idle` transition above; loop around to
                            // actually start the read now that the file
                            // is free again.
                            Op::Write(_)
                            | Op::Seek(_)
                            | Op::SetLen(_)
                            | Op::SetPermissions(_)
                            | Op::SyncAll(_)
                            | Op::SyncData(_)
                            | Op::TryClone(_) => continue,
                        }
                    }
                },
                State::Poisoned => return Poll::Ready(Err(poisoned_error())),
            }
        }
    }
}

impl AsyncWrite for File {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            match &mut self.state {
                State::Idle(_) => {
                    let mut std_file = Self::take_idle(&mut self.state);
                    // Copied into an owned buffer -- `spawn_blocking`'s
                    // closure needs `'static` data, and `buf` only lives
                    // as long as this one `poll_write` call. Capped at
                    // `max_buf_size`, same as `poll_read`'s own chunk
                    // size -- callers writing more than that just get a
                    // short write and loop (`AsyncWriteExt::write_all`
                    // already does), rather than tying up a blocking-pool
                    // thread for an unbounded amount of time.
                    let n = buf.len().min(self.max_buf_size);
                    let data = buf[..n].to_vec();
                    self.state = State::Busy(crate::spawn_blocking(move || {
                        let result = std::io::Write::write(&mut std_file, &data);
                        (std_file, Op::Write(result))
                    }));
                }
                State::Busy(handle) => match Pin::new(handle).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(_join_err)) => {
                        self.state = State::Poisoned;
                        return Poll::Ready(Err(poisoned_error()));
                    }
                    Poll::Ready(Ok((std_file, op))) => {
                        self.state = State::Idle(std_file);
                        match op {
                            Op::Write(result) => return Poll::Ready(result),
                            Op::Read(_)
                            | Op::Seek(_)
                            | Op::SetLen(_)
                            | Op::SetPermissions(_)
                            | Op::SyncAll(_)
                            | Op::SyncData(_)
                            | Op::TryClone(_) => continue,
                        }
                    }
                },
                State::Poisoned => return Poll::Ready(Err(poisoned_error())),
            }
        }
    }

    /// A no-op, like `TcpStream`'s: every `poll_write` call above is
    /// already awaited to completion (`Ready`) before returning, so by
    /// the time a caller gets around to calling `flush`, there's never
    /// anything still in flight left to wait for.
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let _ = self;
        Poll::Ready(Ok(()))
    }

    /// There's no OS-level "half-close" for a plain file the way there
    /// is for a socket's write direction -- this just flushes (a no-op,
    /// per [`poll_flush`](Self::poll_flush)) and nothing else.
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

impl AsyncSeek for File {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: io::SeekFrom,
    ) -> Poll<io::Result<u64>> {
        loop {
            match &mut self.state {
                State::Idle(_) => {
                    let mut std_file = Self::take_idle(&mut self.state);
                    self.state = State::Busy(crate::spawn_blocking(move || {
                        let result = std::io::Seek::seek(&mut std_file, pos);
                        (std_file, Op::Seek(result))
                    }));
                }
                State::Busy(handle) => match Pin::new(handle).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(_join_err)) => {
                        self.state = State::Poisoned;
                        return Poll::Ready(Err(poisoned_error()));
                    }
                    Poll::Ready(Ok((std_file, op))) => {
                        self.state = State::Idle(std_file);
                        match op {
                            Op::Seek(result) => return Poll::Ready(result),
                            Op::Read(_)
                            | Op::Write(_)
                            | Op::SetLen(_)
                            | Op::SetPermissions(_)
                            | Op::SyncAll(_)
                            | Op::SyncData(_)
                            | Op::TryClone(_) => continue,
                        }
                    }
                },
                State::Poisoned => return Poll::Ready(Err(poisoned_error())),
            }
        }
    }
}

/// Creates a new, empty directory at `path`. See `std::fs::create_dir`
/// -- fails if any parent component doesn't already exist; see
/// [`create_dir_all`] for the recursive version.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub async fn create_dir(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    crate::spawn_blocking(move || std::fs::create_dir(path))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}

/// Recursively creates `path` and every missing parent directory. See
/// `std::fs::create_dir_all` -- unlike [`create_dir`], succeeds
/// (without doing anything further) if `path` already exists as a
/// directory.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub async fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    crate::spawn_blocking(move || std::fs::create_dir_all(path))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}

/// Removes the empty directory at `path`. See `std::fs::remove_dir` --
/// fails if `path` isn't empty; see [`remove_dir_all`] for the
/// recursive version.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub async fn remove_dir(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    crate::spawn_blocking(move || std::fs::remove_dir(path))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}

/// Recursively removes `path` and everything under it. See
/// `std::fs::remove_dir_all`.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub async fn remove_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    crate::spawn_blocking(move || std::fs::remove_dir_all(path))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}

/// Returns the canonical, absolute form of `path` (symlinks resolved,
/// `.`/`..` resolved). See `std::fs::canonicalize`.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub async fn canonicalize(path: impl AsRef<Path>) -> io::Result<std::path::PathBuf> {
    let path = path.as_ref().to_path_buf();
    crate::spawn_blocking(move || std::fs::canonicalize(path))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}

/// Queries the metadata of the file/directory at `path`, following a
/// symlink at `path` itself. See `std::fs::metadata`.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub async fn metadata(path: impl AsRef<Path>) -> io::Result<std::fs::Metadata> {
    let path = path.as_ref().to_path_buf();
    crate::spawn_blocking(move || std::fs::metadata(path))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}

/// Like [`metadata`], but doesn't follow a symlink at `path` itself --
/// see `std::fs::symlink_metadata`.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub async fn symlink_metadata(path: impl AsRef<Path>) -> io::Result<std::fs::Metadata> {
    let path = path.as_ref().to_path_buf();
    crate::spawn_blocking(move || std::fs::symlink_metadata(path))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}

/// Whether `path` exists -- unlike a bare `metadata(path).await.is_ok()`,
/// distinguishes "confirmed absent" (`Ok(false)`) from "couldn't tell"
/// (permission denied, etc., still an `Err`). See
/// `std::path::Path::try_exists`.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub async fn try_exists(path: impl AsRef<Path>) -> io::Result<bool> {
    let path = path.as_ref().to_path_buf();
    crate::spawn_blocking(move || path.try_exists())
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}

/// Sets `path`'s permissions. See `std::fs::set_permissions` -- for an
/// already-open [`File`], prefer [`File::set_permissions`], which
/// avoids a second path lookup.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub async fn set_permissions(path: impl AsRef<Path>, perm: std::fs::Permissions) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    crate::spawn_blocking(move || std::fs::set_permissions(path, perm))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}

/// Renames (moves) `from` to `to`, replacing the destination file if
/// it already exists. See `std::fs::rename`.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    let from = from.as_ref().to_path_buf();
    let to = to.as_ref().to_path_buf();
    crate::spawn_blocking(move || std::fs::rename(from, to))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}

/// Creates a hard link at `link` pointing at `original`. See
/// `std::fs::hard_link`.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub async fn hard_link(original: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
    let original = original.as_ref().to_path_buf();
    let link = link.as_ref().to_path_buf();
    crate::spawn_blocking(move || std::fs::hard_link(original, link))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}

/// Reads the target a symlink at `path` points at, without resolving
/// it further. See `std::fs::read_link`.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub async fn read_link(path: impl AsRef<Path>) -> io::Result<std::path::PathBuf> {
    let path = path.as_ref().to_path_buf();
    crate::spawn_blocking(move || std::fs::read_link(path))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}

/// Removes the file at `path` -- see [`remove_dir`] for a directory
/// instead. See `std::fs::remove_file`.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub async fn remove_file(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    crate::spawn_blocking(move || std::fs::remove_file(path))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}

/// Copies the file at `from` to `to`, overwriting `to` if it already
/// exists, and returns the number of bytes copied. See `std::fs::copy`
/// -- distinct from [`crate::io::copy`], which streams between any
/// `AsyncRead`/`AsyncWrite` pair rather than delegating to a single
/// (often more efficient, e.g. `copy_file_range(2)`-backed) OS syscall
/// between two paths.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub async fn copy(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<u64> {
    let from = from.as_ref().to_path_buf();
    let to = to.as_ref().to_path_buf();
    crate::spawn_blocking(move || std::fs::copy(from, to))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}

/// Creates a symlink at `link` pointing at `original`. See
/// `std::os::unix::fs::symlink` -- Windows draws a hard distinction
/// between file and directory symlinks at creation time (`symlink_file`/
/// `symlink_dir`, Windows-only), which Unix doesn't, so this unified
/// form is Unix-only.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
#[cfg(unix)]
pub async fn symlink(original: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
    let original = original.as_ref().to_path_buf();
    let link = link.as_ref().to_path_buf();
    crate::spawn_blocking(move || std::os::unix::fs::symlink(original, link))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}

/// Creates a symlink at `link` pointing at the file `original` -- see
/// `std::os::windows::fs::symlink_file`. Windows-only; `symlink`
/// (Unix-only) is the equivalent file-vs-directory-agnostic function
/// there.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
#[cfg(windows)]
pub async fn symlink_file(original: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
    let original = original.as_ref().to_path_buf();
    let link = link.as_ref().to_path_buf();
    crate::spawn_blocking(move || std::os::windows::fs::symlink_file(original, link))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}

/// Creates a symlink at `link` pointing at the directory `original` --
/// see `std::os::windows::fs::symlink_dir`. Windows-only; see
/// [`symlink_file`] for the file counterpart.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
#[cfg(windows)]
pub async fn symlink_dir(original: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
    let original = original.as_ref().to_path_buf();
    let link = link.as_ref().to_path_buf();
    crate::spawn_blocking(move || std::os::windows::fs::symlink_dir(original, link))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}

/// Reads the entire contents of the file at `path` into a `Vec<u8>` in
/// one call. See `std::fs::read` -- one-shot whole-file convenience,
/// distinct from [`crate::io::AsyncReadExt::read`]/`read_to_end` (which
/// need an already-open [`File`] to read from incrementally).
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub async fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    let path = path.as_ref().to_path_buf();
    crate::spawn_blocking(move || std::fs::read(path))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}

/// Reads the entire contents of the file at `path` as a `String` in one
/// call, failing with `InvalidData` if it isn't valid UTF-8. See
/// `std::fs::read_to_string`.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub async fn read_to_string(path: impl AsRef<Path>) -> io::Result<String> {
    let path = path.as_ref().to_path_buf();
    crate::spawn_blocking(move || std::fs::read_to_string(path))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}

/// Writes `contents` to the file at `path` in one call, creating it if
/// it doesn't exist and truncating it if it does (equivalent to
/// `File::create` followed by writing the whole buffer). See
/// `std::fs::write` -- distinct from
/// [`crate::io::AsyncWriteExt::write`]/`write_all` (which need an
/// already-open [`File`] to write to incrementally).
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub async fn write(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    let contents = contents.as_ref().to_vec();
    crate::spawn_blocking(move || std::fs::write(path, contents))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))
}
