//! [`read_dir`]/[`ReadDir`]/[`DirEntry`]: async directory iteration --
//! see this module's own [`ReadDir`] docs for why it's a chunked
//! [`crate::spawn_blocking`] state machine (an `Idle`/`Busy`/`Poisoned`
//! shape analogous to [`super::File`]'s own) rather than one
//! `spawn_blocking` call per entry.

use super::poisoned_error;
use std::collections::VecDeque;
use std::ffi::OsString;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// How many directory entries a single blocking-pool call reads ahead
/// at once -- matches tokio's own chunk size. Large enough that most
/// small-to-medium directories are fully drained by a single
/// `spawn_blocking` round trip; small enough that reading a huge
/// directory doesn't tie up a blocking-pool thread synchronously
/// reading an unbounded number of entries before ever handing control
/// back.
const CHUNK_SIZE: usize = 32;

/// A batch of already-read entries, the underlying reader (if not yet
/// exhausted), and whether reading the batch itself succeeded -- what
/// one [`next_chunk`] round trip to the blocking pool produces.
type Chunk = (
    VecDeque<Arc<std::fs::DirEntry>>,
    Option<std::fs::ReadDir>,
    io::Result<()>,
);

/// Reads up to `CHUNK_SIZE` more entries from `reader`, run entirely on
/// the calling (blocking-pool) thread -- the actual blocking work
/// [`ReadDir::poll_next_entry`] dispatches via [`crate::spawn_blocking`].
/// `reader` comes back `None` once `std::fs::ReadDir`'s own iterator is
/// exhausted; a mid-batch error stops early (any already-read entries
/// before it are still returned, discarding the error's exact position
/// among them -- the next call to `poll_next_entry` reports the error
/// itself before it discards the reader).
fn next_chunk(mut reader: std::fs::ReadDir) -> Chunk {
    let mut buf = VecDeque::with_capacity(CHUNK_SIZE);
    for _ in 0..CHUNK_SIZE {
        match reader.next() {
            Some(Ok(entry)) => buf.push_back(Arc::new(entry)),
            Some(Err(e)) => return (buf, None, Err(e)),
            None => return (buf, None, Ok(())),
        }
    }
    (buf, Some(reader), Ok(()))
}

enum State {
    /// A batch of already-read entries (possibly empty), plus the
    /// underlying `std::fs::ReadDir` to pull the next batch from once
    /// this one's drained -- `None` once that iterator itself reported
    /// exhaustion (no further batches to read, even after this one
    /// drains).
    Idle(VecDeque<Arc<std::fs::DirEntry>>, Option<std::fs::ReadDir>),
    /// A [`next_chunk`] call is running on the blocking pool right now.
    /// Like [`super::File`]'s own `Busy` state, this persists across a
    /// dropped `poll_next_entry` future (a `select!`/timeout cancelling
    /// it) -- the dispatched blocking call keeps running regardless, and
    /// the next call to `poll_next_entry` picks up its result before
    /// starting anything new.
    Busy(crate::task::JoinHandle<Chunk>),
    /// A previous batch's blocking closure panicked, taking the
    /// underlying `std::fs::ReadDir` down with it -- unrecoverable, so
    /// every further call fails with the same cached error instead of
    /// panicking the calling task in turn.
    Poisoned,
}

/// A stream of a directory's entries -- see [`read_dir`]. Reads ahead in
/// chunks of [`CHUNK_SIZE`] entries per [`crate::spawn_blocking`] round
/// trip (rather than one round trip per entry) the way real tokio's own
/// implementation does, buffering the rest locally for subsequent
/// [`next_entry`](Self::next_entry) calls to hand out without touching
/// the blocking pool again until the buffer runs dry.
pub struct ReadDir {
    state: State,
}

/// Reads the entries within the directory at `path`, without recursing
/// into any subdirectories. See `std::fs::read_dir`.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub async fn read_dir(path: impl AsRef<Path>) -> io::Result<ReadDir> {
    let path = path.as_ref().to_path_buf();
    let reader = crate::spawn_blocking(move || std::fs::read_dir(path))
        .await
        .unwrap_or_else(|_| Err(poisoned_error()))?;
    Ok(ReadDir {
        state: State::Idle(VecDeque::new(), Some(reader)),
    })
}

impl ReadDir {
    /// The next entry, or `Ok(None)` once the directory is exhausted.
    /// Cancel-safe: dropping this call's future before it resolves
    /// loses nothing -- any batch already dispatched to the blocking
    /// pool keeps running and is picked up by the next call instead
    /// (see [`State::Busy`]'s own docs).
    pub async fn next_entry(&mut self) -> io::Result<Option<DirEntry>> {
        std::future::poll_fn(|cx| self.poll_next_entry(cx)).await
    }

    /// Non-`async fn` form of [`next_entry`](Self::next_entry), for a
    /// caller implementing its own `Future`/poll loop.
    pub fn poll_next_entry(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Option<DirEntry>>> {
        loop {
            match &mut self.state {
                State::Idle(buf, reader) => {
                    if let Some(entry) = buf.pop_front() {
                        return Poll::Ready(Ok(Some(DirEntry(entry))));
                    }
                    let Some(reader) = reader.take() else {
                        // The underlying iterator reported exhaustion on
                        // a previous batch, and this one (now drained
                        // above) was the last of its entries.
                        return Poll::Ready(Ok(None));
                    };
                    self.state = State::Busy(crate::spawn_blocking(move || next_chunk(reader)));
                }
                State::Busy(handle) => match Pin::new(handle).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(_join_err)) => {
                        self.state = State::Poisoned;
                        return Poll::Ready(Err(poisoned_error()));
                    }
                    Poll::Ready(Ok((buf, reader, result))) => {
                        self.state = State::Idle(buf, reader);
                        if let Err(e) = result {
                            return Poll::Ready(Err(e));
                        }
                    }
                },
                State::Poisoned => return Poll::Ready(Err(poisoned_error())),
            }
        }
    }
}

/// A single entry within a directory being read by [`ReadDir`]. Wraps
/// `std::fs::DirEntry` in an `Arc` (itself not `Clone`) so
/// [`metadata`](Self::metadata)/[`file_type`](Self::file_type) can move
/// a cloned handle onto the blocking pool without moving `self` itself.
pub struct DirEntry(Arc<std::fs::DirEntry>);

impl DirEntry {
    /// The full path to this entry (the directory's own path, joined
    /// with this entry's file name).
    pub fn path(&self) -> PathBuf {
        self.0.path()
    }

    /// This entry's bare file name, without the rest of the path.
    pub fn file_name(&self) -> OsString {
        self.0.file_name()
    }

    /// This entry's metadata. On most platforms cheaper than
    /// `fs::metadata(entry.path())`, since it can often avoid a second
    /// path lookup -- see `std::fs::DirEntry::metadata`.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn metadata(&self) -> io::Result<std::fs::Metadata> {
        let entry = self.0.clone();
        crate::spawn_blocking(move || entry.metadata())
            .await
            .unwrap_or_else(|_| Err(poisoned_error()))
    }

    /// This entry's file type. Doesn't always need the syscall
    /// [`metadata`](Self::metadata) does -- see
    /// `std::fs::DirEntry::file_type`.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn file_type(&self) -> io::Result<std::fs::FileType> {
        let entry = self.0.clone();
        crate::spawn_blocking(move || entry.file_type())
            .await
            .unwrap_or_else(|_| Err(poisoned_error()))
    }

    /// This entry's inode number, read directly from the directory
    /// listing itself with no extra syscall. Unix-only. See
    /// `std::os::unix::fs::DirEntryExt::ino`.
    #[cfg(unix)]
    pub fn ino(&self) -> u64 {
        use std::os::unix::fs::DirEntryExt;
        self.0.ino()
    }
}
