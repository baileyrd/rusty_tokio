//! An alternative Linux reactor backend (issue #9), built on the
//! `io-uring` crate rather than raw `epoll`. Scoped deliberately
//! narrower than "route socket reads/writes through io_uring": this
//! only replaces the *readiness* mechanism (`IORING_OP_POLL_ADD` in
//! place of `epoll_wait`) behind the exact same [`super::ScheduledIo`]/
//! [`super::poll_io`]/[`super::ready_io`] interface `epoll.rs` uses --
//! the actual `read(2)`/`write(2)` syscalls in `socket/mod.rs` are
//! unchanged, same as ever.
//!
//! That narrower scope is a real safety decision, not a shortcut.
//! io_uring's other read/write opcodes hand the kernel a pointer into a
//! caller-owned buffer for the *duration of an async operation* -- but
//! this crate's `AsyncRead`/`AsyncWrite` traits pass `&mut [u8]`
//! borrows, and a `Future` can be dropped (cancelled) at any `Pending`
//! point per ordinary Rust semantics. Submit a real io_uring read into a
//! borrowed stack buffer, then let that future get dropped (a timeout, a
//! `select!`-style race, plain cancellation) before the kernel operation
//! completes, and the kernel can still write into memory that's since
//! been freed or reused -- a genuine use-after-free, not a hypothetical
//! one. (This is exactly why `tokio-uring`/`monoio` use an
//! owned-buffer-passed-by-value API shape instead of a borrowed one --
//! a different trait design than this crate's `AsyncRead`/`AsyncWrite`,
//! out of scope for issue #9's ask specifically, though not for a
//! `TcpStream`-agnostic API not attempted here.) `IORING_OP_POLL_ADD`
//! has no such hazard: it never references a user buffer at all, only a
//! bare fd and a bitmask, so cancelling the future waiting on it is
//! always safe -- deregistering just stops caring about a completion
//! that, if it still arrives, finds nothing left in `registry` to wake.
//!
//! ## Why the ring is only ever touched by one thread
//!
//! Unlike `epoll_ctl`/`epoll_wait` (independent syscalls the kernel
//! synchronizes internally, so `register`/`deregister` and the event
//! loop's wait can safely run on different threads with no
//! synchronization of their own), an `io_uring` submission/completion
//! queue is caller-owned `mmap`ed memory -- concurrent pushes from
//! multiple threads need external synchronization this crate doesn't
//! provide for you. Rather than wrap the ring in a `Mutex` locked on
//! every `register`/`deregister` call (real contention: every socket
//! creation would fight the reactor thread for the same lock), the ring
//! is moved into the reactor thread's exclusive ownership in [`start`]
//! and never touched by any other thread again. `register`/`deregister`
//! instead queue a [`PendingOp`] and wake the reactor thread (the same
//! `eventfd`-based wake `epoll.rs` uses), which drains that queue and
//! submits the corresponding `PollAdd`/`PollRemove` itself.

use super::{Interest, ScheduledIo};
use io_uring::{opcode, types, IoUring};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Reserved `user_data` for the wake `eventfd`'s own `PollAdd` --
/// `RawFd` is always a small non-negative `i32`, so this (and
/// [`REMOVE_USER_DATA`]) never collide with a real fd cast to `u64`.
const WAKE_USER_DATA: u64 = u64::MAX;
/// Reserved `user_data` for every `PollRemove` submission's own
/// completion -- its result is never interesting (see `drain_pending`),
/// only that it's recognized and skipped rather than mistaken for
/// activity on some fd.
const REMOVE_USER_DATA: u64 = u64::MAX - 1;

const POLL_MASK: u32 = (libc::POLLIN as u32)
    | (libc::POLLOUT as u32)
    | (libc::POLLRDHUP as u32)
    | (libc::POLLHUP as u32)
    | (libc::POLLERR as u32);

enum PendingOp {
    Register(RawFd),
    Deregister(RawFd),
}

pub(crate) struct Reactor {
    /// `Some` until [`start`](Reactor::start) moves it into the reactor
    /// thread; `None` forever after -- see this module's docs for why
    /// nothing else ever touches it.
    ring: Mutex<Option<IoUring>>,
    wake_fd: RawFd,
    registry: Mutex<HashMap<RawFd, Arc<ScheduledIo>>>,
    pending: Mutex<VecDeque<PendingOp>>,
    shutdown: AtomicBool,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Reactor {
    pub(crate) fn new() -> io::Result<Reactor> {
        let ring = IoUring::new(256)?;
        // SAFETY: plain integer arguments, no memory referenced. Non-
        // blocking so a drain-read from the reactor thread never itself
        // blocks -- mirrors `epoll.rs`'s `wake_fd` exactly.
        let wake_fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if wake_fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Reactor {
            ring: Mutex::new(Some(ring)),
            wake_fd,
            registry: Mutex::new(HashMap::new()),
            pending: Mutex::new(VecDeque::new()),
            shutdown: AtomicBool::new(false),
            thread: Mutex::new(None),
        })
    }

    /// Spawns the background reactor thread, moving the ring into it --
    /// split from `new` for the same reason `epoll.rs`'s `start` is:
    /// the thread closure needs an `Arc<Reactor>`, which doesn't exist
    /// until after construction.
    pub(crate) fn start(self: &Arc<Self>) {
        let ring = self
            .ring
            .lock()
            .unwrap()
            .take()
            .expect("Reactor::start called more than once");
        let reactor = self.clone();
        let handle = std::thread::Builder::new()
            .name("rusty_tokio-reactor".to_string())
            .spawn(move || reactor.event_loop(ring))
            .expect("failed to spawn rusty_tokio reactor thread");
        *self.thread.lock().unwrap() = Some(handle);
    }

    fn event_loop(&self, mut ring: IoUring) {
        Self::arm(&mut ring, self.wake_fd, libc::POLLIN as u32, WAKE_USER_DATA);
        loop {
            if self.shutdown.load(Ordering::Acquire) {
                return;
            }
            self.drain_pending(&mut ring);

            match ring.submit_and_wait(1) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                // Nothing sane to do with a fatal io_uring_enter error;
                // exit the thread rather than spin, same as epoll.rs.
                Err(_) => return,
            }

            // Collected into a plain `Vec` first, deliberately: the
            // `CompletionQueue` guard borrows `ring` and writes its
            // consumed head back to the shared ring on drop (freeing
            // those CQE slots for the kernel to reuse), so it needs to
            // be dropped -- ending the loop below -- before this
            // function goes on to call `Self::arm` on `ring` again for
            // each fd found ready.
            let mut completions = Vec::new();
            {
                let mut cq = ring.completion();
                // Reloads the queue's cached tail from the shared,
                // kernel-updated atomic -- without this, a freshly
                // obtained `CompletionQueue` would only see whatever was
                // already visible when the ring was constructed, not
                // what `submit_and_wait` just added.
                cq.sync();
                for cqe in &mut cq {
                    completions.push((cqe.user_data(), cqe.result()));
                }
            }

            for (user_data, result) in completions {
                match user_data {
                    WAKE_USER_DATA => {
                        self.drain_wake_fd();
                        Self::arm(&mut ring, self.wake_fd, libc::POLLIN as u32, WAKE_USER_DATA);
                    }
                    REMOVE_USER_DATA => {
                        // A `PollRemove`'s own completion -- nothing to
                        // do with it either way (`0` if it found and
                        // cancelled the poll, `-ENOENT` if the poll had
                        // already fired or didn't exist).
                    }
                    fd_bits => {
                        let fd = fd_bits as RawFd;
                        let Some(io) = self.registry.lock().unwrap().get(&fd).cloned() else {
                            // Deregistered (and possibly closed) since
                            // this poll was armed -- exactly the stale-
                            // completion case this module's docs
                            // describe as safe to just ignore.
                            continue;
                        };
                        if result < 0 {
                            // A negative result (e.g. `-EBADF` if the fd
                            // was closed without going through
                            // `deregister` first) isn't itself readable
                            // or writable -- but marking both ready lets
                            // whichever `poll_io` retry loop is waiting
                            // immediately re-attempt its real syscall
                            // and surface the actual OS error from that,
                            // rather than this reactor trying to
                            // reinterpret a poll-level errno itself.
                            io.mark_ready(Interest::Read);
                            io.mark_ready(Interest::Write);
                        } else {
                            let events = result as u32;
                            if events
                                & (libc::POLLIN as u32
                                    | libc::POLLHUP as u32
                                    | libc::POLLERR as u32
                                    | libc::POLLRDHUP as u32)
                                != 0
                            {
                                io.mark_ready(Interest::Read);
                            }
                            if events
                                & (libc::POLLOUT as u32
                                    | libc::POLLHUP as u32
                                    | libc::POLLERR as u32)
                                != 0
                            {
                                io.mark_ready(Interest::Write);
                            }
                        }
                        // `PollAdd` defaults to one-shot (see its own
                        // doc comment in the `io-uring` crate): unlike
                        // level-triggered `epoll_wait`, which keeps
                        // reporting readiness on every call for as long
                        // as it holds, a completed poll must be
                        // resubmitted to keep watching this fd -- this
                        // is what makes that resubmission automatic
                        // rather than something a caller needs to
                        // remember, matching epoll's own always-armed
                        // behavior from the rest of this crate's point
                        // of view.
                        Self::arm(&mut ring, fd, POLL_MASK, fd as u64);
                    }
                }
            }
        }
    }

    fn drain_pending(&self, ring: &mut IoUring) {
        let ops: Vec<PendingOp> = std::mem::take(&mut *self.pending.lock().unwrap()).into();
        for op in ops {
            match op {
                PendingOp::Register(fd) => Self::arm(ring, fd, POLL_MASK, fd as u64),
                PendingOp::Deregister(fd) => {
                    let entry = opcode::PollRemove::new(fd as u64)
                        .build()
                        .user_data(REMOVE_USER_DATA);
                    let mut sq = ring.submission();
                    // SAFETY: `PollRemove` (like `PollAdd`) references
                    // no user buffer -- only the `user_data` of the poll
                    // it's cancelling -- so there's nothing that could
                    // become invalid for the operation's duration.
                    let _ = unsafe { sq.push(&entry) };
                }
            }
        }
    }

    /// Submits a one-shot `PollAdd` for `fd` watching `mask`, tagged
    /// with `user_data` -- shared by the wake `eventfd`'s own arming and
    /// every real registered fd's (re-)arming.
    fn arm(ring: &mut IoUring, fd: RawFd, mask: u32, user_data: u64) {
        let entry = opcode::PollAdd::new(types::Fd(fd), mask)
            .build()
            .user_data(user_data);
        let mut sq = ring.submission();
        // SAFETY: see this module's top-level docs -- `PollAdd` never
        // references a user buffer, so nothing can become invalid for
        // the operation's duration regardless of what happens to `fd`
        // afterward.
        if unsafe { sq.push(&entry) }.is_err() {
            // The submission queue is momentarily full -- flush what's
            // already queued to free space and retry once. In practice
            // this shouldn't happen at this ring size for any realistic
            // number of concurrently-registered fds.
            drop(sq);
            let _ = ring.submit();
            let mut sq = ring.submission();
            let _ = unsafe { sq.push(&entry) };
        }
    }

    fn drain_wake_fd(&self) {
        let mut buf = [0u8; 8];
        // SAFETY: `buf` is a valid 8-byte buffer; `wake_fd` is a valid,
        // non-blocking eventfd, so this never blocks even with nothing
        // to drain.
        unsafe {
            libc::read(self.wake_fd, buf.as_mut_ptr().cast(), buf.len());
        }
    }

    fn wake(&self) {
        let one: u64 = 1;
        // SAFETY: `&one` is a valid 8-byte buffer; `wake_fd` is a valid
        // eventfd.
        unsafe {
            libc::write(self.wake_fd, (&one as *const u64).cast(), 8);
        }
    }

    pub(crate) fn register(&self, fd: RawFd) -> io::Result<Arc<ScheduledIo>> {
        let io = Arc::new(ScheduledIo::new());
        self.registry.lock().unwrap().insert(fd, io.clone());
        self.pending
            .lock()
            .unwrap()
            .push_back(PendingOp::Register(fd));
        self.wake();
        Ok(io)
    }

    pub(crate) fn deregister(&self, fd: RawFd) {
        self.registry.lock().unwrap().remove(&fd);
        self.pending
            .lock()
            .unwrap()
            .push_back(PendingOp::Deregister(fd));
        self.wake();
    }

    pub(crate) fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.wake();
        if let Some(handle) = self.thread.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        // The ring itself (and its own internal fd) is dropped by
        // `event_loop`'s local `ring: IoUring` parameter going out of
        // scope when that function returns after `shutdown()` joins it
        // -- only `wake_fd` is this struct's own to close.
        //
        // SAFETY: `wake_fd` is owned exclusively by this `Reactor` and
        // still open at this point.
        unsafe {
            libc::close(self.wake_fd);
        }
    }
}
