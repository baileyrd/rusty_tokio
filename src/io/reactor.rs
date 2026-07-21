//! The I/O reactor: one background thread blocked in `epoll_wait`,
//! translating readiness events into waker calls. Level-triggered, on
//! purpose -- edge-triggered epoll demands that every reader drain a fd
//! until it sees `EWOULDBLOCK` or risk missing events forever, which is
//! an easy invariant to get subtly wrong. Level-triggered costs one
//! extra syscall in the common case and is much harder to misuse.

use std::collections::HashMap;
use std::io;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Interest {
    Read,
    Write,
}

/// Per-registered-fd readiness state: one bit each for readable and
/// writable, plus the waker to fire when that bit flips on.
pub(crate) struct ScheduledIo {
    readable: AtomicBool,
    writable: AtomicBool,
    read_waker: Mutex<Option<Waker>>,
    write_waker: Mutex<Option<Waker>>,
}

impl ScheduledIo {
    fn new() -> Self {
        ScheduledIo {
            // Optimistic: assume both directions are ready until a
            // WouldBlock proves otherwise. This matches every real fd's
            // actual state right after it's created (a listener can
            // usually be written to immediately, a fresh connect result
            // is unknown either way -- either is a safe first guess
            // since a wrong guess just costs one wasted syscall attempt).
            readable: AtomicBool::new(true),
            writable: AtomicBool::new(true),
            read_waker: Mutex::new(None),
            write_waker: Mutex::new(None),
        }
    }

    fn poll_ready(&self, cx: &mut Context<'_>, interest: Interest) -> Poll<()> {
        let (flag, waker_slot) = match interest {
            Interest::Read => (&self.readable, &self.read_waker),
            Interest::Write => (&self.writable, &self.write_waker),
        };
        if flag.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        *waker_slot.lock().unwrap() = Some(cx.waker().clone());
        // Re-check after registering the waker: the epoll thread may
        // have flipped the bit between our first load and taking the
        // lock above, and if we didn't check again that wakeup would be
        // lost (nothing left to observe the flag flip).
        if flag.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        Poll::Pending
    }

    fn clear(&self, interest: Interest) {
        match interest {
            Interest::Read => self.readable.store(false, Ordering::Release),
            Interest::Write => self.writable.store(false, Ordering::Release),
        }
    }

    fn mark_ready(&self, interest: Interest) {
        let (flag, waker_slot) = match interest {
            Interest::Read => (&self.readable, &self.read_waker),
            Interest::Write => (&self.writable, &self.write_waker),
        };
        flag.store(true, Ordering::Release);
        if let Some(waker) = waker_slot.lock().unwrap().take() {
            waker.wake();
        }
    }
}

/// Run `op` once `interest` readiness is available, in a `Poll`-based
/// shape rather than an `async fn` -- the primitive [`AsyncRead`]/
/// [`AsyncWrite`](super::async_io)'s `poll_read`/`poll_write` need,
/// since they can't `.await` anything themselves. [`ready_io`] below is
/// just this wrapped in `poll_fn` for callers that can.
pub(crate) fn poll_io<T>(
    io: &Arc<ScheduledIo>,
    interest: Interest,
    cx: &mut Context<'_>,
    mut op: impl FnMut() -> io::Result<T>,
) -> Poll<io::Result<T>> {
    loop {
        if io.poll_ready(cx, interest).is_pending() {
            return Poll::Pending;
        }
        match op() {
            Ok(v) => return Poll::Ready(Ok(v)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                io.clear(interest);
                continue;
            }
            Err(e) => return Poll::Ready(Err(e)),
        }
    }
}

/// Run `op` in a loop, waiting for `interest` readiness on `io` between
/// attempts, until it succeeds or fails with something other than
/// `WouldBlock`.
pub(crate) async fn ready_io<T>(
    io: &Arc<ScheduledIo>,
    interest: Interest,
    mut op: impl FnMut() -> io::Result<T>,
) -> io::Result<T> {
    std::future::poll_fn(|cx| poll_io(io, interest, cx, &mut op)).await
}

pub(crate) struct Reactor {
    epoll_fd: RawFd,
    wake_fd: RawFd,
    registry: Mutex<HashMap<RawFd, Arc<ScheduledIo>>>,
    shutdown: AtomicBool,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Reactor {
    pub(crate) fn new() -> io::Result<Reactor> {
        // SAFETY: no arguments reference memory.
        let epoll_fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        if epoll_fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: no arguments reference memory. Non-blocking so a
        // drain-read from the epoll thread never itself blocks.
        let wake_fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if wake_fd < 0 {
            let err = io::Error::last_os_error();
            // SAFETY: `epoll_fd` is a valid fd we just created.
            unsafe { libc::close(epoll_fd) };
            return Err(err);
        }

        let mut wake_ev = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: wake_fd as u64,
        };
        // SAFETY: `epoll_fd`/`wake_fd` are both valid, freshly created
        // fds; `&mut wake_ev` outlives the call.
        let r = unsafe { libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, wake_fd, &mut wake_ev) };
        if r < 0 {
            let err = io::Error::last_os_error();
            // SAFETY: both fds are valid, owned by us, not yet shared.
            unsafe {
                libc::close(wake_fd);
                libc::close(epoll_fd);
            }
            return Err(err);
        }

        let reactor = Reactor {
            epoll_fd,
            wake_fd,
            registry: Mutex::new(HashMap::new()),
            shutdown: AtomicBool::new(false),
            thread: Mutex::new(None),
        };
        Ok(reactor)
    }

    /// Spawns the background epoll thread. Split from `new` because the
    /// thread closure needs an `Arc<Reactor>`, which doesn't exist until
    /// after construction.
    pub(crate) fn start(self: &Arc<Self>) {
        let reactor = self.clone();
        let handle = std::thread::Builder::new()
            .name("rusty_tokio-reactor".to_string())
            .spawn(move || reactor.event_loop())
            .expect("failed to spawn rusty_tokio reactor thread");
        *self.thread.lock().unwrap() = Some(handle);
    }

    fn event_loop(&self) {
        let mut events = vec![libc::epoll_event { events: 0, u64: 0 }; 256];
        loop {
            if self.shutdown.load(Ordering::Acquire) {
                return;
            }
            // SAFETY: `events` is a valid, exclusively-borrowed buffer
            // of at least `events.len()` `epoll_event`s; `epoll_fd` is
            // valid for the reactor's whole lifetime.
            let n = unsafe {
                libc::epoll_wait(self.epoll_fd, events.as_mut_ptr(), events.len() as i32, -1)
            };
            if n < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                // Nothing sane to do with a fatal epoll_wait error;
                // exit the thread rather than spin.
                return;
            }
            for ev in &events[..n as usize] {
                let fd = ev.u64 as RawFd;
                if fd == self.wake_fd {
                    self.drain_wake_fd();
                    continue;
                }
                let io = self.registry.lock().unwrap().get(&fd).cloned();
                let Some(io) = io else { continue };
                let flags = ev.events;
                if flags
                    & (libc::EPOLLIN as u32
                        | libc::EPOLLHUP as u32
                        | libc::EPOLLERR as u32
                        | libc::EPOLLRDHUP as u32)
                    != 0
                {
                    io.mark_ready(Interest::Read);
                }
                if flags & (libc::EPOLLOUT as u32 | libc::EPOLLHUP as u32 | libc::EPOLLERR as u32)
                    != 0
                {
                    io.mark_ready(Interest::Write);
                }
            }
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
        let mut ev = libc::epoll_event {
            events: (libc::EPOLLIN | libc::EPOLLOUT | libc::EPOLLRDHUP) as u32,
            u64: fd as u64,
        };
        // SAFETY: `epoll_fd` is valid for the reactor's lifetime; `fd`
        // is a valid, open fd owned by the caller; `&mut ev` outlives
        // the call.
        let r = unsafe { libc::epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_ADD, fd, &mut ev) };
        if r < 0 {
            return Err(io::Error::last_os_error());
        }
        self.registry.lock().unwrap().insert(fd, io.clone());
        Ok(io)
    }

    pub(crate) fn deregister(&self, fd: RawFd) {
        self.registry.lock().unwrap().remove(&fd);
        // SAFETY: `epoll_fd` is valid; `fd` was previously registered
        // (or this is a harmless no-op if it wasn't). The kernel ignores
        // the ignored `event` pointer for `EPOLL_CTL_DEL`, but older
        // kernels (pre-2.6.9) require a non-null pointer, so we pass one
        // anyway for portability.
        let mut dummy = libc::epoll_event { events: 0, u64: 0 };
        unsafe {
            libc::epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_DEL, fd, &mut dummy);
        }
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
        // SAFETY: both fds are owned exclusively by this `Reactor` and
        // still open at this point.
        unsafe {
            libc::close(self.wake_fd);
            libc::close(self.epoll_fd);
        }
    }
}
