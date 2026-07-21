//! The Linux backend: `epoll_wait` plus an `eventfd` to wake it early
//! for registration/shutdown.

use super::{Interest, ScheduledIo};
use std::collections::HashMap;
use std::io;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

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
