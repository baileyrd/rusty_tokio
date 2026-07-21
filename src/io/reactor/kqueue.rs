//! The macOS backend: `kevent` plus a software `EVFILT_USER` event
//! (rather than a separate wake fd like Linux's `eventfd` -- kqueue has
//! a built-in way to do this without one) to wake it early for
//! registration/shutdown.
//!
//! Untested on real hardware as of this writing -- this sandbox is
//! Linux-only, so this file is verified with `cargo check --target
//! x86_64-apple-darwin` (real macOS `libc` bindings, real type-checking)
//! but has never actually been linked or run on macOS. Treat it as
//! reviewed-but-unverified until someone runs the test suite on real
//! hardware -- unlike `socket/mod.rs`'s macOS half, which now builds on
//! rustils' `platform-macos` and inherits that crate's own real
//! `macos-latest` CI (see rustils#48/#52/#53); this reactor is this
//! crate's own code with no such upstream coverage.

use super::{Interest, ScheduledIo};
use std::collections::HashMap;
use std::io;
use std::mem;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Arbitrary, fixed identity for the one `EVFILT_USER` event this
/// reactor registers for waking itself -- never collides with a real fd
/// (`ident` for `EVFILT_READ`/`EVFILT_WRITE` is always a fd, and this
/// reactor only ever registers one `EVFILT_USER` event, so there's
/// nothing else it could be confused with).
const WAKE_IDENT: usize = 0;

fn empty_kevent() -> libc::kevent {
    // SAFETY: an all-zero `kevent` is a valid (if inert) value for this
    // plain-old-data type; every field actually used is set explicitly
    // below before the struct is passed to the kernel.
    unsafe { mem::zeroed() }
}

fn change(ident: usize, filter: i16, flags: u16, fflags: u32) -> libc::kevent {
    let mut ev = empty_kevent();
    ev.ident = ident;
    ev.filter = filter;
    ev.flags = flags;
    ev.fflags = fflags;
    ev
}

pub(crate) struct Reactor {
    kq_fd: RawFd,
    registry: Mutex<HashMap<RawFd, Arc<ScheduledIo>>>,
    shutdown: AtomicBool,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Reactor {
    pub(crate) fn new() -> io::Result<Reactor> {
        // SAFETY: no arguments reference memory.
        let kq_fd = unsafe { libc::kqueue() };
        if kq_fd < 0 {
            return Err(io::Error::last_os_error());
        }

        // `EV_CLEAR`: the event auto-resets after being reported, so it
        // behaves like a one-shot pulse per `wake()` call rather than
        // staying "ready" forever after the first trigger.
        let wake_ev = change(
            WAKE_IDENT,
            libc::EVFILT_USER,
            libc::EV_ADD | libc::EV_CLEAR,
            0,
        );
        // SAFETY: `kq_fd` is valid and freshly created; `&wake_ev` is
        // a valid single-element changelist outliving the call; no
        // output eventlist is requested (`nevents: 0`).
        let r = unsafe {
            libc::kevent(
                kq_fd,
                &wake_ev,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if r < 0 {
            let err = io::Error::last_os_error();
            // SAFETY: `kq_fd` is a valid fd we just created.
            unsafe { libc::close(kq_fd) };
            return Err(err);
        }

        Ok(Reactor {
            kq_fd,
            registry: Mutex::new(HashMap::new()),
            shutdown: AtomicBool::new(false),
            thread: Mutex::new(None),
        })
    }

    /// Spawns the background kqueue thread. Split from `new` because the
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
        let mut events = vec![empty_kevent(); 256];
        loop {
            if self.shutdown.load(Ordering::Acquire) {
                return;
            }
            // SAFETY: `events` is a valid, exclusively-borrowed buffer
            // of at least `events.len()` `kevent`s; `kq_fd` is valid for
            // the reactor's whole lifetime; a null `timeout` blocks
            // indefinitely, the same as epoll's `-1`.
            let n = unsafe {
                libc::kevent(
                    self.kq_fd,
                    std::ptr::null(),
                    0,
                    events.as_mut_ptr(),
                    events.len() as i32,
                    std::ptr::null(),
                )
            };
            if n < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                // Nothing sane to do with a fatal kevent error; exit the
                // thread rather than spin.
                return;
            }
            for ev in &events[..n as usize] {
                if ev.filter == libc::EVFILT_USER && ev.ident == WAKE_IDENT {
                    // Just a nudge to re-check `shutdown` above; nothing
                    // to drain (`EV_CLEAR` already reset it).
                    continue;
                }
                let fd = ev.ident as RawFd;
                let io = self.registry.lock().unwrap().get(&fd).cloned();
                let Some(io) = io else { continue };
                // EOF/error can arrive on either filter but means both
                // directions should be woken -- the same reasoning
                // epoll's EPOLLHUP/EPOLLERR handling uses.
                let eof_or_err = ev.flags & (libc::EV_EOF | libc::EV_ERROR) != 0;
                if ev.filter == libc::EVFILT_READ || eof_or_err {
                    io.mark_ready(Interest::Read);
                }
                if ev.filter == libc::EVFILT_WRITE || eof_or_err {
                    io.mark_ready(Interest::Write);
                }
            }
        }
    }

    fn wake(&self) {
        let ev = change(
            WAKE_IDENT,
            libc::EVFILT_USER,
            libc::EV_ADD,
            libc::NOTE_TRIGGER,
        );
        // SAFETY: `kq_fd` is valid; `&ev` is a valid single-element
        // changelist outliving the call; no output eventlist requested.
        unsafe {
            libc::kevent(
                self.kq_fd,
                &ev,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            );
        }
    }

    pub(crate) fn register(&self, fd: RawFd) -> io::Result<Arc<ScheduledIo>> {
        let io = Arc::new(ScheduledIo::new());
        let changes = [
            change(fd as usize, libc::EVFILT_READ, libc::EV_ADD, 0),
            change(fd as usize, libc::EVFILT_WRITE, libc::EV_ADD, 0),
        ];
        let mut errors = [empty_kevent(); 2];
        // SAFETY: `kq_fd` is valid; `changes` is a valid 2-element
        // changelist and `errors` a valid 2-element output buffer, both
        // outliving the call; `fd` is a valid, open fd owned by the
        // caller.
        let r = unsafe {
            libc::kevent(
                self.kq_fd,
                changes.as_ptr(),
                changes.len() as i32,
                errors.as_mut_ptr(),
                errors.len() as i32,
                std::ptr::null(),
            )
        };
        if r < 0 {
            return Err(io::Error::last_os_error());
        }
        self.registry.lock().unwrap().insert(fd, io.clone());
        Ok(io)
    }

    pub(crate) fn deregister(&self, fd: RawFd) {
        self.registry.lock().unwrap().remove(&fd);
        let changes = [
            change(fd as usize, libc::EVFILT_READ, libc::EV_DELETE, 0),
            change(fd as usize, libc::EVFILT_WRITE, libc::EV_DELETE, 0),
        ];
        let mut errors = [empty_kevent(); 2];
        // SAFETY: see `register`. A per-change `EV_ERROR` (e.g. the
        // kernel already dropped this filter because the fd itself was
        // closed) is reported into `errors`, not treated as a hard
        // failure of the whole call -- deregistering an already-gone
        // filter is a harmless no-op, the same as epoll's `EPOLL_CTL_DEL`
        // on a closed fd.
        unsafe {
            libc::kevent(
                self.kq_fd,
                changes.as_ptr(),
                changes.len() as i32,
                errors.as_mut_ptr(),
                errors.len() as i32,
                std::ptr::null(),
            );
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
        // SAFETY: `kq_fd` is owned exclusively by this `Reactor` and
        // still open at this point.
        unsafe {
            libc::close(self.kq_fd);
        }
    }
}
