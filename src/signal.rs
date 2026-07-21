//! Async signal handling: [`ctrl_c`] resolves once on the next `SIGINT`;
//! [`signal`] returns a [`Signal`] that fires every time a given
//! [`SignalKind`] arrives, for as long as it's held.
//!
//! **The self-pipe trick.** A signal handler can only safely do a very
//! limited set of things (a short, fixed list of async-signal-safe
//! functions -- notably not allocate, not lock a mutex, not touch most
//! of the runtime this crate would otherwise reach for), so
//! `handle_signal` does exactly one thing: an async-signal-safe
//! `write(2)` of the signal number to a pre-created pipe's write end,
//! whose fd is stashed in a plain [`AtomicI32`] so the handler can find
//! it without allocating or locking. Everything else -- looking up which
//! listeners care about that signal number, waking them -- happens later
//! in `reader_loop`, an ordinary spawned task reading the pipe's read
//! end through the same reactor every socket in this crate uses. This is
//! the standard, portable way real-world signal handling is built
//! (tokio's own driver, and most other signal-handling libraries, use
//! the identical shape); doing real work *inside* the OS signal handler
//! itself is the actual footgun this sidesteps.
//!
//! **Coalescing, not queuing.** Each [`Signal`]'s own `ListenerState`
//! is a single pending flag, not a growing counter -- if the same signal
//! kind arrives twice before a listener gets around to polling, that's
//! observed as one `Some(())`, not two. This matches how tokio's own
//! `Signal` behaves, and how signal delivery already tends to coalesce
//! at the OS level (a signal is not itself a queue).
//!
//! **Idempotent, additive installation.** `signal(kind)` installs a
//! `sigaction` handler for `kind` the *first* time any caller asks for
//! it, and never again afterward for that same kind -- calling it twice
//! for `SIGINT`, say, installs nothing the second time, it just adds
//! another independent listener that gets woken alongside the first.
//! Only signal numbers a caller actually requests are ever touched;
//! nothing here preemptively claims every signal, so a process's own
//! handlers for anything this crate was never asked about are left
//! completely alone.
//!
//! **Global, not per-`Runtime`.** The pipe, the reader task, and the
//! `sigaction` installations are process-wide state, set up once (lazily,
//! on the first `signal`/`ctrl_c` call) and reused for the life of the
//! process -- signals themselves are a process-wide concept, there's no
//! such thing as "the SIGINT for this one `Runtime`" if more than one
//! happens to be running. The reader task itself does run on whichever
//! `Runtime` happened to be current at that first call, though -- in the
//! (unusual) case of multiple concurrent `Runtime`s in one process, only
//! that first one's reactor and scheduler actually drive signal delivery
//! for every listener, regardless of which `Runtime` later callers are
//! on. Matches this crate's realistic, single-runtime-per-process usage;
//! not something to design around further without an actual need.

use crate::io::reactor::{ready_io, Interest, ScheduledIo};
use crate::io::socket;
use crate::runtime::Handle;
use libc::c_int;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::task::{Context, Poll, Waker};

/// Generous headroom past the highest standard POSIX signal number (31)
/// -- this crate only hands out constructors for the common named
/// signals, but [`SignalKind::from_raw`] accepts anything in range.
const NSIG: usize = 64;

/// The self-pipe's write end -- read only from inside `handle_signal`,
/// a real OS signal handler, so a plain relaxed atomic load is all it
/// can safely do; never mutated again once `global` first sets it.
static PIPE_WRITE_FD: AtomicI32 = AtomicI32::new(-1);

struct ListenerState {
    pending: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

/// One slot per possible signal number.
struct SignalSlot {
    /// Whether a `sigaction` handler has been installed for this signal
    /// number yet. Checked and set while holding `listeners`'s own lock
    /// (not a separate atomic-swap dance) so "is it installed" and
    /// "append this listener" happen as one atomic step -- otherwise two
    /// callers racing `signal()` for the same brand-new kind could both
    /// decide they need to install it (harmless: `sigaction` with the
    /// same handler twice is a no-op-shaped redundant syscall, not a
    /// correctness bug) while a subtler race -- one caller's listener
    /// getting appended *before* installation actually succeeds, then
    /// that installation failing -- would leave a listener registered
    /// for a signal nothing will ever actually deliver notice of.
    installed: bool,
    listeners: Vec<Weak<ListenerState>>,
}

struct Global {
    slots: Vec<Mutex<SignalSlot>>,
    /// Kept alive only so the write end's fd stays open for the whole
    /// process lifetime, matching what `handle_signal` assumes; never
    /// read back out.
    _write_fd: OwnedFd,
}

static GLOBAL: OnceLock<io::Result<Global>> = OnceLock::new();

/// The only thing that runs inside the actual OS signal handler --
/// async-signal-safe by construction: one atomic load, one `write(2)`,
/// nothing else. See this module's own docs for why real work happens
/// later, in `reader_loop`, instead.
extern "C" fn handle_signal(signum: c_int) {
    let fd = PIPE_WRITE_FD.load(Ordering::Relaxed);
    if fd < 0 {
        return;
    }
    let byte = signum as u8;
    // SAFETY: async-signal-safe -- `write(2)` is on the POSIX list of
    // functions safe to call from a signal handler. `fd` is a valid,
    // process-lifetime-owned pipe write end once this handler could
    // possibly run at all (installed only after the pipe already
    // exists). A short write to a pipe with room for at least one byte
    // (this module never lets it fill: `reader_loop` drains every byte
    // it can see on every wake) cannot itself block or partially write.
    unsafe {
        libc::write(fd, (&byte as *const u8).cast(), 1);
    }
}

fn install_handler(signum: c_int) -> io::Result<()> {
    // SAFETY: `action` is fully initialized before `sigaction` reads it
    // (every field either zeroed or explicitly set below); `signum` is
    // caller-validated to be in range by `signal`'s own bounds check
    // before this is ever called.
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = handle_signal as *const () as usize;
        libc::sigemptyset(&mut action.sa_mask);
        // SA_RESTART: a syscall interrupted by this signal resumes
        // instead of failing with EINTR -- the same "don't surprise
        // unrelated code elsewhere in the process" reasoning
        // `SA_RESTART` always carries, since this handler's own effect
        // (a two-byte pipe write) is otherwise invisible to whatever
        // the process was already doing when the signal arrived.
        action.sa_flags = libc::SA_RESTART;
        if libc::sigaction(signum, &action, std::ptr::null_mut()) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

fn make_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0 as c_int; 2];
    #[cfg(target_os = "linux")]
    // SAFETY: `fds` is a valid, exclusively-borrowed 2-element out-param
    // for the call's duration.
    let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
    #[cfg(target_os = "macos")]
    // SAFETY: same as the Linux arm; macOS has no `pipe2`, so
    // `O_CLOEXEC`/`O_NONBLOCK` are set via `fcntl` right after instead
    // (the same two-step reasoning `socket::new_tcp_socket`'s own macOS
    // arm documents).
    let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: both fds were just returned by `pipe`/`pipe2` above, valid,
    // otherwise-unowned, and each wrapped exactly once.
    let (read_fd, write_fd) =
        unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
    #[cfg(target_os = "macos")]
    {
        socket::set_nonblocking(read_fd.as_raw_fd(), true)?;
        socket::set_nonblocking(write_fd.as_raw_fd(), true)?;
        // SAFETY: both fds are caller-owned and open; `FD_CLOEXEC` is
        // the sole variadic argument `F_SETFD` expects.
        unsafe {
            libc::fcntl(read_fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
            libc::fcntl(write_fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
        }
    }
    Ok((read_fd, write_fd))
}

async fn reader_loop(io: Arc<ScheduledIo>, read_fd: OwnedFd) {
    let raw_fd = read_fd.as_raw_fd();
    loop {
        let mut buf = [0u8; 64];
        let n = match ready_io(&io, Interest::Read, || socket::read(raw_fd, &mut buf)).await {
            Ok(n) if n > 0 => n,
            // The write end lives in `Global` for the whole process
            // lifetime, so `n == 0` (EOF) should never actually happen;
            // an error here means the reactor itself is in trouble.
            // Either way, nothing sensible to do but stop reading --
            // every future `signal()` call already registered its
            // listener, but none will ever be woken again.
            _ => return,
        };
        for &signum in &buf[..n] {
            dispatch(signum as c_int);
        }
    }
}

fn dispatch(signum: c_int) {
    let Some(Ok(global)) = GLOBAL.get() else {
        return;
    };
    let Some(slot) = global.slots.get(signum as usize) else {
        return;
    };
    let mut slot = slot.lock().unwrap();
    slot.listeners.retain(|weak| {
        let Some(listener) = weak.upgrade() else {
            // The `Signal` this listener belonged to was dropped --
            // drop the now-dangling weak reference too instead of
            // carrying it forever.
            return false;
        };
        listener.pending.store(true, Ordering::Release);
        if let Some(waker) = listener.waker.lock().unwrap().take() {
            waker.wake();
        }
        true
    });
}

fn global() -> io::Result<&'static Global> {
    let result = GLOBAL.get_or_init(|| -> io::Result<Global> {
        let reactor = Handle::current().shared.reactor.clone();
        let (read_fd, write_fd) = make_pipe()?;
        PIPE_WRITE_FD.store(write_fd.as_raw_fd(), Ordering::Relaxed);
        let io = reactor.register(read_fd.as_raw_fd())?;
        crate::spawn(reader_loop(io, read_fd));

        let slots = (0..NSIG)
            .map(|_| {
                Mutex::new(SignalSlot {
                    installed: false,
                    listeners: Vec::new(),
                })
            })
            .collect();
        Ok(Global {
            slots,
            _write_fd: write_fd,
        })
    });
    match result {
        Ok(global) => Ok(global),
        // `io::Error` isn't `Clone`, and initialization failing at all
        // is exceptionally unlikely (a `pipe()`/reactor-registration
        // failure) -- report it the same way every call after the
        // first would otherwise see it (a fresh, equivalent error),
        // rather than trying to hand back the original.
        Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
    }
}

/// A signal kind -- either one of the common named constructors below,
/// or [`SignalKind::from_raw`] for anything else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SignalKind(c_int);

impl SignalKind {
    pub fn from_raw(signum: c_int) -> SignalKind {
        SignalKind(signum)
    }

    pub fn as_raw_value(&self) -> c_int {
        self.0
    }

    pub fn hangup() -> SignalKind {
        SignalKind(libc::SIGHUP)
    }

    pub fn interrupt() -> SignalKind {
        SignalKind(libc::SIGINT)
    }

    pub fn quit() -> SignalKind {
        SignalKind(libc::SIGQUIT)
    }

    pub fn terminate() -> SignalKind {
        SignalKind(libc::SIGTERM)
    }

    pub fn alarm() -> SignalKind {
        SignalKind(libc::SIGALRM)
    }

    pub fn child() -> SignalKind {
        SignalKind(libc::SIGCHLD)
    }

    pub fn pipe() -> SignalKind {
        SignalKind(libc::SIGPIPE)
    }

    pub fn user_defined1() -> SignalKind {
        SignalKind(libc::SIGUSR1)
    }

    pub fn user_defined2() -> SignalKind {
        SignalKind(libc::SIGUSR2)
    }

    pub fn window_change() -> SignalKind {
        SignalKind(libc::SIGWINCH)
    }
}

/// A listener for one [`SignalKind`], firing every time it arrives for
/// as long as this value is held. Dropping it stops it from being woken
/// -- other listeners for the same kind (including ones registered
/// before or after) are unaffected.
///
/// # Panics
/// [`signal`] (which every `Signal` is created through) panics if
/// called outside a running [`crate::Runtime`].
pub struct Signal {
    listener: Arc<ListenerState>,
}

impl Signal {
    /// Resolves once this signal kind next arrives -- immediately, if
    /// it already has since the last call (or since this `Signal` was
    /// created, for the first call). Always `Some(())`; the `Option`
    /// shape exists only for consistency with `recv`-style methods
    /// elsewhere in this crate -- a real OS signal source never
    /// meaningfully "ends".
    pub async fn recv(&mut self) -> Option<()> {
        std::future::poll_fn(|cx| self.poll_recv(cx)).await
    }

    fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<Option<()>> {
        if self.listener.pending.swap(false, Ordering::AcqRel) {
            return Poll::Ready(Some(()));
        }
        *self.listener.waker.lock().unwrap() = Some(cx.waker().clone());
        // Re-check after registering: `dispatch` could have set
        // `pending` (and found no waker yet to wake) in the window
        // between the check above and registering the waker just now --
        // the same re-check-after-register shape `ScheduledIo::
        // poll_ready` already uses, for the identical "don't lose a
        // wakeup that raced registration" reason.
        if self.listener.pending.swap(false, Ordering::AcqRel) {
            return Poll::Ready(Some(()));
        }
        Poll::Pending
    }
}

/// Listens for `kind`. Installs a `sigaction` handler for it the first
/// time any caller asks for this particular kind (see this module's own
/// docs); every call, including the first, adds an independent listener
/// that gets woken on every occurrence from here on.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub fn signal(kind: SignalKind) -> io::Result<Signal> {
    let global = global()?;
    let signum = kind.0 as usize;
    let Some(slot) = global.slots.get(signum) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "signal number out of range",
        ));
    };

    let listener = Arc::new(ListenerState {
        pending: AtomicBool::new(false),
        waker: Mutex::new(None),
    });

    let mut slot = slot.lock().unwrap();
    if !slot.installed {
        install_handler(kind.0)?;
        slot.installed = true;
    }
    slot.listeners.push(Arc::downgrade(&listener));
    drop(slot);

    Ok(Signal { listener })
}

/// Resolves once on the next `SIGINT` ("Ctrl-C" at an interactive
/// terminal). Equivalent to `signal(SignalKind::interrupt())?.recv()`,
/// for the common case of only ever caring about one occurrence.
///
/// # Panics
/// Panics if called outside a running [`crate::Runtime`].
pub async fn ctrl_c() -> io::Result<()> {
    signal(SignalKind::interrupt())?.recv().await;
    Ok(())
}
