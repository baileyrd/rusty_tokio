//! [`Command`]: mirrors `std::process::Command`'s builder API, but
//! `spawn()` gives async access to the child's piped stdio
//! ([`ChildStdin`]/[`ChildStdout`]/[`ChildStderr`]), and [`Child::wait`]
//! doesn't block a worker thread waiting for exit.
//!
//! **Built directly on `std::process`, not rustils.** Checked rustils'
//! own `platform::process` first (it has a real `Command`/`Spawner`/
//! `Child` abstraction, deliberately object-safe and cross-platform
//! including Windows) -- but its piped stdio comes back as
//! `Box<dyn platform::fs::File>`, an abstraction that deliberately hides
//! the underlying fd (for Windows portability, where "raw fd" doesn't
//! even mean anything). That's exactly wrong for this crate's actual
//! need: a child's piped stdin/stdout/stderr are *plain pipes*, which --
//! unlike a regular file or stdio connected to a terminal -- genuinely
//! block on read when empty and become readable when data arrives, so
//! they're just as reactor-registerable as a socket. Reactor-driving
//! them (rather than treating them as a `spawn_blocking` round trip per
//! read/write, the way [`crate::fs::File`]/`crate::io::stdio` have to)
//! needs the raw fd, which rustils' `File` trait won't hand over. Rather
//! than hand-rolling `fork`/`exec`/`posix_spawn` a second time just to
//! get raw fds back (real, error-prone unsafe systems code
//! `std::process::Command` already implements correctly), this wraps
//! `std::process::Command`/`Child` directly instead -- the same call
//! [`crate::io::UnixDatagram`] already made for the identical reason
//! (rustils' abstraction not fitting this crate's reactor-integration
//! need, with `std` already having a complete, safe implementation).
//!
//! `std::process::Child` has no async `wait()` of its own either
//! (nothing does, without OS-specific work: a `pidfd` on Linux 5.3+, or
//! `kevent`'s `EVFILT_PROC`/`NOTE_EXIT` on macOS -- two genuinely
//! different reactor-integration paths that would each need their own
//! design, implementation, and -- per this crate's macOS backend's own
//! standing caveat -- verification on real hardware neither is available
//! here). [`Child::wait`] instead runs the real, blocking
//! `std::process::Child::wait()` on the [`crate::spawn_blocking`] pool:
//! not a polling loop (nothing here re-checks on a timer), a genuine
//! blocking wait that wakes immediately and exactly when the child
//! exits, just parked on a dedicated OS thread instead of a
//! reactor-registered fd. A deliberate simplicity trade-off, not a
//! placeholder -- consistent with `fs::File`/`stdio` already choosing
//! this same shape for operations a reactor can't drive directly.

use crate::io::reactor::{poll_io, Interest, Reactor, ScheduledIo};
use crate::io::socket;
use crate::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use crate::runtime::Handle;
use std::ffi::OsStr;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub use std::process::{ExitStatus, Output, Stdio};

fn blocking_pool_panicked() -> io::Error {
    io::Error::other("the blocking-pool task waiting on this child process panicked")
}

/// Mirrors `std::process::Command`'s builder API exactly (method for
/// method, same `&mut self -> &mut Self` chaining shape) -- see this
/// module's own docs for why `spawn()`'s result differs from std's.
pub struct Command {
    inner: std::process::Command,
    kill_on_drop: bool,
}

impl Command {
    pub fn new(program: impl AsRef<OsStr>) -> Command {
        Command {
            inner: std::process::Command::new(program),
            kill_on_drop: false,
        }
    }

    pub fn arg(&mut self, arg: impl AsRef<OsStr>) -> &mut Command {
        self.inner.arg(arg);
        self
    }

    pub fn args<I, S>(&mut self, args: I) -> &mut Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.inner.args(args);
        self
    }

    /// Sets the value passed as `argv[0]` to the child process --
    /// distinct from [`new`](Self::new)'s `program` argument, which is
    /// still what's actually executed (`execve`'s own path argument);
    /// only the name the child *sees itself invoked as* changes. Thin
    /// forward to [`std::os::unix::process::CommandExt::arg0`].
    pub fn arg0(&mut self, arg: impl AsRef<OsStr>) -> &mut Command {
        std::os::unix::process::CommandExt::arg0(&mut self.inner, arg);
        self
    }

    /// Sets the process group ID (`setpgid`) the child is placed into
    /// before `execve`, matching the effect of a real shell's job
    /// control (`0` joins the child's own new group, matching its
    /// `pid`; a positive value joins an existing group). Thin forward
    /// to [`std::os::unix::process::CommandExt::process_group`].
    pub fn process_group(&mut self, pgroup: i32) -> &mut Command {
        std::os::unix::process::CommandExt::process_group(&mut self.inner, pgroup);
        self
    }

    pub fn env(&mut self, key: impl AsRef<OsStr>, val: impl AsRef<OsStr>) -> &mut Command {
        self.inner.env(key, val);
        self
    }

    pub fn envs<I, K, V>(&mut self, vars: I) -> &mut Command
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.inner.envs(vars);
        self
    }

    pub fn env_remove(&mut self, key: impl AsRef<OsStr>) -> &mut Command {
        self.inner.env_remove(key);
        self
    }

    pub fn env_clear(&mut self) -> &mut Command {
        self.inner.env_clear();
        self
    }

    pub fn current_dir(&mut self, dir: impl AsRef<Path>) -> &mut Command {
        self.inner.current_dir(dir);
        self
    }

    pub fn stdin(&mut self, cfg: impl Into<Stdio>) -> &mut Command {
        self.inner.stdin(cfg);
        self
    }

    pub fn stdout(&mut self, cfg: impl Into<Stdio>) -> &mut Command {
        self.inner.stdout(cfg);
        self
    }

    pub fn stderr(&mut self, cfg: impl Into<Stdio>) -> &mut Command {
        self.inner.stderr(cfg);
        self
    }

    /// Borrows the inner [`std::process::Command`] -- an escape hatch for
    /// any std builder option this wrapper doesn't cover itself.
    pub fn as_std(&self) -> &std::process::Command {
        &self.inner
    }

    /// Mutably borrows the inner [`std::process::Command`] -- same escape
    /// hatch as [`as_std`](Self::as_std), but for setters.
    pub fn as_std_mut(&mut self) -> &mut std::process::Command {
        &mut self.inner
    }

    /// Whether the spawned [`Child`] should be killed if it's dropped
    /// while still running -- `false` by default, matching
    /// `std::process::Child`'s own "drop just orphans it" behavior.
    /// When `true`, [`Child`]'s `Drop` impl sends `SIGKILL` (best
    /// effort -- errors are ignored, there's nothing a destructor could
    /// do about them) and, if a [`crate::Runtime`] is still running on
    /// the dropping thread, detaches a [`crate::spawn_blocking`] task to
    /// reap it afterward so it doesn't linger as a zombie.
    pub fn kill_on_drop(&mut self, kill_on_drop: bool) -> &mut Command {
        self.kill_on_drop = kill_on_drop;
        self
    }

    /// The value most recently passed to
    /// [`kill_on_drop`](Self::kill_on_drop) (`false` if never called).
    pub fn get_kill_on_drop(&self) -> bool {
        self.kill_on_drop
    }

    /// Spawns the child. `fork`/`exec` themselves run synchronously,
    /// right here -- like tokio's own `Command::spawn`, this crate treats
    /// process creation as fast enough not to need `spawn_blocking`
    /// (unlike [`Child::wait`], which can legitimately take arbitrarily
    /// long).
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`] *and* any
    /// of `stdin`/`stdout`/`stderr` was set to [`Stdio::piped`] --
    /// adopting a piped fd needs the ambient reactor. Spawning with every
    /// stream left at its default (inherited) needs no runtime at all.
    pub fn spawn(&mut self) -> io::Result<Child> {
        let mut child = self.inner.spawn()?;
        let id = child.id();

        let stdin = child.stdin.take().map(ChildStdin::adopt).transpose()?;
        let stdout = child.stdout.take().map(ChildStdout::adopt).transpose()?;
        let stderr = child.stderr.take().map(ChildStderr::adopt).transpose()?;

        Ok(Child {
            inner: Some(child),
            id,
            status: None,
            kill_on_drop: self.kill_on_drop,
            stdin,
            stdout,
            stderr,
        })
    }

    /// Spawns the child and waits for it to exit, discarding whatever
    /// it writes to stdout/stderr (they stay inherited from this
    /// process, exactly like `std::process::Command::status`). Sugar
    /// for `self.spawn()?.wait().await`.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn status(&mut self) -> io::Result<ExitStatus> {
        self.spawn()?.wait().await
    }

    /// Spawns the child with stdout/stderr captured (overriding
    /// whatever [`stdout`](Self::stdout)/[`stderr`](Self::stderr) were
    /// previously set to), waits for it to exit, and returns everything
    /// at once. Sugar for `self.stdout(Stdio::piped());
    /// self.stderr(Stdio::piped()); self.spawn()?.wait_with_output().await`.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn output(&mut self) -> io::Result<Output> {
        self.stdout(Stdio::piped());
        self.stderr(Stdio::piped());
        self.spawn()?.wait_with_output().await
    }
}

/// A spawned child process. `stdin`/`stdout`/`stderr` are `Some` exactly
/// when the corresponding [`Command`] method was set to
/// [`Stdio::piped`].
pub struct Child {
    /// `None` once [`Child::wait`] (or a [`Child::try_wait`] that
    /// observed termination) has consumed it -- `std::process::Child`
    /// has nothing further to offer once reaped; [`Child::id`] is served
    /// from the cached `id` field below instead, and [`Child::kill`]
    /// becomes a no-op.
    inner: Option<std::process::Child>,
    id: u32,
    /// Cached the first time termination is observed (by either
    /// `wait` or `try_wait`), so a later call of either returns it
    /// directly instead of trying to re-wait an already-reaped child
    /// (which would be, at best, redundant, and on some platforms an
    /// outright error).
    status: Option<ExitStatus>,
    /// Set from [`Command::kill_on_drop`] at [`Command::spawn`] time --
    /// see this `Child`'s own `Drop` impl for what it does.
    kill_on_drop: bool,
    pub stdin: Option<ChildStdin>,
    pub stdout: Option<ChildStdout>,
    pub stderr: Option<ChildStderr>,
}

impl Child {
    /// The OS process identifier. Stays available even after the child
    /// has been waited on.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Sends `SIGKILL`. A no-op if the child has already been fully
    /// reaped (via [`wait`](Self::wait) or a terminating
    /// [`try_wait`](Self::try_wait)) -- there's nothing left to signal,
    /// matching the "killing something already gone isn't an error"
    /// behavior a caller racing shutdown against a fast-exiting child
    /// wants.
    pub fn kill(&mut self) -> io::Result<()> {
        match &mut self.inner {
            Some(inner) => inner.kill(),
            None => Ok(()),
        }
    }

    /// Non-blocking: `Some(status)` if the child has already terminated,
    /// `None` if it's still running. Never blocks the calling thread --
    /// this is a single `waitpid(WNOHANG)`-shaped syscall, not the
    /// genuinely-can-take-forever wait [`wait`](Self::wait) runs on the
    /// blocking pool.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        if let Some(status) = self.status {
            return Ok(Some(status));
        }
        let Some(inner) = &mut self.inner else {
            return Ok(None);
        };
        match inner.try_wait()? {
            Some(status) => {
                self.status = Some(status);
                self.inner = None;
                Ok(Some(status))
            }
            None => Ok(None),
        }
    }

    /// Waits for the child to exit. See this module's own docs for why
    /// this runs the real blocking `wait(2)` on the blocking-task pool
    /// rather than a reactor-driven `pidfd`/`EVFILT_PROC`.
    ///
    /// If `stdin` is still piped and the child is waiting to read
    /// something from it before exiting, this waits right alongside
    /// it -- drop (or otherwise close) `stdin` first if the child is
    /// expected to notice EOF and finish up, the same deadlock caveat
    /// `std::process::Child::wait`'s own docs carry.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        if let Some(status) = self.status {
            return Ok(status);
        }
        let mut inner = self.inner.take().expect(
            "Child::wait: status is None, so inner must still be Some -- \
             try_wait/wait always keep these two in sync",
        );
        let status = crate::spawn_blocking(move || inner.wait())
            .await
            .unwrap_or_else(|_| Err(blocking_pool_panicked()))?;
        self.status = Some(status);
        Ok(status)
    }

    /// Waits for the child to exit, concurrently draining whatever of
    /// `stdout`/`stderr` were piped, and returns everything at once.
    /// Drains both streams *while* waiting rather than one after the
    /// other: sequentially draining one stream (or waiting first) while
    /// a child that's already filled the other's pipe buffer sits
    /// blocked trying to write more would deadlock -- the same reason
    /// `std::process::Child::wait_with_output` also drains
    /// concurrently.
    ///
    /// `stdin` is dropped first (if still piped) so a child waiting to
    /// read EOF from it before exiting isn't left waiting forever, the
    /// same deadlock caveat [`wait`](Self::wait) itself carries, and
    /// the same thing `std::process::Child::wait_with_output` does.
    ///
    /// # Panics
    /// Panics if called outside a running [`crate::Runtime`].
    pub async fn wait_with_output(mut self) -> io::Result<Output> {
        drop(self.stdin.take());
        // Taken out into locals rather than borrowed from `self`
        // directly -- `self.wait()` below needs `&mut self` as a whole
        // (not just its `status`/`inner` fields), which would otherwise
        // conflict with these two futures each separately borrowing
        // `self.stdout`/`self.stderr`.
        let mut stdout = self.stdout.take();
        let mut stderr = self.stderr.take();

        let mut stdout_buf = Vec::new();
        let mut stderr_buf = Vec::new();
        let stdout_fut = async {
            match &mut stdout {
                Some(stdout) => stdout.read_to_end(&mut stdout_buf).await.map(|_| ()),
                None => Ok(()),
            }
        };
        let stderr_fut = async {
            match &mut stderr {
                Some(stderr) => stderr.read_to_end(&mut stderr_buf).await.map(|_| ()),
                None => Ok(()),
            }
        };
        let status_fut = self.wait();

        let (status, _, _) = crate::try_join!(status_fut, stdout_fut, stderr_fut)?;
        Ok(Output {
            status,
            stdout: stdout_buf,
            stderr: stderr_buf,
        })
    }
}

impl Drop for Child {
    /// A no-op unless [`Command::kill_on_drop`] was set: `std`'s (and
    /// this crate's) default is to leave a dropped-but-still-running
    /// child orphaned, exactly like dropping a `std::process::Child`
    /// does. When it was set, sends `SIGKILL` (best effort -- a
    /// destructor has no way to surface or act on a failure) and, if a
    /// [`crate::Runtime`] is still running on the dropping thread,
    /// detaches a [`crate::spawn_blocking`] task to `wait()` on it
    /// afterward so the kernel doesn't have to keep it around as a
    /// zombie until some unrelated `wait` call happens to reap it.
    /// `Drop` can't itself `.await`, so this is the same
    /// signal-now/reap-separately shape [`Child::wait`] already needs
    /// for the blocking `wait(2)` syscall, just fired off rather than
    /// awaited. With no runtime running here, the kill still goes out;
    /// only the reap is skipped (the same zombie-until-reaped state a
    /// signal-only kill would always leave behind).
    fn drop(&mut self) {
        if !self.kill_on_drop {
            return;
        }
        let Some(mut inner) = self.inner.take() else {
            return;
        };
        let _ = inner.kill();
        if let Some(handle) = Handle::try_current() {
            handle.spawn_blocking(move || {
                let _ = inner.wait();
            });
        }
    }
}

/// Shared by [`ChildStdin`]/[`ChildStdout`]/[`ChildStderr`]: a plain pipe
/// fd, non-blocking and registered with the ambient reactor -- see this
/// module's own docs for why a child's piped stdio is reactor-driven
/// rather than a `spawn_blocking` round trip per operation the way
/// [`crate::fs::File`]'s is.
struct PipeIo {
    fd: OwnedFd,
    io: Arc<ScheduledIo>,
    reactor: Arc<Reactor>,
}

impl PipeIo {
    fn adopt(fd: OwnedFd) -> io::Result<PipeIo> {
        let reactor = Handle::current().shared.reactor.clone();
        socket::set_nonblocking(fd.as_raw_fd(), true)?;
        let io = reactor.register(fd.as_raw_fd())?;
        Ok(PipeIo { fd, io, reactor })
    }
}

impl Drop for PipeIo {
    fn drop(&mut self) {
        self.reactor.deregister(self.fd.as_raw_fd());
    }
}

/// The parent's write end of a piped child's stdin. Dropping it (there's
/// no other way to half-close just this direction) delivers EOF to the
/// child, the same as `std::process::ChildStdin`.
pub struct ChildStdin(PipeIo);

impl ChildStdin {
    fn adopt(stdin: std::process::ChildStdin) -> io::Result<ChildStdin> {
        Ok(ChildStdin(PipeIo::adopt(OwnedFd::from(stdin))?))
    }
}

impl AsyncWrite for ChildStdin {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        poll_io(&self.0.io, Interest::Write, cx, || {
            socket::write(self.0.fd.as_raw_fd(), buf)
        })
    }

    /// A no-op: writes to a pipe land directly with nothing further to
    /// flush, same as every other unbuffered writer in this crate.
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let _ = self;
        Poll::Ready(Ok(()))
    }

    /// A no-op, deliberately -- unlike a socket's independent
    /// `shutdown(SHUT_WR)`, a pipe has no way to half-close without
    /// closing the fd outright, which would fight with this value still
    /// existing afterward. Drop `ChildStdin` itself to deliver EOF.
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let _ = self;
        Poll::Ready(Ok(()))
    }
}

/// The parent's read end of a piped child's stdout. Reads return `0` at
/// EOF once the child closes its end (or exits).
pub struct ChildStdout(PipeIo);

impl ChildStdout {
    fn adopt(stdout: std::process::ChildStdout) -> io::Result<ChildStdout> {
        Ok(ChildStdout(PipeIo::adopt(OwnedFd::from(stdout))?))
    }
}

impl AsyncRead for ChildStdout {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match poll_io(&self.0.io, Interest::Read, cx, || {
            socket::read(self.0.fd.as_raw_fd(), buf.unfilled_mut())
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

/// The parent's read end of a piped child's stderr. Same shape as
/// [`ChildStdout`], just the other stream.
pub struct ChildStderr(PipeIo);

impl ChildStderr {
    fn adopt(stderr: std::process::ChildStderr) -> io::Result<ChildStderr> {
        Ok(ChildStderr(PipeIo::adopt(OwnedFd::from(stderr))?))
    }
}

impl AsyncRead for ChildStderr {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match poll_io(&self.0.io, Interest::Read, cx, || {
            socket::read(self.0.fd.as_raw_fd(), buf.unfilled_mut())
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
