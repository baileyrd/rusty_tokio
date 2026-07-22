# rusty_tokio

A hand-rolled async runtime for Rust, built from scratch on `std` -- no
`tokio`, no `mio`. It exists to actually understand how an async runtime
works, not to replace tokio.

The scheduler, reactor, timers, and sync primitives are all original code
here, with one deliberate exception: the scheduler's per-worker
work-stealing queues depend on `crossbeam-deque` (see the "Runtime"
bullet below) rather than hand-rolling a Chase-Lev deque -- real unsafe
concurrent code this project has no `loom`-based verification set up to
trust a new implementation of, unlike the scheduler/reactor/timer logic
elsewhere, which the multi-threaded integration tests already hold to
that bar. Not a dependency on the general-purpose `crossbeam` suite
(channels, epoch GC, etc.) as a shortcut around any of that -- just this
one narrowly-scoped sub-crate for the one piece this project already
decided isn't its point to hand-roll unverified. "No `mio`" means no
dependency on it, not no awareness of it: the Windows reactor implements
the same undocumented AFD-poll protocol mio's own Windows backend uses,
cited directly as this project's reference point (see "Platform support"
below). Socket lifecycle (bind/connect/accept/addressing) is built on top
of [`rustils`](https://github.com/baileyrd/rustils)'
`platform`/`platform-linux`/`platform-macos` crates on Linux/macOS, and
directly on `windows-sys` on Windows, rather than reimplemented a second
time -- see "Built on rustils" below for exactly which seam that is and
which two syscalls stayed hand-rolled because rustils' API can't support
them yet.

## What's here

- **Task system** (`task`): a heap-allocated future plus a small atomic
  state machine that decides, on every wake, whether to (re-)enqueue it.
  The obvious "channel of `Arc<Task>`" design (the one most "build your
  own executor" blog posts use) has a real lost-wakeup bug once you're
  actually multi-threaded: a wake that lands *while* a task is mid-poll
  finds the future temporarily missing from its slot and silently drops
  the wakeup. `task`'s module docs walk through the fix. Also
  `task::yield_now()`, for a task that wants to cooperate with others
  without splitting itself across multiple spawns, and `task::JoinSet`:
  a dynamic collection of spawned tasks -- `join_next().await` resolves
  as soon as *any* member finishes (not spawn order, unlike joining a
  plain `Vec<JoinHandle<T>>` one at a time), `abort_all`/`shutdown` to
  cancel everything at once, and -- unlike a bare `JoinHandle`, which
  never aborts on drop -- dropping the whole set aborts every task still
  in it. Also named tasks and task IDs: every spawned task gets a
  `TaskId` (`JoinHandle::id()`), a stable, process-wide-unique identity
  independent of holding any reference to the task and unaffected by it
  completing, panicking, or being aborted; `task::try_id()`/
  `task::try_name()` read the *currently running* task's own ID/name
  from inside its own future body (a thread-local set for the exact
  duration of each poll call, restored afterward). `task::Builder::new()
  .name("...").spawn(future)` is the alternative to plain `crate::spawn`
  that actually sets a name -- plain `spawn`/`spawn_local`'d tasks always
  have `try_name() == None`.
- **`task_local!`/`task::LocalKey`**: implicit, per-task context (a
  request ID, a connection-scoped config value) that inner async calls
  read via `KEY.with(|v| ...)` without it being threaded through every
  function signature explicitly. `KEY.scope(value, future).await` is
  what actually makes it visible: the real, underlying `thread_local!`
  slot holds `value` only for the exact duration of each poll of
  `future` (restored immediately afterward, even if that poll panics),
  so a *different* task polled on the same worker thread in between two
  polls of this one never sees it -- the exact case a plain
  `std::thread_local!` would get wrong, since many tasks' polls
  interleave on one OS thread. Also `sync_scope` for a plain synchronous
  closure instead of a future, and `try_with` for a non-panicking read.
- **`LocalSet`/`spawn_local`**: a place to spawn `!Send` futures --
  holding an `Rc`, a `RefCell`-guarded value, or any other non-thread-safe
  handle -- which `crate::spawn` can never accept, since every task
  spawned there is an `Arc<Task>` any worker thread may poll.
  `LocalSet::run_until(future)` drives every task spawned onto the set
  (via `LocalSet::spawn_local` or the ambient `task::spawn_local`)
  interleaved with `future`, synchronously, on whichever thread calls
  it -- the same "blocks until done" contract as `Runtime::block_on`.
  A `!Send` future still needs a thread-safe `Waker` (the I/O
  reactor/timer driver wake it from their own background threads, same
  as any other task), so the local task type is `Arc`-counted with an
  `unsafe impl Send + Sync` justified by a genuinely enforced invariant,
  not just a comment: `LocalSet` binds itself to whichever thread first
  calls `spawn_local`/`run_until` on it and panics if used from a
  different one afterward, so the actual `!Send` future inside is never
  touched except on that one thread. Pair a `LocalSet` with a `Runtime`
  for `time::sleep`/I/O to work inside `spawn_local`'d work (a bare
  `LocalSet` has no reactor/timer driver of its own, only scheduling);
  there's no "run this `LocalSet` pinned to one worker of an existing
  multi-threaded `Runtime`" integration the way tokio's `LocalSet: Future`
  impl offers, and no graceful-shutdown draining -- a `LocalSet` going
  out of scope just drops whatever's left queued, same as this crate's
  `Runtime` already does for tasks abandoned mid-shutdown.
- **Runtime** (`Runtime`, `Handle`): a fixed pool of worker threads, each
  with its own run queue, backed by a shared injector queue for tasks
  spawned from outside the pool, with work-stealing between workers when
  one goes idle. Both the per-worker queues and the injector are
  lock-free (`crossbeam_deque::{Worker, Stealer, Injector}` -- issue #8),
  not hand-rolled: a Chase-Lev deque is real unsafe concurrent code, and
  this project has no `loom`-based verification set up to trust a new
  implementation of it, unlike the scheduler/reactor/timer logic
  elsewhere in this crate, which the ordinary multi-threaded integration
  tests already hold to that bar (they're what caught the `Notify`/`mpsc`
  lost-wakeup bugs mentioned above). `crossbeam-deque` is exactly what
  tokio itself uses, and every consumer-facing method
  (`Worker`/`Stealer`/`Injector`) is safe Rust -- all unsafe is internal
  to the crate, already independently audited. Each worker thread owns
  its own `Worker` through a thread-local (`Worker` is `!Sync`, so it
  can't live centrally in `Shared` the way the old `Mutex`-guarded queues
  did); the current-thread flavor's single queue keeps the original
  plain `Mutex<VecDeque<_>>` unchanged, since there's no stealing to
  speed up with only one queue and no siblings. `benches/scheduler.rs`
  (`cargo bench`, same hand-rolled approach as the timer benchmarks)
  measured the swap rather than assuming it helped: on the same Linux dev
  box, the steal-heavy nested-spawn scenario -- the scenario issue #8 was
  actually about, too noisy to draw a confident conclusion on before the
  swap -- now scales cleanly and consistently with worker count (roughly
  350K &rarr; 520K &rarr; 800K tasks/sec across 1/2/4 workers). The
  many-independently-spawned-tasks scenario (which all serialize through
  the single injector) still measurably regresses going from 1 to 4
  worker threads (roughly 1M &rarr; 300K tasks/sec) -- real evidence that
  scenario's bottleneck is elsewhere (scheduler wake/park dynamics under
  that specific access pattern, not the injector's own data structure),
  not something this particular swap was expected to fix.
- **Current-thread runtime** (`Builder::new_current_thread()`): the
  above is the default (also `Builder::new_multi_thread()`, spelled out
  for symmetry), but a runtime built this way has no worker-thread pool
  at all -- spawned tasks run interleaved with polls of `block_on`'s own
  future, entirely on whichever thread calls it, instead of on a
  separate pool in the background. Useful for embedding in an
  environment that doesn't want extra OS threads, or lower overhead for
  single-threaded workloads that don't need work-stealing at all.
  Spawned futures still need to be `Send` -- same as the multi-threaded
  flavor; this alone doesn't enable `!Send` futures, which needs a
  `LocalSet` (tracked separately). The I/O reactor and timer driver still
  run on their own dedicated background threads regardless of flavor --
  unlike real tokio's current-thread runtime, which drives its I/O
  reactor inline on the single scheduling thread; collapsing this
  crate's already-dedicated `Reactor`/`TimerDriver` threads into the
  scheduling thread too would be a materially bigger redesign than a
  worker-pool-free scheduler alone, and isn't attempted here.
- **Graceful shutdown**: plain `drop(runtime)` still tears down
  immediately (abandoning anything mid-poll, unchanged from before), but
  `Runtime::shutdown_background()`/`shutdown_timeout(duration)` and
  `Handle::shutdown_notified()`/`is_shutting_down()` give spawned tasks a
  real chance to notice shutdown and clean up first: `shutdown_notified()`
  resolves once shutdown begins (immediately, if it already has), and a
  task awaiting it directly -- e.g. a dedicated cleanup task that does
  nothing until then -- is guaranteed to actually get scheduled and run
  before teardown proceeds, not just notified a moment before being cut
  off. `shutdown_timeout` additionally waits (bounded) for every
  outstanding task and the blocking pool to finish naturally before
  falling back to the same abrupt teardown `drop`/`shutdown_background`
  give; `shutdown_background` never waits at all. Racing
  `shutdown_notified()` against a task's own ongoing work (rather than
  awaiting it as the task's entire body) can now be done with `select!`
  -- see below.
- **Runtime metrics** (`Runtime::metrics()`/`Handle::metrics()` ->
  `RuntimeMetrics`): a live, read-only view into the scheduler and
  blocking pool -- `num_workers`, `num_alive_tasks`, `global_queue_depth`,
  per-worker `worker_local_queue_depth`/`worker_steal_count`/
  `worker_park_count`, and `num_blocking_threads`. `benches/scheduler.rs`
  and `benches/timers.rs` (issues #8/#13) had to measure contention and
  skew indirectly, through wall-clock timing of the public API, precisely
  because none of this was exposed directly before -- this is a live
  view, not a snapshot frozen at the time `metrics()` was called: every
  method re-reads the current value, each a plain atomic load (or a
  queue's own `Mutex::lock`, already taken on every schedule/steal
  regardless of whether metrics are ever read). Unlike tokio, none of
  this sits behind an `unstable` feature flag -- the actual cost this
  adds on the hot scheduling path is a handful of relaxed
  `AtomicU64::fetch_add` calls at each steal/park site, alongside the
  `active_tasks` counter that already existed (added for graceful
  shutdown, above); not the kind of hot-path cost that justifies
  withholding it by default.
- **Cooperative scheduling budget**: `Task::run` only calls a future's
  `poll` once per scheduling turn, but nothing stops that one call from
  itself looping internally forever -- a `Stream`-like future that keeps
  handing back `Ready`, or a tight `while let Some(x) = rx.recv().await
  { .. }` loop over a channel that's always ready, both look completely
  ordinary from inside that one task (every individual `.await` really
  is resolving) while quietly starving every other task on the same
  worker. Every task now gets a fixed budget of poll operations (128,
  matching tokio's own default -- not load-bearing on its own, just
  already well-exercised), reset at the top of each top-level poll and
  spent by this crate's own poll-heavy primitives: the reactor's
  `poll_io` (covering every socket read/write through one shared choke
  point) and `mpsc`/`oneshot`/`Notify`'s own poll implementations. Once
  exhausted, a self-wake (the same idiom `task::yield_now` already uses)
  forces a `Pending` return *before* the caller's own readiness check or
  dequeue -- so a channel that already has a value sitting in it still
  yields once budget runs out, deferring the read to the next poll
  instead of handing it over immediately, which is what actually breaks
  the starvation case above. A future polled outside of `Task::run` (an
  outer `block_on` future, most notably) has no budget in scope at all.
- **I/O** (`io`): a reactor thread plus non-blocking `TcpStream`,
  `TcpListener`, `UdpSocket` (all three cross-platform), and
  `UnixStream`/`UnixListener`/`UnixDatagram` (`AF_UNIX`, Unix-only).
  Three backends behind the same interface -- `epoll` on Linux, `kevent`
  on macOS, IOCP plus the undocumented AFD-poll trick (mio's own
  production solution to the same problem, cited directly as this
  crate's reference point -- see `io::reactor::windows`'s own module
  docs) on Windows. The POSIX two are level-triggered by choice, since
  edge-triggered epoll/kqueue demands every reader drain a fd to
  `EWOULDBLOCK` or risk missing events forever, an easy invariant to get
  subtly wrong for one extra syscall's worth of savings; IOCP has no
  level/edge distinction of its own (it's completion-, not
  readiness-based to begin with), but the Windows backend re-arms its
  poll request on every completion to match the other two's observable
  behavior anyway. Socket setup goes through `rustils` on Linux/macOS,
  and directly through `windows-sys` on Windows (see "Built on rustils"
  below for why there's no rustils backend to lean on there).
  Also includes an `AsyncRead`/`AsyncWrite` trait pair (shaped like
  tokio's/`futures-io`'s -- `Pin<&mut Self>`, `poll_*` methods -- but
  this crate's own definitions, not a re-export) plus a generic `copy`,
  so code doesn't need to be written against the concrete `TcpStream`
  type. `AsyncReadExt` also has `read_to_end`/`read_to_string`
  (accumulate until EOF, the latter checking UTF-8 validity once at the
  end), and `AsyncWrite` has a `poll_write_vectored`/`is_write_vectored`
  pair (default: writes just the first non-empty buffer via `poll_write`
  and ignores the rest -- correct, just not the syscall-count win a real
  `writev`-backed override would be). `TcpStream::split`/`into_split` (and, identically,
  `UnixStream::split`/`into_split`) give borrowed (`ReadHalf`/
  `WriteHalf`, or `UnixReadHalf`/`UnixWriteHalf`) or owned, independently
  `'static` (`OwnedReadHalf`/`OwnedWriteHalf`, or `OwnedUnixReadHalf`/
  `OwnedUnixWriteHalf`) read/write halves for the two-tasks-one-stream
  pattern, without callers having to reach for `Arc` themselves the way
  the shared `&TcpStream`/`&UnixStream` impls otherwise require.

  Also `AsyncBufRead` (`poll_fill_buf`/`consume`) plus `io::BufReader`/
  `io::BufWriter`, for buffering on top of any `AsyncRead`/`AsyncWrite` --
  this crate's own sockets are unbuffered by design, so these are how a
  protocol that wants to read a line at a time (`AsyncBufReadExt::
  read_line`/`read_until`/`lines()`) or batch small writes into fewer
  syscalls adds that itself. Both require the wrapped type to be
  `Unpin` -- a deliberate simplification versus tokio's own
  `BufReader`/`BufWriter` (which pin-project through to a possibly-
  `!Unpin` inner value); every concrete reader/writer this crate
  actually has is already `Unpin`, so it costs nothing in practice while
  avoiding hand-written unsafe pin projection for a case that would
  never exercise it. `BufWriter` doesn't flush on drop (any
  not-yet-flushed bytes are silently lost) -- the same caveat tokio's
  own carries.

  Also `copy_bidirectional(a, b)`, for a proxy/relay use case (forwarding
  one connection to another) that needs both directions of an `AsyncRead
  + AsyncWrite` pair copied concurrently, instead of hand-writing two
  separately `spawn`ed `copy` calls plus your own half-close
  coordination. Each direction shuts its writer down independently, as
  soon as *that* direction's own reader hits EOF -- e.g. a client that's
  done sending but still expects a response keeps that response
  direction alive until the server closes its own end too -- and an
  error on either direction propagates immediately rather than waiting
  for the other to finish first. Internally, each direction's own
  little copy-loop state machine (reused across however many separate
  polls a single `poll_fn` call takes) is wrapped with an explicit
  "already done" terminal state once it resolves, specifically because
  the combined `poll_fn` polls *both* directions on every wake --
  without that, a direction that had already finished would get
  re-entered and call `poll_shutdown` on its writer a second time,
  which can itself fail once the peer has since fully closed its side
  too (an already-successful direction spuriously turning into an
  error), a real bug this crate's own test suite caught before merging.

  Also `from_std`/`into_std` on `TcpListener`, `TcpStream`, and
  `UdpSocket`, for adopting an already-created `std` socket (handed down
  from a supervisor process, or configured with `socket2` for an option
  this crate exposes no wrapper for) or handing one back out as a plain
  blocking socket. `from_std` just flips the socket non-blocking and
  registers it with the reactor, skipping the bind/connect/listen
  syscall since the `std` socket already did it. `into_std` flips back
  to blocking and duplicates the fd (`try_clone_to_owned`, a `dup(2)`)
  rather than transferring the original one -- `self` still drops
  normally afterward (deregistering from the reactor, closing its own
  fd), and the returned `std` socket is an independent fd onto the same
  underlying open file description, the same guarantee
  `TcpStream::try_clone` already relies on elsewhere in `std`. Costs one
  extra syscall versus a true ownership transfer, which would need
  `mem::forget`/`ManuallyDrop` tricks to skip running `Drop` for the
  *whole* struct without also leaking the `Arc<ScheduledIo>`/
  `Arc<Reactor>` fields' reference counts -- not worth the added unsafe
  code for how rarely this is called.

  Also `AsyncSeek` (`poll_seek(pos) -> Poll<io::Result<u64>>`), seeking
  within a stream -- meaningful for a file, not a socket, so nothing in
  this module implements it; `fs::File` (below) does. A single combined
  method rather than tokio's own two-phase `start_seek`/`poll_complete`
  split: that split exists so a caller can poll something else while a
  seek is pending, which only matters for an implementation that
  interleaves seeking with other buffered state the way tokio's own file
  type does -- `fs::File` already funnels every operation through one
  shared in-flight-blocking-call state machine, so there's nothing else
  meaningful to poll in between.

  **This crate's macOS and Windows integrations have never run on real
  hardware.** It's developed and tested on Linux only; the kqueue
  reactor and the `TcpStream`/`TcpListener`/`UdpSocket` wiring on top of
  rustils' `platform-macos` are verified with `cargo check --target
  x86_64-apple-darwin` (real macOS `libc` bindings, real type-checking)
  but nothing beyond that. `platform-macos` itself is better off: it has
  real `macos-latest` CI upstream, which already caught a genuine
  `AF_UNIX` bug the cross-check alone couldn't -- so the socket layer is
  solid, but this crate's own reactor integration on top of it is still
  reviewed-but-unverified until someone actually runs *this* crate's
  `cargo test` on a Mac. The Windows IOCP+AFD reactor and its hand-rolled
  `windows-sys` socket layer carry the identical caveat, verified only
  with `cargo check --target x86_64-pc-windows-gnu --all-targets` -- and
  with no upstream equivalent to `platform-macos`'s real CI leg backing
  the socket layer this time, since that layer is entirely this crate's
  own code (see "Built on rustils" below). Treat both non-Linux reactor
  paths as reviewed-but-unverified until someone runs *this* crate's own
  test suite on the real OS.
- **Async filesystem I/O** (`fs::File`): a regular file can't be
  registered with `epoll`/`kevent`'s readiness model the way a socket
  can -- the kernel considers it always "ready", and the actual disk
  latency happens synchronously inside the `read`/`write`/`lseek`
  syscall itself. So unlike `TcpStream`, `File` is entirely a
  `spawn_blocking` abstraction: every operation -- including `open`/
  `create` themselves, since opening a file can block too (a network
  filesystem mount, say) -- moves the underlying `std::fs::File` onto a
  blocking-pool thread, runs the real syscall there, and hands the file
  back once it's done. Implements `AsyncRead`/`AsyncWrite`/`AsyncSeek`
  with `&mut self`-exclusive access at every `poll_*` call (unlike
  `TcpStream`'s `&self`-based split: a file has one cursor, so genuinely
  concurrent reads and writes make no sense the way full-duplex socket
  I/O does). Every operation shares one internal state machine -- if the
  future for one gets dropped before completing (a `select!`/timeout
  cancelling a read, say), the blocking closure it already dispatched
  keeps running in the background regardless (the same
  already-abandoned-`spawn_blocking`-call behavior every other blocking
  op has), and the *next* operation on that `File` drains the leftover
  result (discarding it if it doesn't match) before starting its own.
- **Async stdio** (`io::stdin`/`stdout`/`stderr`): the same
  `spawn_blocking`-abstraction shape as `fs::File` -- stdio generally
  can't be registered with a reactor either -- but simpler in one way
  (there's no persistent OS resource to move in and out of the blocking
  closure the way `File`'s `std::fs::File` is; `std::io::stdin()`/
  `stdout()`/`stderr()` are obtained fresh on every call) and more
  involved in another: every `Stdout`/`Stderr` is writing to the exact
  same process-wide stream as every other one, so two tasks' writes
  interleaving mid-buffer would be a real, visible bug (garbled output),
  not a theoretical one, the way it isn't for two independent
  `TcpStream`s. Fixed two ways together: `poll_write` always calls
  `std::io::Write::write_all` internally, so one call is always
  all-or-nothing -- never a partial count a caller's own `write_all` loop
  might otherwise interleave with someone else's between chunks -- and
  each call holds a process-wide `Mutex` (one per stream, so writing to
  `stdout` never waits on something reading `stdin`) for its *entire*
  duration, not just the underlying syscall. Together, no two concurrent
  logical writes to the same stream can interleave, regardless of how
  many syscalls `write_all` itself ends up needing.
- **In-memory duplex pipe** (`io::duplex(max_buf_size)`): a connected
  pair of `DuplexStream`s, backed by two mutex-guarded byte buffers (one
  per direction) with no socket, fd, or reactor registration at all --
  closer in shape to `sync::mpsc` than to `TcpStream`. A write blocks
  (returns `Pending`) once the peer's read-side buffer is full, a read
  blocks once it's empty, the same backpressure a bounded `mpsc` channel
  has. Useful for testing anything generic over `AsyncRead`/`AsyncWrite`
  without standing up a real loopback `TcpListener`/`TcpStream` pair just
  to exercise protocol logic that isn't actually testing networking.
  Dropping (or `shutdown`-ing) one side marks its write direction closed,
  so the peer's reads drain whatever's left and then see EOF -- and
  marks its own read direction gone, so the peer's writes fail fast
  (`BrokenPipe`) instead of endlessly buffering into a pipe nobody's left
  to drain.
- **`TcpSocket` builder** (`io::TcpSocket`): `TcpListener::bind`/
  `TcpStream::connect` go straight from nothing to bound-and-listening/
  connected in one call, with no opportunity to set a socket option in
  between. `TcpSocket::new_v4()`/`new_v6()` creates a bare, unbound,
  unconnected socket first; `set_reuseaddr`/`set_reuseport`/
  `set_send_buffer_size`/`set_recv_buffer_size` (each with a matching
  getter) configure it; `bind`/`listen` or `connect` then turns it into
  an ordinary `TcpListener`/`TcpStream`, same as ever from that point on.
  None of those four options are in rustils' own `TcpStream`/
  `TcpListener` traits (only `set_nodelay` is), so each is a hand-rolled
  `setsockopt`/`getsockopt` call in `socket/mod.rs`, alongside the
  non-blocking `connect` and `getsockopt(SO_ERROR)` slivers already
  there. `bind`/`listen` are hand-rolled too, as two separate syscalls --
  rustils' own `TcpListener::bind` only exposes the combined "bind and
  immediately listen" operation, which wouldn't leave room to configure
  the socket in between.
- **Async DNS resolution** (`io::lookup_host`): resolves a hostname
  (`"example.com:443"`, an `(&str, u16)` pair, or anything else
  implementing `std::net::ToSocketAddrs`) to its `SocketAddr`s without
  blocking a worker thread. There's no portable non-blocking
  `getaddrinfo` -- DNS (and `/etc/hosts`/NSS) resolution is a genuinely
  blocking operation everywhere -- so this runs the real,
  blocking `ToSocketAddrs::to_socket_addrs` on the `spawn_blocking` pool
  and collects the results eagerly into a `LookupHost` iterator, the
  same "looks async, is actually a `spawn_blocking` round trip" shape
  `fs::File`/`io::stdio`/`process::Child::wait` already use for
  operations with no reactor-driven alternative.
- **`UnixDatagram`** (`io::UnixDatagram`, Unix-only -- absent from the
  crate entirely on Windows, along with `UnixStream`/`UnixListener`/
  `process`/`signal`, rather than compiling to stub methods that would
  panic at runtime): the connectionless `AF_UNIX`
  counterpart of `UdpSocket` -- one socket both sends and receives,
  addressed by filesystem path, no listener/stream split. The one socket
  type in `io` *not* built on a rustils concrete type: rustils' `Net`
  trait has no `AF_UNIX` datagram support at all (only `unix_connect`/
  `unix_listen` for connection-oriented `AF_UNIX` sockets), and rather
  than hand-rolling a third copy of `AF_UNIX` sockaddr packing and
  `sendto`/`recvfrom` in this crate (`socket/mod.rs` already has one
  hand-rolled copy for non-blocking `AF_UNIX` stream `connect`, and
  rustils has its own internal one), this wraps
  `std::os::unix::net::UnixDatagram` directly -- std's own
  implementation is already complete and needs zero new unsafe code
  here, just a `set_nonblocking(true)` and reactor registration, the
  same bridge `TcpStream::from_std` already builds for adopting a `std`
  socket into this crate's reactor.
- **Async child processes** (`process::Command`): mirrors
  `std::process::Command`'s builder API (`arg`/`args`/`env`/`envs`/
  `env_remove`/`env_clear`/`current_dir`/`stdin`/`stdout`/`stderr`,
  same `&mut self -> &mut Self` chaining), but `spawn()`'s `Child` gives
  async access to piped stdio and a `wait()` that doesn't block a
  worker thread. Built directly on `std::process`, not rustils: rustils
  does have a real `Command`/`Spawner`/`Child` abstraction, but its
  piped stdio comes back as an object-safe `File` trait that
  deliberately hides the underlying fd (for Windows portability, where
  "raw fd" doesn't mean anything) -- incompatible with this crate's
  actual need, since a child's piped stdin/stdout/stderr are plain
  pipes and, unlike a regular file or a terminal, genuinely block on
  read when empty and become readable when data arrives, exactly like a
  socket. Rather than hand-rolling `fork`/`exec`/`posix_spawn` a second
  time just to get raw fds back, this wraps `std::process::Command`/
  `Child` directly -- the same call `io::UnixDatagram` already made for
  the identical reason (rustils' abstraction not fitting this crate's
  reactor-integration need, with `std` already having a complete, safe
  implementation). `ChildStdin`/`ChildStdout`/`ChildStderr` are
  reactor-registered the same way `TcpStream` is (non-blocking,
  readiness-driven) -- a real difference from `fs::File`/stdio, which
  can't be. `wait()` still runs the real, blocking
  `std::process::Child::wait()` on the `spawn_blocking` pool rather
  than a reactor-driven `pidfd`/`EVFILT_PROC` -- not a polling loop
  (nothing re-checks on a timer), a genuine blocking wait that wakes
  immediately and exactly when the child exits, just parked on a
  dedicated OS thread instead of a reactor-registered fd. A pidfd
  (Linux 5.3+) or kqueue's `EVFILT_PROC`/`NOTE_EXIT` (macOS) would each
  need their own from-scratch reactor integration and their own
  real-hardware verification -- a deliberate simplicity trade-off, not
  a placeholder, consistent with `fs::File`/stdio already choosing this
  same shape for operations a reactor can't drive directly.
- **Signal handling** (`signal`): `signal::ctrl_c()` resolves once on
  the next `SIGINT`; `signal::signal(kind)` returns a `Signal` that
  fires every time that `SignalKind` arrives, for as long as it's held.
  Uses the self-pipe trick -- a signal handler can only safely call a
  short, fixed list of async-signal-safe functions (not allocate, not
  lock a mutex), so the actual OS handler installed via `sigaction` does
  exactly one thing, an async-signal-safe `write(2)` of the signal
  number to a pre-created pipe. Everything else -- figuring out which
  listeners care, waking them -- happens later, in an ordinary spawned
  task reading the pipe's other end through the same reactor every
  socket in this crate uses. Each `Signal` coalesces rather than queues
  (three occurrences before a poll are observed as one `Some(())`, not
  three, matching how signal delivery already behaves at the OS level),
  and installation is idempotent and additive: the first call for a
  given kind installs its `sigaction`, every call (including the first)
  adds an independent listener, and a kind this crate was never asked
  about is never touched. This state is process-wide, not per-`Runtime`
  -- signals are a process-wide concept -- driven by whichever `Runtime`
  happens to be current at the first `signal`/`ctrl_c` call; see that
  module's own docs for the (unusual) multiple-`Runtime` caveat this
  implies.
- **Timers** (`time`): `sleep`, `sleep_until`, `timeout`, `interval`, and
  `interval_at` (like `interval`, but the first tick fires at a given
  `Instant` instead of always `now + period`), backed by a single
  background thread holding a min-heap of deadlines. `Interval` also has
  `MissedTickBehavior` (`Burst` -- the default, and this crate's
  original unconditional behavior -- `Delay`, or `Skip`), for choosing
  what happens when a tick isn't collected before more than one period
  has already elapsed.
  `benches/timers.rs` (`cargo bench`, no `criterion` -- a plain
  `harness = false` binary that times things by hand) measures this
  rather than assuming it: on the Linux dev box this was built on, a
  5ms `sleep` typically overshoots by ~150-200µs (p99 well under 1ms,
  with an occasional outlier into the tens of ms attributable to host
  scheduling noise, not the driver), a 2,000-timer simultaneous burst
  doesn't measurably delay a canary sleep queued just behind it (no
  head-of-line blocking at that scale), `Interval`'s drift correction
  holds exactly (0ns cumulative drift after 500 ticks), and
  register+cancel churn sustains roughly 2M ops/sec. Numbers will vary
  by machine; re-run the benchmark rather than trusting these if it
  matters for your use case.
- **Pausable clock for tests** (`time::pause`/`resume`/`advance`): every
  timer deadline is compared against a `Clock` abstraction rather than
  raw `Instant::now()` directly, so a `Builder::new_current_thread`
  runtime's clock can be frozen (`time::pause()`) and then jumped
  forward instantly (`time::advance(duration).await`), firing every
  timer that would have fired during that span, in order, without any
  real waiting -- letting a test assert on long-timeout/interval-drift
  behavior in milliseconds instead of real minutes. `advance` re-checks
  for newly due timers fresh after each firing (not from a
  pre-snapshotted list), so a task woken partway through that
  immediately registers another, shorter sleep still gets it picked up
  within the same `advance` call, as long as it falls within the
  advanced span. Only available on the current-thread flavor -- pausing
  wall-clock time shared by every task on a runtime would be incoherent
  if other worker threads could be concurrently relying on real timing,
  matching tokio's own restriction.
- **Sync primitives** (`sync`): `Notify` (an async condition variable),
  an async-aware `Mutex`, a `oneshot` channel, a bounded `mpsc` channel,
  and `mpsc::unbounded_channel` -- same one-lock-covers-queue-and-wakers
  design as the bounded channel (see `mpsc`'s module docs for the real
  lost-wakeup bug that shape avoids), just without a capacity check or
  anywhere for a sender to wait, so `UnboundedSender::send` is a plain
  synchronous method, not `async fn`. Also `RwLock`: many concurrent
  readers or one exclusive writer, write-preferring like tokio's own
  (once a writer is queued, later readers queue behind it too, rather
  than jumping ahead just because the write lock itself isn't held
  yet -- otherwise constant read traffic could starve a waiting writer
  indefinitely). Also `Semaphore`: caps concurrency at N permits, fair
  (FIFO) like tokio's own -- both borrowed (`SemaphorePermit`) and
  `Arc`-owned (`OwnedSemaphorePermit`, for holding a permit across a
  spawned task boundary) permit flavors, plus `acquire_many` for
  reserving more than one permit at once. Also `watch`: a single-
  latest-value broadcast (`watch::channel`/`Sender`/`Receiver`) --
  `changed().await` resolves once the value's been updated since this
  receiver last saw it, no queue and no lagging (a receiver that misses
  several updates in a row just sees the latest one, not every
  intermediate value). Useful for "the current configuration" or "has
  shutdown been requested"-shaped state -- the same shape
  `Handle::shutdown_notified`/`is_shutting_down` hand-rolled as a
  one-off special case before this existed. Also `OnceCell`: initialize
  a value exactly once no matter how many tasks concurrently ask for
  it -- `get_or_init(|| async { .. }).await` runs the initializer at
  most once, and a caller that arrives *while* another's initializer is
  already in flight (not just one that arrives before it starts) parks
  instead of racing to initialize independently, then gets back the
  same result. If the winning initializer panics or is dropped
  mid-`.await` (its task gets aborted), the cell resets to uninitialized
  rather than getting stuck reporting "initializing" forever -- every
  other parked caller wakes back up and one of them becomes the new
  initializer. Also `broadcast`: a multi-producer, multi-consumer
  channel where every receiver gets its own copy of every message -- a
  real, distinct set of semantics from `mpsc`'s (one message, one
  receiver), not just "mpsc with multiple receivers." A fixed-capacity
  ring buffer backs it; `send` never waits for room, simply overwriting
  the oldest message once full. Each `Receiver` tracks its own read
  position, so a receiver that falls behind the oldest message still
  buffered gets `RecvError::Lagged(n)` on its next `recv()` (reporting
  exactly how many it missed) and resumes from the oldest still-available
  message afterward, rather than reporting `Lagged` forever or blocking
  the sender. `Sender::subscribe()` adds receivers after the fact --
  fresh ones only see messages sent from then on.

  Also `Barrier`: a rendezvous point for a fixed number of tasks --
  `Barrier::new(n)`, then every `wait().await` call blocks until `n` of
  them have all called it, all `n` resolve together, and the barrier
  immediately resets to accept the next round (one caller's
  `BarrierWaitResult::is_leader()` is arbitrarily `true` per round, for
  a task that wants to do one-time per-round bookkeeping without every
  task racing to do it). Hand-rolls its own waiter list (a `Vec<Waker>`
  behind the same plain `std::sync::Mutex` guarding the arrival count
  and a generation counter) rather than building on `Notify`: `Notify`'s
  own waiters queue lives behind a *separate* lock from whatever
  external state a caller checks before registering with it, which means
  a caller has to get "check the condition" and "register to be woken"
  to happen as one atomic step relative to whatever might complete that
  condition concurrently, on a different lock -- easy to get subtly
  wrong. Folding the waiter list into the *same* lock already guarding
  the arrival count sidesteps that: a waiter either observes its round
  already completed (no registration needed at all) or is guaranteed to
  land in the waiter list before the completing arrival can possibly
  drain it, since both only ever happen while holding one mutex.
- **`select!`**: race two to five futures, running whichever resolves
  first and dropping (cancelling) the rest -- `rusty_tokio::select! { pat
  = future => body, ... }`. Deliberately scoped rather than a full
  reimplementation of tokio's macro: exactly 2 through 5 branches
  (`macro_rules!` has no clean way to generate a fresh binding per
  repetition on stable Rust without either a `paste!`-style proc-macro
  dependency or a recursive tt-muncher, and explicit enumeration is more
  legible for a macro this central); every branch's pattern must be
  irrefutable (a plain binding or `_`, not `Some(x)`/`Ok(v)` -- tokio's
  own macro lets a non-matching value fall through to re-poll just that
  branch, which needs more machinery than a first pass takes on); and
  branches are always polled in the order written, not tokio's
  randomized order, so if two are simultaneously and permanently ready
  the earlier one always wins (no starvation protection for that case).
  No `else` branch, no `,if <condition>` guards, no biased mode. See the
  macro's own doc comment for the full scope statement.
- **`join!`/`try_join!`**: run two to five futures concurrently *within
  the calling task* (no extra `spawn`, no extra scheduler `Task`) and
  resolve once every one of them has, returning a tuple of their
  outputs -- `rusty_tokio::join!(fut1, fut2, fut3)`. Shares `select!`'s
  "poll every branch each wake" shape and its 2-5-branch scope limit, but
  waits for all branches instead of stopping at the first; a branch that
  finishes early isn't re-polled while its slower siblings catch up.
  `try_join!` is the `Result`-aware sibling: every branch must resolve to
  a `Result` with the *same* error type, and it short-circuits (drops the
  remaining branches without polling them again) on the first `Err`
  rather than waiting for the rest to finish pointlessly.
- **`spawn_blocking`**: offloads a genuinely blocking closure onto a
  separate thread pool that grows on demand (up to a configurable cap)
  and shrinks back down when idle, instead of stalling an async worker
  thread. Implemented as a `oneshot` channel plus an ordinary spawned
  task awaiting it -- not a parallel handle type -- so panics, abort,
  and `.await` on the returned `JoinHandle` all reuse the same task
  machinery every other spawned task does.
- **`task::block_in_place`**: a different pattern from `spawn_blocking`
  -- borrows the *current* worker thread for a blocking closure instead
  of delegating to a separate one, useful when the blocking call needs
  to interleave with non-`Send` local state that can't cross into a
  `spawn_blocking` closure (which must be `Send + 'static`). Since the
  closure runs on the same thread as the calling task, that thread would
  otherwise stop servicing the rest of the pool for however long it
  takes; to avoid that, `block_in_place` hands the calling thread's
  other queued work off to a freshly spawned replacement worker thread
  before running the closure, then retires (exits) the original thread
  once its current task finishes, rather than looping back to service
  the same worker index a second time alongside the replacement. A
  simpler trade-off than tokio's own approach (which hands a "core" back
  and forth and can reuse the blocked thread as a *future* replacement)
  -- an extra OS thread spawn per call, in exchange for a much easier
  implementation to get right. Panics if called outside a task actually
  running on a multi-threaded runtime's worker pool -- directly inside
  `block_on`, from a `spawn_blocking` closure, or on a
  `Builder::new_current_thread()` runtime, which has no worker pool to
  hand work off to in the first place.
- **`tracing`/`tokio-console` instrumentation** (off-by-default `tracing`
  Cargo feature): every spawned task gets a `tracing::Span` shaped
  exactly the way real (unstable, `tokio_unstable`-gated) tokio's own
  instrumentation shapes it -- verified against `console-subscriber`'s
  actual source, not guessed: a task registers the moment
  `console-subscriber` sees a span named `"runtime.spawn"`, reading
  `kind`/`task.name`/`task.id`/`loc.file`/`loc.line`/`loc.col` off its
  fields for display, with poll count and busy/idle time coming for free
  from ordinary span enter/exit (wrapping the spawned future in
  `tracing::Instrument::instrument` -- a standard, non-console-specific
  part of the `tracing` crate -- is all that takes). So the real
  `console-subscriber`/`tokio-console` tool, built against that wire
  format and not this crate specifically, works against this runtime
  with zero changes on its end. `spawn_blocking`'s blocking-pool closure
  gets its own span (matching tokio's own split between a regular task's
  span and a blocking task's), separate from the ordinary task span its
  rendezvous wrapper task gets alongside it. Deliberately not attempted:
  waker clone/drop/self-wake instrumentation and resource/async-op
  instrumentation for `sync` primitives -- both real parts of tokio's
  full console support, but both secondary to a task actually showing up
  at all, per `console-subscriber`'s own task-registration logic; see
  `task::trace`'s module docs for the full reasoning.
- **`#[rusty_tokio::main]`/`#[rusty_tokio::test]`**: attribute macros
  rewriting an `async fn` into the `Runtime::new().unwrap().block_on(
  async { .. })` boilerplate every example and test in this crate used
  to spell out by hand. `#[rusty_tokio::test]` also emits `#[test]`
  itself. Both accept an optional `worker_threads = N` argument
  (`#[rusty_tokio::main(worker_threads = 4)]`); no other arguments --
  tokio's own `flavor`/`start_paused`/etc. don't apply here, since this
  crate has exactly one runtime flavor and no pausable clock (issue
  #56). Defined in a separate `rusty_tokio-macros` proc-macro crate
  (this project's first `syn`/`quote`/`proc-macro2` dependency --
  `proc-macro = true` crates can't export anything but proc-macros, so
  it can't live inside `rusty_tokio` itself) and re-exported from the
  main crate, mirroring tokio's own `tokio`/`tokio-macros` split.

## Example

```rust
use rusty_tokio::Runtime;
use rusty_tokio::io::TcpListener;

fn main() -> std::io::Result<()> {
    let rt = Runtime::new()?;
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:7878".parse().unwrap())?;
        loop {
            let (stream, _peer) = listener.accept().await?;
            rusty_tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                while let Ok(n) = stream.read(&mut buf).await {
                    if n == 0 || stream.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            });
        }
    })
}
```

More in `examples/`: `tcp_echo.rs`, `timers.rs`, `channels.rs`. Run with
`cargo run --example <name>`.

## Built on rustils

[`rustils`](https://github.com/baileyrd/rustils) is a cross-platform
syscall-wrapper library (portable traits + per-OS backends) with a
blocking-only, object-safe (`Box<dyn TcpStream>`-style) net layer -- not
built with async in mind. Getting a reactor to sit on top of it needed a
raw fd and a way to flip `O_NONBLOCK`, neither of which the net layer
exposed, so that gap was filed as
[rustils#41](https://github.com/baileyrd/rustils/issues/41) and landed in
[rustils#42](https://github.com/baileyrd/rustils/pull/42): `AsFd`/`AsRawFd`
and `set_nonblocking` on the five concrete Linux socket types, plus
concrete (non-`Box<dyn Trait>`) `connect`/`bind`/`accept` constructors --
without which the new methods would've been unreachable, since the
object-safe trait methods only ever hand back a type-erased `Box`.

With that in place, `io/tcp.rs` and `io/udp.rs` build directly on
`platform_linux::{LinuxTcpListener, LinuxTcpStream, LinuxUdpSocket}` for
bind/listen/accept, addressing (`local_addr`/`peer_addr`), `set_nodelay`,
and (for UDP) `send_to`/`recv_from` -- all of rustils' own sockaddr
packing/unpacking, `SO_REUSEADDR`, and stale-socket handling, not
reimplemented a second time here. `io/unix.rs` does the same for
`platform_linux::{LinuxUnixListener, LinuxUnixStream}` -- including the
mode-`0600` bind and stale-socket-file reclaim rustils' own
`unix_listen` already does internally (a throwaway probe connect tells a
dead listener's leftover socket file apart from a live one's, so a
crashed-and-restarted listener can rebind at the same path without
manual cleanup).

Two things stayed hand-rolled in `io/socket/mod.rs`, both because of a
real mismatch rather than taste, and both apply equally to `UnixStream`
as to `TcpStream`:

- **Non-blocking `connect`.** `LinuxTcpStream::connect`/
  `LinuxUnixStream::connect` create a *blocking* socket and call a
  blocking `connect(2)` -- correct for rustils' own callers, but it would
  stall an entire worker thread for a connection's RTT if used here
  directly. An async connect needs the socket non-blocking *before*
  `connect(2)`, so it's created and connected by hand (`new_tcp_socket`/
  `connect` for TCP, `new_unix_socket`/`unix_connect` for `AF_UNIX` --
  the latter packing a `sockaddr_un` from a `Path` the same way the
  former packs a `sockaddr_in`/`sockaddr_in6` from a `SocketAddr`), then
  the resulting fd is adopted into the concrete stream type via
  `From<OwnedFd>` for everything after.
- **`read`/`write`.** `platform::net::TcpStream::read`/`write` and
  `UnixStream::read`/`write` take `&mut self` -- fine for rustils'
  blocking callers, wrong for this runtime's stream types, which
  deliberately expose `&self` methods so one task can read while another
  writes the same stream. Bypassing the trait for these two (a raw
  `read`/`write` on an fd already in hand via `AsRawFd`) keeps that API
  instead of hiding a mutex behind it.

`rustils` had no macOS backend at all for a while, so this crate first
had to hand-roll one (`io/socket/macos.rs`, since deleted). That gap was
filed as [rustils#48](https://github.com/baileyrd/rustils/issues/48) --
pointing at the hand-rolled shim as reference material -- and landed as
`platform-macos` in
[rustils#52](https://github.com/baileyrd/rustils/pull/52), shaped to
match `platform_linux` closely enough (same method names, same
`platform::error::Result`, the rustils#41/#42 `AsFd`/`set_nonblocking`
surface included from day one -- and, it turned out, `UnixStream`/
`UnixListener` included from day one too, not just TCP/UDP) that
`io/tcp.rs`/`io/udp.rs`/`io/unix.rs` need only a `#[cfg]`-gated type
alias between `platform_linux`/`platform_macos`, not their own branching.
The same two hand-rolled exceptions above apply on macOS too, for the
same reasons.
[rustils#53](https://github.com/baileyrd/rustils/pull/53) then added a
real `macos-latest` CI leg for `platform-macos`, which caught a genuine
`AF_UNIX` behavioral bug the cross-target `cargo check` this crate still
relies on could not have found -- see the macOS caveat above for what
that does and doesn't cover on *this* crate's side of the boundary.

Windows is the one exception to "socket setup goes through rustils":
`rustils` does have a `platform-windows` crate, but its net module
predates rustils#41/#42's escape hatch above and has no equivalent
surface (no non-blocking toggle, no `AsRawSocket`, no `From<OwnedSocket>`
adoption) -- depending on it would mean hand-rolling the exact same
missing pieces on top of it anyway. `io/socket/windows.rs` goes straight
to `windows-sys` instead (Microsoft's own low-level FFI bindings, the
same crate mio itself depends on for its entire Windows backend),
providing `WindowsTcpListener`/`WindowsTcpStream`/`WindowsUdpSocket` with
the identical inherent-method surface `platform_linux`/`platform_macos`
give their concrete types, so `io/tcp.rs`/`io/udp.rs` need only a third
`#[cfg]`-gated type alias, same as the Linux/macOS split. The two
hand-rolled exceptions above (non-blocking connect, `&self`-based
read/write) apply here too, plus a third that's Windows-specific:
`SO_REUSEPORT` has no Windows equivalent at all, so
`set_reuseport`/`reuseport` fall back to `SO_REUSEADDR` there -- a
strict superset of the POSIX option's behavior, not an exact match, but
the closest available primitive (the same pragmatic choice most
cross-platform networking libraries make).

## What's deliberately not here (yet)

This is a real, working runtime, not a toy -- but it's honest about its
edges instead of papering over them:

- **Linux, macOS, and Windows -- not generic BSD.** All three now have a
  reactor backend (`epoll`, `kevent`, and IOCP+the AFD-poll trick,
  respectively -- see the caveat above for what "have" means for the
  latter two: reviewed, compile-checked, never run on real hardware).
  Generic BSD (FreeBSD/OpenBSD/etc.) could likely reuse the `kevent`
  reactor as-is, but `platform-macos` itself only claims
  `target_os = "macos"`, not BSD generally, so there's no socket layer
  to pair it with yet either.
- **`AsyncRead`/`AsyncWrite` are this crate's own traits, not tokio's or
  `futures-io`'s.** Same shape, so generic code within this project works
  the same way, but a third-party codec/framing crate built against
  tokio's actual trait won't accept this crate's `TcpStream` without a
  shim. The optional `futures-io-compat` feature (off by default) adds
  one for `futures-io` specifically -- `rusty_tokio::io::Compat::new(x)`
  wraps anything implementing this crate's own `AsyncRead`/`AsyncWrite`
  so it also implements `futures_io`'s traits of the same name, for
  codec/framing crates that target `futures-io` directly or
  transitively. No equivalent exists for tokio's own traits: doing that
  properly would mean depending on tokio itself, which this crate
  otherwise avoids entirely (see the top of this README).
- **io_uring is readiness-only.** The optional `io-uring-reactor`
  feature (off by default, Linux only) swaps `epoll_wait` for
  `IORING_OP_POLL_ADD` behind the same `Reactor`/`ScheduledIo` interface
  every other backend uses -- transparent to `TcpStream`/`UdpSocket`/
  `UnixStream`/`AsyncRead`/`AsyncWrite`, no code changes needed to opt
  in, and every existing integration test passes unchanged against it
  (stable across 5 repeated runs on this dev box). It does *not* route
  actual `read`/`write` syscalls through io_uring's own read/write
  opcodes -- those still go through `socket/mod.rs` exactly as before.
  That's a deliberate safety boundary, not an oversight: io_uring's
  read/write opcodes hand the kernel a pointer into a buffer for the
  whole duration of an async operation, but this crate's `AsyncRead`/
  `AsyncWrite` pass borrowed `&mut [u8]`, and a `Future` holding one can
  be dropped (cancelled) at any `Pending` point under ordinary Rust
  semantics -- do that with a real io_uring read still in flight and the
  kernel can write into memory that's since been freed, a genuine
  use-after-free. `tokio-uring`/`monoio` solve this with an owned,
  passed-by-value buffer API instead of a borrowed one; adopting that
  shape here is a bigger, different-shaped change than issue #9 asked
  for and isn't attempted. Built on the `io-uring` crate (what
  `tokio-uring`/`glommio`/`monoio` all use) for the same reason the
  "Runtime" bullet above depends on `crossbeam-deque` rather than
  hand-rolling: ring setup/mmap/SQE/CQE layout is real unsafe code this
  project has no `loom`-style verification for.

## Testing

```
cargo test      # unit tests for the task state machine, plus integration
                # tests covering multi-threaded scheduling, abort, panics,
                # TCP/UDP over the real reactor, and every sync primitive
cargo clippy --all-targets
cargo bench     # timer skew/drift/churn and scheduler contention
                # measurements -- see "Timers"/"Runtime" above

# The futures-io compat shim (off by default -- see "What's deliberately
# not here" below) has its own feature-gated test target:
cargo test --features futures-io-compat

# The io_uring reactor backend (off by default, Linux only -- see
# "What's deliberately not here" below) is transparent to every existing
# test, so there's no separate test target for it -- just re-run
# everything against it:
cargo test --features io-uring-reactor

# This crate's macOS reactor integration: compiles and type-checks, but
# nothing beyond that -- see the macOS caveat above.
rustup target add x86_64-apple-darwin
cargo check --target x86_64-apple-darwin --all-targets

# Same caveat, same reason, for Windows:
rustup target add x86_64-pc-windows-gnu
cargo check --target x86_64-pc-windows-gnu --all-targets
```

The integration tests deliberately exercise real concurrency (many
spawned tasks, real timer-driven yields, actual socket I/O) rather than
mocking the scheduler, since the bugs this kind of code has are races
that only show up under real multi-threaded execution.
