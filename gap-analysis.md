# gap-analysis.md — rusty_tokio vs tokio 1.53.1

Reference: `tokio` 1.53.1 (pinned for this run), `full` feature set, public API extracted
with `cargo public-api`. Target: `rusty_tokio` (this repo), `--all-features`.

Scope for this run (per step 0): a full `cargo public-api` diff against tokio's public
API surface, plus the three items already documented in README's "What's deliberately
not here (yet)" section (source `roadmap` below — that section is this repo's
hand-curated scope doc), filed as issues too per explicit instruction even though each
carries a documented reason for exclusion.

Matching is by bare symbol name (module path ignored) — see "Limitations" in the
parity-loop skill. Rows below already correct several name-collision false
positives/negatives the mechanical diff produced (noted per row); the full noise/
exclusion list from each subsystem pass is kept out of this table for length but was
reviewed before anything below was included.

## Documented gaps (source: roadmap — README "What's deliberately not here (yet)")

| Symbol | Category | Source | Platforms | Reference | Breaking? | Est. size | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Generic BSD reactor/socket support | platform | roadmap | BSD (FreeBSD/OpenBSD/etc.) | README "What's deliberately not here" | no | L | `kevent` reactor likely reusable as-is; blocked on upstream `rustils` having no BSD-generic socket layer (`platform-macos` only claims `target_os = "macos"`). Real work spans this repo *and* rustils. |
| tokio/`futures-io` trait compatibility for `AsyncRead`/`AsyncWrite` | type | roadmap | all | README "What's deliberately not here" | no (additive feature, mirroring existing `futures-io-compat`) | M | **Flag before filing:** closing this properly (accepting tokio's actual traits, not just `futures-io`'s) means depending on real `tokio` — which is this crate's stated foundational non-goal ("no tokio, no mio... to actually understand how an async runtime works, not to replace tokio"). This isn't a stop-and-ask on breaking-change grounds, it's a conflict with the project's own premise; recommend confirming intent before an issue is even filed, separate from the loop's normal breaking-change gate. |
| io_uring full read/write opcodes (owned-buffer API) | fn (existing, behavior) | roadmap | linux (`io-uring-reactor` feature) | README "What's deliberately not here" | **yes** | L | README already explains why: current `AsyncRead`/`AsyncWrite` pass borrowed `&mut [u8]`, but a `Future` holding an in-flight io_uring read can be dropped mid-flight under ordinary Rust semantics — a real use-after-free. Closing this needs an owned/passed-by-value buffer API (`tokio-uring`/`monoio`-shaped), a materially bigger, different-shaped redesign. Genuinely breaking; must stop-and-ask per loop rules, not auto-implement. |

## `net` (diff)

| Symbol | Category | Source | Platforms | Reference | Breaking? | Est. size | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Generic readiness-based I/O: `poll_read_ready`/`poll_write_ready`/`poll_accept`/`poll_recv`/`poll_send`/`poll_recv_from`/`poll_send_to`/`poll_recv_ready`/`poll_send_ready`/`poll_peek_from`/`poll_peek_sender`/`try_io`/`async_io`/`readable`/`writable`/`ready` on `TcpStream`/`TcpListener`/`UdpSocket`/`UnixStream`/`UnixListener` | fn | diff | linux/macos/windows | `net::{TcpStream,TcpListener,UdpSocket,UnixStream,UnixListener}` readiness methods | no | L | Internal `ScheduledIo`/`poll_io` (`src/io/reactor/mod.rs`) already does this; `pub(crate)` only. Consumes the same public `Interest`/`Ready` types as the `io` bucket's `AsyncFd` row below — do that one first or in lockstep. |
| Peek family: `peek`/`poll_peek`/`peek_from`/`poll_peek_from`/`peek_sender`/`poll_peek_sender`/`try_peek`/`try_peek_from`/`try_peek_sender` | fn | diff | linux/macos/windows | `TcpStream`/`UdpSocket`/owned read halves `peek*` | no | M | No `MSG_PEEK`-based recv anywhere in `socket/posix.rs` today. |
| `try_read`/`try_write`/`try_send`/`try_recv`/`try_send_to`/`try_recv_from` (+ `_vectored`) | fn | diff | all | `TcpStream`/`UdpSocket`/`UnixStream` + owned halves `try_*` | no | M | Non-vectored: cheap, call `socket::read`/`write` once and propagate `WouldBlock`. `_vectored` needs real `readv`/`writev` (current `poll_write_vectored` only writes the first buffer per README). |
| `try_read_buf`/`recv_buf`/`recv_buf_from` (`bytes::Buf`-based recv) | fn | diff | all | `TcpStream`/`UdpSocket` `*_buf` methods | no | M | **New dependency**: `bytes` crate not currently in `Cargo.toml`. Overlaps `io` bucket's `read_buf`/`write_buf` row — implement together. |
| UDP multicast: `join_multicast_v4/v6`, `leave_multicast_v4/v6`, `multicast_loop_v4/v6` (get+set), `multicast_ttl_v4` (get+set) | fn | diff | linux/macos | `UdpSocket` multicast methods | no | M | No `IP_ADD_MEMBERSHIP`/`IPV6_JOIN_GROUP`/etc. setsockopt plumbing yet. |
| UDP broadcast: `set_broadcast` + `broadcast` getter | fn | diff | all | `UdpSocket::{set_broadcast,broadcast}` | no | S | `broadcast()` getter didn't even surface in the diff — masked because rusty_tokio's own `sync::broadcast` module shares the bare name. Real gap in both directions. |
| TCP options: `linger`/`set_linger`/`set_zero_linger`, `keepalive`/`set_keepalive`, `quickack`/`set_quickack` (linux-only), `nodelay` getter, `tos_v4`/`set_tos_v4`, `tclass_v6`/`set_tclass_v6`, `ttl`/`set_ttl`, `from_std_stream` | fn | diff | linux/macos/windows (`quickack` linux/android only) | `TcpSocket`/`TcpStream`/`TcpListener` option methods | no | M | Same shape as `TcpSocket`'s existing hand-rolled `set_reuseaddr`/`set_reuseport`/buffer-size options. |
| UDP options: `ttl`/`set_ttl`, `tos_v4`/`set_tos_v4`, `tclass_v6`/`set_tclass_v6` | fn | diff | linux/macos/windows | `UdpSocket` option methods | no | S | Same pattern, UDP side. |
| `take_error` (`SO_ERROR` passthrough) | fn | diff | all | `TcpSocket`/`TcpStream`/`UdpSocket`/`UnixListener`/`UnixStream::take_error` | no | S | Private `take_socket_error` already exists internally (used by non-blocking connect) — just needs a public non-consuming wrapper per type. |
| `peer_cred`/`UCred`/`uid`/`gid`/`pid` (+ `_t` typedefs) | type + fn | diff | linux/macos (unix-only) | `net::unix::{UCred, UnixStream::peer_cred}` | no | M | `SO_PEERCRED` (linux) vs `LOCAL_PEERCRED`/`getpeereid` (macos) need their own platform split. |
| `reunite`/`ReuniteError` (+ `AsRef` on owned halves) | fn/type | diff | all (unix side unix-only) | `tcp`/`unix` owned split-half `reunite` | no | S | Owned halves are already `Arc`-based (`tcp.rs`/`unix.rs`) — natural `Arc::ptr_eq` + `Arc::try_unwrap` addition. Currently no way back from split halves to one stream. |
| `UnixSocket` builder (`new_stream`/`new_datagram`/`bind`/`listen`/`connect`, raw-fd ctors) | type | diff | linux/macos | `net::UnixSocket` | no | M | `TcpSocket` already has this "bare socket before commit" shape; nothing analogous for `AF_UNIX`. |
| Unix abstract-namespace addressing: `SocketAddr`/`as_pathname`/`as_abstract_name`/`is_unnamed`, `UnixListener::bind_addr`, `UnixStream::connect_addr` | type | diff | linux (abstract-namespace part; type itself linux/macos) | `net::unix::SocketAddr` | no | M | `to_sockaddr_un` only packs a real filesystem path today — no abstract-namespace (leading NUL) or unnamed-address support; `local_addr`/`peer_addr` return plain `Option<PathBuf>`. |
| `UnixStream::pair` (`socketpair(2)`) | fn | diff | linux/macos | `net::UnixStream::pair` | no | S | No connected-pair constructor without a filesystem listener today. |
| Unix named pipes (`net::unix::pipe` module: `OpenOptions`, `open_receiver`/`open_sender`, `from_file[_unchecked]`, `from_owned_fd[_unchecked]`, `into_{blocking,nonblocking}_fd`, `Sender`/`Receiver` + readiness surface) | type | diff | linux/macos | `net::unix::pipe::*` | no | L | Whole new I/O type (FIFO open + reactor registration). `Sender`/`Receiver` names themselves were masked by rusty_tokio's four unrelated `Sender`/`Receiver` pairs (mpsc/oneshot/watch/broadcast) — real gap, just hidden from the bare-name list under those headers. |
| Generic `ToSocketAddrs`-based `bind`/`connect` (hostname/`&str`/tuple, not just `SocketAddr`) | trait/fn | diff | all | `net::ToSocketAddrs` | no (source-compatible generalization) | M | Today every `bind`/`connect` takes a concrete `SocketAddr`; resolving a hostname needs a manual `io::lookup_host` call first. |
| Raw fd/handle interop: `AsFd`/`AsRawFd`/`as_fd`/`as_raw_fd`/`from_raw_fd`/`into_raw_fd` on net types | trait impl | diff | unix (`AsRawFd`) / windows (`AsRawSocket`) | across `TcpListener`/`TcpStream`/`UdpSocket`/`UnixListener`/`UnixStream`/`TcpSocket`/`UnixSocket` | no | S | Internal `AsRawIo`/`OwnedIo` abstraction already covers both platforms uniformly — mostly a visibility/trait-impl exercise. Shared theme with `process`'s pipe fd-interop row; consider one issue covering the common pattern plus per-module impls, or sibling issues that explicitly cross-reference. |
| `bind_device`/`device` (`SO_BINDTODEVICE`) | fn | diff | linux-only (matches tokio's own gating) | `TcpSocket`/`UdpSocket::{bind_device,device}` | no | S | New setsockopt sliver. |

## `io` (diff)

| Symbol | Category | Source | Platforms | Reference | Breaking? | Est. size | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `AsyncFd`/`AsyncFdReadyGuard`/`AsyncFdReadyMutGuard`/`AsyncFdTryNewError`/`TryIoError` + public `Interest`/`Ready` bitflags | type | diff | unix (matches tokio's own gating) | `tokio::io::unix::AsyncFd<T>` | no | L | Real gap: lets a user register their *own* raw fd (custom device, eventfd, GPIO) with the reactor. `reactor::Interest`/`ScheduledIo` (`src/io/reactor/mod.rs`) already do the readiness work internally but are `pub(crate)`. Biggest single row in this bucket; do before/alongside `net`'s generic-readiness row, which will want the same public `Interest`/`Ready` types. |
| `read_u8`/`i8`/`u16`/`i16`/`u32`/`i32`/`u64`/`i64`/`u128`/`i128`/`f32`/`f64` (+ `_le`) and matching `write_*` on `AsyncReadExt`/`AsyncWriteExt` | fn | diff | all | `AsyncReadExt`/`AsyncWriteExt` byte-order methods | no | M | Tokio hand-rolls these too (`to_be_bytes`/`from_be_bytes`, no `byteorder` dep) — zero new dependency, just default-method repetition. |
| `read_buf`/`write_buf`/`write_all_buf`/`try_read_buf` (`bytes::Buf`/`BufMut` integration) | fn | diff | all | `AsyncReadExt`/`AsyncWriteExt` `*_buf` | no | M | **New dependency**: `bytes` crate. Overlaps `net` bucket's `try_read_buf`/`recv_buf*` row — implement together. |
| `chain`/`Chain`, `take`/`Take` (+ `is_empty`) | type | diff | all | `AsyncReadExt::{chain,take}`, `io::{Chain,Take}` | no | S | No equivalents today; straightforward wrappers analogous to existing `BufReader`. |
| `empty`/`Empty`, `repeat`/`Repeat`, `sink`/`Sink` | fn/type | diff | all | `io::{empty,repeat,sink}` | no | S | Trivial no-op stream constructors, self-contained. |
| `simplex`/`SimplexStream` | fn/type | diff | all | `io::{simplex,SimplexStream}` | no | S | rusty_tokio only has bidirectional `io::duplex`; no one-directional variant. Can reuse most of `duplex.rs`'s single-`Pipe` machinery. |
| `BufStream` | type | diff | all | `io::BufStream<RW>` | no | S | Combined buffered reader+writer; rusty_tokio only has separate `BufReader`/`BufWriter`. Essentially `BufWriter<BufReader<RW>>` given both already exist. |
| Generic `io::split`/`Join` (+ `new_unsplit`) | fn/type | diff | all | `io::{split,Join}` | no | M | Distinct from `TcpStream::split` (already present, concrete): a standalone function splitting *any* `T: AsyncRead+AsyncWrite`, needs its own `Arc<Mutex<T>>`-style shared state. |
| `copy_buf`, `copy_bidirectional_with_sizes` | fn | diff | all | `io::{copy_buf,copy_bidirectional_with_sizes}` | no | S | Variants on the existing `copy`/`copy_bidirectional`; `copy_bidirectional_with_sizes` just parameterizes the current hardcoded 8192-byte buffer. |
| `fill_buf`/`stream_position`/`rewind` (`AsyncBufReadExt`/`AsyncSeekExt` sugar) | fn | diff | all | `AsyncBufReadExt::fill_buf`, `AsyncSeekExt::{stream_position,rewind}` | no | S | Underlying `AsyncBufRead`/`AsyncSeek` are already structurally sufficient — pure convenience-wrapper sugar, no deeper gap. |

## `fs` (diff)

| Symbol | Category | Source | Platforms | Reference | Breaking? | Est. size | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Directory creation/removal: `create_dir`, `create_dir_all`, `remove_dir`, `remove_dir_all` | fn | diff | all | `fs::{create_dir,create_dir_all,remove_dir,remove_dir_all}` | no | S | Thin `spawn_blocking` wrappers, same shape as existing `File::open`/`create`. |
| Path metadata/existence/permissions: `canonicalize`, `metadata`, `symlink_metadata`, `try_exists`, `set_permissions` (free fn) | fn | diff | all | `fs::{canonicalize,metadata,symlink_metadata,try_exists,set_permissions}` | no | S/M | Same thin-wrapper shape. |
| Rename/copy/link: `rename`, `hard_link`, `read_link`, `symlink`, `remove_file`, `copy` | fn | diff | all | `fs::{rename,hard_link,read_link,symlink,remove_file,copy}` | no | S/M | `copy` is masked by name-collision with `io::copy` in the mechanical diff (present under that unrelated symbol) — real gap despite not surfacing as a candidate. |
| Whole-file convenience functions: `read`, `write`, `read_to_string` (`tokio::fs::*`, not per-stream methods) | fn | diff | all | `fs::{read,write,read_to_string}` | no | S | Masked entirely by name-collision with `AsyncReadExt::{read,read_to_string}`/`AsyncWriteExt::write` — genuine gap the mechanical diff hid completely; these are one-shot whole-file ops, unrelated to the per-stream extension methods. |
| `DirBuilder` (+ `create`) | type | diff | all (`mode` unix-only) | `fs::DirBuilder` | no | S/M | `create` also masked by collision with `File::create`; ship together, builder is useless without it. |
| Directory iteration: `DirEntry`, `ReadDir`, `read_dir`, `next_entry`, `poll_next_entry`, `file_name`, `file_type`, `path`, `ino` (unix-only) | type | diff | all (`ino` unix-only) | `fs::{DirEntry,ReadDir,read_dir}` | no | M/L | Largest fs row — needs an iterator-shaped state machine analogous to `File`'s existing `Idle`/`Busy`/`Poisoned` machine. |
| `OpenOptions` builder (+ `create` flag, `append`/`create_new`/`truncate`/`mode`/`custom_flags`) | type | diff | all (`mode`/`custom_flags` unix-only) | `fs::OpenOptions` | no | M | Plain boolean `create` flag also masked by the `File::create`/`DirBuilder::create` name collision — ship together with `File::create_new`/`File::options()`. |
| `File` lifecycle methods: `set_len`, `max_buf_size`/`set_max_buf_size`, `set_permissions`, `sync_all`, `sync_data`, `try_clone`, `try_into_std`, `options` | fn | diff | all | `fs::File` methods | no | M | Most reuse the existing spawn_blocking state machine; `max_buf_size`/`set_max_buf_size` are pure in-memory config, no I/O. |

## `sync` (diff)

| Symbol | Category | Source | Platforms | Reference | Breaking? | Est. size | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `Mutex` owned/mapped guards: `OwnedMutexGuard`, `MappedMutexGuard`, `OwnedMappedMutexGuard` (+ `lock_owned`/`map`) | type/fn | diff | all | `sync::{OwnedMutexGuard,MappedMutexGuard,OwnedMappedMutexGuard}` | no | M | `Mutex` only has borrowed `lock`/`try_lock`/`get_mut` today; `Semaphore`'s existing `OwnedSemaphorePermit` is a precedent to mirror. |
| `RwLock` owned/mapped guards: `OwnedRwLockReadGuard`, `OwnedRwLockWriteGuard`, `OwnedRwLockMappedWriteGuard`, `RwLockMappedWriteGuard` (+ downgrade) | type/fn | diff | all | `sync::{OwnedRwLock*,RwLockMappedWriteGuard}` | no | L | Largest sync row: downgrade (write→read without releasing) has to interact correctly with the existing write-preferring fairness state machine. |
| `Semaphore::close`/`is_closed` + `AcquireError`/`TryAcquireError` | fn/type | diff | all | `sync::{Semaphore::close,AcquireError,TryAcquireError}` | **yes** | M | `acquire`/`acquire_many` are currently infallible; `try_acquire*` return `Option`, not `Result`. Supporting a "closed" semaphore means widening these return types — an actual signature break, not additive. |
| `OwnedSemaphorePermit` extras: `num_permits`, `semaphore`, `merge` | fn | diff | all | `sync::OwnedSemaphorePermit::{num_permits,semaphore,merge}` | no | S | Private fields, no public getters/merge today. |
| `Semaphore::MAX_PERMITS`/`const_new`/`forget_permits` | const/fn | diff | all | `sync::Semaphore::{MAX_PERMITS,const_new,forget_permits}` | no | S | No const-fn constructor (blocks `static` semaphores) or way to permanently drop N permits without a guard. |
| `mpsc::Permit`/`OwnedPermit`/`PermitIterator` (+ `reserve*` family) | type/fn | diff | all | `sync::mpsc::{Permit,OwnedPermit,PermitIterator}` | no | M | Distinct from Semaphore's owned-permit support (confirmed, not a naming collision) — reserve-a-slot-then-fill pattern, absent entirely. |
| `mpsc::error::TrySendError`/`SendTimeoutError` (`try_send`/`send_timeout`) | fn/type | diff | all | `sync::mpsc::error::{TrySendError,SendTimeoutError}` | no | S | `Sender` only has `send().await` today; `send_timeout` needs no new dependency (`time` already exists). |
| `mpsc::WeakSender`/`WeakUnboundedSender` (+ `broadcast::WeakSender`) | type | diff | all | `sync::mpsc::{WeakSender,WeakUnboundedSender}` | no | S | No `downgrade()`/weak-sender variant anywhere in rusty_tokio's channels today. |
| `Notify::notified_owned`/`OwnedNotified` (+ `enable`) | fn/type | diff | all | `sync::{futures::OwnedNotified,Notify::notified_owned}` | no | M | `notified()` only borrows `&self` today — no Arc-owned variant for holding a pending wait across a spawned-task boundary. |
| `Notify::notify_last`/`const_new` | fn | diff | all | `sync::Notify::{notify_last,const_new}` | no | S | No LIFO-wake variant (`notify_one` always pops the oldest waiter) or const-fn constructor. |
| `SetOnce`/`SetOnceError` | type | diff | all | `sync::{SetOnce,SetOnceError}` | no | S/M | Distinct from existing `OnceCell`: no-initializer "someone sets it once, everyone else waits" primitive. Real but lower-priority given the conceptual overlap. |

## `runtime` (diff)

| Symbol | Category | Source | Platforms | Reference | Breaking? | Est. size | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `Builder` thread config: `thread_name`/`thread_name_fn`/`thread_stack_size`/`thread_keep_alive` | fn | diff | all | `runtime::Builder` thread-config methods | no | M | Worker threads are hardcoded `"rusty_tokio-worker-{idx}"`; blocking pool hardcoded name + 10s idle timeout. Needs new `Builder` fields threaded into `Shared`/`spawn_worker`/`BlockingPool::new`. |
| `EnterGuard` / `Handle::enter()` / `Runtime::enter()` | type/fn | diff | all | `runtime::EnterGuard` | no | S | Internal `context::enter`/`EnterGuard` already exist (used by `block_on`/worker startup) but are `pub(crate)` — mostly a visibility change. |
| `LocalRuntime`/`build_local` (+ `LocalOptions`) | type/fn | diff | all | `runtime::{LocalRuntime,Builder::build_local}` | no | L | Distinct from existing `LocalSet`: one owned type bundling current-thread scheduling + reactor/timer + native `!Send` acceptance, vs. today's "manually build a `Runtime`, separately drive a `LocalSet`" split (and a bare `LocalSet` has no reactor/timer of its own per README). |
| `TryCurrentError` (+ `is_missing_context`/`is_thread_local_destroyed`/`is_rt_shutdown_err`) | type/fn | diff | all | `runtime::TryCurrentError` | no (implement as a new method alongside `try_current()`, not a signature change) | M | `Handle::try_current()` returns a bare `Option` today, collapsing "never in a runtime" and "runtime shutting down" into one `None`. Changing `try_current()` itself would break its signature — add a `try_current_detailed()`-shaped alternative instead. |
| `runtime_flavor`/`name` (+ `runtime::Id`) | fn/type | diff | all | `Handle::runtime_flavor`, `Builder::name`, `runtime::Id` | no | S/M | No runtime naming/flavor introspection at all; internally a private `Flavor` enum already exists. `Id` didn't surface directly (masked by `JoinHandle::id()`) but the underlying "unique per-Runtime-instance identifier" capability is equally absent. |
| `RuntimeMetrics::worker_park_unpark_count`/`worker_total_busy_duration` | fn | diff | all | `runtime::RuntimeMetrics` additional fields | no | M | `worker_park_count` exists; no unpark-side counter or busy/idle duration tracking. Adds a genuinely new (small) hot-path timing cost — worth weighing against this project's own stated "not the kind of hot-path cost that justifies withholding by default" bar (README), but flag the cost explicitly in the PR since it's a bit more than the existing atomic counters. |

*Left out of this table*: `enable_io`/`enable_time`/`enable_all` (not applicable — tokio-only concept, this runtime always has both) and `global_queue_interval`/`event_interval`/`max_io_events_per_tick` (niche fairness-tuning knobs tied to tokio's inline I/O-driver ticking, which this crate's always-dedicated-reactor-thread design doesn't have an equivalent for; no demonstrated starvation symptom today — worth a real look later, not filed as a parity gap here).

## `time` (diff)

| Symbol | Category | Source | Platforms | Reference | Breaking? | Est. size | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `timeout_at` | fn | diff | all | `time::timeout_at` | no | S | Only `sleep_until`/`interval_at` have `_at` deadline variants today; `timeout` has none. |
| `Interval::poll_tick` | fn | diff | all | `time::Interval::poll_tick` | no | S | `tick()` only exists as `async fn` wrapping an internal `poll_fn`; no poll-based entry point for manual `Future`/`Stream` impls. |
| `Interval::reset`/`reset_at`/`reset_after`/`reset_immediately`/`period` | fn | diff | all | `time::Interval` reconfiguration methods | no | M | Zero mid-stream reconfiguration today; only way to change period/realign is drop-and-recreate. |
| `Sleep::reset` | fn | diff | all | `time::Sleep::reset` | no | S | No in-place re-arm to a new deadline; only `deadline()` exists. |
| `Sleep::is_elapsed` | fn | diff | all | `time::Sleep::is_elapsed` | no | S | No non-polling elapsed check; `Sleep::poll` already computes this internally. |
| `TimerDriver` silent-hang on shutdown | fn (existing, behavior) | diff | all | `time::error::Error::is_shutdown` (tokio's explicit variant) | no | S | Not really a "missing symbol" so much as a bug this diff surfaced: `TimerDriver::shutdown()` joins the background thread, but `register()` never checks a shutdown flag — a timer registered after shutdown silently sits forever with nothing to ever wake it. Worth its own narrowly-scoped fix rather than reproducing tokio's whole error enum (this crate's `BinaryHeap`-based driver has no capacity ceiling, so `at_capacity`/`invalid` have no real analog here). |

*Left out of this table*: `time::Instant` and its whole arithmetic/comparison cluster (`checked_add`, `duration_since`, `elapsed`, etc.) — **not applicable**. rusty_tokio's paused clock already represents every deadline as a plain `std::time::Instant` (real `Instant::now()` frozen on pause, advanced via ordinary `Duration` arithmetic), so there's no separate "now" type to intercept the way tokio's design requires. See the time-bucket assessment for full reasoning.

## `task` (diff)

| Symbol | Category | Source | Platforms | Reference | Breaking? | Est. size | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `AbortHandle` (+ `JoinHandle::abort_handle()`) | type | diff | all | `task::AbortHandle` | no | M | Distinct from existing `JoinHandle::abort()`: a separate, cloneable, abort-only capability that outlives the original `JoinHandle`. |
| `task::coop` public module: `Coop`/`RestoreOnPending`/`Unconstrained`/`unconstrained`/`consume_budget`/`has_budget_remaining`/`made_progress`/`poll_proceed` | fn/type | diff | all | `task::coop::*` | no | M | The underlying budget mechanism (crate-root `coop.rs`, 128-poll budget) already exists but is entirely `pub(crate)`. Exposes it for custom poll loops and opt-out. |
| `LocalEnterGuard` (`LocalSet::enter()`) | type/fn | diff | all | `task::LocalEnterGuard` | no | S | Today `spawn_local`'s ambient thread-local can only be populated via `LocalSet::run_until`, which also drives the queue. No way to make a `LocalSet` ambient without simultaneously driving it. |
| `JoinError::into_panic`/`try_into_panic` + `JoinHandle::is_finished` | fn | diff | all | `task::JoinError::into_panic`, `JoinHandle::is_finished` | no | S | `JoinError` already privately retains the panic payload; only `is_cancelled()`/`is_panic()` booleans are public. `JoinHandle` has no non-blocking completion check today. |
| `pin!` re-export | macro | diff | all | `tokio::pin!` | no | S | Not a capability gap — `std::pin::pin!` (stable) already covers this and rusty_tokio's own macros already use it internally. Just a `pub use std::pin::pin;` re-export for discoverability/surface parity. |

## `process` (diff)

| Symbol | Category | Source | Platforms | Reference | Breaking? | Est. size | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `arg0` | fn | diff | unix | `process::Command::arg0` | no | S | Thin forward to `std::os::unix::process::CommandExt::arg0`. |
| `as_std`/`as_std_mut` | fn | diff | unix | `process::Command::as_std`/`as_std_mut` | no | S | No accessor to the inner `std::process::Command` today; unlocks any std builder option this wrapper doesn't cover. |
| `process_group` | fn | diff | unix | `process::Command::process_group` | no | S | Thin forward, same shape as `arg0`. |
| `kill_on_drop`/`get_kill_on_drop` (+ `Child` `Drop` impl) | fn | diff | unix | `process::Command::kill_on_drop` | no | M | Real behavioral gap: `Child` has no `Drop` impl at all today — dropping it orphans the child (matches std's default, but tokio makes killing-on-drop opt-in). `Drop` can't `.await`, so the kill+reap needs a detached `spawn_blocking`/signal-only approach. |
| `status` (spawn + wait, discard stdio) | fn | diff | unix | `process::Command::status` | no | S | No equivalent; today even "just run it and get exit code" needs manual `spawn()` + `child.wait()`. |
| `output`/`wait_with_output` (spawn/wait, capture stdout+stderr) | fn | diff | unix | `process::Command::output`, `Child::wait_with_output` | no | M | No equivalent; today requires manual concurrent draining of both piped streams (naive sequential draining risks deadlock on a chatty child) then hand-assembling `std::process::Output`. Buildable from existing `join!`/`read_to_end`/`Child::wait`. |
| `into_owned_fd`/`try_into` (raw-fd/`Stdio` interop for `ChildStdin`/`ChildStdout`/`ChildStderr`) | fn | diff | unix | `process::{ChildStdin,ChildStdout,ChildStderr}::into_owned_fd` | no | M | Part of the same project-wide "no public type implements `AsFd`/`AsRawFd` yet" gap as `net`'s raw-fd row — share an approach. Needs care to avoid double-deregistering from the reactor on extraction. |

---

**Totals**: 3 documented + 17 net + 10 io + 8 fs + 11 sync + 6 runtime + 6 time + 5 task + 7
process = **73 candidate issues**, before any further splitting at filing time (a few
rows above, e.g. fs's directory-iteration row or sync's RwLock-guard row, may
reasonably become 2 issues each once actually scoped).

This is a large backlog — see chat for the scope-trim checkpoint before step 2 runs.
