//! The Windows backend: I/O Completion Ports plus the undocumented
//! "AFD poll" trick every production async runtime on Windows actually
//! uses (mio, libuv, .NET) to get epoll/kqueue-style readiness
//! notifications out of IOCP, which is natively completion-based, not
//! readiness-based -- there's no OS primitive that means "tell me when
//! this socket *could* be read/written", only "tell me when this
//! specific read/write I already started has finished".
//!
//! The trick: Windows' `AFD.sys` (Ancillary Function Driver -- every
//! Winsock socket is secretly a handle to this driver) accepts an
//! `IOCTL_AFD_POLL` request that completes once a socket becomes
//! readable/writable/etc, exactly like a `poll(2)` call -- except
//! submitted as an ordinary overlapped I/O request, so its *completion*
//! arrives through the same IOCP this reactor already waits on. See
//! [`Afd::poll`] below for the actual syscall, and
//! `mio::sys::windows::afd`/`selector` (this implementation's reference
//! point, real production code this crate's own dependency graph doesn't
//! otherwise pull in) for the protocol this mirrors.
//!
//! Simplifications versus mio's own implementation, both safe to make
//! here and both called out since they're real scope decisions, not
//! oversights:
//!
//! - **One shared AFD device handle for every registered socket**, not
//!   mio's pool of one handle per 32 sockets. That pooling exists to
//!   spread load across multiple open device handles; a single handle
//!   supports arbitrarily many concurrent outstanding `IOCTL_AFD_POLL`
//!   requests just fine (that's the entire point of overlapped I/O on a
//!   shared handle), so this is a scaling optimization this crate's
//!   scope doesn't need yet, not a correctness requirement.
//! - **No dynamic interest changes.** Every other backend
//!   (`epoll.rs`/`kqueue.rs`) registers a socket for both readability and
//!   writability once, forever -- never just one or the other, and never
//!   changed after the fact. This backend does the same: each poll
//!   request always asks for the full read+write+close mask, so there's
//!   no need for mio's own "cancel the pending poll and resubmit with a
//!   new mask" reregistration dance.
//! - **No `SIO_BSP_HANDLE_{SELECT,POLL}` fallback chain** for sockets
//!   sitting under a broken Layered Service Provider that doesn't
//!   implement plain `SIO_BASE_HANDLE` correctly -- an edge case for
//!   third-party firewall/antivirus LSPs mio works around; out of scope
//!   here.

use super::{Interest, RawIo, ScheduledIo};
use std::collections::HashMap;
use std::ffi::c_void;
use std::io;
use std::mem;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use windows_sys::Wdk::Foundation::OBJECT_ATTRIBUTES;
use windows_sys::Wdk::Storage::FileSystem::{NtCancelIoFileEx, NtCreateFile, FILE_OPEN};
use windows_sys::Wdk::System::IO::NtDeviceIoControlFile;
use windows_sys::Win32::Foundation::{
    CloseHandle, RtlNtStatusToDosError, HANDLE, INVALID_HANDLE_VALUE, NTSTATUS, STATUS_NOT_FOUND,
    STATUS_PENDING, STATUS_SUCCESS, UNICODE_STRING,
};
use windows_sys::Win32::Networking::WinSock::{
    WSAGetLastError, WSAIoctl, SIO_BASE_HANDLE, SOCKET, SOCKET_ERROR,
};
use windows_sys::Win32::Storage::FileSystem::{
    SetFileCompletionNotificationModes, FILE_SHARE_READ, FILE_SHARE_WRITE, SYNCHRONIZE,
};
use windows_sys::Win32::System::WindowsProgramming::FILE_SKIP_SET_EVENT_ON_HANDLE;
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatusEx, PostQueuedCompletionStatus,
    IO_STATUS_BLOCK, IO_STATUS_BLOCK_0, OVERLAPPED_ENTRY,
};

/// Undocumented by Microsoft, but stable and widely relied upon (mio,
/// wepoll, libuv, .NET all hard-code the same value).
const IOCTL_AFD_POLL: u32 = 0x0001_2024;

const POLL_RECEIVE: u32 = 0b0_0000_0001;
const POLL_SEND: u32 = 0b0_0000_0100;
const POLL_DISCONNECT: u32 = 0b0_0000_1000;
const POLL_ABORT: u32 = 0b0_0001_0000;
const POLL_LOCAL_CLOSE: u32 = 0b0_0010_0000;
const POLL_ACCEPT: u32 = 0b0_1000_0000;
const POLL_CONNECT_FAIL: u32 = 0b1_0000_0000;

const READABLE_FLAGS: u32 =
    POLL_RECEIVE | POLL_DISCONNECT | POLL_ACCEPT | POLL_ABORT | POLL_CONNECT_FAIL;
const WRITABLE_FLAGS: u32 = POLL_SEND | POLL_ABORT | POLL_CONNECT_FAIL;
/// Always registered for, regardless of what the caller actually wants
/// this round -- see this module's docs on why there's no dynamic
/// interest tracking here.
const WATCHED_FLAGS: u32 = READABLE_FLAGS | WRITABLE_FLAGS | POLL_LOCAL_CLOSE;

#[repr(C)]
struct AfdPollHandleInfo {
    handle: HANDLE,
    events: u32,
    status: NTSTATUS,
}

#[repr(C)]
struct AfdPollInfo {
    timeout: i64,
    // AFD supports polling a batch of handles in one request; this
    // reactor only ever polls one socket per request (unlike mio, which
    // doesn't batch either, for the same reason: a batch's single
    // completion would conflate every member's readiness into one event,
    // which doesn't fit this crate's per-fd `ScheduledIo` model).
    number_of_handles: u32,
    exclusive: u32,
    handles: [AfdPollHandleInfo; 1],
}

/// A handle to `\Device\Afd`, the Winsock kernel driver every socket is
/// secretly backed by. Not a real file -- `NtCreateFile`'d directly
/// against the device, bypassing the Win32 `CreateFile` layer entirely,
/// which is why this needs the `Wdk` (Windows Driver Kit) bindings
/// rather than plain `Win32` ones.
struct Afd(OwnedHandle);

// SAFETY: `Afd` wraps a plain kernel handle; every operation performed
// on it here (`NtDeviceIoControlFile`, `NtCancelIoFileEx`) is documented
// safe to call concurrently, from any thread, on a shared handle -- the
// entire point of overlapped I/O against an IOCP-associated handle.
unsafe impl Send for Afd {}
unsafe impl Sync for Afd {}

impl Afd {
    /// Opens the shared AFD device handle and associates it with `cp` --
    /// every poll request submitted through the returned `Afd` delivers
    /// its completion via that port.
    fn new(cp: HANDLE) -> io::Result<Afd> {
        // UTF-16 for `\Device\Afd\rusty_tokio` -- spelled out as a code
        // unit array rather than a `\0`-terminated literal since
        // `UNICODE_STRING` is length-prefixed, not NUL-terminated, and
        // needs an exact byte length below.
        const NAME: &[u16] = &[
            '\\' as u16,
            'D' as u16,
            'e' as u16,
            'v' as u16,
            'i' as u16,
            'c' as u16,
            'e' as u16,
            '\\' as u16,
            'A' as u16,
            'f' as u16,
            'd' as u16,
            '\\' as u16,
            'r' as u16,
            'u' as u16,
            's' as u16,
            't' as u16,
            'y' as u16,
            '_' as u16,
            't' as u16,
            'o' as u16,
            'k' as u16,
            'i' as u16,
            'o' as u16,
        ];
        let name_bytes = mem::size_of_val(NAME) as u16;
        let name = UNICODE_STRING {
            Length: name_bytes,
            MaximumLength: name_bytes,
            Buffer: NAME.as_ptr() as *mut u16,
        };
        let attrs = OBJECT_ATTRIBUTES {
            Length: mem::size_of::<OBJECT_ATTRIBUTES>() as u32,
            RootDirectory: ptr::null_mut(),
            ObjectName: &name,
            Attributes: 0,
            SecurityDescriptor: ptr::null(),
            SecurityQualityOfService: ptr::null(),
        };
        let mut handle: HANDLE = INVALID_HANDLE_VALUE;
        let mut iosb = IO_STATUS_BLOCK {
            Anonymous: IO_STATUS_BLOCK_0 { Status: 0 },
            Information: 0,
        };
        // SAFETY: `attrs`/`name` are valid and outlive this call (both
        // are locals kept alive for the whole function body); `&mut
        // handle`/`&mut iosb` are valid, exclusively borrowed out-params.
        let status = unsafe {
            NtCreateFile(
                &mut handle,
                SYNCHRONIZE,
                &attrs,
                &mut iosb,
                ptr::null(),
                0,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                FILE_OPEN,
                0,
                ptr::null(),
                0,
            )
        };
        if status != STATUS_SUCCESS {
            // SAFETY: converts an NTSTATUS to its Win32 equivalent; no
            // memory is referenced.
            let code = unsafe { RtlNtStatusToDosError(status) };
            return Err(io::Error::from_raw_os_error(code as i32));
        }
        // SAFETY: `handle` was just returned by `NtCreateFile` above and
        // is valid, otherwise-unowned, and wrapped exactly once.
        let owned = unsafe { OwnedHandle::from_raw_handle(handle as RawHandle) };
        // SAFETY: `owned`/`cp` are both valid, open handles; associating
        // a handle with a completion port more than once (never done
        // here) would be the only unsafe use of this call.
        let assoc = unsafe { CreateIoCompletionPort(owned.as_raw_handle() as HANDLE, cp, 0, 0) };
        if assoc.is_null() {
            return Err(io::Error::last_os_error());
        }
        // Not correctness-critical (only silences a redundant
        // handle-signaled-event path this reactor never waits on), but
        // mirrors mio's own setup exactly rather than leaving unexplained
        // behavior deltas from the reference implementation.
        //
        // SAFETY: `owned` is a valid, open handle.
        let ok = unsafe {
            SetFileCompletionNotificationModes(
                owned.as_raw_handle() as HANDLE,
                FILE_SKIP_SET_EVENT_ON_HANDLE as u8,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Afd(owned))
    }

    /// Submits an `IOCTL_AFD_POLL` request. `Ok(true)` means it already
    /// completed synchronously, `Ok(false)` means it's in flight -- either
    /// way, because `self`'s handle is IOCP-associated, a completion
    /// packet is still on its way to [`Reactor::event_loop`]; the two
    /// only differ in timing, never in whether a completion needs to be
    /// retrieved. `Err` is a real failure to even submit.
    ///
    /// # Safety
    /// `iosb` and `info` (transitively, via `overlapped`) must both
    /// remain valid, and must not be touched by anything else, for as
    /// long as the request stays outstanding -- until its completion is
    /// retrieved via IOCP, or [`Afd::cancel`] is called and *its*
    /// completion is retrieved. The kernel writes into both
    /// asynchronously for that whole window.
    unsafe fn poll(
        &self,
        info: &mut AfdPollInfo,
        iosb: *mut IO_STATUS_BLOCK,
        overlapped: *mut c_void,
    ) -> io::Result<bool> {
        let info_ptr = info as *mut AfdPollInfo as *mut c_void;
        // SAFETY: `iosb` is valid per this function's own safety
        // contract.
        unsafe {
            (*iosb).Anonymous.Status = STATUS_PENDING;
        }
        // SAFETY: `self.0` is a valid, open AFD device handle; `iosb`/
        // `overlapped` are valid for the call's duration (and beyond, if
        // it goes pending) per this function's contract; `info_ptr` is a
        // valid, properly sized buffer used as both input and output,
        // matching the (undocumented, but stable) `IOCTL_AFD_POLL`
        // protocol.
        let status = unsafe {
            NtDeviceIoControlFile(
                self.0.as_raw_handle() as HANDLE,
                ptr::null_mut(),
                None,
                overlapped,
                iosb,
                IOCTL_AFD_POLL,
                info_ptr,
                mem::size_of::<AfdPollInfo>() as u32,
                info_ptr,
                mem::size_of::<AfdPollInfo>() as u32,
            )
        };
        match status {
            STATUS_SUCCESS => Ok(true),
            STATUS_PENDING => Ok(false),
            _ => {
                // SAFETY: converts an NTSTATUS to its Win32 equivalent.
                let code = unsafe { RtlNtStatusToDosError(status) };
                Err(io::Error::from_raw_os_error(code as i32))
            }
        }
    }

    /// Cancels a still-outstanding [`Afd::poll`] request. Its completion
    /// still arrives via IOCP as usual (now carrying `STATUS_CANCELLED`)
    /// -- this only asks the kernel to finish it early, it doesn't skip
    /// the completion itself.
    ///
    /// # Safety
    /// `iosb` must be the same pointer a still-outstanding `poll` call
    /// was given -- calling this after that request has already
    /// completed (and its completion retrieved) is undefined behavior,
    /// per `NtCancelIoFileEx`'s own contract.
    unsafe fn cancel(&self, iosb: *mut IO_STATUS_BLOCK) -> io::Result<()> {
        let mut cancel_iosb = IO_STATUS_BLOCK {
            Anonymous: IO_STATUS_BLOCK_0 { Status: 0 },
            Information: 0,
        };
        // SAFETY: `self.0` is a valid, open AFD device handle; `iosb` is
        // valid per this function's own safety contract; `&mut
        // cancel_iosb` is a valid, exclusively borrowed out-param.
        let status =
            unsafe { NtCancelIoFileEx(self.0.as_raw_handle() as HANDLE, iosb, &mut cancel_iosb) };
        if status == STATUS_SUCCESS || status == STATUS_NOT_FOUND {
            Ok(())
        } else {
            // SAFETY: converts an NTSTATUS to its Win32 equivalent.
            let code = unsafe { RtlNtStatusToDosError(status) };
            Err(io::Error::from_raw_os_error(code as i32))
        }
    }
}

/// Per-socket AFD poll state -- the Windows counterpart of
/// `epoll.rs`/`kqueue.rs` just storing an `Arc<ScheduledIo>` in a
/// `HashMap`, except a completion-based backend also needs somewhere
/// stable to point the kernel at while a poll request is outstanding.
///
/// Referenced two ways at once: the reactor's own registry
/// (`Reactor::registry`) holds one `Arc`, and -- for as long as a poll
/// request is actually in flight -- the kernel effectively holds another,
/// smuggled through `NtDeviceIoControlFile`'s `overlapped` parameter as a
/// raw pointer (see [`Reactor::submit_poll`]/[`Reactor::event_loop`]) and
/// reclaimed via `Arc::from_raw` once that request's completion is
/// retrieved.
struct SockState {
    scheduled_io: Arc<ScheduledIo>,
    base_socket: RawIo,
    iosb: IO_STATUS_BLOCK,
    poll_info: AfdPollInfo,
    /// True for as long as a poll request the kernel might still
    /// complete into `iosb`/`poll_info` is outstanding.
    pending: bool,
    /// Set by `deregister` -- once true, a completion for this socket is
    /// drained and dropped rather than resubmitted.
    deleting: bool,
}

// SAFETY: `iosb`/`poll_info` embed raw pointers (a `HANDLE`, an
// `IO_STATUS_BLOCK` union's `Pointer` variant) that are never actually
// dereferenced by this code -- only handed to the kernel or read back as
// plain integers/status codes. Access is always through a `Mutex`, so
// there's never true concurrent access, only exclusive access handed off
// between threads, which is exactly what `Send` (not `Sync`, which
// `Mutex<T: Send>` already derives) asserts.
unsafe impl Send for SockState {}

/// Windows implementation of `sys::reactor::Reactor`, backed by IOCP
/// (readiness delivery) plus the shared [`Afd`] device (readiness
/// detection) -- see this module's own docs for the overall design and
/// its deliberate simplifications versus mio's reference implementation.
pub(crate) struct Reactor {
    cp: HANDLE,
    afd: Afd,
    registry: Mutex<HashMap<RawIo, Arc<Mutex<SockState>>>>,
    shutdown: AtomicBool,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

// SAFETY: `cp` is a plain IOCP handle; `CreateIoCompletionPort`/
// `GetQueuedCompletionStatusEx`/`PostQueuedCompletionStatus`/
// `CloseHandle` are all documented safe to call concurrently, from any
// thread, on a handle shared across threads -- the entire point of an
// I/O completion port.
unsafe impl Send for Reactor {}
unsafe impl Sync for Reactor {}

impl Reactor {
    pub(crate) fn new() -> io::Result<Reactor> {
        // SAFETY: `INVALID_HANDLE_VALUE`/null are the documented
        // arguments for creating a fresh completion port not yet
        // associated with any file handle.
        let cp = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, ptr::null_mut(), 0, 0) };
        if cp.is_null() {
            return Err(io::Error::last_os_error());
        }
        let afd = match Afd::new(cp) {
            Ok(afd) => afd,
            Err(e) => {
                // SAFETY: `cp` was just created above and is still open,
                // not yet shared with anything else.
                unsafe {
                    CloseHandle(cp);
                }
                return Err(e);
            }
        };
        Ok(Reactor {
            cp,
            afd,
            registry: Mutex::new(HashMap::new()),
            shutdown: AtomicBool::new(false),
            thread: Mutex::new(None),
        })
    }

    /// Spawns the background completion-port thread. Split from `new`
    /// for the same reason `epoll.rs`/`kqueue.rs` split theirs: the
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
        let mut entries: Vec<OVERLAPPED_ENTRY> = vec![
            OVERLAPPED_ENTRY {
                lpCompletionKey: 0,
                lpOverlapped: ptr::null_mut(),
                Internal: 0,
                dwNumberOfBytesTransferred: 0,
            };
            256
        ];
        loop {
            if self.shutdown.load(Ordering::Acquire) {
                return;
            }
            let mut removed: u32 = 0;
            // SAFETY: `entries` is a valid, exclusively borrowed buffer
            // of at least `entries.len()` `OVERLAPPED_ENTRY`s; `self.cp`
            // is valid for the reactor's whole lifetime; `&mut removed`
            // is a valid, exclusively borrowed out-param.
            let ok = unsafe {
                GetQueuedCompletionStatusEx(
                    self.cp,
                    entries.as_mut_ptr(),
                    entries.len() as u32,
                    &mut removed,
                    u32::MAX,
                    0,
                )
            };
            if ok == 0 {
                // Nothing sane to do with a fatal
                // GetQueuedCompletionStatusEx error (an infinite timeout
                // never legitimately times out); exit the thread rather
                // than spin, matching epoll.rs/kqueue.rs.
                return;
            }
            for entry in &entries[..removed as usize] {
                if entry.lpOverlapped.is_null() {
                    // Our own wake/shutdown sentinel from `shutdown`
                    // below -- the `shutdown` flag check at the top of
                    // this loop is what actually drives exit.
                    continue;
                }
                // SAFETY: `lpOverlapped` is exactly the raw
                // `Arc<Mutex<SockState>>` pointer `submit_poll` leaked
                // via `Arc::into_raw`, handed back unchanged by the
                // kernel -- reclaiming it here is that leak's other half.
                let state = unsafe { Arc::from_raw(entry.lpOverlapped as *const Mutex<SockState>) };
                let mut guard = state.lock().unwrap();
                guard.pending = false;
                if guard.deleting {
                    // `state`/`guard` drop at the end of this iteration,
                    // releasing the kernel's former reference for good;
                    // nothing left to resubmit for a socket being torn
                    // down.
                    continue;
                }
                // SAFETY: `iosb.Anonymous` is read only after this
                // completion was actually retrieved, i.e. only once the
                // kernel is done writing it.
                let status = unsafe { guard.iosb.Anonymous.Status };
                if status < 0 {
                    // The overlapped poll itself failed in an unexpected
                    // way (distinct from a deliberate cancel, already
                    // handled by `deleting` above). Surface both
                    // directions as ready so the caller's next real
                    // read/write attempt discovers the actual error
                    // itself, rather than this socket silently going
                    // quiet forever.
                    guard.scheduled_io.mark_ready(Interest::Read);
                    guard.scheduled_io.mark_ready(Interest::Write);
                    drop(guard);
                    let _ = self.submit_poll(&state);
                    continue;
                }
                let events = guard.poll_info.handles[0].events;
                if events & POLL_LOCAL_CLOSE != 0 {
                    guard.deleting = true;
                    continue;
                }
                if events & READABLE_FLAGS != 0 {
                    guard.scheduled_io.mark_ready(Interest::Read);
                }
                if events & WRITABLE_FLAGS != 0 {
                    guard.scheduled_io.mark_ready(Interest::Write);
                }
                drop(guard);
                // Level-triggered, like every other backend (see this
                // crate's top-level reactor docs): immediately re-arm so
                // a future readiness flip is still observed, rather than
                // only ever firing once per registration.
                let _ = self.submit_poll(&state);
            }
        }
    }

    fn wake(&self) {
        // SAFETY: `self.cp` is valid for the reactor's whole lifetime; a
        // null `lpOverlapped` is exactly the sentinel `event_loop` checks
        // for to distinguish this wakeup from a real AFD completion.
        unsafe {
            PostQueuedCompletionStatus(self.cp, 0, 0, ptr::null());
        }
    }

    /// Submits (or resubmits) an AFD poll request for `state`'s socket.
    /// Called both from `register` (the first submission) and from
    /// `event_loop` (every resubmission after a completion).
    fn submit_poll(&self, state: &Arc<Mutex<SockState>>) -> io::Result<()> {
        let mut guard = state.lock().unwrap();
        if guard.deleting {
            return Ok(());
        }
        guard.poll_info = AfdPollInfo {
            timeout: i64::MAX,
            number_of_handles: 1,
            exclusive: 0,
            handles: [AfdPollHandleInfo {
                handle: guard.base_socket as HANDLE,
                events: WATCHED_FLAGS,
                status: 0,
            }],
        };
        guard.pending = true;
        let iosb_ptr: *mut IO_STATUS_BLOCK = &mut guard.iosb;
        let poll_info_ptr: *mut AfdPollInfo = &mut guard.poll_info;
        // Bump the refcount: for as long as this request stays
        // outstanding, the kernel effectively holds a reference to
        // `state`, reclaimed in `event_loop` once its completion (or
        // cancellation) is retrieved.
        let overlapped = Arc::into_raw(state.clone()) as *mut c_void;
        // SAFETY: `iosb_ptr`/`poll_info_ptr` point into `guard`'s locked
        // storage, stable for `state`'s lifetime (it's heap-allocated via
        // `Arc`, never moved); `overlapped` is the live `Arc` reference
        // just leaked above, reclaimed in `event_loop`. Both remain valid
        // until this specific request's completion is retrieved, per
        // `Afd::poll`'s own safety contract.
        let result = unsafe { self.afd.poll(&mut *poll_info_ptr, iosb_ptr, overlapped) };
        drop(guard);
        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                // The submission itself failed outright -- the kernel
                // never got far enough to need the reference just handed
                // to it, so reclaim it here instead of leaking it.
                //
                // SAFETY: `overlapped` is exactly the pointer
                // `Arc::into_raw` produced above, not yet handed to a
                // successful kernel call.
                drop(unsafe { Arc::from_raw(overlapped as *const Mutex<SockState>) });
                Err(e)
            }
        }
    }

    pub(crate) fn register(&self, sock: RawIo) -> io::Result<Arc<ScheduledIo>> {
        let base_socket = get_base_socket(sock)?;
        let scheduled_io = Arc::new(ScheduledIo::new());
        let state = Arc::new(Mutex::new(SockState {
            scheduled_io: scheduled_io.clone(),
            base_socket,
            // SAFETY: an all-zero `IO_STATUS_BLOCK`/`AfdPollInfo` is a
            // valid (if inert) value for these plain-old-data types;
            // both are fully overwritten by `submit_poll` below before
            // ever being handed to the kernel.
            iosb: unsafe { mem::zeroed() },
            poll_info: unsafe { mem::zeroed() },
            pending: false,
            deleting: false,
        }));
        self.registry.lock().unwrap().insert(sock, state.clone());
        self.submit_poll(&state)?;
        Ok(scheduled_io)
    }

    pub(crate) fn deregister(&self, sock: RawIo) {
        let Some(state) = self.registry.lock().unwrap().remove(&sock) else {
            return;
        };
        let mut guard = state.lock().unwrap();
        guard.deleting = true;
        if guard.pending {
            let iosb_ptr: *mut IO_STATUS_BLOCK = &mut guard.iosb;
            // SAFETY: `guard.pending` confirms a poll request is
            // genuinely still outstanding against `iosb_ptr`, exactly
            // `Afd::cancel`'s precondition.
            let _ = unsafe { self.afd.cancel(iosb_ptr) };
        }
        // If nothing was pending, the kernel was never handed a
        // reference to `state` to begin with, so there's no completion
        // coming and nothing further to release -- `state`'s only
        // reference left is this function's local one, dropped when it
        // returns. If something *was* pending, the cancellation's own
        // completion (still delivered via IOCP as usual) is what
        // reclaims the kernel's reference, in `event_loop` above.
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
        // SAFETY: `self.cp` is owned exclusively by this `Reactor` and
        // still open at this point; `self.afd`'s own `Drop` (via its
        // inner `OwnedHandle`) closes the AFD device handle first, which
        // is fine to happen either before or after the port itself goes
        // away -- closing an IOCP-associated handle and closing the port
        // are independent operations, neither ordering leaks or
        // double-frees anything.
        unsafe {
            CloseHandle(self.cp);
        }
    }
}

/// Resolves the real, poll-able OS socket handle underneath `sock` --
/// almost always `sock` itself, except when a Layered Service Provider
/// (a third-party firewall/antivirus/VPN Winsock shim) is installed,
/// where AFD needs the *base* handle beneath the LSP's own layered one.
/// See this module's docs on why the `SIO_BSP_HANDLE_*` fallback chain
/// mio uses for LSPs broken enough to not even implement this correctly
/// is out of scope here.
fn get_base_socket(sock: RawIo) -> io::Result<RawIo> {
    let mut base: RawIo = 0;
    let mut bytes: u32 = 0;
    // SAFETY: `&mut base` is a valid, exclusively borrowed out-param
    // sized to `RawIo`; `sock` is caller-owned and open.
    let r = unsafe {
        WSAIoctl(
            sock as SOCKET,
            SIO_BASE_HANDLE,
            ptr::null(),
            0,
            (&mut base as *mut RawIo).cast(),
            mem::size_of::<RawIo>() as u32,
            &mut bytes,
            ptr::null_mut(),
            None,
        )
    };
    if r == SOCKET_ERROR {
        // SAFETY: reads the calling thread's last-error slot right after
        // a failed Winsock call.
        return Err(io::Error::from_raw_os_error(unsafe { WSAGetLastError() }));
    }
    Ok(base)
}
