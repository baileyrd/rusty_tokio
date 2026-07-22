//! Dispatches to the per-OS socket layer: `posix.rs` (Linux/macOS, a thin
//! sliver of hand-rolled `libc` on top of rustils' `platform_linux`/
//! `platform_macos`) or `windows.rs` (a full hand-rolled `windows-sys`
//! layer -- see that module's docs for why there's no vendored crate to
//! lean on there the way POSIX has one). Both expose the identical
//! free-function surface this re-exports (`connect`/`read`/`write`/
//! `bind`/`listen`/socket-option helpers/etc.), so `tcp.rs`/`udp.rs`
//! never need their own `#[cfg]` for which one is live.

#[cfg(unix)]
mod posix;
#[cfg(unix)]
pub(crate) use posix::*;

#[cfg(windows)]
pub(crate) mod windows;
#[cfg(windows)]
pub(crate) use windows::*;

use platform::error::{OsCode, PlatformError};
use std::io;

/// Adapts `rustils`' two-axis `PlatformError` to `std::io::Error` so it
/// composes with the rest of this crate's (and every caller's) plain
/// `io::Result`-based API. Both `OsCode` arms round-trip through std's
/// own raw-os-error mapping -- `Errno` on Unix, `Win32` on Windows (where
/// `io::Error::from_raw_os_error`/`.raw_os_error()` already speak the
/// same `GetLastError`-style number space) -- so e.g. `EAGAIN`/
/// `WSAEWOULDBLOCK` still comes back as `io::ErrorKind::WouldBlock` on
/// their respective platforms, exactly what `reactor::poll_io`'s retry
/// loop checks for.
pub(crate) fn from_platform_err(e: PlatformError) -> io::Error {
    match e.os {
        OsCode::Errno(errno) => io::Error::from_raw_os_error(errno),
        OsCode::Win32(code) => io::Error::from_raw_os_error(code as i32),
        OsCode::None => io::Error::other(e),
    }
}
