//! [`Interest`]/[`Ready`]: which readiness direction(s) a caller wants,
//! and which direction(s) actually fired. Shared by [`super::AsyncFd`]
//! today; the generic per-socket readiness methods tracked separately
//! (issue #134) will consume the same two types once they land.
//!
//! Only tracks readable/writable: this crate's reactor always monitors
//! both directions unconditionally for every registered fd (see
//! `io::reactor::epoll::Reactor::register`'s `EPOLLIN | EPOLLOUT`), so
//! there's no separate read-closed/write-closed/priority/error bit to
//! report the way real tokio's fuller `Ready` does -- a read that hits
//! EOF or a peer close still just shows up as ordinary
//! readable-then-short-read, same as every other type in this crate
//! already handles it.

use std::ops::{BitOr, BitOrAssign};

const READABLE: u8 = 0b01;
const WRITABLE: u8 = 0b10;

/// Which readiness direction(s) a caller is interested in -- passed to
/// [`super::AsyncFd::with_interest`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Interest(u8);

impl Interest {
    pub const READABLE: Interest = Interest(READABLE);
    pub const WRITABLE: Interest = Interest(WRITABLE);

    pub fn is_readable(&self) -> bool {
        self.0 & READABLE != 0
    }

    pub fn is_writable(&self) -> bool {
        self.0 & WRITABLE != 0
    }
}

impl BitOr for Interest {
    type Output = Interest;

    fn bitor(self, rhs: Interest) -> Interest {
        Interest(self.0 | rhs.0)
    }
}

impl BitOrAssign for Interest {
    fn bitor_assign(&mut self, rhs: Interest) {
        self.0 |= rhs.0;
    }
}

/// Which readiness direction(s) actually fired -- reported by
/// [`super::AsyncFdReadyGuard`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Ready(u8);

impl Ready {
    pub const EMPTY: Ready = Ready(0);
    pub const READABLE: Ready = Ready(READABLE);
    pub const WRITABLE: Ready = Ready(WRITABLE);

    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    pub fn is_readable(&self) -> bool {
        self.0 & READABLE != 0
    }

    pub fn is_writable(&self) -> bool {
        self.0 & WRITABLE != 0
    }
}

impl BitOr for Ready {
    type Output = Ready;

    fn bitor(self, rhs: Ready) -> Ready {
        Ready(self.0 | rhs.0)
    }
}

impl BitOrAssign for Ready {
    fn bitor_assign(&mut self, rhs: Ready) {
        self.0 |= rhs.0;
    }
}
