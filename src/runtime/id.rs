//! [`Id`]: an opaque identity for a running [`crate::Runtime`], assigned
//! once at [`super::Builder::build`] time from a process-global monotonic
//! counter -- the same `next_id` shape [`crate::task::id`]'s `TaskId`
//! already uses, and [`crate::time::TimerDriver`]'s own timer
//! registrations before that.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// An opaque identifier for a [`crate::Runtime`], obtained via
/// [`super::Handle::id`]. The only guarantee is uniqueness among other
/// *currently running* runtimes -- IDs happen to be handed out in
/// increasing order, but that's an implementation detail, not something
/// to rely on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Id(u64);

impl Id {
    pub(crate) fn next() -> Self {
        Id(NEXT_ID.fetch_add(1, Ordering::Relaxed))
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
