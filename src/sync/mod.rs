//! Async-aware synchronization primitives: things that suspend the
//! *task* while waiting, rather than blocking the worker thread the way
//! `std::sync` equivalents do.

pub mod mpsc;
pub mod oneshot;

mod mutex;
mod notify;
mod rwlock;

pub use mutex::{Mutex, MutexGuard};
pub use notify::{Notified, Notify};
pub use rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
