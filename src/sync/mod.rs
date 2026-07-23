//! Async-aware synchronization primitives: things that suspend the
//! *task* while waiting, rather than blocking the worker thread the way
//! `std::sync` equivalents do.

pub mod broadcast;
pub mod mpsc;
pub mod oneshot;
pub mod watch;

mod barrier;
mod mutex;
mod notify;
mod once_cell;
mod rwlock;
mod semaphore;

pub use barrier::{Barrier, BarrierWaitResult};
pub use mutex::{MappedMutexGuard, Mutex, MutexGuard, OwnedMappedMutexGuard, OwnedMutexGuard};
pub use notify::{Notified, Notify, OwnedNotified};
pub use once_cell::{OnceCell, SetError};
pub use rwlock::{
    OwnedRwLockMappedWriteGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock,
    RwLockMappedWriteGuard, RwLockReadGuard, RwLockWriteGuard,
};
pub use semaphore::{OwnedSemaphorePermit, Semaphore, SemaphorePermit};
