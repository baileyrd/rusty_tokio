//! [`block_in_place`]: run a blocking closure inline, on the calling
//! worker thread, instead of moving it to a different thread the way
//! [`crate::spawn_blocking`] does.

/// Runs `f` -- expected to block the calling thread, unlike ordinary
/// async code -- inline, without moving it to a different thread the
/// way [`crate::spawn_blocking`] does. Useful when the blocking call
/// needs to interleave with non-`Send` local state that can't cross into
/// a `spawn_blocking` closure (that closure must be `Send + 'static`;
/// this one doesn't have to be either).
///
/// Since `f` runs on the very thread that called this (rather than
/// delegating to a separate pool the way `spawn_blocking` does), that
/// thread would otherwise stop servicing the rest of the worker pool for
/// however long `f` takes. To avoid that, this hands the calling
/// thread's other queued work off to a freshly spawned replacement
/// worker thread *before* running `f` -- the calling thread's own task
/// keeps running here, uninterrupted, but nothing else on the pool
/// stalls waiting on it.
///
/// # Panics
/// - If there's no ambient [`crate::Runtime`] at all.
/// - On a [`crate::Builder::new_current_thread`] runtime -- there's no
///   worker pool to hand other queued work off to, and there's only ever
///   the one thread to begin with, so stalling it *is* stalling the
///   whole runtime. Use [`crate::spawn_blocking`] instead.
/// - If called from a thread that isn't currently running as part of a
///   multi-threaded runtime's worker pool -- directly inside
///   `Runtime::block_on`'s own future, or from a `spawn_blocking`
///   closure's own thread, neither of which has "other queued work" of
///   the kind this hands off.
pub fn block_in_place<R>(f: impl FnOnce() -> R) -> R {
    crate::runtime::Handle::current().block_in_place(f)
}
