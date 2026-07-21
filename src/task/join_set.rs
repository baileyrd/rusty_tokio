//! [`JoinSet`]: a dynamic collection of spawned tasks, joined as they
//! finish rather than in spawn order -- what a plain `Vec<JoinHandle<T>>`
//! (the way every test/example in this crate spawning a dynamic number
//! of tasks has to do it so far) can't give you.

use super::{JoinError, JoinHandle};
use crate::runtime::Handle;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A dynamic set of spawned tasks. Unlike holding a `Vec<JoinHandle<T>>`
/// yourself, [`join_next`](Self::join_next) resolves as soon as *any*
/// member finishes, not in spawn order, and dropping the set aborts
/// every task still in it (a bare `JoinHandle` does *not* abort on drop
/// -- this is a real behavioral difference, not just convenience).
pub struct JoinSet<T> {
    handles: Vec<JoinHandle<T>>,
}

impl<T> JoinSet<T> {
    pub fn new() -> Self {
        JoinSet {
            handles: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.handles.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    /// Spawns `future` onto the currently running runtime and adds it to
    /// this set.
    ///
    /// # Panics
    /// Panics if called from a thread with no ambient runtime -- same as
    /// [`crate::spawn`].
    pub fn spawn<F>(&mut self, future: F)
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.handles.push(crate::spawn(future));
    }

    /// Like [`spawn`](Self::spawn), but onto an explicit [`Handle`]
    /// rather than whatever runtime is ambient on the calling thread.
    pub fn spawn_on<F>(&mut self, future: F, handle: &Handle)
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.handles.push(handle.spawn(future));
    }

    /// Resolves once *any* task in the set finishes, returning its
    /// result -- `None` once the set is empty. Every remaining handle is
    /// polled (round-robin, refreshing each one's registered waker)
    /// every time this is polled; `O(n)` in the number of still-running
    /// tasks rather than the smarter shared-completion-queue approach
    /// tokio's own `JoinSet` uses internally, a deliberate simplicity
    /// trade-off -- correct, just not the most efficient possible for a
    /// set with many long-lived members.
    pub async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
        std::future::poll_fn(|cx| self.poll_join_next(cx)).await
    }

    fn poll_join_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<T, JoinError>>> {
        if self.handles.is_empty() {
            return Poll::Ready(None);
        }
        for i in 0..self.handles.len() {
            // `JoinHandle<T>` is `Unpin` (its only fields are an `Arc`
            // and an `Option<Weak<_>>`), so pinning a `&mut` into the
            // `Vec` needs no unsafe code.
            if let Poll::Ready(result) = Pin::new(&mut self.handles[i]).poll(cx) {
                // `swap_remove` (not `remove`): a `JoinSet` has no
                // ordering to preserve, and this is the one still
                // running whose completion order was never
                // deterministic anyway.
                self.handles.swap_remove(i);
                return Poll::Ready(Some(result));
            }
        }
        Poll::Pending
    }

    /// Requests cancellation of every task still in the set -- the same
    /// best-effort, asynchronous semantics as [`JoinHandle::abort`], not
    /// a guarantee they've actually stopped by the time this returns.
    pub fn abort_all(&self) {
        for handle in &self.handles {
            handle.abort();
        }
    }

    /// Aborts every remaining task, then waits for all of them to
    /// actually finish (reporting cancellation) before returning --
    /// unlike [`abort_all`](Self::abort_all) alone, this leaves the set
    /// empty.
    pub async fn shutdown(&mut self) {
        self.abort_all();
        while self.join_next().await.is_some() {}
    }
}

impl<T> Default for JoinSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for JoinSet<T> {
    fn drop(&mut self) {
        // Unlike a bare `JoinHandle` (which never aborts on drop, so its
        // task keeps running in the background), a `JoinSet` going out
        // of scope aborts every task still in it -- matching tokio's
        // own `JoinSet`, and generally the more useful default for "this
        // set of related tasks is no longer wanted."
        self.abort_all();
    }
}
