//! The task system: a heap-allocated, reference-counted future plus a
//! small lock-free state machine that decides, on every wake, whether
//! the task needs to be (re-)enqueued on the scheduler or is already
//! spoken for.
//!
//! Naively you could push `Arc<Task>` onto a channel every time a waker
//! fires and have workers pop-lock-poll-unlock. That's what most "build
//! your own executor" blog posts do, and it has a real bug under actual
//! multi-threaded concurrency: if a wake happens *while* a task is being
//! polled on another thread, the future is temporarily absent from its
//! slot (taken out for polling), so the duplicate wakeup finds nothing
//! to do and is silently dropped -- a lost wakeup, which means a task
//! can go to sleep forever even though something tried to wake it.
//!
//! The fix is the same one tokio and `async-task` use: an explicit
//! `RUNNING` / `NOTIFIED` pair of state bits. A wake that arrives while
//! the task is running doesn't try to touch the future at all -- it
//! just leaves a note (`NOTIFIED`) for the poller to see when it's done,
//! and the poller reschedules itself if that note is present.

mod builder;
mod id;
mod join;
mod join_set;
mod local;
mod local_key;
mod state;
mod yield_now;

pub use builder::Builder;
pub use id::{try_id, try_name, TaskId};
pub use join::{JoinError, JoinHandle};
pub use join_set::JoinSet;
pub use local::{spawn_local, LocalSet};
pub use local_key::{AccessError, LocalKey, TaskLocalFuture};
pub use yield_now::{yield_now, YieldNow};

use crate::runtime::Shared;
use join::{AbnormalHook, JoinInner, Outcome};
use state::{State, StateSnapshot};
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// A scheduled unit of work. Type-erased to `Future<Output = ()>` --
/// the actual output value (if any) is threaded out separately through
/// a [`JoinInner`], so `Task` itself never needs to be generic.
pub(crate) struct Task {
    id: TaskId,
    name: Option<Arc<str>>,
    state: State,
    future: Mutex<Option<BoxFuture>>,
    scheduler: Weak<Shared>,
    /// Fires exactly once if the task ends abnormally (aborted before
    /// completion, or panicked) so the `JoinHandle` doesn't hang
    /// forever waiting for a completion that will never come through
    /// the future's own body.
    abnormal_hook: Mutex<Option<AbnormalHook>>,
}

impl Task {
    fn schedule(self: &Arc<Self>) {
        if let Some(shared) = self.scheduler.upgrade() {
            shared.schedule(self.clone());
        }
    }

    /// Called by a worker immediately after popping this task off a run
    /// queue. Runs at most one poll, handling completion, panics, and
    /// abort requests, and re-schedules itself if it was woken again
    /// while it was running.
    pub(crate) fn run(self: Arc<Self>) {
        if !self.state.begin_poll() {
            // Someone aborted us before we ever got polled; the future
            // (if still present) is dropped without being polled again.
            self.future.lock().unwrap().take();
            self.fire_abnormal_hook(Outcome::Aborted);
            self.mark_finished();
            return;
        }

        if self.state.is_aborted() {
            self.future.lock().unwrap().take();
            self.state.end_poll(true);
            self.fire_abnormal_hook(Outcome::Aborted);
            self.mark_finished();
            return;
        }

        let mut slot = self.future.lock().unwrap();
        let Some(mut future) = slot.take() else {
            // Nothing left to poll (defensive -- shouldn't happen given
            // the state machine's invariants, but a stray extra wakeup
            // must never panic a worker thread, and the joiner must
            // never be left hanging because of it).
            drop(slot);
            self.state.end_poll(true);
            self.fire_abnormal_hook(Outcome::Aborted);
            self.mark_finished();
            return;
        };
        drop(slot);

        let waker = std::task::Waker::from(self.clone());
        let mut cx = std::task::Context::from_waker(&waker);

        let poll_result = {
            // Scoped tightly around just this poll call -- see
            // `id::EnterGuard`'s docs for why that's the only span
            // "the task currently running on this thread" is
            // well-defined for.
            let _id_guard = id::enter(id::CurrentTask {
                id: self.id,
                name: self.name.clone(),
            });
            crate::coop::budget(|| {
                panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut cx)))
            })
        };

        match poll_result {
            Ok(std::task::Poll::Ready(())) => {
                self.state.end_poll(true);
                // The future's own body already notified its JoinInner
                // on the normal-completion path; drop the hook so it
                // doesn't hold that Arc alive for no reason.
                self.abnormal_hook.lock().unwrap().take();
                self.mark_finished();
            }
            Ok(std::task::Poll::Pending) => {
                *self.future.lock().unwrap() = Some(future);
                if self.state.end_poll(false) {
                    // Woken again while we were polling: go around once
                    // more instead of waiting for an external wake.
                    self.schedule();
                }
            }
            Err(payload) => {
                *self.future.lock().unwrap() = None;
                self.state.end_poll(true);
                self.fire_abnormal_hook(Outcome::Panicked(payload));
                self.mark_finished();
            }
        }
    }

    /// Decrements the scheduler's live-task count -- see
    /// [`Shared::wait_for_tasks_drain`] and issue #12's graceful
    /// shutdown, the only consumer of this count. Called from every
    /// terminal path in `run` (normal completion, panic, or abort),
    /// never on `Pending`, since a `Pending` task is still alive and
    /// expected to be polled again.
    fn mark_finished(&self) {
        if let Some(shared) = self.scheduler.upgrade() {
            shared.task_finished();
        }
    }

    fn fire_abnormal_hook(&self, outcome: Outcome) {
        if let Some(hook) = self.abnormal_hook.lock().unwrap().take() {
            hook(outcome);
        }
    }

    pub(crate) fn abort(self: &Arc<Self>) {
        if self.state.request_abort() {
            self.schedule();
        }
    }
}

impl std::task::Wake for Task {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.state.wake() == StateSnapshot::ShouldSchedule {
            self.schedule();
        }
    }
}

/// Spawn `future` onto `shared`'s scheduler, returning a handle that can
/// be awaited for its output (or used to abort it).
pub(crate) fn spawn<F>(shared: &Arc<Shared>, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    spawn_named(shared, future, None)
}

/// Like [`spawn`], but carrying an optional name -- the
/// [`builder::Builder`] spawn path this backs.
pub(crate) fn spawn_named<F>(
    shared: &Arc<Shared>,
    future: F,
    name: Option<Arc<str>>,
) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let id = TaskId::next();
    let join_inner = Arc::new(JoinInner::new());
    let handle = JoinHandle::new(join_inner.clone(), id);

    let hook_inner = join_inner.clone();
    let hook: AbnormalHook = Box::new(move |outcome| hook_inner.finish_abnormal(outcome));

    let wrapped: BoxFuture = Box::pin(async move {
        let output = future.await;
        join_inner.complete(output);
    });

    let task = Arc::new(Task {
        id,
        name,
        state: State::new(),
        future: Mutex::new(Some(wrapped)),
        scheduler: Arc::downgrade(shared),
        abnormal_hook: Mutex::new(Some(hook)),
    });

    shared.task_spawned();
    shared.schedule(task.clone());
    handle.with_task(task)
}
