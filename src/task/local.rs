//! [`LocalSet`]: a place to spawn `!Send` futures -- ones holding an
//! `Rc`, a `RefCell`-guarded value, or any other non-thread-safe handle
//! -- which [`crate::spawn`]/[`crate::runtime::Handle::spawn`] can never
//! accept, since every task spawned there is an `Arc<Task>` any worker
//! thread may poll.
//!
//! ## How a `!Send` future gets a thread-safe `Waker` anyway
//!
//! A [`std::task::Waker`] must still be `Send + Sync` even for a local
//! task -- the I/O reactor and timer driver background threads wake a
//! local task's registered interest from *their* thread, exactly like
//! any other task's. [`LocalTask`] is reference-counted via `Arc` (not
//! `Rc`) specifically so that clone/drop of the handle -- and calling
//! `wake()` -- is safe from any thread. Only the boxed future itself
//! (behind a plain, non-atomic [`RefCell`]) is genuinely `!Send`, and it
//! is *never* actually touched except from [`LocalTask::run`], which is
//! only ever invoked from [`LocalSet::run_until`]'s own loop --
//! [`LocalSet`] asserts (`bind_or_check_thread`) that every
//! `spawn_local`/`run_until` call on it happens on the one thread that
//! first used it. See the `unsafe impl Send + Sync for LocalTask` below
//! for the exact safety argument -- the same "documented exclusivity
//! invariant instead of relying on the type system alone" shape as
//! `sync::Mutex`/`sync::RwLock`'s own `unsafe impl`s.
//!
//! ## Scope
//!
//! - A `LocalSet`'s tasks are driven only while [`LocalSet::run_until`]
//!   is actually executing, on whichever thread calls it -- there is no
//!   "run this `LocalSet` as a spawned future pinned to one worker of an
//!   existing multi-threaded [`crate::Runtime`]" integration (what real
//!   tokio's `LocalSet: Future` impl lets you do via `rt.spawn(local)` or
//!   similar). Pair `LocalSet::run_until` with
//!   `Builder::new_current_thread` if a single OS thread end-to-end is
//!   the goal; nesting it inside a multi-threaded runtime's task works
//!   too (the same "a synchronous call blocks whatever thread invokes
//!   it" caveat any nested `block_on` already has), just without the
//!   scheduler-level pinning tokio offers.
//! - No graceful-shutdown/task-count draining -- a `LocalSet` going out
//!   of scope simply drops whatever's left in its queue (and thus their
//!   still-boxed futures) without delivering a `JoinError` to any
//!   `JoinHandle` still awaiting them, which then just hangs forever if
//!   polled again. This matches (not: improves on) how this crate's
//!   multi-threaded `Runtime` already behaves for tasks abandoned by a
//!   hard shutdown -- not a new gap introduced here.

use super::id::{self, CurrentTask};
use super::join::{JoinHandle, JoinInner, Outcome};
use super::state::{State, StateSnapshot};
use super::TaskId;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, ThreadId};
use std::time::Duration;

type LocalBoxFuture = Pin<Box<dyn Future<Output = ()>>>;
type LocalAbnormalHook = Box<dyn FnOnce(Outcome)>;

pub(super) struct LocalTask {
    id: TaskId,
    state: State,
    future: RefCell<Option<LocalBoxFuture>>,
    scheduler: Weak<LocalShared>,
    /// Same "fires exactly once on abnormal completion" role as
    /// `Task::abnormal_hook` -- see that field's docs. `RefCell`, not
    /// `Mutex`: nothing here needs cross-thread synchronization on its
    /// own, since (like `future` above) it's only ever touched from
    /// `LocalTask::run`.
    abnormal_hook: RefCell<Option<LocalAbnormalHook>>,
}

// SAFETY: `future` and `abnormal_hook` are only ever touched from
// `LocalTask::run`, which is only ever called from `LocalSet::
// run_until`'s own loop -- and `LocalSet` asserts, via
// `bind_or_check_thread`, that every `spawn_local`/`run_until` call
// happens on the single thread that first used it. So even though this
// `Arc` -- and the cross-thread-capable `Waker` built from it, needed so
// the reactor/timer background threads can wake a local task's I/O or
// timer interest just like any other task's -- can be freely cloned,
// sent to, and woken from any thread, the `!Send`/`!Sync` future (and
// hook) it wraps are in practice never actually accessed except on that
// one thread.
unsafe impl Send for LocalTask {}
unsafe impl Sync for LocalTask {}

impl LocalTask {
    fn schedule(self: &Arc<Self>) {
        if let Some(shared) = self.scheduler.upgrade() {
            shared.schedule(self.clone());
        }
    }

    fn fire_abnormal_hook(&self, outcome: Outcome) {
        if let Some(hook) = self.abnormal_hook.borrow_mut().take() {
            hook(outcome);
        }
    }

    /// Mirrors `Task::run` exactly (see that method's docs for the
    /// state-machine reasoning) -- `Mutex::lock().unwrap()` swapped for
    /// `RefCell::borrow_mut()`, and no `mark_finished()` call, since a
    /// `LocalSet` doesn't track an active-task count the way `Runtime`
    /// does (nothing here needs to wait for local tasks to drain).
    fn run(self: Arc<Self>) {
        if !self.state.begin_poll() {
            self.future.borrow_mut().take();
            self.fire_abnormal_hook(Outcome::Aborted);
            return;
        }

        if self.state.is_aborted() {
            self.future.borrow_mut().take();
            self.state.end_poll(true);
            self.fire_abnormal_hook(Outcome::Aborted);
            return;
        }

        let mut slot = self.future.borrow_mut();
        let Some(mut future) = slot.take() else {
            drop(slot);
            self.state.end_poll(true);
            self.fire_abnormal_hook(Outcome::Aborted);
            return;
        };
        drop(slot);

        let waker = Waker::from(self.clone());
        let mut cx = Context::from_waker(&waker);

        let poll_result = {
            let _id_guard = id::enter(CurrentTask {
                id: self.id,
                // `LocalSet`'s spawn path has no name-setting builder
                // of its own (only `task::Builder`, for the
                // multi-threaded scheduler), so this is always `None`.
                name: None,
            });
            panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut cx)))
        };

        match poll_result {
            Ok(Poll::Ready(())) => {
                self.state.end_poll(true);
                self.abnormal_hook.borrow_mut().take();
            }
            Ok(Poll::Pending) => {
                *self.future.borrow_mut() = Some(future);
                if self.state.end_poll(false) {
                    self.schedule();
                }
            }
            Err(payload) => {
                *self.future.borrow_mut() = None;
                self.state.end_poll(true);
                self.fire_abnormal_hook(Outcome::Panicked(payload));
            }
        }
    }

    pub(super) fn abort(self: &Arc<Self>) {
        if self.state.request_abort() {
            self.schedule();
        }
    }
}

impl Wake for LocalTask {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.state.wake() == StateSnapshot::ShouldSchedule {
            self.schedule();
        }
    }
}

/// A `LocalSet`'s own tiny scheduling core -- deliberately separate from
/// `runtime::Shared` rather than reused, since that type is tightly
/// coupled to the multi-threaded scheduler's worker-index-based local
/// queues, injector, and reactor/timer/shutdown plumbing; a `LocalSet`
/// needs none of that, just one queue and a park/wake signal.
struct LocalShared {
    queue: Mutex<VecDeque<Arc<LocalTask>>>,
    park_lock: Mutex<()>,
    park_condvar: Condvar,
}

impl LocalShared {
    fn new() -> Self {
        LocalShared {
            queue: Mutex::new(VecDeque::new()),
            park_lock: Mutex::new(()),
            park_condvar: Condvar::new(),
        }
    }

    fn schedule(&self, task: Arc<LocalTask>) {
        self.queue.lock().unwrap().push_back(task);
        self.park_condvar.notify_all();
    }

    fn next_task(&self) -> Option<Arc<LocalTask>> {
        self.queue.lock().unwrap().pop_front()
    }

    /// Same bounded, not-precisely-woken park `runtime::Shared::park`
    /// uses -- see that method's docs.
    fn park(&self) {
        let guard = self.park_lock.lock().unwrap();
        let _ = self
            .park_condvar
            .wait_timeout(guard, Duration::from_millis(50));
    }

    fn wake_all_parked(&self) {
        self.park_condvar.notify_all();
    }
}

struct ParkingWaker {
    shared: Arc<LocalShared>,
}

impl Wake for ParkingWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.shared.wake_all_parked();
    }
}

fn spawn<F>(shared: &Arc<LocalShared>, future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    let id = TaskId::next();
    let join_inner = Arc::new(JoinInner::new());
    let handle = JoinHandle::new(join_inner.clone(), id);

    let hook_inner = join_inner.clone();
    let hook: LocalAbnormalHook = Box::new(move |outcome| hook_inner.finish_abnormal(outcome));

    let wrapped: LocalBoxFuture = Box::pin(async move {
        let output = future.await;
        join_inner.complete(output);
    });

    let task = Arc::new(LocalTask {
        id,
        state: State::new(),
        future: RefCell::new(Some(wrapped)),
        scheduler: Arc::downgrade(shared),
        abnormal_hook: RefCell::new(Some(hook)),
    });

    shared.schedule(task.clone());
    handle.with_local_task(task)
}

/// Mirrors `runtime::current_thread::block_on`'s loop exactly (poll,
/// drain every locally-runnable task, park) -- see that function's docs
/// for the reasoning, all of which applies unchanged here.
fn drive<F: Future>(shared: &Arc<LocalShared>, future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let waker = Waker::from(Arc::new(ParkingWaker {
        shared: shared.clone(),
    }));
    let mut cx = Context::from_waker(&waker);

    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }

        let mut ran_any = false;
        while let Some(task) = shared.next_task() {
            task.run();
            ran_any = true;
        }
        if ran_any {
            continue;
        }

        shared.park();
    }
}

thread_local! {
    static CURRENT_LOCAL: RefCell<Option<Weak<LocalShared>>> = const { RefCell::new(None) };
}

#[must_use]
struct EnterGuard {
    previous: Option<Weak<LocalShared>>,
}

fn enter(shared: Arc<LocalShared>) -> EnterGuard {
    let previous = CURRENT_LOCAL.with(|c| c.borrow_mut().replace(Arc::downgrade(&shared)));
    EnterGuard { previous }
}

impl Drop for EnterGuard {
    fn drop(&mut self) {
        CURRENT_LOCAL.with(|c| *c.borrow_mut() = self.previous.take());
    }
}

/// Spawn a `!Send` future onto whichever [`LocalSet`] is currently
/// driving the calling thread via [`LocalSet::run_until`]. Prefer
/// [`LocalSet::spawn_local`] when the `LocalSet` itself is at hand; this
/// free function exists for code deep inside a `run_until`-driven future
/// that doesn't have (and shouldn't need to thread through) a direct
/// reference to it -- mirrors `crate::spawn` vs. `Handle::spawn`.
///
/// # Panics
/// Panics if called from a thread that isn't currently inside a
/// `LocalSet::run_until` call.
pub fn spawn_local<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    let shared = CURRENT_LOCAL
        .with(|c| c.borrow().as_ref().and_then(Weak::upgrade))
        .expect(
            "`task::spawn_local` called outside of a `LocalSet::run_until` call -- \
             use `LocalSet::spawn_local` if you don't have one running on this thread",
        );
    spawn(&shared, future)
}

/// A place to spawn `!Send` futures. See the module docs for the full
/// picture -- in short: [`LocalSet::spawn_local`]/[`spawn_local`] queue
/// work, and nothing runs until [`LocalSet::run_until`] actually drives
/// it, on whichever thread calls `run_until`.
///
/// A `LocalSet` binds itself to the first thread that calls
/// `spawn_local` or `run_until` on it (not at [`LocalSet::new`] time),
/// so it's fine to construct one on a thread that isn't the one
/// eventually going to use it -- just not to use it concurrently from
/// two different threads, which panics.
pub struct LocalSet {
    shared: Arc<LocalShared>,
    owner_thread: Cell<Option<ThreadId>>,
}

impl LocalSet {
    pub fn new() -> Self {
        LocalSet {
            shared: Arc::new(LocalShared::new()),
            owner_thread: Cell::new(None),
        }
    }

    fn bind_or_check_thread(&self) {
        let current = thread::current().id();
        match self.owner_thread.get() {
            Some(owner) => assert_eq!(
                owner, current,
                "a LocalSet may only be used (spawn_local/run_until) from the \
                 single thread that first used it"
            ),
            None => self.owner_thread.set(Some(current)),
        }
    }

    /// Spawn a `!Send` future onto this set. It doesn't run until a
    /// [`LocalSet::run_until`] call on this same set actually drives it.
    ///
    /// # Panics
    /// Panics if called from a different thread than whichever one
    /// first called `spawn_local`/`run_until` on this set.
    pub fn spawn_local<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        self.bind_or_check_thread();
        spawn(&self.shared, future)
    }

    /// Drive every task in this set, interleaved with polls of `future`,
    /// until `future` resolves -- synchronously, on the calling thread,
    /// the same "blocks until done" contract as `Runtime::block_on`.
    /// Tasks still queued when `future` resolves are left in the set;
    /// call `run_until` again (with another future, even just `async {}`
    /// if nothing else needs driving right now) to keep making progress
    /// on them.
    ///
    /// # Panics
    /// Panics if called from a different thread than whichever one
    /// first called `spawn_local`/`run_until` on this set.
    pub fn run_until<F: Future>(&self, future: F) -> F::Output {
        self.bind_or_check_thread();
        let _guard = enter(self.shared.clone());
        drive(&self.shared, future)
    }
}

impl Default for LocalSet {
    fn default() -> Self {
        Self::new()
    }
}
