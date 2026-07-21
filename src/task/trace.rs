//! Optional `tracing` instrumentation, feature-gated behind the
//! `tracing` Cargo feature (off by default -- see that feature's
//! Cargo.toml comment for why it's opt-in). When enabled, every spawned
//! task gets a [`tracing::Span`] shaped exactly the way real (unstable,
//! `tokio_unstable`-gated) tokio's own instrumentation shapes it, so the
//! real `console-subscriber`/`tokio-console` tool -- built against that
//! wire format, not this crate specifically -- works against this
//! runtime too, with zero changes on its end. When the feature is off,
//! every function in this module is a no-op that doesn't even touch its
//! arguments, so there's zero runtime cost and no `tracing` dependency
//! pulled in at all.
//!
//! **What matches real tokio's shape, and why it matters.** Verified
//! against `console-subscriber`'s actual source, not guessed: a task is
//! registered the moment it sees a span whose *name* is `"runtime.spawn"`
//! (the target string isn't checked for this decision), and it reads
//! `kind`, `task.name`, `task.id`, `loc.file`, `loc.line`, `loc.col` off
//! that span's fields for display. Poll count and busy/idle time come
//! for free from ordinary span enter/exit -- `console-subscriber`'s
//! `Layer` hooks `on_enter`/`on_exit` the same way any
//! `tracing_subscriber::Layer` would, so wrapping the spawned future in
//! [`tracing::Instrument::instrument`] (a standard, non-console-specific
//! part of the `tracing` crate -- this module adds no bespoke enter/exit
//! plumbing of its own) is all that's needed for that half.
//!
//! **What's deliberately not here.** Waker clone/drop/wake
//! instrumentation (target `"tokio::task::waker"`, driving
//! `console-subscriber`'s "self-wake" stat) and resource/async-op
//! instrumentation (for visualizing `sync::Mutex`/`Semaphore`/etc.
//! contention in the console UI) are both real parts of tokio's full
//! console support, but `console-subscriber`'s own task-registration
//! logic treats both as secondary -- a task registers and shows up
//! (name, ID, spawn location, poll count, busy/idle time, the actual
//! point of the tool) without either. Adding waker tracking would mean
//! replacing this crate's current `std::task::Wake`-based waker (a
//! `std`-provided blanket vtable, not something this crate can hook into
//! directly) with a hand-rolled `RawWaker`/`RawWakerVTable` purely for
//! instrumentation, and resource tracking would mean a parallel
//! span-per-primitive scheme threaded across every `sync` type -- both
//! genuinely separate, much larger pieces of work, not attempted here.
//!
//! [`crate::runtime::Handle::spawn_blocking`]'s blocking-pool closure
//! gets its own span (target `tokio::task::blocking`, matching real
//! tokio's split between a regular task's span and a blocking task's),
//! entered for the closure's actual execution on its blocking-pool
//! thread -- in *addition* to the ordinary task span the rendezvous
//! wrapper task spawned alongside it gets (see that method's own docs
//! for why `spawn_blocking` here is built from two separate pieces of
//! work rather than one). Both will show up as independent entries in
//! the console; there's no first-class "this task's real work happened
//! over there" link between them beyond sharing the same spawn location.

#[cfg(feature = "tracing")]
mod imp {
    use tracing::instrument::{Instrument, Instrumented};

    #[track_caller]
    pub(crate) fn spawn_span(
        kind: &'static str,
        name: Option<&str>,
        task_id: u64,
    ) -> tracing::Span {
        let location = std::panic::Location::caller();
        tracing::trace_span!(
            target: "tokio::task",
            parent: None,
            "runtime.spawn",
            %kind,
            task.name = %name.unwrap_or_default(),
            task.id = task_id,
            loc.file = location.file(),
            loc.line = location.line(),
            loc.col = location.column(),
        )
    }

    #[track_caller]
    pub(crate) fn blocking_span(name: Option<&str>, task_id: u64) -> tracing::Span {
        let location = std::panic::Location::caller();
        tracing::trace_span!(
            target: "tokio::task::blocking",
            parent: None,
            "runtime.spawn",
            kind = %"blocking",
            task.name = %name.unwrap_or_default(),
            task.id = task_id,
            loc.file = location.file(),
            loc.line = location.line(),
            loc.col = location.column(),
        )
    }

    pub(crate) fn instrument<F: std::future::Future>(
        future: F,
        span: tracing::Span,
    ) -> Instrumented<F> {
        future.instrument(span)
    }

    /// Enters `span` for as long as the returned guard lives -- used
    /// around the actual synchronous execution of a blocking-pool
    /// closure, which (unlike a spawned future) is called once rather
    /// than polled repeatedly, so there's no `Future` to `.instrument()`.
    pub(crate) fn enter(span: &tracing::Span) -> tracing::span::Entered<'_> {
        span.enter()
    }
}

#[cfg(not(feature = "tracing"))]
mod imp {
    pub(crate) fn spawn_span(_kind: &'static str, _name: Option<&str>, _task_id: u64) {}

    pub(crate) fn blocking_span(_name: Option<&str>, _task_id: u64) {}

    pub(crate) fn instrument<F: std::future::Future>(future: F, _span: ()) -> F {
        future
    }

    pub(crate) fn enter(_span: &()) {}
}

pub(crate) use imp::*;
