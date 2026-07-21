//! `select!`: race two or more futures, running whichever one resolves
//! first and dropping (cancelling) the rest. This crate's first macro
//! -- see [`crate::Handle::shutdown_notified`]'s doc comment, which
//! flagged this as a real, repeated limitation before it existed.
//!
//! ## Scope, stated up front rather than discovered by surprise
//!
//! - **2 to 5 branches.** `macro_rules!` has no clean way to generate a
//!   fresh, uniquely-named local binding per repetition of a `$()*`
//!   pattern on stable Rust without either a `paste!`-style proc macro
//!   (a new dependency) or a recursive tt-muncher (fiddly to get right
//!   and harder to verify by inspection than explicit enumeration for a
//!   macro this central). Five explicit arities, each spelled out
//!   directly, covers the overwhelming majority of real `select!` uses
//!   and is mechanical to extend if a real need for more ever comes up.
//! - **Every branch's pattern must be irrefutable** (a plain binding
//!   like `result`, or `_` -- not `Some(x)` or `Ok(v)`). Real tokio's
//!   `select!` lets a branch's future resolve to a value that *doesn't*
//!   match its pattern, in which case that branch is treated as not
//!   ready yet and polling continues on the others -- supporting that
//!   needs meaningfully more machinery (re-arming just the one branch,
//!   not the whole `select!`) than a first pass needs to take on.
//! - **No `else` branch, no `,if <condition>` guards, no biased mode.**
//!   Branches are polled in the order written, every time the combined
//!   future is polled -- not tokio's own randomized order (meant to
//!   keep an always-ready earlier branch from permanently starving a
//!   later one under contention). A deliberate simplicity trade-off,
//!   not an oversight: worth knowing about if two branches are ever
//!   simultaneously and permanently ready in the same program.

/// Race two or more futures; run whichever one resolves first and drop
/// the rest. See this module's docs for exactly what's (and isn't)
/// supported.
///
/// ```
/// # use rusty_tokio::Runtime;
/// # let rt = Runtime::new().unwrap();
/// # rt.block_on(async {
/// let winner = rusty_tokio::select! {
///     a = async { 1 } => a,
///     b = async { 2 } => b,
/// };
/// assert!(winner == 1 || winner == 2);
/// # });
/// ```
#[macro_export]
macro_rules! select {
    ($p0:pat = $f0:expr => $b0:expr, $p1:pat = $f1:expr => $b1:expr $(,)?) => {{
        use ::std::future::Future as _;
        let mut __f0 = ::std::pin::pin!($f0);
        let mut __f1 = ::std::pin::pin!($f1);
        ::std::future::poll_fn(move |cx| {
            if let ::std::task::Poll::Ready(__out) = __f0.as_mut().poll(cx) {
                let $p0 = __out;
                return ::std::task::Poll::Ready($b0);
            }
            if let ::std::task::Poll::Ready(__out) = __f1.as_mut().poll(cx) {
                let $p1 = __out;
                return ::std::task::Poll::Ready($b1);
            }
            ::std::task::Poll::Pending
        })
        .await
    }};
    (
        $p0:pat = $f0:expr => $b0:expr,
        $p1:pat = $f1:expr => $b1:expr,
        $p2:pat = $f2:expr => $b2:expr $(,)?
    ) => {{
        use ::std::future::Future as _;
        let mut __f0 = ::std::pin::pin!($f0);
        let mut __f1 = ::std::pin::pin!($f1);
        let mut __f2 = ::std::pin::pin!($f2);
        ::std::future::poll_fn(move |cx| {
            if let ::std::task::Poll::Ready(__out) = __f0.as_mut().poll(cx) {
                let $p0 = __out;
                return ::std::task::Poll::Ready($b0);
            }
            if let ::std::task::Poll::Ready(__out) = __f1.as_mut().poll(cx) {
                let $p1 = __out;
                return ::std::task::Poll::Ready($b1);
            }
            if let ::std::task::Poll::Ready(__out) = __f2.as_mut().poll(cx) {
                let $p2 = __out;
                return ::std::task::Poll::Ready($b2);
            }
            ::std::task::Poll::Pending
        })
        .await
    }};
    (
        $p0:pat = $f0:expr => $b0:expr,
        $p1:pat = $f1:expr => $b1:expr,
        $p2:pat = $f2:expr => $b2:expr,
        $p3:pat = $f3:expr => $b3:expr $(,)?
    ) => {{
        use ::std::future::Future as _;
        let mut __f0 = ::std::pin::pin!($f0);
        let mut __f1 = ::std::pin::pin!($f1);
        let mut __f2 = ::std::pin::pin!($f2);
        let mut __f3 = ::std::pin::pin!($f3);
        ::std::future::poll_fn(move |cx| {
            if let ::std::task::Poll::Ready(__out) = __f0.as_mut().poll(cx) {
                let $p0 = __out;
                return ::std::task::Poll::Ready($b0);
            }
            if let ::std::task::Poll::Ready(__out) = __f1.as_mut().poll(cx) {
                let $p1 = __out;
                return ::std::task::Poll::Ready($b1);
            }
            if let ::std::task::Poll::Ready(__out) = __f2.as_mut().poll(cx) {
                let $p2 = __out;
                return ::std::task::Poll::Ready($b2);
            }
            if let ::std::task::Poll::Ready(__out) = __f3.as_mut().poll(cx) {
                let $p3 = __out;
                return ::std::task::Poll::Ready($b3);
            }
            ::std::task::Poll::Pending
        })
        .await
    }};
    (
        $p0:pat = $f0:expr => $b0:expr,
        $p1:pat = $f1:expr => $b1:expr,
        $p2:pat = $f2:expr => $b2:expr,
        $p3:pat = $f3:expr => $b3:expr,
        $p4:pat = $f4:expr => $b4:expr $(,)?
    ) => {{
        use ::std::future::Future as _;
        let mut __f0 = ::std::pin::pin!($f0);
        let mut __f1 = ::std::pin::pin!($f1);
        let mut __f2 = ::std::pin::pin!($f2);
        let mut __f3 = ::std::pin::pin!($f3);
        let mut __f4 = ::std::pin::pin!($f4);
        ::std::future::poll_fn(move |cx| {
            if let ::std::task::Poll::Ready(__out) = __f0.as_mut().poll(cx) {
                let $p0 = __out;
                return ::std::task::Poll::Ready($b0);
            }
            if let ::std::task::Poll::Ready(__out) = __f1.as_mut().poll(cx) {
                let $p1 = __out;
                return ::std::task::Poll::Ready($b1);
            }
            if let ::std::task::Poll::Ready(__out) = __f2.as_mut().poll(cx) {
                let $p2 = __out;
                return ::std::task::Poll::Ready($b2);
            }
            if let ::std::task::Poll::Ready(__out) = __f3.as_mut().poll(cx) {
                let $p3 = __out;
                return ::std::task::Poll::Ready($b3);
            }
            if let ::std::task::Poll::Ready(__out) = __f4.as_mut().poll(cx) {
                let $p4 = __out;
                return ::std::task::Poll::Ready($b4);
            }
            ::std::task::Poll::Pending
        })
        .await
    }};
}

/// `join!`/`try_join!`: run two or more futures concurrently *within the
/// calling task* (no extra [`spawn`](crate::spawn), no extra `Task`) and
/// resolve once every one of them has, returning a tuple of their
/// outputs.
///
/// Shares [`select!`]'s "poll N sub-futures every wake, sharing one
/// `poll_fn`" shape and the same **2 to 5 branches** scope limit and the
/// same reasoning for it (see that macro's docs) -- the difference is
/// `join!` waits for *all* branches instead of stopping at the first.
/// Each branch is polled at most once per already-resolved -> not
/// re-polled after it completes, so a branch that finishes early doesn't
/// get spuriously polled again while waiting on its slower siblings.
///
/// `try_join!` is the `Result`-aware sibling: every branch must resolve
/// to a `Result` with the *same* error type (no `From`/`?`-style
/// conversion between differing error types -- keeping every branch's
/// error type identical sidesteps an inference question a first pass
/// doesn't need to take on), and it short-circuits on the first `Err`,
/// returning immediately rather than waiting for the remaining branches
/// to finish pointlessly. As with `select!`, "short-circuits" means the
/// still-pending branches' futures are dropped as soon as the
/// macro-generated future resolves (when their local `pin!` bindings go
/// out of scope), not actively cancelled mid-poll.
///
/// ```
/// # use rusty_tokio::Runtime;
/// # let rt = Runtime::new().unwrap();
/// # rt.block_on(async {
/// let (a, b) = rusty_tokio::join!(async { 1 }, async { 2 });
/// assert_eq!((a, b), (1, 2));
///
/// let ok: Result<(i32, i32), &str> =
///     rusty_tokio::try_join!(async { Ok(1) }, async { Ok(2) });
/// assert_eq!(ok, Ok((1, 2)));
///
/// let err: Result<(i32, i32), &str> =
///     rusty_tokio::try_join!(async { Ok(1) }, async { Err("boom") });
/// assert_eq!(err, Err("boom"));
/// # });
/// ```
#[macro_export]
macro_rules! join {
    ($f0:expr, $f1:expr $(,)?) => {{
        use ::std::future::Future as _;
        let mut __f0 = ::std::pin::pin!($f0);
        let mut __f1 = ::std::pin::pin!($f1);
        let mut __o0 = ::std::option::Option::None;
        let mut __o1 = ::std::option::Option::None;
        ::std::future::poll_fn(move |cx| {
            if __o0.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f0.as_mut().poll(cx) {
                    __o0 = ::std::option::Option::Some(__out);
                }
            }
            if __o1.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f1.as_mut().poll(cx) {
                    __o1 = ::std::option::Option::Some(__out);
                }
            }
            if __o0.is_some() && __o1.is_some() {
                ::std::task::Poll::Ready((__o0.take().unwrap(), __o1.take().unwrap()))
            } else {
                ::std::task::Poll::Pending
            }
        })
        .await
    }};
    ($f0:expr, $f1:expr, $f2:expr $(,)?) => {{
        use ::std::future::Future as _;
        let mut __f0 = ::std::pin::pin!($f0);
        let mut __f1 = ::std::pin::pin!($f1);
        let mut __f2 = ::std::pin::pin!($f2);
        let mut __o0 = ::std::option::Option::None;
        let mut __o1 = ::std::option::Option::None;
        let mut __o2 = ::std::option::Option::None;
        ::std::future::poll_fn(move |cx| {
            if __o0.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f0.as_mut().poll(cx) {
                    __o0 = ::std::option::Option::Some(__out);
                }
            }
            if __o1.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f1.as_mut().poll(cx) {
                    __o1 = ::std::option::Option::Some(__out);
                }
            }
            if __o2.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f2.as_mut().poll(cx) {
                    __o2 = ::std::option::Option::Some(__out);
                }
            }
            if __o0.is_some() && __o1.is_some() && __o2.is_some() {
                ::std::task::Poll::Ready((
                    __o0.take().unwrap(),
                    __o1.take().unwrap(),
                    __o2.take().unwrap(),
                ))
            } else {
                ::std::task::Poll::Pending
            }
        })
        .await
    }};
    ($f0:expr, $f1:expr, $f2:expr, $f3:expr $(,)?) => {{
        use ::std::future::Future as _;
        let mut __f0 = ::std::pin::pin!($f0);
        let mut __f1 = ::std::pin::pin!($f1);
        let mut __f2 = ::std::pin::pin!($f2);
        let mut __f3 = ::std::pin::pin!($f3);
        let mut __o0 = ::std::option::Option::None;
        let mut __o1 = ::std::option::Option::None;
        let mut __o2 = ::std::option::Option::None;
        let mut __o3 = ::std::option::Option::None;
        ::std::future::poll_fn(move |cx| {
            if __o0.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f0.as_mut().poll(cx) {
                    __o0 = ::std::option::Option::Some(__out);
                }
            }
            if __o1.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f1.as_mut().poll(cx) {
                    __o1 = ::std::option::Option::Some(__out);
                }
            }
            if __o2.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f2.as_mut().poll(cx) {
                    __o2 = ::std::option::Option::Some(__out);
                }
            }
            if __o3.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f3.as_mut().poll(cx) {
                    __o3 = ::std::option::Option::Some(__out);
                }
            }
            if __o0.is_some() && __o1.is_some() && __o2.is_some() && __o3.is_some() {
                ::std::task::Poll::Ready((
                    __o0.take().unwrap(),
                    __o1.take().unwrap(),
                    __o2.take().unwrap(),
                    __o3.take().unwrap(),
                ))
            } else {
                ::std::task::Poll::Pending
            }
        })
        .await
    }};
    ($f0:expr, $f1:expr, $f2:expr, $f3:expr, $f4:expr $(,)?) => {{
        use ::std::future::Future as _;
        let mut __f0 = ::std::pin::pin!($f0);
        let mut __f1 = ::std::pin::pin!($f1);
        let mut __f2 = ::std::pin::pin!($f2);
        let mut __f3 = ::std::pin::pin!($f3);
        let mut __f4 = ::std::pin::pin!($f4);
        let mut __o0 = ::std::option::Option::None;
        let mut __o1 = ::std::option::Option::None;
        let mut __o2 = ::std::option::Option::None;
        let mut __o3 = ::std::option::Option::None;
        let mut __o4 = ::std::option::Option::None;
        ::std::future::poll_fn(move |cx| {
            if __o0.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f0.as_mut().poll(cx) {
                    __o0 = ::std::option::Option::Some(__out);
                }
            }
            if __o1.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f1.as_mut().poll(cx) {
                    __o1 = ::std::option::Option::Some(__out);
                }
            }
            if __o2.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f2.as_mut().poll(cx) {
                    __o2 = ::std::option::Option::Some(__out);
                }
            }
            if __o3.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f3.as_mut().poll(cx) {
                    __o3 = ::std::option::Option::Some(__out);
                }
            }
            if __o4.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f4.as_mut().poll(cx) {
                    __o4 = ::std::option::Option::Some(__out);
                }
            }
            if __o0.is_some()
                && __o1.is_some()
                && __o2.is_some()
                && __o3.is_some()
                && __o4.is_some()
            {
                ::std::task::Poll::Ready((
                    __o0.take().unwrap(),
                    __o1.take().unwrap(),
                    __o2.take().unwrap(),
                    __o3.take().unwrap(),
                    __o4.take().unwrap(),
                ))
            } else {
                ::std::task::Poll::Pending
            }
        })
        .await
    }};
}

/// See [`join!`]'s docs -- this is its `Result`-aware, short-circuiting
/// sibling.
#[macro_export]
macro_rules! try_join {
    ($f0:expr, $f1:expr $(,)?) => {{
        use ::std::future::Future as _;
        let mut __f0 = ::std::pin::pin!($f0);
        let mut __f1 = ::std::pin::pin!($f1);
        let mut __o0 = ::std::option::Option::None;
        let mut __o1 = ::std::option::Option::None;
        ::std::future::poll_fn(move |cx| {
            if __o0.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f0.as_mut().poll(cx) {
                    match __out {
                        ::std::result::Result::Ok(__v) => __o0 = ::std::option::Option::Some(__v),
                        ::std::result::Result::Err(__e) => {
                            return ::std::task::Poll::Ready(::std::result::Result::Err(__e))
                        }
                    }
                }
            }
            if __o1.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f1.as_mut().poll(cx) {
                    match __out {
                        ::std::result::Result::Ok(__v) => __o1 = ::std::option::Option::Some(__v),
                        ::std::result::Result::Err(__e) => {
                            return ::std::task::Poll::Ready(::std::result::Result::Err(__e))
                        }
                    }
                }
            }
            if __o0.is_some() && __o1.is_some() {
                ::std::task::Poll::Ready(::std::result::Result::Ok((
                    __o0.take().unwrap(),
                    __o1.take().unwrap(),
                )))
            } else {
                ::std::task::Poll::Pending
            }
        })
        .await
    }};
    ($f0:expr, $f1:expr, $f2:expr $(,)?) => {{
        use ::std::future::Future as _;
        let mut __f0 = ::std::pin::pin!($f0);
        let mut __f1 = ::std::pin::pin!($f1);
        let mut __f2 = ::std::pin::pin!($f2);
        let mut __o0 = ::std::option::Option::None;
        let mut __o1 = ::std::option::Option::None;
        let mut __o2 = ::std::option::Option::None;
        ::std::future::poll_fn(move |cx| {
            if __o0.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f0.as_mut().poll(cx) {
                    match __out {
                        ::std::result::Result::Ok(__v) => __o0 = ::std::option::Option::Some(__v),
                        ::std::result::Result::Err(__e) => {
                            return ::std::task::Poll::Ready(::std::result::Result::Err(__e))
                        }
                    }
                }
            }
            if __o1.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f1.as_mut().poll(cx) {
                    match __out {
                        ::std::result::Result::Ok(__v) => __o1 = ::std::option::Option::Some(__v),
                        ::std::result::Result::Err(__e) => {
                            return ::std::task::Poll::Ready(::std::result::Result::Err(__e))
                        }
                    }
                }
            }
            if __o2.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f2.as_mut().poll(cx) {
                    match __out {
                        ::std::result::Result::Ok(__v) => __o2 = ::std::option::Option::Some(__v),
                        ::std::result::Result::Err(__e) => {
                            return ::std::task::Poll::Ready(::std::result::Result::Err(__e))
                        }
                    }
                }
            }
            if __o0.is_some() && __o1.is_some() && __o2.is_some() {
                ::std::task::Poll::Ready(::std::result::Result::Ok((
                    __o0.take().unwrap(),
                    __o1.take().unwrap(),
                    __o2.take().unwrap(),
                )))
            } else {
                ::std::task::Poll::Pending
            }
        })
        .await
    }};
    ($f0:expr, $f1:expr, $f2:expr, $f3:expr $(,)?) => {{
        use ::std::future::Future as _;
        let mut __f0 = ::std::pin::pin!($f0);
        let mut __f1 = ::std::pin::pin!($f1);
        let mut __f2 = ::std::pin::pin!($f2);
        let mut __f3 = ::std::pin::pin!($f3);
        let mut __o0 = ::std::option::Option::None;
        let mut __o1 = ::std::option::Option::None;
        let mut __o2 = ::std::option::Option::None;
        let mut __o3 = ::std::option::Option::None;
        ::std::future::poll_fn(move |cx| {
            if __o0.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f0.as_mut().poll(cx) {
                    match __out {
                        ::std::result::Result::Ok(__v) => __o0 = ::std::option::Option::Some(__v),
                        ::std::result::Result::Err(__e) => {
                            return ::std::task::Poll::Ready(::std::result::Result::Err(__e))
                        }
                    }
                }
            }
            if __o1.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f1.as_mut().poll(cx) {
                    match __out {
                        ::std::result::Result::Ok(__v) => __o1 = ::std::option::Option::Some(__v),
                        ::std::result::Result::Err(__e) => {
                            return ::std::task::Poll::Ready(::std::result::Result::Err(__e))
                        }
                    }
                }
            }
            if __o2.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f2.as_mut().poll(cx) {
                    match __out {
                        ::std::result::Result::Ok(__v) => __o2 = ::std::option::Option::Some(__v),
                        ::std::result::Result::Err(__e) => {
                            return ::std::task::Poll::Ready(::std::result::Result::Err(__e))
                        }
                    }
                }
            }
            if __o3.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f3.as_mut().poll(cx) {
                    match __out {
                        ::std::result::Result::Ok(__v) => __o3 = ::std::option::Option::Some(__v),
                        ::std::result::Result::Err(__e) => {
                            return ::std::task::Poll::Ready(::std::result::Result::Err(__e))
                        }
                    }
                }
            }
            if __o0.is_some() && __o1.is_some() && __o2.is_some() && __o3.is_some() {
                ::std::task::Poll::Ready(::std::result::Result::Ok((
                    __o0.take().unwrap(),
                    __o1.take().unwrap(),
                    __o2.take().unwrap(),
                    __o3.take().unwrap(),
                )))
            } else {
                ::std::task::Poll::Pending
            }
        })
        .await
    }};
    ($f0:expr, $f1:expr, $f2:expr, $f3:expr, $f4:expr $(,)?) => {{
        use ::std::future::Future as _;
        let mut __f0 = ::std::pin::pin!($f0);
        let mut __f1 = ::std::pin::pin!($f1);
        let mut __f2 = ::std::pin::pin!($f2);
        let mut __f3 = ::std::pin::pin!($f3);
        let mut __f4 = ::std::pin::pin!($f4);
        let mut __o0 = ::std::option::Option::None;
        let mut __o1 = ::std::option::Option::None;
        let mut __o2 = ::std::option::Option::None;
        let mut __o3 = ::std::option::Option::None;
        let mut __o4 = ::std::option::Option::None;
        ::std::future::poll_fn(move |cx| {
            if __o0.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f0.as_mut().poll(cx) {
                    match __out {
                        ::std::result::Result::Ok(__v) => __o0 = ::std::option::Option::Some(__v),
                        ::std::result::Result::Err(__e) => {
                            return ::std::task::Poll::Ready(::std::result::Result::Err(__e))
                        }
                    }
                }
            }
            if __o1.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f1.as_mut().poll(cx) {
                    match __out {
                        ::std::result::Result::Ok(__v) => __o1 = ::std::option::Option::Some(__v),
                        ::std::result::Result::Err(__e) => {
                            return ::std::task::Poll::Ready(::std::result::Result::Err(__e))
                        }
                    }
                }
            }
            if __o2.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f2.as_mut().poll(cx) {
                    match __out {
                        ::std::result::Result::Ok(__v) => __o2 = ::std::option::Option::Some(__v),
                        ::std::result::Result::Err(__e) => {
                            return ::std::task::Poll::Ready(::std::result::Result::Err(__e))
                        }
                    }
                }
            }
            if __o3.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f3.as_mut().poll(cx) {
                    match __out {
                        ::std::result::Result::Ok(__v) => __o3 = ::std::option::Option::Some(__v),
                        ::std::result::Result::Err(__e) => {
                            return ::std::task::Poll::Ready(::std::result::Result::Err(__e))
                        }
                    }
                }
            }
            if __o4.is_none() {
                if let ::std::task::Poll::Ready(__out) = __f4.as_mut().poll(cx) {
                    match __out {
                        ::std::result::Result::Ok(__v) => __o4 = ::std::option::Option::Some(__v),
                        ::std::result::Result::Err(__e) => {
                            return ::std::task::Poll::Ready(::std::result::Result::Err(__e))
                        }
                    }
                }
            }
            if __o0.is_some()
                && __o1.is_some()
                && __o2.is_some()
                && __o3.is_some()
                && __o4.is_some()
            {
                ::std::task::Poll::Ready(::std::result::Result::Ok((
                    __o0.take().unwrap(),
                    __o1.take().unwrap(),
                    __o2.take().unwrap(),
                    __o3.take().unwrap(),
                    __o4.take().unwrap(),
                )))
            } else {
                ::std::task::Poll::Pending
            }
        })
        .await
    }};
}
