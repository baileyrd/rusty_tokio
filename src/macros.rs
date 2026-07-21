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
