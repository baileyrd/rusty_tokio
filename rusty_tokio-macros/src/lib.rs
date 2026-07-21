//! Proc-macro attributes for `rusty_tokio`: `#[rusty_tokio::main]` and
//! `#[rusty_tokio::test]`. Each rewrites an `async fn` into the plain
//! `fn` that builds a `Runtime` and blocks on the original body --
//! exactly the boilerplate every example and test in the main crate
//! used to spell out by hand (`let rt = Runtime::new().unwrap();
//! rt.block_on(async { .. });`).
//!
//! ## Why a separate crate
//!
//! `proc-macro = true` crates can export *only* proc-macros -- no plain
//! functions or types alongside them -- so this can't live inside
//! `rusty_tokio` itself; it has to be its own workspace member,
//! re-exported from the main crate (`pub use
//! rusty_tokio_macros::{main, test};`). This mirrors tokio's own
//! `tokio`/`tokio-macros` split exactly.
//!
//! This is also this crate's first `syn`/`quote`/`proc-macro2`
//! dependency. Hand-parsing an arbitrary `async fn`'s signature
//! (generics, attributes, argument list, return type) correctly without
//! them, just to avoid the dependency, would be a lot of fragile code
//! to save on three extremely widely used, well-audited crates that
//! essentially every proc-macro in the ecosystem already builds on --
//! not the same tradeoff as the main crate's "no mio, no tokio, no
//! crossbeam" posture, which is about not reimplementing this project's
//! actual subject matter (runtime internals), not about avoiding
//! standard proc-macro tooling.
//!
//! ## Scope
//!
//! - The annotated function must be `async`, take no arguments, and
//!   have no generic parameters -- the same restrictions `fn main`
//!   itself already has, applied to `#[test]` functions too for
//!   consistency.
//! - The only accepted argument is `worker_threads = N` (e.g.
//!   `#[rusty_tokio::main(worker_threads = 4)]`). Tokio's own attribute
//!   also accepts `flavor`/`start_paused`/etc., none of which apply here
//!   -- this crate has exactly one runtime flavor (multi-threaded; see
//!   issue #22) and no pausable clock (issue #56).

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

mod args;

use args::MacroArgs;

/// Rewrites `async fn main() -> T { body }` into `fn main() -> T` that
/// builds a `rusty_tokio::Runtime` and blocks on `body`. See the crate
/// docs for the full scope (no arguments, no generics, the optional
/// `worker_threads = N` argument).
#[proc_macro_attribute]
pub fn main(args: TokenStream, item: TokenStream) -> TokenStream {
    expand(args, item, false)
}

/// Like [`macro@main`], but also emits `#[test]` so the annotated
/// function is picked up by the ordinary test harness without writing
/// `#[test]` separately.
#[proc_macro_attribute]
pub fn test(args: TokenStream, item: TokenStream) -> TokenStream {
    expand(args, item, true)
}

fn expand(args: TokenStream, item: TokenStream, is_test: bool) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let macro_args = parse_macro_input!(args as MacroArgs);

    if input.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            input.sig.fn_token,
            "the `async` keyword is missing from the function declaration",
        )
        .to_compile_error()
        .into();
    }
    if !input.sig.inputs.is_empty() {
        return syn::Error::new_spanned(
            &input.sig.inputs,
            "the annotated function must not take any arguments",
        )
        .to_compile_error()
        .into();
    }
    if !input.sig.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &input.sig.generics,
            "the annotated function must not have generic parameters",
        )
        .to_compile_error()
        .into();
    }

    let attrs = &input.attrs;
    let vis = &input.vis;
    let ident = &input.sig.ident;
    let output = &input.sig.output;
    let block = &input.block;
    let rt_expr = macro_args.runtime_expr();

    let test_attr = if is_test {
        quote! { #[::core::prelude::v1::test] }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #test_attr
        #(#attrs)*
        #vis fn #ident() #output {
            #rt_expr.block_on(async move #block)
        }
    };

    expanded.into()
}
