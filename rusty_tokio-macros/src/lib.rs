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
//! ## Why no `syn`/`quote`/`proc-macro2`
//!
//! This crate used to depend on all three. It doesn't any more (issue
//! #268), and the reason the original argument for them didn't hold is
//! specific: hand-parsing an *arbitrary* `async fn` signature -- generics,
//! argument lists, where-clauses -- really would be fragile, but this
//! macro doesn't accept any of that. A function with generic parameters or
//! arguments is a hard error here (see "Scope" below), so the parser only
//! has to *detect* those cases, not parse them. What's left is a short walk
//! over a token list: attributes, optional visibility, `async fn`, a name,
//! an empty parameter list, an optional return type, and a body.
//!
//! The one thing that genuinely matters is span fidelity -- a compile
//! error inside the user's function body has to keep pointing at the
//! user's own source. That rules out rebuilding the output by
//! stringifying and re-parsing, so everything originating in the caller
//! (attributes, visibility, name, return type, body) is re-emitted as the
//! original [`TokenTree`]s, spans intact. Only the wrapper this macro
//! synthesizes -- the builder expression and the `.block_on(async move
//! ...)` call around the body -- is built from scratch, and it carries
//! [`Span::call_site`] because none of it came from the caller.
//!
//! These were compile-time-only dependencies that never shipped in a
//! consumer's binary, but they were also the highest-trust-cost entry in
//! the tree: a proc macro executes arbitrary code at compile time.
//!
//! ## Scope
//!
//! - The annotated function must be `async`, take no arguments, and
//!   have no generic parameters -- the same restrictions `fn main`
//!   itself already has, applied to `#[test]` functions too for
//!   consistency.
//! - The only accepted arguments are `worker_threads = N` (e.g.
//!   `#[rusty_tokio::main(worker_threads = 4)]`) and `flavor =
//!   "thread_per_core"`. Tokio's own attribute also accepts
//!   `start_paused`/etc., which don't apply here -- no pausable clock
//!   (issue #56).

use proc_macro::{Delimiter, Group, Ident, Literal, Punct, Spacing, Span, TokenStream, TokenTree};
use std::str::FromStr;

mod args;

use args::{MacroArgs, MacroError};

/// Rewrites `async fn main() -> T { body }` into `fn main() -> T` that
/// builds a `rusty_tokio::Runtime` and blocks on `body`. See the crate
/// docs for the full scope (no arguments, no generics, the optional
/// `worker_threads = N` / `flavor = "thread_per_core"` arguments).
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
    let macro_args = match MacroArgs::parse(args) {
        Ok(parsed) => parsed,
        Err(MacroError { span, message }) => return compile_error(span, &message),
    };
    let func = match ItemFn::parse(item) {
        Ok(parsed) => parsed,
        Err(MacroError { span, message }) => return compile_error(span, &message),
    };

    // `RT_EXPR.block_on(async move #body)` -- `body` is the caller's own
    // brace group, moved across untouched so its spans survive.
    let runtime_expr = TokenStream::from_str(&macro_args.runtime_expr())
        .expect("runtime expression is built from fixed source text");

    let mut call: Vec<TokenTree> = runtime_expr.into_iter().collect();
    call.push(TokenTree::Punct(Punct::new('.', Spacing::Alone)));
    call.push(TokenTree::Ident(Ident::new("block_on", Span::call_site())));
    call.push(TokenTree::Group(Group::new(
        Delimiter::Parenthesis,
        [
            TokenTree::Ident(Ident::new("async", Span::call_site())),
            TokenTree::Ident(Ident::new("move", Span::call_site())),
            TokenTree::Group(func.body),
        ]
        .into_iter()
        .collect(),
    )));

    let mut out: Vec<TokenTree> = Vec::new();
    if is_test {
        // `#[::core::prelude::v1::test]`
        out.push(TokenTree::Punct(Punct::new('#', Spacing::Alone)));
        out.push(TokenTree::Group(Group::new(
            Delimiter::Bracket,
            path_tokens(&["core", "prelude", "v1", "test"])
                .into_iter()
                .collect(),
        )));
    }
    out.extend(func.attrs);
    out.extend(func.vis);
    out.push(TokenTree::Ident(Ident::new("fn", Span::call_site())));
    out.push(TokenTree::Ident(func.name));
    out.push(TokenTree::Group(Group::new(
        Delimiter::Parenthesis,
        TokenStream::new(),
    )));
    out.extend(func.output);
    out.push(TokenTree::Group(Group::new(
        Delimiter::Brace,
        call.into_iter().collect(),
    )));

    out.into_iter().collect()
}

/// The pieces of the annotated function this macro actually needs. Every
/// field holds the caller's original tokens, spans included.
struct ItemFn {
    attrs: Vec<TokenTree>,
    vis: Vec<TokenTree>,
    name: Ident,
    /// The return type including its `->`, empty for `()`. A `where`
    /// clause, if somehow present, rides along here.
    output: Vec<TokenTree>,
    body: Group,
}

impl ItemFn {
    fn parse(item: TokenStream) -> Result<Self, MacroError> {
        let tokens: Vec<TokenTree> = item.into_iter().collect();
        let mut i = 0;

        // Attributes: `#` followed by a bracketed group, repeated.
        let mut attrs = Vec::new();
        while matches!(tokens.get(i), Some(TokenTree::Punct(p)) if p.as_char() == '#') {
            let bracket = match tokens.get(i + 1) {
                Some(tt @ TokenTree::Group(g)) if g.delimiter() == Delimiter::Bracket => tt.clone(),
                _ => break,
            };
            attrs.push(tokens[i].clone());
            attrs.push(bracket);
            i += 2;
        }

        // Visibility: `pub`, optionally followed by `(crate)`/`(in ...)`.
        let mut vis = Vec::new();
        if matches!(tokens.get(i), Some(TokenTree::Ident(id)) if id.to_string() == "pub") {
            vis.push(tokens[i].clone());
            i += 1;
            if let Some(tt @ TokenTree::Group(g)) = tokens.get(i) {
                if g.delimiter() == Delimiter::Parenthesis {
                    vis.push(tt.clone());
                    i += 1;
                }
            }
        }

        // `async`. Reported against `fn` when that's what's there instead,
        // which is where the missing keyword would have gone.
        match tokens.get(i) {
            Some(TokenTree::Ident(id)) if id.to_string() == "async" => i += 1,
            Some(other) => {
                return Err(MacroError {
                    span: other.span(),
                    message: "the `async` keyword is missing from the function declaration"
                        .to_string(),
                })
            }
            None => {
                return Err(MacroError {
                    span: Span::call_site(),
                    message: NOT_A_FN.to_string(),
                })
            }
        }

        match tokens.get(i) {
            Some(TokenTree::Ident(id)) if id.to_string() == "fn" => i += 1,
            Some(other) => {
                return Err(MacroError {
                    span: other.span(),
                    message: NOT_A_FN.to_string(),
                })
            }
            None => {
                return Err(MacroError {
                    span: Span::call_site(),
                    message: NOT_A_FN.to_string(),
                })
            }
        }

        let name = match tokens.get(i) {
            Some(TokenTree::Ident(id)) => id.clone(),
            Some(other) => {
                return Err(MacroError {
                    span: other.span(),
                    message: NOT_A_FN.to_string(),
                })
            }
            None => {
                return Err(MacroError {
                    span: Span::call_site(),
                    message: NOT_A_FN.to_string(),
                })
            }
        };
        i += 1;

        // Generics are rejected, not parsed -- that's what keeps this
        // parser short enough to hand-roll at all.
        if let Some(TokenTree::Punct(p)) = tokens.get(i) {
            if p.as_char() == '<' {
                return Err(MacroError {
                    span: p.span(),
                    message: "the annotated function must not have generic parameters".to_string(),
                });
            }
        }

        let params = match tokens.get(i) {
            Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Parenthesis => g.clone(),
            Some(other) => {
                return Err(MacroError {
                    span: other.span(),
                    message: NOT_A_FN.to_string(),
                })
            }
            None => {
                return Err(MacroError {
                    span: Span::call_site(),
                    message: NOT_A_FN.to_string(),
                })
            }
        };
        if !params.stream().is_empty() {
            return Err(MacroError {
                span: params.span(),
                message: "the annotated function must not take any arguments".to_string(),
            });
        }
        i += 1;

        // Everything from here to the final brace group is the return type
        // (and a where-clause, if one somehow appears without generics).
        let body_index = tokens
            .iter()
            .enumerate()
            .skip(i)
            .rev()
            .find_map(|(idx, tt)| match tt {
                TokenTree::Group(g) if g.delimiter() == Delimiter::Brace => Some(idx),
                _ => None,
            });
        let body_index = match body_index {
            Some(idx) => idx,
            None => {
                return Err(MacroError {
                    span: name.span(),
                    message: "the annotated function has no body".to_string(),
                })
            }
        };
        let output = tokens[i..body_index].to_vec();
        let body = match &tokens[body_index] {
            TokenTree::Group(g) => g.clone(),
            _ => unreachable!("body_index only ever points at a brace group"),
        };

        Ok(ItemFn {
            attrs,
            vis,
            name,
            output,
            body,
        })
    }
}

const NOT_A_FN: &str = "this attribute can only be applied to a function";

/// `::core::compile_error!{ "message" }`, with every token carrying `span`
/// so the diagnostic lands on the offending source rather than the macro.
fn compile_error(span: Span, message: &str) -> TokenStream {
    let mut tokens = path_tokens(&["core", "compile_error"]);
    let mut bang = Punct::new('!', Spacing::Alone);
    bang.set_span(span);
    tokens.push(TokenTree::Punct(bang));

    let mut literal = Literal::string(message);
    literal.set_span(span);
    let mut group = Group::new(
        Delimiter::Brace,
        [TokenTree::Literal(literal)].into_iter().collect(),
    );
    group.set_span(span);
    tokens.push(TokenTree::Group(group));

    for token in &mut tokens {
        token.set_span(span);
    }
    tokens.into_iter().collect()
}

/// A leading-`::` path, e.g. `::core::prelude::v1::test`.
fn path_tokens(segments: &[&str]) -> Vec<TokenTree> {
    let mut tokens: Vec<TokenTree> = Vec::new();
    for segment in segments {
        tokens.push(TokenTree::Punct(Punct::new(':', Spacing::Joint)));
        tokens.push(TokenTree::Punct(Punct::new(':', Spacing::Alone)));
        tokens.push(TokenTree::Ident(Ident::new(segment, Span::call_site())));
    }
    tokens
}
