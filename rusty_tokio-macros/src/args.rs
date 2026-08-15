//! Parses the optional argument list on `#[rusty_tokio::main(...)]` /
//! `#[rusty_tokio::test(...)]` -- `worker_threads = N`, and (behind the
//! `thread-per-core` feature) `flavor = "thread_per_core"` to select
//! [`rusty_tokio::Builder::new_thread_per_core`] instead of the default
//! multi-threaded builder. More of tokio's real options (`start_paused`,
//! ...) don't apply -- no pausable clock yet (issue #56), and there's no
//! `current_thread` macro flavor since nothing here builds a `LocalSet`
//! for it (`Builder::build_local`/`LocalRuntime` have to be called
//! directly for that).
//!
//! The grammar is small enough to walk by hand: a comma-separated list of
//! `ident = literal`, with exactly two accepted names. See the crate docs
//! for why this doesn't use `syn` (issue #268).

use proc_macro::{Span, TokenStream, TokenTree};

/// One parse failure -- argument list or function signature alike: the
/// message, and the span to point it at.
pub(crate) struct MacroError {
    pub(crate) span: Span,
    pub(crate) message: String,
}

#[derive(Default)]
pub(crate) struct MacroArgs {
    worker_threads: Option<usize>,
    flavor: Option<String>,
}

impl MacroArgs {
    /// Walks `ident = literal` pairs separated by `,`, tolerating a
    /// trailing comma.
    pub(crate) fn parse(input: TokenStream) -> Result<Self, MacroError> {
        let tokens: Vec<TokenTree> = input.into_iter().collect();
        let mut args = MacroArgs::default();
        let mut i = 0;

        while i < tokens.len() {
            let name = match &tokens[i] {
                TokenTree::Ident(ident) => ident.to_string(),
                other => {
                    return Err(MacroError {
                        span: other.span(),
                        message: UNSUPPORTED_ARG.to_string(),
                    })
                }
            };
            let name_span = tokens[i].span();
            i += 1;

            match tokens.get(i) {
                Some(TokenTree::Punct(p)) if p.as_char() == '=' => i += 1,
                Some(other) => {
                    return Err(MacroError {
                        span: other.span(),
                        message: UNSUPPORTED_ARG.to_string(),
                    })
                }
                None => {
                    return Err(MacroError {
                        span: name_span,
                        message: UNSUPPORTED_ARG.to_string(),
                    })
                }
            }

            let value = match tokens.get(i) {
                Some(TokenTree::Literal(lit)) => lit.clone(),
                Some(other) => {
                    return Err(MacroError {
                        span: other.span(),
                        message: match name.as_str() {
                            "worker_threads" => "expected an integer literal".to_string(),
                            "flavor" => "expected a string literal".to_string(),
                            _ => UNSUPPORTED_ARG.to_string(),
                        },
                    })
                }
                None => {
                    return Err(MacroError {
                        span: name_span,
                        message: UNSUPPORTED_ARG.to_string(),
                    })
                }
            };
            let value_span = value.span();
            let value_text = value.to_string();
            i += 1;

            match name.as_str() {
                "worker_threads" => {
                    args.worker_threads =
                        Some(parse_usize(&value_text).ok_or_else(|| MacroError {
                            span: value_span,
                            message: "expected an integer literal".to_string(),
                        })?);
                }
                "flavor" => {
                    let text = parse_str(&value_text).ok_or_else(|| MacroError {
                        span: value_span,
                        message: "expected a string literal".to_string(),
                    })?;
                    if text != "thread_per_core" {
                        return Err(MacroError {
                            span: value_span,
                            message: "unsupported `flavor` -- only \"thread_per_core\" is \
                                      supported (the default, multi-threaded flavor needs no \
                                      `flavor` argument at all)"
                                .to_string(),
                        });
                    }
                    args.flavor = Some(text);
                }
                _ => {
                    return Err(MacroError {
                        span: name_span,
                        message: UNSUPPORTED_ARG.to_string(),
                    })
                }
            }

            match tokens.get(i) {
                Some(TokenTree::Punct(p)) if p.as_char() == ',' => i += 1,
                None => break,
                Some(other) => {
                    return Err(MacroError {
                        span: other.span(),
                        message: UNSUPPORTED_ARG.to_string(),
                    })
                }
            }
        }

        Ok(args)
    }

    /// The `rusty_tokio::Runtime` construction expression to block on the
    /// annotated function's body with. Returned as source text rather than
    /// tokens: it contains nothing from the caller, so it needs no spans of
    /// its own, and a `String` keeps this unit-testable (a
    /// `proc_macro::TokenStream` can't be built outside the compiler).
    pub(crate) fn runtime_expr(&self) -> String {
        let builder = match self.flavor.as_deref() {
            Some("thread_per_core") => "::rusty_tokio::Builder::new_thread_per_core()",
            _ => "::rusty_tokio::Builder::new()",
        };
        match self.worker_threads {
            Some(n) => format!("{builder}.worker_threads({n}usize).build().unwrap()"),
            None => format!("{builder}.build().unwrap()"),
        }
    }
}

const UNSUPPORTED_ARG: &str = "unsupported argument -- only `worker_threads = N` and \
                               `flavor = \"thread_per_core\"` are supported";

/// `Literal::to_string` keeps the original spelling, so an integer can
/// arrive with underscores and/or a type suffix (`4`, `4usize`, `1_000`).
fn parse_usize(text: &str) -> Option<usize> {
    let text = text.replace('_', "");
    let digits = text
        .find(|c: char| !c.is_ascii_digit())
        .map_or(text.as_str(), |end| &text[..end]);
    if digits.is_empty() {
        return None;
    }
    // Anything after the digits is only acceptable as an integer type
    // suffix -- notably not a float's `.` or an `e` exponent.
    let suffix = &text[digits.len()..];
    const INT_SUFFIXES: &[&str] = &[
        "", "usize", "isize", "u8", "u16", "u32", "u64", "u128", "i8", "i16", "i32", "i64", "i128",
    ];
    if !INT_SUFFIXES.contains(&suffix) {
        return None;
    }
    digits.parse().ok()
}

/// Same idea for a string literal: strip the quotes, rejecting anything
/// that isn't a plain (non-raw, non-byte) string.
fn parse_str(text: &str) -> Option<String> {
    let inner = text.strip_prefix('"')?.strip_suffix('"')?;
    // No escape handling: every value this accepts is a bare identifier-ish
    // word, and an escape would only ever appear in one that's rejected
    // anyway.
    if inner.contains('\\') {
        return None;
    }
    Some(inner.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(worker_threads: Option<usize>, flavor: Option<&str>) -> MacroArgs {
        MacroArgs {
            worker_threads,
            flavor: flavor.map(str::to_string),
        }
    }

    #[test]
    fn default_builder_with_no_arguments() {
        assert_eq!(
            args(None, None).runtime_expr(),
            "::rusty_tokio::Builder::new().build().unwrap()"
        );
    }

    #[test]
    fn worker_threads_is_emitted_with_an_explicit_usize_suffix() {
        assert_eq!(
            args(Some(4), None).runtime_expr(),
            "::rusty_tokio::Builder::new().worker_threads(4usize).build().unwrap()"
        );
    }

    #[test]
    fn thread_per_core_flavor_selects_the_other_builder() {
        assert_eq!(
            args(None, Some("thread_per_core")).runtime_expr(),
            "::rusty_tokio::Builder::new_thread_per_core().build().unwrap()"
        );
    }

    #[test]
    fn flavor_and_worker_threads_combine() {
        assert_eq!(
            args(Some(2), Some("thread_per_core")).runtime_expr(),
            "::rusty_tokio::Builder::new_thread_per_core().worker_threads(2usize).build().unwrap()"
        );
    }

    #[test]
    fn integer_literals_accept_suffixes_and_underscores() {
        assert_eq!(parse_usize("4"), Some(4));
        assert_eq!(parse_usize("4usize"), Some(4));
        assert_eq!(parse_usize("1_000"), Some(1000));
        assert_eq!(parse_usize("0"), Some(0));
    }

    #[test]
    fn non_integer_literals_are_rejected() {
        assert_eq!(parse_usize("\"4\""), None);
        assert_eq!(parse_usize("4.5"), None);
        assert_eq!(parse_usize("1e3"), None);
        assert_eq!(parse_usize(""), None);
        assert_eq!(parse_usize("usize"), None);
    }

    #[test]
    fn string_literals_are_unquoted_and_odd_forms_rejected() {
        assert_eq!(
            parse_str("\"thread_per_core\"").as_deref(),
            Some("thread_per_core")
        );
        assert_eq!(parse_str("4"), None);
        assert_eq!(parse_str("r\"raw\""), None);
        assert_eq!(parse_str("\"has\\nescape\""), None);
    }
}
