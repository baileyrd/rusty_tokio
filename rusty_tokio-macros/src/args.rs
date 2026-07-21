//! Parses the optional argument list on `#[rusty_tokio::main(...)]` /
//! `#[rusty_tokio::test(...)]` -- currently just `worker_threads = N`,
//! the only builder option this crate's `Runtime` actually exposes that
//! makes sense to set this way. More of tokio's real options (`flavor`,
//! `start_paused`, ...) don't apply -- this crate has only the one
//! (multi-threaded) runtime flavor, and no pausable clock yet (issue
//! #56).

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{Expr, ExprLit, Lit, Meta, Token};

pub(crate) struct MacroArgs {
    worker_threads: Option<usize>,
}

impl Parse for MacroArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let metas = Punctuated::<Meta, Token![,]>::parse_terminated(input)?;
        let mut worker_threads = None;

        for meta in metas {
            match meta {
                Meta::NameValue(nv) if nv.path.is_ident("worker_threads") => {
                    worker_threads = Some(literal_usize(&nv.value)?);
                }
                other => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "unsupported argument -- only `worker_threads = N` is supported",
                    ));
                }
            }
        }

        Ok(MacroArgs { worker_threads })
    }
}

fn literal_usize(expr: &Expr) -> syn::Result<usize> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Int(int), ..
        }) => int.base10_parse::<usize>(),
        other => Err(syn::Error::new_spanned(
            other,
            "expected an integer literal",
        )),
    }
}

impl MacroArgs {
    /// The `rusty_tokio::Runtime` construction expression to block on
    /// the annotated function's body with.
    pub(crate) fn runtime_expr(&self) -> TokenStream {
        match self.worker_threads {
            Some(n) => quote! {
                ::rusty_tokio::Runtime::builder().worker_threads(#n).build().unwrap()
            },
            None => quote! {
                ::rusty_tokio::Runtime::new().unwrap()
            },
        }
    }
}
