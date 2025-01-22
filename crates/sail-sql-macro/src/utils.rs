use syn::parse::Parser;
use syn::spanned::Spanned;
use syn::{Expr, ExprLit, Lit};

pub fn parse_string_value<F: Parser>(e: &Expr, parser: F) -> syn::Result<F::Output> {
    let Expr::Lit(ExprLit {
        lit: Lit::Str(lit), ..
    }) = e
    else {
        return Err(syn::Error::new(
            e.span(),
            "the value must be a string literal",
        ));
    };
    if !lit.suffix().is_empty() {
        return Err(syn::Error::new(
            lit.span(),
            "the value cannot have a suffix",
        ));
    }
    lit.parse_with(parser)
}
