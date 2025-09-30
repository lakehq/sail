use syn::parse::Parser;
use syn::spanned::Spanned;
use syn::{Expr, ExprLit, Lit, LitStr};

/// Try to get a string literal from an expression.
/// The string literal cannot have a suffix.
pub fn try_get_literal_string(e: &Expr) -> syn::Result<&LitStr> {
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
    Ok(lit)
}

/// Parse a string literal expression using the given parser.
pub fn parse_string_value<F: Parser>(e: &Expr, parser: F) -> syn::Result<F::Output> {
    let lit = try_get_literal_string(e)?;
    lit.parse_with(parser)
}
