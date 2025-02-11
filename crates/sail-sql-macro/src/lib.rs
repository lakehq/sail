extern crate proc_macro;
extern crate proc_macro2;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

mod attribute;
mod tree;
pub(crate) mod utils;

/// Derives the `TreeParser` trait by generating a recursive descent parser for the type.
///
/// The type can be an enum with struct or tuple variants, or a struct with named or unnamed fields.
/// For enums, the variants are parsed as choices (or nested choices for enums with many variants).
/// For structs, the fields are parsed sequentially.
///
/// The parser cannot be derived for enums with unit variants, or structs with no fields.
/// The parser cannot be derived for types corresponding to a grammar with left recursion,
/// or a grammar requiring operator precedence handling.
/// In such cases, the `TreeParser` trait should be implemented manually.
/// `TreeParser` should also be implemented manually for terminals such as keywords, literals, and
/// operators.
///
/// The attribute `parser` can be used to control how the parsers are derived.
/// There are a few supported arguments for the attribute.
///
/// * `parser(dependency = "type")`
///
///     This can be specified at the top level for the enum or the struct,
///     where `type` is a single type `T` or a tuple type `(T1, T2, ...)`. Note that the dependency
///     needs to be specified as a string literal.
///     For a single type `T`, the derived `parser()` method will expect a parser for `T` as the
///     argument. For a tuple type `(T1, T2, ...)`, the derived `parser()` method will expect a
///     tuple of parsers for each type as the argument.
///
///     This argument is used to support recursive types, where the parser needs to first be
///     declared via `chumsky::recursive::Recursive::declare()`. `parser()` receives the declared
///     parser(s) and the returned parser can be used for `chumsky::recursive::Recursive::define()`.
///
///     By default, the `parser()` method will expect unit argument (`()`).
///
/// * `parser(label = expr)`
///
///     This can be specified at the top level for the enum or the struct.
///     The label is used to represent the class of tokens that the parser is expected to parse.
///     The label is used to provide better error messages when the parser fails.
///     When the label is not specified, the error message would show the list of expected tokens
///     for the invalid input.
///
/// * `parser(function = expr)`
///
///     This can be specified for individual fields (named or unnamed fields in
///     enum variants or structs), where `expr` is a function that takes the argument (one or a tuple
///     of declared parsers) and SQL parser options, and returns the parser for the field.
///
///     By default, the parser for the field is derived by calling the `parser()`
///     method of the field type with unit argument (`()`) and SQL parser options.
///     Such unit argument is accepted for terminal parsers or derived parsers without the
///     `parser(dependency = "...")` attribute.
///
/// The `parser` attribute is not allowed for at the enum variant level.
#[proc_macro_derive(TreeParser, attributes(parser))]
pub fn derive_tree_parser(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    tree::parser::derive_tree_parser(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
