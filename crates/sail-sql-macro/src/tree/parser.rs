use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::parse::Parse;
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Expr, Field, Fields, Ident, Type, Variant};

use crate::attribute::AttributeExtractor;
use crate::utils::parse_string_value;

/// The trait to derive for tree parsers.
const TRAIT: &str = "TreeParser";
/// The attribute name used when deriving the tree parser trait.
const ATTRIBUTE: &str = "parser";

/// Argument names used when deriving the tree parser trait.
struct AttributeArgument;

impl AttributeArgument {
    const DEPENDENCY: &'static str = "dependency";
    const LABEL: &'static str = "label";
    const FUNCTION: &'static str = "function";
}

/// The maximum number of choices in a flat list of parser choices.
/// If there are more choices, they will be grouped into nested choices.
/// `chumsky` allows at most 26 choices for `choice` so this number
/// must be less than that.
const MAX_CHOICES: usize = 20;

/// The minimum number of variants in an enum to use a boxed parser.
/// The boxed parser can reduce compile times for large enum parsers.
const BOXED_ENUM_MIN_VARIANTS: usize = 5;

enum ParserDependency {
    None,
    One(Type),
    Tuple(Vec<Type>),
}

impl ParserDependency {
    fn extract(e: Option<Expr>) -> syn::Result<Self> {
        e.map(|value| {
            let t = parse_string_value(&value, Type::parse)?;
            match t {
                Type::Path(ref path) if path.qself.is_none() => Ok(Self::One(t)),
                Type::Tuple(tuple) => Ok(Self::Tuple(tuple.elems.into_iter().collect())),
                _ => Err(syn::Error::new(
                    t.span(),
                    format!(
                        "`{}` must be a single type or a tuple type",
                        AttributeArgument::DEPENDENCY
                    ),
                )),
            }
        })
        .unwrap_or_else(|| Ok(Self::None))
    }
}

struct ParseFields {
    parser: TokenStream,
    args: TokenStream,
    initializer: TokenStream,
}

fn derive_fields_inner<'a>(
    spanned: impl Spanned,
    fields: impl IntoIterator<Item = &'a Field>,
) -> syn::Result<ParseFields> {
    fields
        .into_iter()
        .enumerate()
        .try_fold(None, |acc, (i, field)| -> syn::Result<_> {
            let field_function = {
                let mut extractor = AttributeExtractor::try_new(ATTRIBUTE, &field.attrs)?;
                let f = extractor.extract_argument_value(AttributeArgument::FUNCTION, Ok)?;
                extractor.expect_empty()?;
                f
            };
            let field_arg = field
                .ident
                .to_owned()
                .unwrap_or_else(|| format_ident!("v{}", i));
            let field_type = &field.ty;
            let field_parser = if let Some(function) = field_function {
                quote! { { let f = #function; f(args.clone(), options) } }
            } else {
                quote! { <#field_type>::parser((), options) }
            };
            match acc {
                Some(ParseFields {
                    parser,
                    args,
                    initializer,
                }) => Ok(Some(ParseFields {
                    parser: quote! { #parser.then(#field_parser) },
                    args: quote! { (#args, #field_arg) },
                    initializer: quote! { #initializer, #field_arg },
                })),
                None => Ok(Some(ParseFields {
                    parser: field_parser,
                    args: quote! { #field_arg },
                    initializer: quote! { #field_arg },
                })),
            }
        })?
        .ok_or_else(|| {
            syn::Error::new(
                spanned.span(),
                format!("cannot derive `{TRAIT}` for no fields"),
            )
        })
}

fn derive_fields(
    name: TokenStream,
    spanned: impl Spanned,
    fields: &Fields,
) -> syn::Result<TokenStream> {
    match fields {
        Fields::Named(fields) => {
            let ParseFields {
                parser,
                args,
                initializer,
            } = derive_fields_inner(spanned, &fields.named)?;
            // Use boxed parser for structs or enum variants with named fields.
            Ok(quote! {
                #parser.map(|#args| #name { #initializer }).boxed()
            })
        }
        Fields::Unnamed(fields) => {
            let ParseFields {
                parser,
                args,
                initializer,
            } = derive_fields_inner(spanned, &fields.unnamed)?;
            Ok(quote! {
                #parser.map(|#args| #name ( #initializer ))
            })
        }
        Fields::Unit => Err(syn::Error::new(
            spanned.span(),
            format!("cannot derive `{TRAIT}` for unit fields"),
        )),
    }
}

fn derive_enum_variant(enum_name: &Ident, variant: &Variant) -> syn::Result<TokenStream> {
    AttributeExtractor::try_new(ATTRIBUTE, &variant.attrs)?.expect_empty()?;
    let variant_name = &variant.ident;
    let name = quote! { #enum_name::#variant_name };
    derive_fields(name, variant, &variant.fields)
}

fn derive_struct(struct_name: &Ident, fields: &Fields) -> syn::Result<TokenStream> {
    derive_fields(quote! { #struct_name }, fields, fields)
}

fn derive_choices(choices: Vec<TokenStream>) -> TokenStream {
    let choices = if choices.len() <= MAX_CHOICES {
        choices
    } else {
        let chunk_size = choices.len().div_ceil(MAX_CHOICES);
        choices
            .chunks(chunk_size)
            .map(|chunk| derive_choices(chunk.to_vec()))
            .collect()
    };
    if choices.len() > 1 {
        quote! { chumsky::prelude::choice((#(#choices),*)) }
    } else {
        quote! { #(#choices),* }
    }
}

pub(crate) fn derive_tree_parser(input: DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;

    let parser = match &input.data {
        Data::Enum(data) => {
            if data.variants.is_empty() {
                return Err(syn::Error::new(
                    data.variants.span(),
                    format!("cannot derive `{TRAIT}` for empty enums"),
                ));
            }
            let choices = data
                .variants
                .iter()
                .map(|variant| derive_enum_variant(name, variant))
                .collect::<syn::Result<Vec<_>>>()?;
            let n = choices.len();
            let parser = derive_choices(choices);
            if n < BOXED_ENUM_MIN_VARIANTS {
                quote! { #parser }
            } else {
                quote! { #parser.boxed() }
            }
        }
        Data::Struct(data) => derive_struct(name, &data.fields)?,
        _ => {
            return Err(syn::Error::new(
                input.span(),
                format!("`{TRAIT}` can only be derived for enums or structs"),
            ))
        }
    };

    let (dependency, label) = {
        let mut extractor = AttributeExtractor::try_new(ATTRIBUTE, &input.attrs)?;
        let dependency = extractor
            .extract_argument_value(AttributeArgument::DEPENDENCY, ParserDependency::extract)?;
        let label = extractor.extract_argument_value(AttributeArgument::LABEL, Ok)?;
        extractor.expect_empty()?;
        (dependency, label)
    };
    let parser = match label {
        Some(label) => quote! { #parser.labelled(#label) },
        None => parser,
    };
    let (generics, args_type, args_bounds) = match dependency {
        ParserDependency::One(t) => (
            quote! { I, E, P },
            quote! { P },
            quote! { P: chumsky::Parser<'a, I, #t, E> + Clone + 'a },
        ),
        ParserDependency::Tuple(t) => {
            let params = (0..t.len())
                .map(|i| format_ident!("P{}", i + 1))
                .collect::<Vec<_>>();
            let bounds = t
                .iter()
                .zip(params.iter())
                .map(|(t, p)| {
                    quote! { #p: chumsky::Parser<'a, I, #t, E> + Clone + 'a }
                })
                .collect::<Vec<_>>();
            (
                quote! { I, E, #(#params),* },
                quote! { (#(#params),*,) },
                quote! { #(#bounds),* },
            )
        }
        ParserDependency::None => (quote! { I, E }, quote! { () }, quote! {}),
    };

    let trait_name = format_ident!("{TRAIT}");

    Ok(quote! {
        impl <'a, #generics> crate::tree::#trait_name <'a, I, E, #args_type> for #name
        where
            I: chumsky::input::Input<'a, Token = crate::token::Token<'a>>
                + chumsky::input::ValueInput<'a>,
            I::Span: std::convert::Into<crate::span::TokenSpan> + Clone,
            E: chumsky::extra::ParserExtra<'a, I>,
            E::Error: chumsky::label::LabelError<'a, I, crate::token::TokenLabel>,
            #args_bounds
        {
            fn parser(
                args: #args_type,
                options: &'a crate::options::ParserOptions
            ) -> impl chumsky::Parser<'a, I, Self, E> + Clone {
                use chumsky::Parser;

                #parser
            }
        }
    })
}
