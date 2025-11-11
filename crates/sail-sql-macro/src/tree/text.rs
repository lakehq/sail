use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Field, Fields};

/// The trait to derive for tree text.
const TRAIT: &str = "TreeText";

struct TextFields {
    pattern: TokenStream,
    statements: TokenStream,
}

fn derive_fields_inner<'a>(
    fields: impl IntoIterator<Item = &'a Field>,
) -> (TokenStream, Vec<Ident>) {
    let mut initializer = quote! {};
    let mut variables = vec![];
    for (i, field) in fields.into_iter().enumerate() {
        let v = format_ident!("v{}", i);
        if let Some(name) = &field.ident {
            initializer.extend(quote! { #name: #v, });
        } else {
            initializer.extend(quote! { #v, });
        }
        variables.push(v);
    }
    (initializer, variables)
}

fn derive_fields(spanned: impl Spanned, fields: &Fields) -> syn::Result<TextFields> {
    let (pattern, variables) = match fields {
        Fields::Named(named) => {
            let (initializer, variables) = derive_fields_inner(&named.named);
            (quote! { { #initializer } }, variables)
        }
        Fields::Unnamed(unnamed) => {
            let (initializer, variables) = derive_fields_inner(&unnamed.unnamed);
            (quote! { ( #initializer ) }, variables)
        }
        Fields::Unit => {
            return Err(syn::Error::new(
                spanned.span(),
                format!("cannot derive `{TRAIT}` for unit fields"),
            ))
        }
    };
    Ok(TextFields {
        pattern,
        statements: quote! { #( result.push_str(&#variables.text()); )* },
    })
}

pub(crate) fn derive_tree_text(input: DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;

    let statements = match &input.data {
        Data::Enum(data) => {
            if data.variants.is_empty() {
                return Err(syn::Error::new(
                    data.variants.span(),
                    format!("cannot derive `{TRAIT}` for empty enums"),
                ));
            }
            let arms = data
                .variants
                .iter()
                .map(|variant| {
                    let variant_name = &variant.ident;
                    let TextFields {
                        pattern,
                        statements,
                    } = derive_fields(variant, &variant.fields)?;
                    Ok(quote! {
                        Self::#variant_name #pattern => {
                            #statements
                        }
                    })
                })
                .collect::<syn::Result<Vec<_>>>()?;
            quote! {
                match self {
                    #(#arms)*
                }
            }
        }
        Data::Struct(data) => {
            let TextFields {
                pattern,
                statements,
            } = derive_fields(&input, &data.fields)?;
            quote! {
                let Self #pattern = self;
                #statements
            }
        }
        _ => {
            return Err(syn::Error::new(
                input.span(),
                format!("`{TRAIT}` can only be derived for enums or structs"),
            ));
        }
    };

    Ok(quote! {
        impl crate::tree::TreeText for #name {
            fn text(&self) -> std::string::String {
                let mut result = std::string::String::new();
                #statements
                result
            }
        }
    })
}
