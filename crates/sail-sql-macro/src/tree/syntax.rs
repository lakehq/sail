use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Fields};

use crate::attribute::AttributeExtractor;
use crate::utils::try_get_literal_string;

/// The trait to derive for tree syntax.
const TRAIT: &str = "TreeSyntax";
/// The attribute name used when deriving the tree syntax trait.
const ATTRIBUTE: &str = "syntax";

/// Argument names used when deriving the tree syntax trait.
struct AttributeArgument;

impl AttributeArgument {
    const NAME: &'static str = "name";
}

struct SyntaxFields {
    node: TokenStream,
    children: Vec<TokenStream>,
}

fn derive_fields(spanned: impl Spanned, fields: &Fields) -> syn::Result<SyntaxFields> {
    let fields = match fields {
        Fields::Named(named) => &named.named,
        Fields::Unnamed(unnamed) => &unnamed.unnamed,
        Fields::Unit => {
            return Err(syn::Error::new(
                spanned.span(),
                format!("cannot derive `{TRAIT}` for unit fields"),
            ))
        }
    };
    let field_types: Vec<_> = fields
        .iter()
        .map(|f| {
            AttributeExtractor::try_new(ATTRIBUTE, &f.attrs)?.expect_empty()?;
            Ok(&f.ty)
        })
        .collect::<syn::Result<Vec<_>>>()?;
    if field_types.is_empty() {
        return Err(syn::Error::new(
            spanned.span(),
            format!("cannot derive `{TRAIT}` for no fields"),
        ));
    }
    let nodes: Vec<_> = field_types
        .iter()
        .map(|ty| quote! { crate::tree::SyntaxNode::NonTerminal(std::any::TypeId::of::<#ty>()) })
        .collect();
    let node = quote! { crate::tree::SyntaxNode::Sequence(vec![ #(#nodes),* ]) };
    let children: Vec<_> = field_types
        .iter()
        .map(|ty| quote! { (std::any::TypeId::of::<#ty>(), Box::new(<#ty>::syntax)) })
        .collect();
    Ok(SyntaxFields { node, children })
}

pub(crate) fn derive_tree_syntax(input: DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;

    let mut extractor = AttributeExtractor::try_new(ATTRIBUTE, &input.attrs)?;
    let syntax_name = extractor
        .extract_argument_value(AttributeArgument::NAME, Ok)?
        .map(|e| try_get_literal_string(&e).map(|x| x.value()))
        .transpose()?
        .unwrap_or_else(|| name.to_string());
    extractor.expect_empty()?;

    let (node, children) = match &input.data {
        Data::Enum(data) => {
            if data.variants.is_empty() {
                return Err(syn::Error::new(
                    data.variants.span(),
                    format!("cannot derive `{TRAIT}` for empty enums"),
                ));
            }
            let mut nodes = vec![];
            let mut children = vec![];
            for variant in &data.variants {
                AttributeExtractor::try_new(ATTRIBUTE, &variant.attrs)?.expect_empty()?;
                let SyntaxFields { node, children: c } = derive_fields(variant, &variant.fields)?;
                nodes.push(node);
                children.extend(c);
            }
            (
                quote! { crate::tree::SyntaxNode::Choice(vec![ #(#nodes),* ]) },
                children,
            )
        }
        Data::Struct(data) => {
            let fields = &data.fields;
            let SyntaxFields { node, children } = derive_fields(fields, fields)?;
            (node, children)
        }
        _ => {
            return Err(syn::Error::new(
                input.span(),
                format!("`{TRAIT}` can only be derived for enums or structs"),
            ));
        }
    };

    Ok(quote! {
        impl crate::tree::TreeSyntax for #name {
            fn syntax() -> crate::tree::SyntaxDescriptor {
                crate::tree::SyntaxDescriptor {
                    name: #syntax_name.to_string(),
                    node: #node,
                    children: vec![ #(#children),* ],
                }
            }
        }
    })
}
