use proc_macro2::TokenStream;
use quote::quote;

use crate::openapi::utils::name::RustName;

#[derive(Clone)]
pub enum RustType {
    Unit,
    Bool,
    I32,
    I64,
    F32,
    F64,
    String,
    JsonValue,
    Named {
        qualifier: Vec<RustName>,
        name: RustName,
    },
    Box(Box<RustType>),
    Option(Box<RustType>),
    Vec(Box<RustType>),
    Map(Box<RustType>),
}

impl RustType {
    pub fn is_unit(&self) -> bool {
        matches!(self, Self::Unit)
    }

    pub fn is_string(&self) -> bool {
        match self {
            Self::String => true,
            Self::Option(value) | Self::Box(value) => value.is_string(),
            _ => false,
        }
    }

    pub fn is_vec(&self) -> bool {
        matches!(self, Self::Vec(_))
    }

    pub fn is_option(&self) -> bool {
        matches!(self, Self::Option(_))
    }
}

#[derive(Clone, Copy)]
pub(crate) enum TypePosition {
    Normal,
    Nested,
}

impl quote::ToTokens for RustType {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let value = match self {
            Self::Unit => quote! { () },
            Self::Bool => quote! { bool },
            Self::I32 => quote! { i32 },
            Self::I64 => quote! { i64 },
            Self::F32 => quote! { f32 },
            Self::F64 => quote! { f64 },
            Self::String => quote! { String },
            Self::JsonValue => quote! { serde_json::Value },
            Self::Named { qualifier, name } => {
                let segments = qualifier
                    .iter()
                    .chain(std::iter::once(name))
                    .collect::<Vec<_>>();
                quote! { #(#segments)::* }
            }
            Self::Box(value) => quote! { Box<#value> },
            Self::Option(value) => quote! { Option<#value> },
            Self::Vec(value) => quote! { Vec<#value> },
            Self::Map(value) => quote! { std::collections::BTreeMap<String, #value> },
        };
        tokens.extend(value);
    }
}
