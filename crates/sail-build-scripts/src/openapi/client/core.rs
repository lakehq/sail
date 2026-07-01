use std::path::Path;

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};

use super::config::OpenApiConfig;
use super::context::Context;
use super::operation::{generate_error_enum, generate_method};
use super::schema::generate_schemas;
use crate::error::BuildError;
use crate::openapi::types::OpenApi;

pub(super) const JSON_MEDIA_TYPE: &str = "application/json";

pub fn write_client(
    openapi: &OpenApi,
    config: &OpenApiConfig,
    path: impl AsRef<Path>,
) -> Result<(), BuildError> {
    std::fs::write(path, generate_client(openapi, config)?)?;
    Ok(())
}

pub fn generate_client(openapi: &OpenApi, config: &OpenApiConfig) -> Result<String, BuildError> {
    let context = Context::new(openapi, config);
    let operations = context.operations()?;
    let methods = operations
        .iter()
        .map(|operation| generate_method(&context, operation))
        .collect::<Result<Vec<_>, _>>()?;
    let errors = operations
        .iter()
        .map(|operation| generate_error_enum(&context, operation))
        .collect::<Result<Vec<_>, _>>()?;
    let schemas = generate_schemas(&context)?;

    let tokens = quote! {
        #[derive(Debug)]
        pub struct ApiClient {
            pub base: String,
            pub client: reqwest::Client,
            pub headers: reqwest::header::HeaderMap,
        }

        #[derive(Debug)]
        pub struct Response<T> {
            pub inner: T,
            pub status: reqwest::StatusCode,
            pub headers: reqwest::header::HeaderMap,
        }

        #[derive(Debug)]
        pub enum ApiError<E> {
            Request(reqwest::Error),
            Response(Response<E>),
        }

        impl<E> From<reqwest::Error> for ApiError<E> {
            fn from(error: reqwest::Error) -> Self {
                Self::Request(error)
            }
        }

        impl ApiClient {
            pub fn new(
                base: impl Into<String>,
                client: reqwest::Client,
                headers: reqwest::header::HeaderMap,
            ) -> Self {
                Self {
                    base: base.into(),
                    client,
                    headers,
                }
            }

            #(#methods)*
        }

        #(#errors)*
        #(#schemas)*
    };
    let ast = syn::parse2(tokens)?;
    Ok(prettyplease::unparse(&ast))
}

#[derive(Clone)]
pub(super) struct RustType {
    pub(super) tokens: TokenStream,
    pub(super) is_unit: bool,
}

impl RustType {
    pub(super) fn new(tokens: TokenStream) -> Self {
        Self {
            tokens,
            is_unit: false,
        }
    }

    pub(super) fn unit() -> Self {
        Self {
            tokens: quote! { () },
            is_unit: true,
        }
    }

    pub(super) fn option(self) -> Self {
        let tokens = self.tokens;
        Self::new(quote! { Option<#tokens> })
    }
}

#[derive(Clone, Copy)]
pub(super) enum TypePosition {
    Normal,
    Nested,
}

pub(super) fn type_ident(value: &str) -> Ident {
    let value = type_name_text(value);
    format_ident!("{value}")
}

pub(super) fn value_ident(value: &str) -> Ident {
    let value = identifier_text(value);
    if syn::parse_str::<Ident>(&value).is_ok() {
        format_ident!("{value}")
    } else if syn::parse_str::<Ident>(&format!("r#{value}")).is_ok() {
        format_ident!("r#{value}")
    } else {
        format_ident!("{value}_")
    }
}

pub(super) fn to_snake_case(value: &str) -> String {
    let mut output = String::new();
    let characters = value.chars().collect::<Vec<_>>();
    for (index, character) in characters.iter().copied().enumerate() {
        if character.is_ascii_uppercase() {
            let previous = index.checked_sub(1).and_then(|index| characters.get(index));
            let next = characters.get(index + 1);
            if previous.is_some_and(|character| {
                character.is_ascii_lowercase()
                    || character.is_ascii_digit()
                    || character.is_ascii_uppercase()
                        && next.is_some_and(|character| character.is_ascii_lowercase())
            }) {
                output.push('_');
            }
            output.push(character.to_ascii_lowercase());
        } else if character.is_ascii_alphanumeric() {
            output.push(character.to_ascii_lowercase());
        } else {
            output.push('_');
        }
    }
    identifier_text(&output)
}

pub(super) fn type_name_text(value: &str) -> String {
    let value = to_snake_case(value);
    let mut output = String::new();
    for part in value.split('_') {
        let mut characters = part.chars();
        if let Some(first) = characters.next() {
            output.push(first.to_ascii_uppercase());
            for character in characters {
                output.push(character.to_ascii_lowercase());
            }
        }
    }
    if output.is_empty() {
        "Value".to_owned()
    } else if output
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_digit())
    {
        format!("Value{output}")
    } else {
        output
    }
}

fn identifier_text(value: &str) -> String {
    let mut output = String::new();
    for character in value.chars() {
        if character.is_ascii_alphanumeric() || character == '_' {
            output.push(character.to_ascii_lowercase());
        } else {
            output.push('_');
        }
    }
    while output.contains("__") {
        output = output.replace("__", "_");
    }
    let output = output.trim_matches('_').to_owned();
    let starts_with_digit = output
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_digit());
    if output.is_empty() {
        "value".to_owned()
    } else if starts_with_digit {
        format!("_{output}")
    } else {
        output
    }
}
