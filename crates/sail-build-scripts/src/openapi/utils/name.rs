use std::fmt::{Display, Formatter};

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, TokenStreamExt};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RustName(String);

impl RustName {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl Display for RustName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl quote::ToTokens for RustName {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.append(format_ident!("{}", self.0));
    }
}

pub fn lowercase_parts(value: &str) -> Vec<String> {
    let mut output = Vec::new();
    let mut part = String::new();
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
                if !part.is_empty() {
                    output.push(std::mem::take(&mut part));
                }
            }
            part.push(character.to_ascii_lowercase());
        } else if character.is_ascii_alphanumeric() {
            part.push(character.to_ascii_lowercase());
        } else if !part.is_empty() {
            output.push(std::mem::take(&mut part));
        }
    }
    if !part.is_empty() {
        output.push(part);
    }
    output
}

pub fn type_name(value: &str) -> RustName {
    let mut output = String::new();
    for part in lowercase_parts(value) {
        let mut characters = part.chars();
        if let Some(first) = characters.next() {
            output.push(first.to_ascii_uppercase());
            for character in characters {
                output.push(character.to_ascii_lowercase());
            }
        }
    }
    RustName::new(if output.is_empty() {
        "Value".to_owned()
    } else if output
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_digit())
    {
        format!("Value{output}")
    } else {
        output
    })
}

pub fn value_name(value: &str) -> RustName {
    let value = lowercase_parts(value).join("_");
    let value = if value.is_empty() {
        "value".to_owned()
    } else if value
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_digit())
    {
        format!("_{value}")
    } else {
        value
    };
    if syn::parse_str::<Ident>(&value).is_ok() {
        RustName::new(value)
    } else if syn::parse_str::<Ident>(&format!("r#{value}")).is_ok() {
        RustName::new(format!("r#{value}"))
    } else {
        RustName::new(format!("{value}_"))
    }
}
