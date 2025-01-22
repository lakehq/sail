use std::mem;

use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Attribute, Expr, Meta, MetaNameValue, Path, Token};

/// An extractor for a specific attribute name.
/// The attribute can have named arguments such as `#[attribute(argument = value)]`,
/// or paths such as `#[attribute(path)]`.
pub struct AttributeExtractor<'a> {
    name: &'a str,
    arguments: Vec<MetaNameValue>,
    paths: Vec<Path>,
}

impl<'a> AttributeExtractor<'a> {
    /// Creates an extractor for the given attribute name.
    /// The arguments and paths are collected from the attribute list and
    /// stored in the extractor for further extraction.
    pub fn try_new(name: &'a str, attributes: &[Attribute]) -> syn::Result<Self> {
        let mut arguments = Vec::new();
        let mut paths = Vec::new();
        for attr in attributes {
            if !attr.path().is_ident(name) {
                continue;
            }
            let nested = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
            for meta in nested {
                match meta {
                    Meta::Path(x) => {
                        paths.push(x);
                    }
                    Meta::NameValue(x) => {
                        arguments.push(x);
                    }
                    _ => return Err(syn::Error::new(meta.span(), "invalid attribute value")),
                }
            }
        }
        Ok(Self {
            name,
            arguments,
            paths,
        })
    }

    /// Returns an error if there are any remaining arguments or paths for the attribute.
    pub fn expect_empty(&self) -> syn::Result<()> {
        if let Some(x) = self.arguments.first() {
            Err(syn::Error::new(
                x.span(),
                format!("unexpected `{}` attribute argument", self.name),
            ))
        } else if let Some(x) = self.paths.first() {
            Err(syn::Error::new(
                x.span(),
                format!("unexpected `{}` attribute path", self.name),
            ))
        } else {
            Ok(())
        }
    }

    /// Extracts a single argument value from the attribute.
    /// The argument is removed from the extractor.
    /// Returns an error if there are multiple arguments with the same name.
    pub fn extract_argument_value<T, F>(&mut self, argument: &str, transform: F) -> syn::Result<T>
    where
        F: FnOnce(Option<Expr>) -> syn::Result<T>,
    {
        let arguments = mem::take(&mut self.arguments);
        let (mut extracted, remaining) = arguments
            .into_iter()
            .partition::<Vec<_>, _>(|x| x.path.is_ident(argument));
        self.arguments = remaining;
        let one = extracted.pop();
        if let Some(other) = extracted.last() {
            Err(syn::Error::new(
                other.span(),
                format!(
                    "duplicated `{}` argument for the `{}` attribute",
                    argument, self.name
                ),
            ))
        } else {
            transform(one.map(|x| x.value))
        }
    }

    /// Extracts a single path from the attribute.
    /// The path is removed from the extractor.
    /// Returns an error if there are multiple paths with the same name.
    #[allow(unused)]
    pub fn extract_path(&mut self, path: &str) -> syn::Result<Option<()>> {
        let paths = mem::take(&mut self.paths);
        let (mut extracted, remaining) = paths
            .into_iter()
            .partition::<Vec<_>, _>(|x| x.is_ident(path));
        self.paths = remaining;
        let one = extracted.pop();
        if let Some(other) = extracted.last() {
            Err(syn::Error::new(
                other.span(),
                format!(
                    "duplicated `{}` path for the `{}` attribute",
                    path, self.name
                ),
            ))
        } else {
            Ok(one.map(|_| ()))
        }
    }
}
