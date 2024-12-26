use std::path::PathBuf;

use quote::{format_ident, quote};

/// Converts a SQL keyword (`SCREAMING_SNAKE_CASE`) to an identifier (`PascalCase`).
fn sql_keyword_to_identifier(value: &str) -> String {
    value
        .split('_')
        .map(|part| {
            let part = part.to_lowercase();
            let mut part = part.chars();
            match part.next() {
                Some(first) => first.to_uppercase().chain(part).collect::<String>(),
                None => String::new(),
            }
        })
        .collect::<String>()
}

fn build_keywords_module() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=data/keywords.txt");

    let data = std::fs::read_to_string("data/keywords.txt")?;

    let keywords = data
        .lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect::<Vec<_>>();
    let identifiers = keywords
        .iter()
        .map(|x| {
            let ident = format_ident!("{}", sql_keyword_to_identifier(x));
            quote! { #ident }
        })
        .collect::<Vec<_>>();
    let entries = keywords
        .iter()
        .zip(identifiers.iter())
        .map(|(k, i)| quote! { #k => Keyword::#i })
        .collect::<Vec<_>>();

    let tokens = quote! {
        /// A SQL keyword.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        #[allow(unused)]
        pub enum Keyword {
            #(#identifiers,)*
        }

        macro_rules! keyword_map {
            () => { phf::phf_map! { #(#entries,)* } }
        }

        static KEYWORD_MAP: phf::Map<&'static str, Keyword> = keyword_map!();

        #[cfg(test)]
        static KEYWORD_VALUES: &[&str] = &[ #(#keywords,)* ];
    };

    let tokens_ast = quote! {
        #(define_keyword_type!(#identifiers);)*
    };

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    std::fs::write(
        out_dir.join("keywords.rs"),
        prettyplease::unparse(&syn::parse2(tokens)?),
    )?;
    std::fs::write(
        out_dir.join("keywords.ast.rs"),
        prettyplease::unparse(&syn::parse2(tokens_ast)?),
    )?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    build_keywords_module()?;
    Ok(())
}
