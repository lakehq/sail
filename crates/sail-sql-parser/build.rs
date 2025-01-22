use std::path::PathBuf;

use quote::{format_ident, quote};

/// Converts a SQL keyword string in `"SCREAMING_SNAKE_CASE"` to
/// a Rust identifier in `PascalCase`.
fn keyword_identifier(value: &str) -> String {
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

/// Define macros that can be used to generate code for SQL keywords.
fn build_keywords_macros() -> Result<(), Box<dyn std::error::Error>> {
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
            let ident = format_ident!("{}", keyword_identifier(x));
            quote! { #ident }
        })
        .collect::<Vec<_>>();
    let items = keywords
        .iter()
        .zip(identifiers.iter())
        .map(|(k, i)| quote! {(#k, #i)})
        .collect::<Vec<_>>();
    let entries = keywords
        .iter()
        .zip(identifiers.iter())
        .map(|(k, i)| quote! { #k => $value!(#i) })
        .collect::<Vec<_>>();

    let tokens = quote! {
        /// Invoke a `callback` macro for the keyword list.
        /// The keyword list contains tuples where the first element is the keyword string,
        /// and the second element is the keyword identifier.
        macro_rules! for_all_keywords {
            ($callback:ident) => { $callback!([#(#items,)*]); }
        }

        /// Define a compile-time map of SQL keywords where the map key is the keyword string.
        /// The `value` macro specifies how to define the map value given the keyword identifier.
        /// Note that we cannot define the map via the `for_all_keywords` macro because
        /// `phf::phf_map` requires the key to be a string literal, and Rust macros do not
        /// support eager expansion in general.
        macro_rules! keyword_map {
            ($value:ident) => { phf::phf_map! { #(#entries,)* } }
        }
    };

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    std::fs::write(
        out_dir.join("keywords.rs"),
        prettyplease::unparse(&syn::parse2(tokens)?),
    )?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    build_keywords_macros()?;
    Ok(())
}
