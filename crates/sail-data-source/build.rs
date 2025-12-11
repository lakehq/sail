use std::iter::once;
use std::path::PathBuf;

use quote::{format_ident, quote};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OptionEntry {
    /// The key for the option (case-insensitive).
    key: String,
    /// The aliases for the option (case-insensitive), if any.
    aliases: Option<Vec<String>>,
    /// The default value for the option, or [`None`] if the option should not have a default.
    /// If the option is not set by the user, there are two scenarios:
    ///   1. If the option has a default, the default value is used.
    ///   2. If the option does not have a default, certain global configuration options may apply.
    default: Option<String>,
    /// The option description in Markdown format.
    #[expect(unused)]
    description: String,
    /// Whether the option is supported by the data source.
    /// Unsupported options will be excluded from the generated code.
    supported: bool,
    /// The Rust type for the option, which defaults to `String`.
    rust_type: Option<String>,
    /// The Rust deserialization function, which defaults to a `String` deserializer.
    /// The function should deserialize `Option<T>` when `rust_type` is `T`.
    rust_deserialize_with: Option<String>,
}

fn build_options(name: &str, kind: &str) -> Result<(), Box<dyn std::error::Error>> {
    let path = format!("src/options/data/{kind}.yaml");
    println!("cargo:rerun-if-changed={path}");

    let content = std::fs::read_to_string(path)?;
    let options: Vec<OptionEntry> = serde_yaml::from_str(&content)?;
    let key_pattern = regex::Regex::new(r"^[a-z][a-z0-9_]+$")?;

    let ident = format_ident!("{name}");
    let fields = options
        .iter()
        .filter(|entry| entry.supported)
        .map(|entry| {
            if !key_pattern.is_match(&entry.key) {
                return Err(syn::Error::new(
                    proc_macro2::Span::call_site(),
                    format!("invalid key: {}", entry.key),
                ));
            }
            let key = format_ident!("{}", &entry.key);
            let rust_type = entry.rust_type.as_deref().unwrap_or("String");
            let rust_type: syn::Type = syn::parse_str(rust_type)?;
            let rust_deserialize_with = entry
                .rust_deserialize_with
                .as_deref()
                .unwrap_or("crate::options::serde::deserialize_string");
            let aliases = if let Some(aliases) = &entry.aliases {
                let tokens = aliases.iter().map(|alias| {
                    let alias = alias.to_lowercase();
                    quote! { #[serde(alias = #alias)] }
                });
                quote! { #(#tokens)* }
            } else {
                quote! {}
            };
            // All the fields are optional, so that some options can be unset,
            // or some options can have no default value.
            Ok(quote! {
                #[serde(default)]
                #[serde(deserialize_with = #rust_deserialize_with)]
                #aliases
                pub #key: Option<#rust_type>,
            })
        })
        .collect::<Result<Vec<_>, syn::Error>>()?;
    let allowed_keys = options
        .iter()
        .filter(|entry| entry.supported)
        .flat_map(|entry| {
            once(entry.key.clone()).chain(entry.aliases.iter().flatten().map(|x| x.to_lowercase()))
        })
        .map(|key| quote! { #key, })
        .collect::<Vec<_>>();
    let default_values = options
        .iter()
        .filter(|entry| entry.supported)
        .map(|entry| {
            if let Some(default) = &entry.default {
                let key = &entry.key;
                let value = &default;
                quote! { (#key, #value), }
            } else {
                quote! {}
            }
        })
        .collect::<Vec<_>>();

    let tokens = quote! {
        #[derive(Debug, Clone, serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct #ident {
            #(#fields)*
        }

        impl crate::options::DataSourceOptions for #ident {
            const ALLOWED_KEYS: &'static [&'static str] = &[
                #(#allowed_keys)*
            ];
            const DEFAULT_VALUES: &'static [(&'static str, &'static str)] = &[
                #(#default_values)*
            ];
        }
    };

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?).join("options");
    match std::fs::create_dir_all(&out_dir) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(Box::new(e)),
    };
    std::fs::write(
        out_dir.join(format!("{kind}.rs")),
        prettyplease::unparse(&syn::parse2(tokens)?),
    )?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    build_options("BinaryReadOptions", "binary_read")?;
    build_options("CsvReadOptions", "csv_read")?;
    build_options("CsvWriteOptions", "csv_write")?;
    build_options("JsonReadOptions", "json_read")?;
    build_options("JsonWriteOptions", "json_write")?;
    build_options("ParquetReadOptions", "parquet_read")?;
    build_options("ParquetWriteOptions", "parquet_write")?;
    build_options("DeltaReadOptions", "delta_read")?;
    build_options("DeltaWriteOptions", "delta_write")?;
    build_options("IcebergReadOptions", "iceberg_read")?;
    build_options("IcebergWriteOptions", "iceberg_write")?;
    build_options("TextReadOptions", "text_read")?;
    build_options("TextWriteOptions", "text_write")?;
    build_options("SocketReadOptions", "socket_read")?;
    build_options("RateReadOptions", "rate_read")?;
    Ok(())
}
