use std::path::PathBuf;

use lazy_static::lazy_static;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use regex::Regex;
use serde::Deserialize;

lazy_static! {
    static ref KEY_PATTERN: Regex = {
        #[expect(clippy::unwrap_used)]
        Regex::new(r"^[a-z][a-z0-9_]*$").unwrap()
    };
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DefaultValue {
    value: String,
    parser: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OptionEntry {
    /// The key for the option.
    key: String,
    /// The option description in Markdown format.
    #[expect(unused)]
    description: String,
    /// The default value for the option, or [`None`] if the option does not have a default value.
    default: Option<DefaultValue>,
    /// Whether the option is supported by the data source.
    supported: bool,
    /// The Rust type for the option.
    rust_type: String,
    /// The scopes in which the option is applicable.
    /// Each scope corresponds to a data source operation (e.g. read or write).
    scopes: Vec<OptionScope>,
    /// The origins from which the option can be set.
    origins: Vec<OptionOrigin>,
}

#[derive(Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum OptionScope {
    Read,
    Write,
}

impl OptionScope {
    fn name(&self) -> &'static str {
        match self {
            OptionScope::Read => "Read",
            OptionScope::Write => "Write",
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum OptionOrigin {
    Option {
        keys: Vec<String>,
        #[serde(default)]
        case_sensitive: bool,
        parser: String,
    },
    TableProperty {
        keys: Vec<String>,
        #[serde(default)]
        case_sensitive: bool,
        parser: String,
    },
    TableLocation,
    AsOfTimestamp,
    AsOfIntegerVersion,
    AsOfStringVersion,
}

#[derive(Clone, Copy)]
#[expect(clippy::enum_variant_names)]
enum TemporalClauseKind {
    AsOfTimestamp,
    AsOfIntegerVersion,
    AsOfStringVersion,
}

#[derive(Clone, Copy)]
enum OptionKeyKind {
    Option,
    TableProperty,
}

/// Generates structs and trait implementations for data source options.
pub fn build_data_source_options(name: &str, kind: &str) -> Result<(), Box<dyn std::error::Error>> {
    let path = format!("src/options/data/{kind}.yaml");
    println!("cargo:rerun-if-changed={path}");

    let content = std::fs::read_to_string(path)?;
    let entries: Vec<OptionEntry> = serde_yaml::from_str(&content)?;

    for entry in &entries {
        if !KEY_PATTERN.is_match(&entry.key) {
            return Err(format!("invalid key: {}", entry.key).into());
        }
    }

    let read_tokens = generate_data_source_options(name, OptionScope::Read, &entries)?;
    let write_tokens = generate_data_source_options(name, OptionScope::Write, &entries)?;
    let tokens = quote! {
        #read_tokens
        #write_tokens
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

fn generate_data_source_options(
    name: &str,
    scope: OptionScope,
    entries: &[OptionEntry],
) -> Result<TokenStream, Box<dyn std::error::Error>> {
    let scope_entries: Vec<&OptionEntry> = entries
        .iter()
        .filter(|e| e.scopes.contains(&scope) && e.supported)
        .collect();
    let scope_name = scope.name();

    let options = generate_options_struct(name, scope_name, &scope_entries)?;
    let partial_options = generate_partial_options_struct(name, scope_name, &scope_entries)?;
    let partial_impl = generate_partial_options_impl(name, scope_name, &scope_entries)?;
    let build_impl = generate_build_partial_options_impl(name, scope_name, &scope_entries)?;

    Ok(quote! {
        #options
        #partial_options
        #partial_impl
        #build_impl
    })
}

/// Generates the final (non-optional) options struct.
fn generate_options_struct(
    name: &str,
    scope: &str,
    entries: &[&OptionEntry],
) -> Result<TokenStream, Box<dyn std::error::Error>> {
    let struct_name = format_ident!("{name}{scope}Options");
    let fields = entries
        .iter()
        .map(|entry| -> Result<TokenStream, syn::Error> {
            let field = format_ident!("{}", entry.key);
            let rust_type: syn::Type = syn::parse_str(&entry.rust_type)?;
            Ok(quote! { pub #field: #rust_type, })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(quote! {
        #[derive(Debug, Clone)]
        pub struct #struct_name {
            #(#fields)*
        }
    })
}

/// Generates the partial (all-optional) options struct.
fn generate_partial_options_struct(
    name: &str,
    scope: &str,
    entries: &[&OptionEntry],
) -> Result<TokenStream, Box<dyn std::error::Error>> {
    let struct_name = format_ident!("{name}{scope}PartialOptions");
    let fields = entries
        .iter()
        .map(|entry| -> Result<TokenStream, syn::Error> {
            let field = format_ident!("{}", entry.key);
            let rust_type: syn::Type = syn::parse_str(&entry.rust_type)?;
            Ok(quote! { pub #field: Option<#rust_type>, })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(quote! {
        #[derive(Debug, Clone, Default)]
        pub struct #struct_name {
            #(#fields)*
        }
    })
}

/// Generates `impl PartialOptions for {Name}{Scope}PartialOptions`.
fn generate_partial_options_impl(
    name: &str,
    scope: &str,
    entries: &[&OptionEntry],
) -> Result<TokenStream, Box<dyn std::error::Error>> {
    let partial_options_name = format_ident!("{name}{scope}PartialOptions");
    let options_name = format_ident!("{name}{scope}Options");

    let has_defaults = entries.iter().any(|e| e.default.is_some());

    let initialize_fields = entries
        .iter()
        .map(|entry| -> Result<TokenStream, syn::Error> {
            let field = format_ident!("{}", entry.key);
            if let Some(default) = &entry.default {
                let parser: syn::Path = syn::parse_str(&default.parser)?;
                let key = &entry.key;
                let value = &default.value;
                Ok(quote! {
                    #field: Some(#parser(#key, #value).expect("valid default value")),
                })
            } else {
                Ok(quote! { #field: None, })
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    let merge_fields = entries.iter().map(|entry| {
        let field = format_ident!("{}", entry.key);
        quote! {
            if let Some(v) = other.#field { self.#field = Some(v); }
        }
    });

    let other_param = if entries.is_empty() {
        quote! { _other: Self }
    } else {
        quote! { other: Self }
    };

    let finalize_fields = entries.iter().map(|entry| {
        let field = format_ident!("{}", entry.key);
        let key = &entry.key;
        quote! {
            #field: self.#field.ok_or(crate::error::DataSourceError::MissingOption { key: #key })?,
        }
    });

    let initialize_lint = if has_defaults {
        quote! {
            #[expect(clippy::expect_used)]
        }
    } else {
        quote! {}
    };

    Ok(quote! {
        impl crate::options::PartialOptions for #partial_options_name {
            type Options = #options_name;

            #initialize_lint
            fn initialize() -> Self {
                Self {
                    #(#initialize_fields)*
                }
            }

            fn merge(&mut self, #other_param) {
                #(#merge_fields)*
            }

            fn finalize(self) -> crate::error::DataSourceResult<#options_name> {
                Ok(#options_name {
                    #(#finalize_fields)*
                })
            }
        }
    })
}

/// Generates `impl BuildPartialOptions<{Name}{Scope}PartialOptions> for OptionLayer`.
fn generate_build_partial_options_impl(
    name: &str,
    scope: &str,
    entries: &[&OptionEntry],
) -> Result<TokenStream, Box<dyn std::error::Error>> {
    let partial_options_name = format_ident!("{name}{scope}PartialOptions");

    let option_list_arm = build_key_match_arm(entries, OptionKeyKind::Option)?;
    let table_property_list_arm = build_key_match_arm(entries, OptionKeyKind::TableProperty)?;
    let table_location_arm = build_table_location_match_arm(entries)?;
    let timestamp_arm =
        build_temporal_clause_match_arm(entries, TemporalClauseKind::AsOfTimestamp)?;
    let integer_version_arm =
        build_temporal_clause_match_arm(entries, TemporalClauseKind::AsOfIntegerVersion)?;
    let string_version_arm =
        build_temporal_clause_match_arm(entries, TemporalClauseKind::AsOfStringVersion)?;

    let arms = [
        &option_list_arm,
        &table_property_list_arm,
        &table_location_arm,
        &timestamp_arm,
        &integer_version_arm,
        &string_version_arm,
    ];
    let has_any_arm = arms.iter().any(|arm| !arm.is_empty());
    let all_arms_present = arms.iter().all(|arm| !arm.is_empty());

    let result_binding = if has_any_arm {
        quote! { let mut result = #partial_options_name::default(); }
    } else {
        quote! { let result = #partial_options_name::default(); }
    };

    let wildcard_arm = if has_any_arm && !all_arms_present {
        quote! { _ => {} }
    } else {
        quote! {}
    };

    let matching = if has_any_arm {
        quote! {
            match self {
                #option_list_arm
                #table_property_list_arm
                #table_location_arm
                #timestamp_arm
                #integer_version_arm
                #string_version_arm
                #wildcard_arm
            }
        }
    } else {
        quote! {}
    };

    Ok(quote! {
        impl crate::options::BuildPartialOptions<#partial_options_name>
            for sail_common_datafusion::datasource::OptionLayer
        {
            fn build_partial_options(
                self,
            ) -> crate::error::DataSourceResult<#partial_options_name> {
                #result_binding
                #matching
                Ok(result)
            }
        }
    })
}

/// Returns a match arm for `OptionLayer::OptionList` or `OptionLayer::TablePropertyList`,
/// or an empty token stream if there are no relevant entries.
fn build_key_match_arm(
    entries: &[&OptionEntry],
    kind: OptionKeyKind,
) -> Result<TokenStream, Box<dyn std::error::Error>> {
    let mut statements = Vec::new();
    for entry in entries {
        for origin in &entry.origins {
            let (keys, case_sensitive, parser) = match (origin, kind) {
                (
                    OptionOrigin::Option {
                        keys,
                        case_sensitive,
                        parser,
                    },
                    OptionKeyKind::Option,
                ) => (keys, *case_sensitive, parser),
                (
                    OptionOrigin::TableProperty {
                        keys,
                        case_sensitive,
                        parser,
                    },
                    OptionKeyKind::TableProperty,
                ) => (keys, *case_sensitive, parser),
                _ => continue,
            };
            let field = format_ident!("{}", entry.key);
            let parser_path: syn::Path = syn::parse_str(parser)?;
            let condition = build_key_condition(keys, case_sensitive);
            statements.push(quote! {
                if #condition {
                    result.#field = Some(#parser_path(&key, &value)?);
                }
            });
        }
    }
    if statements.is_empty() {
        return Ok(quote! {});
    }
    let variant = match kind {
        OptionKeyKind::Option => {
            quote! { sail_common_datafusion::datasource::OptionLayer::OptionList { items } }
        }
        OptionKeyKind::TableProperty => {
            quote! { sail_common_datafusion::datasource::OptionLayer::TablePropertyList { items } }
        }
    };
    Ok(quote! {
        #variant => {
            for (key, value) in items {
                #(#statements)*
            }
        }
    })
}

/// Builds a key comparison condition for use in generated `if` statements.
fn build_key_condition(keys: &[String], case_sensitive: bool) -> TokenStream {
    if case_sensitive {
        let conditions: Vec<_> = keys.iter().map(|k| quote! { key == #k }).collect();
        quote! { #(#conditions)||* }
    } else {
        let conditions: Vec<_> = keys
            .iter()
            .map(|k| quote! { key.eq_ignore_ascii_case(#k) })
            .collect();
        quote! { #(#conditions)||* }
    }
}

/// Returns the match arm for `OptionLayer::TableLocation`, or an empty token stream
/// if there are no relevant entries.
fn build_table_location_match_arm(
    entries: &[&OptionEntry],
) -> Result<TokenStream, Box<dyn std::error::Error>> {
    let mut statements = Vec::new();
    for entry in entries {
        for origin in &entry.origins {
            if let OptionOrigin::TableLocation = origin {
                let field = format_ident!("{}", entry.key);
                statements.push(quote! {
                    result.#field = Some(value);
                });
            }
        }
    }
    if statements.is_empty() {
        return Ok(quote! {});
    }
    Ok(quote! {
        sail_common_datafusion::datasource::OptionLayer::TableLocation { value } => {
            #(#statements)*
        }
    })
}

/// Returns the match arm for the given [`TemporalClauseKind`], or an empty token stream
/// if there are no relevant entries.
fn build_temporal_clause_match_arm(
    entries: &[&OptionEntry],
    kind: TemporalClauseKind,
) -> Result<TokenStream, Box<dyn std::error::Error>> {
    let mut statements = Vec::new();
    for entry in entries {
        for origin in &entry.origins {
            if !matches!(
                (origin, kind),
                (
                    OptionOrigin::AsOfTimestamp,
                    TemporalClauseKind::AsOfTimestamp
                ) | (
                    OptionOrigin::AsOfIntegerVersion,
                    TemporalClauseKind::AsOfIntegerVersion
                ) | (
                    OptionOrigin::AsOfStringVersion,
                    TemporalClauseKind::AsOfStringVersion
                )
            ) {
                continue;
            }
            let field = format_ident!("{}", entry.key);
            statements.push(quote! {
                result.#field = Some(value);
            });
        }
    }
    if statements.is_empty() {
        return Ok(quote! {});
    }
    let variant = match kind {
        TemporalClauseKind::AsOfTimestamp => {
            quote! { sail_common_datafusion::datasource::OptionLayer::AsOfTimestamp { value } }
        }
        TemporalClauseKind::AsOfIntegerVersion => {
            quote! {
                sail_common_datafusion::datasource::OptionLayer::AsOfIntegerVersion { value }
            }
        }
        TemporalClauseKind::AsOfStringVersion => {
            quote! {
                sail_common_datafusion::datasource::OptionLayer::AsOfStringVersion { value }
            }
        }
    };
    Ok(quote! {
        #variant => {
            #(#statements)*
        }
    })
}
