use std::path::PathBuf;

use prettyplease;
use quote::{format_ident, quote};
use regex;
use serde::Deserialize;
use syn;

fn build_proto() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let descriptor_path = out_dir.join("spark_connect_descriptor.bin");
    tonic_build::configure()
        .file_descriptor_set_path(&descriptor_path)
        .compile_well_known_types(true)
        .extern_path(".google.protobuf", "::pbjson_types")
        .build_server(true)
        .compile(
            &[
                "proto/spark/connect/base.proto",
                "proto/spark/connect/catalog.proto",
                "proto/spark/connect/commands.proto",
                "proto/spark/connect/common.proto",
                "proto/spark/connect/expressions.proto",
                "proto/spark/connect/relations.proto",
                "proto/spark/connect/types.proto",
            ],
            &["proto"],
        )?;

    let descriptors = std::fs::read(descriptor_path)?;
    pbjson_build::Builder::new()
        .register_descriptors(&descriptors)?
        .build(&[".spark.connect"])?;
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SparkConfig {
    #[allow(dead_code)]
    spark_version: String,
    #[allow(dead_code)]
    comment: String,
    entries: Vec<SparkConfigEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SparkConfigEntry {
    key: String,
    doc: String,
    default_value: Option<String>,
    #[serde(default)]
    alternatives: Vec<String>,
    fallback: Option<String>,
}

fn build_spark_config() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=data/spark_config.json");

    let word_boundary = regex::Regex::new(r"(?P<first>[a-z])(?P<second>[A-Z])")?;
    let key_const_name = |key: &str| -> String {
        let key = word_boundary.replace_all(key, "${first}_${second}");
        key.replace(".", "_").to_uppercase()
    };

    let config =
        serde_json::from_str::<SparkConfig>(&std::fs::read_to_string("data/spark_config.json")?)?;

    let keys = config
        .entries
        .iter()
        .map(|entry| {
            let key = &entry.key;
            let doc = &entry.doc;
            let const_name = format_ident!("{}", key_const_name(key));
            quote! {
                #[doc = #doc]
                pub const #const_name: &str = #key;
            }
        })
        .collect::<Vec<_>>();

    let entries = config
        .entries
        .iter()
        .map(|entry| {
            let key = &entry.key;
            let default_value = match &entry.default_value {
                None => quote! { None },
                Some(x) => quote! { Some(#x) },
            };
            let alternatives = &entry.alternatives;
            let fallback = match &entry.fallback {
                None => quote! { None },
                Some(x) => {
                    quote! { Some(#x) }
                }
            };
            quote! {
                #key => SparkConfigEntry {
                    key: #key,
                    default_value: #default_value,
                    alternatives: &[#(#alternatives),*],
                    fallback: #fallback,
                },
            }
        })
        .collect::<Vec<_>>();

    let tokens = quote! {
        use phf::phf_map;

        #[derive(Debug, Clone, PartialEq)]
        pub struct SparkConfigEntry<'a> {
            pub key: &'a str,
            pub default_value: Option<&'a str>,
            pub alternatives: &'a [&'a str],
            pub fallback: Option<&'a str>,
        }

        #(#keys)*

        static SPARK_CONFIG: phf::Map<&'static str, SparkConfigEntry<'static>> = phf_map! {
            #(#entries)*
        };
    };

    let tree = syn::parse2(tokens)?;
    let formatted = prettyplease::unparse(&tree);
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    std::fs::write(out_dir.join("spark_config.rs"), formatted)?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    build_proto()?;
    build_spark_config()?;
    Ok(())
}
