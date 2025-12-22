use std::path::PathBuf;

use prost_build::Config;
use quote::{format_ident, quote};
use serde::Deserialize;

fn build_proto() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let descriptor_path = out_dir.join("spark_connect_descriptor.bin");
    let mut config = Config::new();
    config.skip_debug([
        "spark.connect.LocalRelation",
        "spark.connect.ExecutePlanResponse.ArrowBatch",
    ]);
    tonic_prost_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(&descriptor_path)
        .compile_well_known_types(true)
        .extern_path(".google.protobuf", "::pbjson_types")
        .build_server(true)
        .compile_with_config(
            config,
            &[
                "proto/spark/connect/base.proto",
                "proto/spark/connect/catalog.proto",
                "proto/spark/connect/commands.proto",
                "proto/spark/connect/common.proto",
                "proto/spark/connect/example_plugins.proto",
                "proto/spark/connect/expressions.proto",
                "proto/spark/connect/ml.proto",
                "proto/spark/connect/ml_common.proto",
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
    #[serde(default)]
    is_static: bool,
    deprecated: Option<SparkConfigNotice>,
    removed: Option<SparkConfigNotice>,
    // Spark supports "prepending" the value of another configuration key.
    // This is only used in a few places such as the Java options configuration.
    // We do not support this feature in the Rust implementation.
    // Reference: `org.apache.spark.internal.config.ConfigBuilder#withPrepended`
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SparkConfigNotice {
    version: String,
    comment: String,
}

fn build_spark_config() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=data/spark_config.json");

    let word_boundary = regex::Regex::new(r"(?P<first>[a-z])(?P<second>[A-Z])")?;
    let key_const_name = |key: &str| -> String {
        let key = word_boundary.replace_all(key, "${first}_${second}");
        key.replace('.', "_").to_uppercase()
    };

    let mut config =
        serde_json::from_str::<SparkConfig>(&std::fs::read_to_string("data/spark_config.json")?)?;

    // TODO: remove these overrides
    config.entries.iter_mut().for_each(|entry| {
        // Spark 4.1 changes the local relation cache threshold from 64 MB to 1 MB
        // which causes DataFrame creation more likely to fail since we do not support
        // caching local relations as artifacts yet.
        // Here we override the default value to a very high value (2 GB) to
        // effectively turn off local relation caching.
        if entry.key == "spark.sql.session.localRelationCacheThreshold" {
            entry.default_value = Some("2147483648".to_string())
        }
        // Spark 4.1 enables safe conversion to Arrow by default, but we turn it off
        // to avoid a few PySpark test failures caused by datetime conversions.
        // More investigation is needed here.
        if entry.key == "spark.sql.execution.pandas.convertToArrowArraySafely" {
            entry.default_value = Some("false".to_string())
        }
    });

    let keys = config
        .entries
        .iter()
        .map(|entry| {
            let key = &entry.key;
            let doc = if !entry.doc.is_empty() {
                &entry.doc
            } else {
                "(Missing documentation)"
            };
            let const_name = format_ident!("{}", key_const_name(key));
            quote! {
                #[doc = #doc]
                pub const #const_name: &str = #key;
            }
        })
        .collect::<Vec<_>>();

    let notice_to_token_stream = |notice: &Option<SparkConfigNotice>| match notice {
        None => quote! { None },
        Some(notice) => {
            let version = &notice.version;
            let comment = &notice.comment;
            quote! {
                Some(SparkConfigNotice {
                    version: #version,
                    comment: #comment,
                })
            }
        }
    };

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
            let is_static = entry.is_static;
            let deprecated = notice_to_token_stream(&entry.deprecated);
            let removed = notice_to_token_stream(&entry.removed);
            quote! {
                #key => SparkConfigEntry {
                    key: #key,
                    default_value: #default_value,
                    alternatives: &[#(#alternatives),*],
                    fallback: #fallback,
                    is_static: #is_static,
                    deprecated: #deprecated,
                    removed: #removed,
                },
            }
        })
        .collect::<Vec<_>>();

    let tokens = quote! {
        #[derive(Debug, Clone, PartialEq)]
        pub struct SparkConfigEntry<'a> {
            pub key: &'a str,
            pub default_value: Option<&'a str>,
            pub alternatives: &'a [&'a str],
            pub fallback: Option<&'a str>,
            pub is_static: bool,
            pub deprecated: Option<SparkConfigNotice<'a>>,
            pub removed: Option<SparkConfigNotice<'a>>,
        }

        #[derive(Debug, Clone, PartialEq)]
        pub struct SparkConfigNotice<'a> {
            pub version: &'a str,
            pub comment: &'a str,
        }

        #(#keys)*

        // We define the map in a separate macro to avoid slowing down the IDE
        // when previewing the definition of `SPARK_CONFIG`.
        macro_rules! spark_config_map {
            () => { phf::phf_map! { #(#entries)* } }
        }

        pub static SPARK_CONFIG: phf::Map<&'static str, SparkConfigEntry<'static>> = spark_config_map!();
    };

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    std::fs::write(
        out_dir.join("spark_config.rs"),
        prettyplease::unparse(&syn::parse2(tokens)?),
    )?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    build_proto()?;
    build_spark_config()?;
    Ok(())
}
