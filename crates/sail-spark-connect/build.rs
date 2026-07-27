//! Build script that generates Rust from the .proto files.
//! and also converts json to Rust constants.

use std::collections::BTreeMap;
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
    #[expect(dead_code)]
    spark_version: String,
    #[expect(dead_code)]
    comment: String,
    entries: Vec<SparkConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SparkConfigNotice {
    version: String,
    comment: String,
}

fn apply_spark_config_overrides(config: &mut SparkConfig) {
    // TODO: remove these overrides
    config.entries.iter_mut().for_each(|entry| {
        // Spark 4.1+ changes the local relation cache threshold from 64 MB to 1 MB
        // which causes DataFrame creation more likely to fail since we do not support
        // caching local relations as artifacts yet.
        // Here we override the default value to 2^31-1 (2147483647, Integer.MAX_VALUE)
        // to effectively turn off local relation caching. We use 2^31-1 instead of 2^31
        // because the JVM Spark Connect client parses this value with Integer.parseInt,
        // which overflows for values greater than 2^31-1.
        if entry.key == "spark.sql.session.localRelationCacheThreshold" {
            entry.default_value = Some("2147483647".to_string())
        }
        // Spark 4.1+ enables safe conversion to Arrow by default, but we turn it off
        // to avoid a few PySpark test failures caused by datetime conversions.
        // More investigation is needed here.
        if entry.key == "spark.sql.execution.pandas.convertToArrowArraySafely" {
            entry.default_value = Some("false".to_string())
        }
        // Spark 4.1+ allows plan compression which we do not support yet, so we turn it off.
        if entry.key == "spark.connect.session.planCompression.threshold" {
            entry.default_value = Some("-1".to_string())
        }
    });
}

fn build_spark_config(versions: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    let word_boundary = regex::Regex::new(r"(?P<first>[a-z])(?P<second>[A-Z])")?;
    let key_const_name = |key: &str| -> String {
        let key = word_boundary.replace_all(key, "${first}_${second}");
        key.replace('.', "_").to_uppercase()
    };

    let mut configs = Vec::with_capacity(versions.len());
    for &version in versions {
        let path = format!("data/config/spark-{version}.json");
        println!("cargo:rerun-if-changed={path}");
        let mut config = serde_json::from_str::<SparkConfig>(&std::fs::read_to_string(&path)?)?;
        apply_spark_config_overrides(&mut config);
        configs.push((version, config));
    }

    let mut entries_by_key: BTreeMap<String, Vec<(SparkConfigEntry, Vec<&str>)>> = BTreeMap::new();
    for (version, config) in &configs {
        for entry in &config.entries {
            let variants = entries_by_key.entry(entry.key.clone()).or_default();
            if let Some((_, versions)) = variants.iter_mut().find(|(x, _)| x == entry) {
                versions.push(*version);
            } else {
                variants.push((entry.clone(), vec![*version]));
            }
        }
    }

    let keys = entries_by_key
        .keys()
        .map(|key| {
            let const_name = format_ident!("{}", key_const_name(key));
            quote! {
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

    let entry_to_token_stream = |entry: &SparkConfigEntry| {
        let key_const_name = format_ident!("{}", key_const_name(&entry.key));
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
            SparkConfigEntry {
                key: SparkConfigKey::#key_const_name,
                default_value: #default_value,
                alternatives: &[#(#alternatives),*],
                fallback: #fallback,
                is_static: #is_static,
                deprecated: #deprecated,
                removed: #removed,
            }
        }
    };

    let mut entries = Vec::new();
    for variants in entries_by_key.values_mut() {
        variants.sort_by_key(|(_, versions)| versions[0]);
        entries.extend(variants.iter().cloned());
    }

    let mut entry_names = BTreeMap::new();
    let mut entry_definitions = Vec::with_capacity(entries.len());
    for (entry, versions) in &entries {
        let name = format_ident!(
            "_SPARK_CONFIG_ENTRY_V{}_{}",
            versions[0].replace('.', "_"),
            key_const_name(&entry.key),
        );
        for version in versions {
            entry_names.insert(((*version).to_string(), entry.key.clone()), name.clone());
        }
        let doc = if entry.doc.is_empty() {
            "(Missing documentation)"
        } else {
            &entry.doc
        };
        let entry = entry_to_token_stream(entry);
        entry_definitions.push(quote! {
            #[doc = #doc]
            static #name: SparkConfigEntry<'static> = #entry;
        });
    }

    let config_maps = configs
        .iter()
        .map(|(version, config)| {
            let name = format_ident!("SPARK_CONFIG_V{}", version.replace('.', "_"));
            let macro_name = format_ident!("spark_config_map_v{}", version.replace('.', "_"));
            let map_entries = config.entries.iter().map(|entry| {
                let key = &entry.key;
                let entry_name = &entry_names[&((*version).to_string(), key.clone())];
                quote! { #key => &#entry_name, }
            });
            // We define the map in a separate macro to avoid slowing down the IDE
            // when previewing the definitions of the versioned config maps.
            quote! {
                macro_rules! #macro_name {
                    () => { phf::phf_map! { #(#map_entries)* } }
                }

                pub static #name: phf::Map<&'static str, &'static SparkConfigEntry<'static>> =
                    #macro_name!();
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

        pub struct SparkConfigKey;

        impl SparkConfigKey {
            #(#keys)*
        }

        #(#entry_definitions)*

        #(#config_maps)*
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
    build_spark_config(&["3.5", "4.0", "4.1", "4.2"])?;
    Ok(())
}
