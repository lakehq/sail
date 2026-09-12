// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Read and write options for the Lance table format.
//!
//! Option names follow the Apache Spark connector for Lance
//! (<https://github.com/lance-format/lance-spark>) so that a Spark job keeps
//! working when it is pointed at Sail, with one deliberate exception noted on
//! [`OPTION_MAX_ROWS_PER_FILE`].

use datafusion_common::{Result, not_impl_err, plan_datafusion_err, plan_err};
use sail_common_datafusion::datasource::{OptionLayer, SinkMode};

/// Dataset version to read, for time travel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatasetRef {
    Version(u64),
    Tag(String),
}

/// Vector search pushed into the Lance scan.
#[derive(Debug, Clone, PartialEq)]
pub struct NearestOptions {
    pub column: String,
    pub query: Vec<f32>,
    pub k: usize,
    pub nprobes: Option<usize>,
    pub refine_factor: Option<u32>,
    pub use_index: bool,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct LanceReadOptions {
    pub version: Option<DatasetRef>,
    pub batch_size: Option<usize>,
    pub with_row_id: bool,
    pub nearest: Option<NearestOptions>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd)]
pub struct LanceWriteOptions {
    pub max_rows_per_file: Option<usize>,
    pub max_rows_per_group: Option<usize>,
    pub max_bytes_per_file: Option<usize>,
    pub file_format_version: Option<String>,
    pub enable_stable_row_ids: Option<bool>,
}

/// How a write resolves against an existing dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
pub enum LanceWriteMode {
    /// Add rows to the dataset, creating it if it does not exist.
    Append,
    /// Replace the dataset contents, creating it if it does not exist.
    Overwrite,
    /// Create the dataset, failing if it already exists.
    ErrorIfExists,
    /// Create the dataset, doing nothing if it already exists.
    IgnoreIfExists,
}

const OPTION_VERSION: &str = "version";
const OPTION_BATCH_SIZE: &str = "batch_size";
const OPTION_WITH_ROW_ID: &str = "with_row_id";
const OPTION_NEAREST_COLUMN: &str = "nearest.column";
const OPTION_NEAREST_QUERY: &str = "nearest.query";
const OPTION_NEAREST_K: &str = "nearest.k";
const OPTION_NEAREST_NPROBES: &str = "nearest.nprobes";
const OPTION_NEAREST_REFINE_FACTOR: &str = "nearest.refine_factor";
const OPTION_NEAREST_USE_INDEX: &str = "nearest.use_index";

const READ_OPTIONS: &[&str] = &[
    OPTION_VERSION,
    OPTION_BATCH_SIZE,
    OPTION_WITH_ROW_ID,
    OPTION_NEAREST_COLUMN,
    OPTION_NEAREST_QUERY,
    OPTION_NEAREST_K,
    OPTION_NEAREST_NPROBES,
    OPTION_NEAREST_REFINE_FACTOR,
    OPTION_NEAREST_USE_INDEX,
];

/// The Spark connector spells this option `max_row_per_file`; this crate uses
/// the name of the underlying Lance write parameter instead.
const OPTION_MAX_ROWS_PER_FILE: &str = "max_rows_per_file";
const OPTION_MAX_ROWS_PER_GROUP: &str = "max_rows_per_group";
const OPTION_MAX_BYTES_PER_FILE: &str = "max_bytes_per_file";
const OPTION_FILE_FORMAT_VERSION: &str = "file_format_version";
const OPTION_ENABLE_STABLE_ROW_IDS: &str = "enable_stable_row_ids";

const WRITE_OPTIONS: &[&str] = &[
    OPTION_MAX_ROWS_PER_FILE,
    OPTION_MAX_ROWS_PER_GROUP,
    OPTION_MAX_BYTES_PER_FILE,
    OPTION_FILE_FORMAT_VERSION,
    OPTION_ENABLE_STABLE_ROW_IDS,
];

/// Option keys that Sail itself attaches to every layer.
const RESERVED_OPTIONS: &[&str] = &["path", "location"];

impl LanceReadOptions {
    pub fn resolve(layers: &[OptionLayer]) -> Result<Self> {
        let mut options = Self::default();
        let mut nearest = NearestBuilder::default();
        for layer in layers {
            match layer {
                OptionLayer::AsOfIntegerVersion { value } => {
                    let version = u64::try_from(*value).map_err(|_| {
                        plan_datafusion_err!("dataset version must not be negative: {value}")
                    })?;
                    options.version = Some(DatasetRef::Version(version));
                }
                OptionLayer::AsOfStringVersion { value } => {
                    options.version = Some(DatasetRef::Tag(value.clone()));
                }
                OptionLayer::AsOfTimestamp { value } => {
                    return not_impl_err!(
                        "time travel by timestamp ({value}) for the Lance table format; \
                         use a version number or a tag instead"
                    );
                }
                OptionLayer::TableLocation { .. } => {}
                OptionLayer::OptionList { items } => {
                    for (key, value) in items {
                        options.apply(key, value, &mut nearest, true)?;
                    }
                }
                OptionLayer::TablePropertyList { items } => {
                    for (key, value) in items {
                        options.apply(key, value, &mut nearest, false)?;
                    }
                }
            }
        }
        options.nearest = nearest.build()?;
        Ok(options)
    }

    fn apply(
        &mut self,
        key: &str,
        value: &str,
        nearest: &mut NearestBuilder,
        strict: bool,
    ) -> Result<()> {
        let key = normalize_key(key);
        match key.as_str() {
            OPTION_VERSION => self.version = Some(parse_version(value)?),
            OPTION_BATCH_SIZE => self.batch_size = Some(parse_usize(&key, value)?),
            OPTION_WITH_ROW_ID => self.with_row_id = parse_bool(&key, value)?,
            OPTION_NEAREST_COLUMN => nearest.column = Some(value.to_string()),
            OPTION_NEAREST_QUERY => nearest.query = Some(parse_vector(&key, value)?),
            OPTION_NEAREST_K => nearest.k = Some(parse_usize(&key, value)?),
            OPTION_NEAREST_NPROBES => nearest.nprobes = Some(parse_usize(&key, value)?),
            OPTION_NEAREST_REFINE_FACTOR => {
                let factor = u32::try_from(parse_usize(&key, value)?)
                    .map_err(|_| plan_datafusion_err!("option '{key}' is out of range: {value}"))?;
                nearest.refine_factor = Some(factor);
            }
            OPTION_NEAREST_USE_INDEX => nearest.use_index = Some(parse_bool(&key, value)?),
            _ => return unknown_option(&key, strict, READ_OPTIONS),
        }
        Ok(())
    }
}

impl LanceWriteOptions {
    pub fn resolve(layers: &[OptionLayer]) -> Result<Self> {
        let mut options = Self::default();
        for layer in layers {
            match layer {
                OptionLayer::OptionList { items } => {
                    for (key, value) in items {
                        options.apply(key, value, true)?;
                    }
                }
                OptionLayer::TablePropertyList { items } => {
                    for (key, value) in items {
                        options.apply(key, value, false)?;
                    }
                }
                OptionLayer::TableLocation { .. } => {}
                OptionLayer::AsOfIntegerVersion { .. }
                | OptionLayer::AsOfStringVersion { .. }
                | OptionLayer::AsOfTimestamp { .. } => {
                    return plan_err!("time travel options are not valid for a write");
                }
            }
        }
        Ok(options)
    }

    fn apply(&mut self, key: &str, value: &str, strict: bool) -> Result<()> {
        let key = normalize_key(key);
        match key.as_str() {
            OPTION_MAX_ROWS_PER_FILE => self.max_rows_per_file = Some(parse_usize(&key, value)?),
            OPTION_MAX_ROWS_PER_GROUP => self.max_rows_per_group = Some(parse_usize(&key, value)?),
            OPTION_MAX_BYTES_PER_FILE => self.max_bytes_per_file = Some(parse_usize(&key, value)?),
            OPTION_FILE_FORMAT_VERSION => self.file_format_version = Some(value.to_string()),
            OPTION_ENABLE_STABLE_ROW_IDS => {
                self.enable_stable_row_ids = Some(parse_bool(&key, value)?)
            }
            _ => return unknown_option(&key, strict, WRITE_OPTIONS),
        }
        Ok(())
    }
}

impl LanceWriteMode {
    pub fn resolve(mode: &SinkMode) -> Result<Self> {
        match mode {
            SinkMode::Append => Ok(Self::Append),
            SinkMode::Overwrite => Ok(Self::Overwrite),
            SinkMode::ErrorIfExists => Ok(Self::ErrorIfExists),
            SinkMode::IgnoreIfExists => Ok(Self::IgnoreIfExists),
            SinkMode::OverwriteIf { .. } => {
                not_impl_err!("conditional overwrite for the Lance table format")
            }
            SinkMode::OverwritePartitions => {
                not_impl_err!("dynamic partition overwrite for the Lance table format")
            }
        }
    }
}

#[derive(Debug, Default)]
struct NearestBuilder {
    column: Option<String>,
    query: Option<Vec<f32>>,
    k: Option<usize>,
    nprobes: Option<usize>,
    refine_factor: Option<u32>,
    use_index: Option<bool>,
}

impl NearestBuilder {
    fn build(self) -> Result<Option<NearestOptions>> {
        match (self.column, self.query, self.k) {
            (None, None, None) => {
                if self.nprobes.is_some()
                    || self.refine_factor.is_some()
                    || self.use_index.is_some()
                {
                    return plan_err!(
                        "options '{OPTION_NEAREST_NPROBES}', '{OPTION_NEAREST_REFINE_FACTOR}' and \
                         '{OPTION_NEAREST_USE_INDEX}' require '{OPTION_NEAREST_COLUMN}', \
                         '{OPTION_NEAREST_QUERY}' and '{OPTION_NEAREST_K}'"
                    );
                }
                Ok(None)
            }
            (Some(column), Some(query), Some(k)) => {
                if query.is_empty() {
                    return plan_err!("option '{OPTION_NEAREST_QUERY}' must not be empty");
                }
                if k == 0 {
                    return plan_err!("option '{OPTION_NEAREST_K}' must be greater than zero");
                }
                Ok(Some(NearestOptions {
                    column,
                    query,
                    k,
                    nprobes: self.nprobes,
                    refine_factor: self.refine_factor,
                    use_index: self.use_index.unwrap_or(true),
                }))
            }
            _ => plan_err!(
                "vector search requires all of '{OPTION_NEAREST_COLUMN}', \
                 '{OPTION_NEAREST_QUERY}' and '{OPTION_NEAREST_K}'"
            ),
        }
    }
}

fn normalize_key(key: &str) -> String {
    let key = key.strip_prefix("lance.").unwrap_or(key);
    key.to_ascii_lowercase()
}

fn unknown_option(key: &str, strict: bool, supported: &[&str]) -> Result<()> {
    if !strict || RESERVED_OPTIONS.contains(&key) {
        // Catalog properties and the table location travel in the same option
        // layers as user options and are not addressed to this table format.
        return Ok(());
    }
    plan_err!(
        "unknown option for the Lance table format: '{key}'. Supported options: {}",
        supported.join(", ")
    )
}

fn parse_version(value: &str) -> Result<DatasetRef> {
    match value.parse::<u64>() {
        Ok(version) => Ok(DatasetRef::Version(version)),
        // A non-numeric version is a tag, which is how `lance-spark` reads it.
        Err(_) => Ok(DatasetRef::Tag(value.to_string())),
    }
}

fn parse_usize(key: &str, value: &str) -> Result<usize> {
    value.parse::<usize>().map_err(|e| {
        plan_datafusion_err!("option '{key}' must be a non-negative integer, got '{value}': {e}")
    })
}

fn parse_bool(key: &str, value: &str) -> Result<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => plan_err!("option '{key}' must be 'true' or 'false', got '{value}'"),
    }
}

fn parse_vector(key: &str, value: &str) -> Result<Vec<f32>> {
    value
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split(',')
        .map(|item| {
            item.trim().parse::<f32>().map_err(|e| {
                plan_datafusion_err!(
                    "option '{key}' must be a comma separated list of numbers, \
                     got '{value}': {e}"
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error_message;

    fn option_list(items: &[(&str, &str)]) -> Vec<OptionLayer> {
        vec![OptionLayer::OptionList {
            items: items
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        }]
    }

    #[test]
    fn read_options_accept_lance_prefixed_and_bare_keys() -> Result<()> {
        let options = LanceReadOptions::resolve(&option_list(&[
            ("lance.batch_size", "1024"),
            ("with_row_id", "true"),
        ]))?;
        assert_eq!(options.batch_size, Some(1024));
        assert!(options.with_row_id);
        Ok(())
    }

    #[test]
    fn version_option_falls_back_to_a_tag() -> Result<()> {
        let numeric = LanceReadOptions::resolve(&option_list(&[("version", "3")]))?;
        assert_eq!(numeric.version, Some(DatasetRef::Version(3)));
        let tag = LanceReadOptions::resolve(&option_list(&[("version", "release")]))?;
        assert_eq!(tag.version, Some(DatasetRef::Tag("release".to_string())));
        Ok(())
    }

    #[test]
    fn time_travel_layers_map_to_a_version_or_a_tag() -> Result<()> {
        let by_version =
            LanceReadOptions::resolve(&[OptionLayer::AsOfIntegerVersion { value: 7 }])?;
        assert_eq!(by_version.version, Some(DatasetRef::Version(7)));
        let by_tag = LanceReadOptions::resolve(&[OptionLayer::AsOfStringVersion {
            value: "stable".to_string(),
        }])?;
        assert_eq!(by_tag.version, Some(DatasetRef::Tag("stable".to_string())));
        Ok(())
    }

    #[test]
    fn vector_search_options_are_validated_together() -> Result<()> {
        let options = LanceReadOptions::resolve(&option_list(&[
            ("nearest.column", "vector"),
            ("nearest.query", "[1.0, 2.5, 3]"),
            ("nearest.k", "10"),
            ("nearest.nprobes", "20"),
        ]))?;
        let nearest = options.nearest.ok_or_else(|| {
            plan_datafusion_err!("expected the vector search options to be resolved")
        })?;
        assert_eq!(nearest.column, "vector");
        assert_eq!(nearest.query, vec![1.0, 2.5, 3.0]);
        assert_eq!(nearest.k, 10);
        assert_eq!(nearest.nprobes, Some(20));
        assert!(nearest.use_index);

        let incomplete = error_message(LanceReadOptions::resolve(&option_list(&[(
            "nearest.column",
            "vector",
        )])));
        assert!(incomplete.contains("nearest.k"), "{incomplete}");
        Ok(())
    }

    #[test]
    fn unknown_user_options_are_rejected_but_table_properties_are_not() -> Result<()> {
        let error = error_message(LanceReadOptions::resolve(&option_list(&[("bogus", "1")])));
        assert!(
            error.contains("unknown option"),
            "unexpected error: {error}"
        );
        assert!(
            LanceReadOptions::resolve(&option_list(&[("path", "/tmp/x.lance")])).is_ok(),
            "the table location must be ignored rather than rejected"
        );
        let properties = LanceReadOptions::resolve(&[OptionLayer::TablePropertyList {
            items: vec![("owner".to_string(), "analytics".to_string())],
        }])?;
        assert_eq!(properties, LanceReadOptions::default());
        Ok(())
    }

    #[test]
    fn invalid_option_values_report_the_key_and_the_value() {
        let error = error_message(LanceReadOptions::resolve(&option_list(&[(
            "batch_size",
            "many",
        )])));
        assert!(error.contains("batch_size"), "{error}");
        assert!(error.contains("many"), "{error}");
    }

    #[test]
    fn write_modes_map_to_lance_semantics() -> Result<()> {
        assert_eq!(
            LanceWriteMode::resolve(&SinkMode::Append)?,
            LanceWriteMode::Append
        );
        assert_eq!(
            LanceWriteMode::resolve(&SinkMode::Overwrite)?,
            LanceWriteMode::Overwrite
        );
        assert!(LanceWriteMode::resolve(&SinkMode::OverwritePartitions).is_err());
        Ok(())
    }
}
