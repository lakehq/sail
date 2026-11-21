// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use chrono::NaiveDate;
use datafusion::catalog::Session;
use datafusion_common::{Result, ScalarValue, internal_datafusion_err, plan_err};
use datafusion_datasource::ListingTableUrl;
use object_store::path::Path;
use percent_encoding::percent_decode_str;
use sail_common_datafusion::datasource::OptionLayer;
use sail_common_datafusion::session::config::SessionConfigService;

use crate::url::resolve_table_url;

const HIVE_NULL: &str = "__HIVE_DEFAULT_PARTITION__";

pub(super) fn resolve_base_path(
    options: &[OptionLayer],
    urls: &[ListingTableUrl],
) -> Result<Option<ListingTableUrl>> {
    let path = options.iter().rev().find_map(|layer| {
        let (items, key) = match layer {
            OptionLayer::OptionList { items } => (items, "basePath"),
            OptionLayer::TablePropertyList { items } => (items, "option.basePath"),
            _ => return None,
        };
        items
            .iter()
            .rev()
            .find_map(|(k, v)| k.eq_ignore_ascii_case(key).then_some(v))
    });
    let Some(path) = path else { return Ok(None) };
    let url = resolve_table_url(path.clone())?;
    if url.scheme() == "file" && !url.to_file_path().is_ok_and(|p| p.is_dir()) {
        return plan_err!("Option 'basePath' must be an existing directory: {path}");
    }
    let base = ListingTableUrl::try_new(url, None)?;
    for url in urls {
        if base.object_store() != url.object_store() || !url.prefix().prefix_matches(base.prefix())
        {
            return plan_err!("Wrong basePath {path} for the root path: {url}");
        }
        if base.prefix() == url.prefix() && !url.is_collection() {
            return plan_err!("Option 'basePath' must be a directory: {path}");
        }
    }
    Ok(Some(base))
}

pub(super) fn partition_type_inference_enabled(ctx: &dyn Session) -> Result<bool> {
    let key = "spark.sql.sources.partitionColumnTypeInference.enabled";
    let Some(service) = ctx.config().get_extension::<SessionConfigService>() else {
        return Ok(true);
    };
    match service.get_option(key)?.as_deref() {
        None => Ok(true),
        Some(v) if v.eq_ignore_ascii_case("true") => Ok(true),
        Some(v) if v.eq_ignore_ascii_case("false") => Ok(false),
        Some(v) => plan_err!("expected a boolean for {key}, got {v}"),
    }
}

/// Hive partition directories below the discovery root, excluding the file name.
fn partition_parts<'a>(root: &ListingTableUrl, file: &'a Path) -> Result<Vec<(&'a str, &'a str)>> {
    let parts = root.strip_prefix(file).ok_or_else(|| {
        internal_datafusion_err!("file {file} is outside the partition discovery root {root}")
    })?;
    let mut parts = parts.collect::<Vec<_>>();
    parts.pop();
    Ok(parts
        .into_iter()
        .filter_map(|s| s.split_once('='))
        .filter(|(name, _)| !name.is_empty())
        .collect())
}

fn decoded(value: &str) -> Cow<'_, str> {
    percent_decode_str(value)
        .decode_utf8()
        .unwrap_or(Cow::Borrowed(value))
}

fn infer_partition_type(current: &DataType, value: &str) -> DataType {
    let inferred = if value == HIVE_NULL {
        DataType::Null
    } else if value.parse::<i32>().is_ok() {
        DataType::Int32
    } else if value.parse::<i64>().is_ok() {
        DataType::Int64
    } else if value.parse::<f64>().is_ok() {
        DataType::Float64
    } else if value.parse::<NaiveDate>().is_ok() {
        DataType::Date32
    } else {
        DataType::Utf8
    };
    match (current, &inferred) {
        (DataType::Null, _) => inferred,
        (_, DataType::Null) => current.clone(),
        (a, b) if a == b => inferred,
        (DataType::Int32, DataType::Int64) | (DataType::Int64, DataType::Int32) => DataType::Int64,
        (DataType::Int32, DataType::Float64) | (DataType::Float64, DataType::Int32) => {
            DataType::Float64
        }
        _ => DataType::Utf8,
    }
}

#[derive(Default)]
pub(super) struct PartitionDiscovery {
    columns: Option<Vec<(String, DataType)>>,
}

impl PartitionDiscovery {
    pub(super) fn observe(
        &mut self,
        root: &ListingTableUrl,
        file: &Path,
        infer_types: bool,
    ) -> Result<()> {
        let parts = partition_parts(root, file)?;
        let columns = self.columns.get_or_insert_with(|| {
            parts
                .iter()
                .map(|(key, _)| (key.to_string(), DataType::Null))
                .collect()
        });
        if columns
            .iter()
            .map(|(key, _)| key.as_str())
            .ne(parts.iter().map(|(key, _)| *key))
        {
            return plan_err!("found mixed partition columns at {file}: {columns:?} and {parts:?}");
        }
        for ((_, ty), (_, value)) in columns.iter_mut().zip(parts) {
            if *ty == DataType::Utf8 {
                continue;
            }
            if !infer_types {
                *ty = DataType::Utf8;
                continue;
            }
            let value = decoded(value);
            *ty = infer_partition_type(ty, &value);
        }
        Ok(())
    }

    pub(super) fn into_fields(self, nullable: bool) -> Vec<FieldRef> {
        self.columns
            .unwrap_or_default()
            .into_iter()
            .map(|(key, ty)| {
                let ty = if ty == DataType::Null {
                    DataType::Utf8
                } else {
                    ty
                };
                Arc::new(Field::new(key, ty, nullable))
            })
            .collect()
    }
}

pub(super) fn partition_values(
    root: &ListingTableUrl,
    file: &Path,
    fields: &[FieldRef],
) -> Result<Vec<ScalarValue>> {
    let parts = partition_parts(root, file)?;
    if parts.len() < fields.len() {
        return plan_err!("missing partition columns in {file}");
    }
    fields
        .iter()
        .zip(parts)
        .map(|(field, (name, value))| {
            if !name.eq_ignore_ascii_case(field.name()) {
                return plan_err!(
                    "expected partition column {} in {file}, found {name}",
                    field.name()
                );
            }
            let value = decoded(value);
            if value == HIVE_NULL {
                ScalarValue::try_from(field.data_type())
            } else {
                ScalarValue::try_from_string(value.into_owned(), field.data_type())
            }
        })
        .collect()
}

/// Partition values come from directories even when a file has a column with the same name.
/// A user schema may override the discovered type or omit the partition column altogether.
pub(super) fn split_discovered_partitions(
    schema: &Schema,
    discovered: &[FieldRef],
    explicit_schema: bool,
    case_sensitive: bool,
) -> (SchemaRef, Vec<FieldRef>) {
    let matches = |a: &str, b: &str| {
        if case_sensitive {
            a == b
        } else {
            a.eq_ignore_ascii_case(b)
        }
    };
    let mut partitions = discovered.to_vec();
    let mut file_fields = Vec::new();
    for field in schema.fields() {
        if let Some(i) = discovered
            .iter()
            .position(|p| matches(p.name(), field.name()))
        {
            if explicit_schema {
                partitions[i] = Arc::clone(field);
            }
        } else {
            file_fields.push(Arc::clone(field));
        }
    }
    (
        Arc::new(Schema::new_with_metadata(
            file_fields,
            schema.metadata().clone(),
        )),
        partitions,
    )
}
