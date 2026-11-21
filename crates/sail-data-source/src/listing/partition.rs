// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::{NaiveDate, NaiveDateTime};
use datafusion::catalog::Session;
use datafusion_common::{Result, ScalarValue, internal_datafusion_err, plan_err};
use datafusion_datasource::ListingTableUrl;
use object_store::path::Path;
use percent_encoding::percent_decode_str;
use sail_common_datafusion::datasource::OptionLayer;
use sail_common_datafusion::session::config::SessionConfigService;
use sail_common_datafusion::utils::datetime::arrow_timezone_label;

use crate::url::resolve_table_url;

const HIVE_NULL: &str = "__HIVE_DEFAULT_PARTITION__";

#[derive(Clone, Debug)]
pub struct BasePathPartitions {
    pub(super) root: ListingTableUrl,
    case_sensitive: bool,
    timestamp_type: Option<DataType>,
    validate: bool,
    ignore_invalid_paths: bool,
    timezone: Arc<str>,
}

pub(super) fn read_option<'a>(options: &'a [OptionLayer], name: &str) -> Option<&'a str> {
    let property = format!("option.{name}");
    options.iter().rev().find_map(|layer| {
        let (items, key) = match layer {
            OptionLayer::OptionList { items } => (items, name),
            OptionLayer::TablePropertyList { items } => (items, property.as_str()),
            _ => return None,
        };
        items
            .iter()
            .rev()
            .find_map(|(k, v)| k.eq_ignore_ascii_case(key).then_some(v.as_str()))
    })
}

pub(super) fn boolean_option(key: &str, value: Option<&str>, default: bool) -> Result<bool> {
    match value {
        None => Ok(default),
        Some(v) if v.eq_ignore_ascii_case("true") => Ok(true),
        Some(v) if v.eq_ignore_ascii_case("false") => Ok(false),
        Some(v) => plan_err!("expected a boolean for {key}, got {v}"),
    }
}

pub(super) fn resolve_base_path(
    ctx: &dyn Session,
    case_sensitive: bool,
    options: &[OptionLayer],
    urls: &[ListingTableUrl],
    path: &str,
) -> Result<BasePathPartitions> {
    let url = resolve_table_url(path.to_owned())?;
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
    let service = ctx.config().get_extension::<SessionConfigService>();
    let option = |key| {
        service
            .as_ref()
            .map(|s| s.get_option(key))
            .transpose()
            .map(Option::flatten)
    };
    let boolean = |key| boolean_option(key, option(key)?.as_deref(), true);
    let infer = boolean("spark.sql.sources.partitionColumnTypeInference.enabled")?;
    let ignore_invalid_paths = match read_option(options, "ignoreInvalidPartitionPaths") {
        Some(v) => boolean_option("ignoreInvalidPartitionPaths", Some(v), false)?,
        None => {
            let key = "spark.sql.files.ignoreInvalidPartitionPaths";
            boolean_option(key, option(key)?.as_deref(), false)?
        }
    };
    let ntz =
        option("spark.sql.timestampType")?.is_some_and(|v| v.eq_ignore_ascii_case("TIMESTAMP_NTZ"));
    let session_zone = ctx
        .config_options()
        .execution
        .time_zone
        .as_deref()
        .unwrap_or("UTC");
    let timezone = read_option(options, "timeZone").unwrap_or(session_zone);
    let timestamp_type = DataType::Timestamp(
        TimeUnit::Microsecond,
        (!ntz).then(|| Arc::from(arrow_timezone_label(session_zone).as_ref())),
    );
    Ok(BasePathPartitions {
        root: base,
        case_sensitive,
        timestamp_type: infer.then_some(timestamp_type),
        validate: boolean("spark.sql.sources.validatePartitionColumns")?,
        timezone: Arc::from(arrow_timezone_label(timezone).as_ref()),
        ignore_invalid_paths,
    })
}

fn names_match(a: &str, b: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        a == b
    } else {
        a.eq_ignore_ascii_case(b)
    }
}

struct PartitionPath<'a> {
    base: String,
    columns: Vec<(Cow<'a, str>, &'a str)>,
}

fn partition_path<'a>(
    root: &ListingTableUrl,
    file: &'a Path,
    with_base_path: bool,
) -> Result<PartitionPath<'a>> {
    let parts = root.strip_prefix(file).ok_or_else(|| {
        internal_datafusion_err!("file {file} is outside the partition discovery root {root}")
    })?;
    let mut directories = parts.collect::<Vec<_>>();
    directories.pop();
    let mut columns = Vec::new();
    let mut base_len = directories.len();
    for (i, part) in directories.iter().enumerate().rev() {
        if let Some((name, value)) = part.split_once('=') {
            if with_base_path {
                if name.is_empty() || value.is_empty() {
                    return plan_err!("Empty partition column name or value in {file}: {part}");
                }
                columns.push((decoded(name), value));
            } else if !name.is_empty() {
                columns.push((Cow::Borrowed(name), value));
            }
            base_len = i;
        } else if with_base_path && !columns.is_empty() {
            break;
        }
    }
    columns.reverse();
    Ok(PartitionPath {
        base: directories[..base_len].join("/"),
        columns,
    })
}

fn decoded(value: &str) -> Cow<'_, str> {
    percent_decode_str(value)
        .decode_utf8()
        .unwrap_or(Cow::Borrowed(value))
}

fn integer_decimal(value: &str) -> Option<ScalarValue> {
    // BigDecimal accepts underscores in numeric literals. Spark uses Java decimal syntax.
    if value.contains('_') {
        return None;
    }
    let decimal = value.parse::<BigDecimal>().ok()?;
    let scale = decimal.fractional_digit_count();
    if scale > 0 {
        return None;
    }
    let precision = decimal.digits().checked_add(scale.unsigned_abs())?;
    if precision > 38 {
        return None;
    }
    Some(ScalarValue::Decimal128(
        Some(decimal.to_i128()?),
        precision as u8,
        0,
    ))
}

fn infer_partition_type(current: &DataType, value: &str, timestamp_type: &DataType) -> DataType {
    let inferred = if value == HIVE_NULL {
        DataType::Null
    } else if value.parse::<i32>().is_ok() {
        DataType::Int32
    } else if value.parse::<i64>().is_ok() {
        DataType::Int64
    } else if let Some(decimal) = integer_decimal(value) {
        decimal.data_type()
    } else if value.parse::<f64>().is_ok() {
        DataType::Float64
    } else if value.parse::<NaiveDate>().is_ok() {
        DataType::Date32
    } else if NaiveDateTime::parse_from_str(&decoded(value), "%Y-%m-%d %H:%M:%S%.f").is_ok() {
        timestamp_type.clone()
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
        (DataType::Decimal128(a, 0), DataType::Decimal128(b, 0)) => {
            DataType::Decimal128(*a.max(b), 0)
        }
        (DataType::Decimal128(p, 0), DataType::Int32)
        | (DataType::Int32, DataType::Decimal128(p, 0)) => DataType::Decimal128((*p).max(10), 0),
        (DataType::Decimal128(p, 0), DataType::Int64)
        | (DataType::Int64, DataType::Decimal128(p, 0)) => DataType::Decimal128((*p).max(20), 0),
        (DataType::Date32, DataType::Timestamp(..))
        | (DataType::Timestamp(..), DataType::Date32) => timestamp_type.clone(),
        _ => DataType::Utf8,
    }
}

#[derive(Default)]
pub(super) struct PartitionDiscovery {
    columns: Option<Vec<(String, DataType)>>,
    base: Option<String>,
    conflicting_bases: bool,
}

impl PartitionDiscovery {
    pub(super) fn observe(
        &mut self,
        root: &ListingTableUrl,
        file: &Path,
        options: Option<&BasePathPartitions>,
    ) -> Result<bool> {
        let path = partition_path(root, file, options.is_some())?;
        let parts = path.columns;
        if let Some(options) = options {
            self.conflicting_bases |= !options.ignore_invalid_paths
                && self
                    .base
                    .as_ref()
                    .is_some_and(|b| !b.eq_ignore_ascii_case(&path.base));
            self.base.get_or_insert(path.base);
            if parts.is_empty() {
                return Ok(false);
            }
        }
        let columns = self.columns.get_or_insert_with(|| {
            parts
                .iter()
                .map(|(key, _)| (key.to_string(), DataType::Null))
                .collect()
        });
        let case_sensitive = options.is_none_or(|o| o.case_sensitive);
        if columns.len() != parts.len()
            || columns
                .iter()
                .zip(&parts)
                .any(|((key, _), (name, _))| !names_match(key, name, case_sensitive))
        {
            return plan_err!("found mixed partition columns at {file}: {columns:?} and {parts:?}");
        }
        for ((_, ty), (_, value)) in columns.iter_mut().zip(parts) {
            if *ty == DataType::Utf8 || (options.is_some() && value == HIVE_NULL) {
                continue;
            }
            let Some(timestamp_type) = options.and_then(|o| o.timestamp_type.as_ref()) else {
                *ty = DataType::Utf8;
                continue;
            };
            *ty = infer_partition_type(ty, value, timestamp_type);
        }
        Ok(true)
    }

    pub(super) fn into_fields(self, nullable: bool) -> Result<Vec<FieldRef>> {
        if self.conflicting_bases && self.columns.is_some() {
            return plan_err!("Conflicting partition directory structures below basePath");
        }
        Ok(self
            .columns
            .unwrap_or_default()
            .into_iter()
            .map(|(key, ty)| Arc::new(Field::new(key, ty, nullable)))
            .collect())
    }
}

pub(super) fn partition_values(
    options: &BasePathPartitions,
    file: &Path,
    fields: &[FieldRef],
) -> Result<Option<Vec<ScalarValue>>> {
    let parts = partition_path(&options.root, file, true)?.columns;
    if parts.is_empty() {
        return Ok(None);
    }
    if parts.len() < fields.len() {
        return plan_err!("missing partition columns in {file}");
    }
    fields
        .iter()
        .zip(parts)
        .map(|(field, (name, value))| {
            if !names_match(&name, field.name(), options.case_sensitive) {
                return plan_err!(
                    "expected partition column {} in {file}, found {name}",
                    field.name()
                );
            }
            let ty = field.data_type();
            if value == HIVE_NULL {
                return ScalarValue::try_from(ty);
            }
            let result = match ty {
                DataType::Timestamp(unit, Some(_)) => {
                    let local = DataType::Timestamp(*unit, Some(options.timezone.clone()));
                    ScalarValue::try_from_string(decoded(value).into_owned(), &local)
                        .and_then(|v| v.cast_to(ty))
                }
                DataType::Timestamp(_, None) | DataType::Utf8 => {
                    ScalarValue::try_from_string(decoded(value).into_owned(), ty)
                }
                DataType::Decimal128(_, _) if let Some(decimal) = integer_decimal(value) => {
                    decimal.cast_to(ty)
                }
                _ => ScalarValue::try_from_string(value.to_owned(), ty),
            };
            match result {
                Err(_) if !options.validate => ScalarValue::try_from(ty),
                result => result,
            }
        })
        .collect::<Result<_>>()
        .map(Some)
}

/// Partition values come from directories even when a file has a column with the same name.
/// A user schema may override the discovered type or omit the partition column altogether.
pub(super) fn split_discovered_partitions(
    schema: &Schema,
    discovered: &[FieldRef],
    explicit_schema: bool,
    case_sensitive: bool,
    positional_schema: Option<&Schema>,
) -> (SchemaRef, Vec<FieldRef>, Option<Vec<String>>) {
    let partition_index = |name: &str| {
        discovered
            .iter()
            .position(|f| names_match(name, f.name(), case_sensitive))
    };
    let mut partitions = discovered.to_vec();
    let mut file_fields = Vec::new();
    let mut output = Vec::new();
    for field in schema.fields() {
        if let Some(i) = partition_index(field.name()) {
            if explicit_schema {
                partitions[i] = Arc::new(field.as_ref().clone().with_nullable(true));
            } else {
                // Keep an overlapping partition column at its inferred file position.
                output.push(partitions[i].name().clone());
            }
        } else {
            file_fields.push(Arc::clone(field));
            output.push(field.name().clone());
        }
    }
    for field in &partitions {
        if !output.contains(field.name()) {
            output.push(field.name().clone());
        }
    }

    // CSV and text readers need the physical width even for projected-away columns.
    // With an explicit schema CSV assigns the remaining data fields from the first slot.
    if let Some(physical) = positional_schema
        && physical
            .fields()
            .iter()
            .any(|f| partition_index(f.name()).is_some())
    {
        let mut data = file_fields.into_iter();
        let mut physical_fields = Vec::new();
        for (i, field) in physical.fields().iter().enumerate() {
            if (explicit_schema || partition_index(field.name()).is_none())
                && let Some(field) = data.next()
            {
                physical_fields.push(field);
                continue;
            }
            let mut name = format!("__sail_file_field_{i}");
            while output.contains(&name) {
                name.push('_');
            }
            physical_fields.push(Arc::new(Field::new(name, DataType::Utf8, true)));
        }
        physical_fields.extend(data);
        file_fields = physical_fields;
    }
    let projection = file_fields
        .iter()
        .chain(&partitions)
        .map(|f| f.name())
        .ne(&output)
        .then_some(output);
    (
        Arc::new(Schema::new_with_metadata(
            file_fields,
            schema.metadata().clone(),
        )),
        partitions,
        projection,
    )
}
