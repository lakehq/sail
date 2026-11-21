// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::num::NonZeroU8;
use std::sync::{Arc, LazyLock};

use arrow::datatypes::Date32Type;
use arrow_schema::{DataType, Field, FieldRef, IntervalUnit, Schema, SchemaRef, TimeUnit};
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::NaiveDate;
use datafusion::catalog::Session;
use datafusion_common::{
    Result, ScalarValue, internal_datafusion_err, plan_datafusion_err, plan_err,
};
use datafusion_datasource::ListingTableUrl;
use icu_properties::CodePointMapData;
use icu_properties::props::GeneralCategory;
use object_store::path::Path;
use regex::Regex;
use sail_common::spec;
use sail_common_datafusion::datasource::OptionLayer;
use sail_common_datafusion::session::config::SessionConfigService;
use sail_common_datafusion::utils::datetime::{SparkZone, arrow_timezone_label};
use sail_function::scalar::datetime::timestamp::TimestampParser;
use sail_function::scalar::datetime::utils::parse_time_nanoseconds;
use sail_sql_analyzer::literal::interval::{IntervalValue, parse_interval_with_qualifier};
use sail_sql_analyzer::parser::parse_date;

use crate::url::resolve_base_directory_url;

const HIVE_NULL: &str = "__HIVE_DEFAULT_PARTITION__";

#[derive(Debug)]
pub(super) struct BasePathOptions {
    roots: HashSet<String>,
    case_sensitive: bool,
    timestamp_type: Option<DataType>,
    supports_time: bool,
    validate: bool,
    ansi: bool,
    ignore_invalid_paths: bool,
    local_timestamp_parser: TimestampParser,
}

impl BasePathOptions {
    fn timestamp_parser(&self, ty: &DataType) -> &TimestampParser {
        if matches!(ty, DataType::Timestamp(_, Some(_))) {
            &self.local_timestamp_parser
        } else {
            &TimestampParser::Ntz
        }
    }
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
    urls: &[Vec<ListingTableUrl>],
    path: Option<&str>,
) -> Result<BasePathOptions> {
    let roots = if let Some(path) = path {
        let url = resolve_base_directory_url(path)?;
        if url.scheme() == "file" && !url.to_file_path().is_ok_and(|p| p.is_dir()) {
            return plan_err!("Option 'basePath' must be an existing directory: {path}");
        }
        let base = ListingTableUrl::try_new(url, None)?;
        for url in urls.iter().flatten() {
            if base.object_store() != url.object_store()
                || !url.prefix().prefix_matches(base.prefix())
            {
                return plan_err!("Wrong basePath {path} for the root path: {url}");
            }
            if base.prefix() == url.prefix() && !url.is_collection() {
                return plan_err!("Option 'basePath' must be a directory: {path}");
            }
        }
        HashSet::from([qualified_partition_path(&base, base.prefix().as_ref())])
    } else {
        // Spark stops partition discovery at any input directory or parent of an input file.
        urls.iter()
            .flatten()
            .map(|url| {
                let path = url.prefix().as_ref();
                let directory = if url.is_collection() {
                    path
                } else {
                    parent_and_name(path).0
                };
                qualified_partition_path(url, directory)
            })
            .collect()
    };
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
    SparkZone::parse(timezone)?;
    let timestamp_type = DataType::Timestamp(
        TimeUnit::Microsecond,
        (!ntz).then(|| Arc::from(arrow_timezone_label(session_zone).as_ref())),
    );
    Ok(BasePathOptions {
        roots,
        case_sensitive,
        timestamp_type: infer.then_some(timestamp_type),
        // Spark 4.1 introduced TIME partition discovery. It infers TIME even when
        // the SQL syntax for declaring a TIME column is disabled.
        supports_time: service.is_none() || option("spark.sql.timeType.enabled")?.is_some(),
        validate: boolean("spark.sql.sources.validatePartitionColumns")?,
        ansi: boolean("spark.sql.ansi.enabled")?,
        local_timestamp_parser: TimestampParser::Ltz {
            default_timezone: timezone.to_owned(),
        },
        ignore_invalid_paths,
    })
}

pub(super) fn names_match(a: &str, b: &str, case_sensitive: bool) -> bool {
    a == b || (!case_sensitive && a.to_lowercase() == b.to_lowercase())
}

fn parent_and_name(path: &str) -> (&str, &str) {
    path.rsplit_once('/').unwrap_or(("", path))
}

/// Object store paths exclude the scheme and authority. Spark compares qualified directories.
pub(super) fn qualified_partition_path(root: &ListingTableUrl, directory: &str) -> String {
    format!(
        "{}/{directory}",
        &root.get_url()[..url::Position::BeforePath]
    )
}

struct PartitionPath<'a> {
    base: &'a str,
    columns: Vec<(Cow<'a, str>, &'a str)>,
}

fn partition_path<'a>(
    root: &ListingTableUrl,
    file: &'a Path,
    options: Option<&BasePathOptions>,
) -> Result<Option<PartitionPath<'a>>> {
    let Some(options) = options else {
        let parts = root.strip_prefix(file).ok_or_else(|| {
            internal_datafusion_err!("file {file} is outside the partition discovery root {root}")
        })?;
        let mut directories = parts.collect::<Vec<_>>();
        directories.pop();
        let columns = directories
            .into_iter()
            .filter_map(|part| part.split_once('='))
            .filter(|(name, _)| !name.is_empty())
            .map(|(name, value)| (Cow::Borrowed(name), value))
            .collect();
        return Ok(Some(PartitionPath { base: "", columns }));
    };

    let directory = parent_and_name(file.as_ref()).0;
    let mut base = directory;
    let mut columns = Vec::new();
    while !base.is_empty() {
        let (parent, part) = parent_and_name(base);
        if part.eq_ignore_ascii_case("_temporary") {
            return Ok(None);
        }
        if options
            .roots
            .contains(&qualified_partition_path(root, base))
        {
            break;
        }
        if let Some((name, value)) = part.split_once('=') {
            if name.is_empty() || value.is_empty() {
                return plan_err!("Empty partition column name or value in {file}: {part}");
            }
            columns.push((decoded(name), value));
        } else if !columns.is_empty() {
            break;
        }
        base = parent;
    }
    columns.reverse();
    Ok(Some(PartitionPath {
        base: if columns.is_empty() { directory } else { base },
        columns,
    }))
}

fn decoded(value: &str) -> Cow<'_, str> {
    let Some(first) = value.find('%') else {
        return Cow::Borrowed(value);
    };
    let mut result = value[..first].to_owned();
    let mut rest = &value[first..];
    // Hive unescapes each %XX as a character while preserving literal Unicode and invalid escapes.
    while let Some((prefix, suffix)) = rest.split_once('%') {
        result.push_str(prefix);
        if let Some(hex) = suffix.get(..2)
            && hex.as_bytes().iter().all(u8::is_ascii_hexdigit)
            && let Ok(byte) = u8::from_str_radix(hex, 16)
        {
            result.push(char::from(byte));
            rest = &suffix[2..];
        } else {
            result.push('%');
            rest = suffix;
        }
    }
    result.push_str(rest);
    Cow::Owned(result)
}

fn java_double(value: &str) -> Option<f64> {
    // Double.parseDouble accepts ASCII whitespace, numeric suffixes, and C99 hex floats.
    let value = value.trim_matches(|c| c <= ' ');
    let unsigned = value.strip_prefix(['+', '-']).unwrap_or(value);
    let number = match unsigned {
        "Infinity" => f64::INFINITY,
        "NaN" => f64::NAN,
        _ => {
            let literal = unsigned
                .strip_suffix(['d', 'D', 'f', 'F'])
                .unwrap_or(unsigned);
            if let Some(hex) = literal
                .strip_prefix("0x")
                .or_else(|| literal.strip_prefix("0X"))
            {
                hex_double(hex)?
            } else {
                if !literal.starts_with(|c: char| c.is_ascii_digit() || c == '.') {
                    return None;
                }
                literal.parse().ok()?
            }
        }
    };
    Some(if value.starts_with('-') {
        -number
    } else {
        number
    })
}

fn hex_double(hex: &str) -> Option<f64> {
    // A Java hex float uses a binary exponent. Express its significand in binary too,
    // so lexical can round the entire value once, including subnormal values.
    const FORMAT: u128 = lexical_core::NumberFormatBuilder::new()
        .mantissa_radix(2)
        .exponent_radix(NonZeroU8::new(10))
        .required_exponent_notation(true)
        .no_special(true)
        .build_strict();
    let (significand, exponent) = hex.split_once(['p', 'P'])?;
    let mut binary = String::new();
    for c in significand.chars() {
        if c == '.' {
            binary.push('.');
        } else {
            write!(binary, "{:04b}", c.to_digit(16)?).ok()?;
        }
    }
    binary.push('p');
    binary.push_str(exponent);
    lexical_core::parse_with_options::<f64, FORMAT>(
        binary.as_bytes(),
        &lexical_core::parse_float_options::HEX_FLOAT,
    )
    .ok()
}

// Spark uses yyyy-MM-dd and yyyy-MM-dd HH:mm:ss[.S] during partition inference.
#[expect(clippy::expect_used)]
static PARTITION_DATETIME: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?:[0-9]{4}|-[0-9]{4,}|\+[0-9]{5,})-[0-9]{2}-[0-9]{2}(?: [0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9])?)?$")
        .expect("valid partition datetime pattern")
});

// Spark infers TIME using HH:mm:ss[.SSSSSS].
#[expect(clippy::expect_used)]
static PARTITION_TIME: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\.[0-9]{1,6})?$")
        .expect("valid partition time pattern")
});

// Java integer and BigDecimal parsers read UTF-16 code units. Each BMP decimal
// digit range consists of ten consecutive characters starting with zero.
static DECIMAL_DIGIT_RANGES: LazyLock<Vec<std::ops::RangeInclusive<u32>>> = LazyLock::new(|| {
    CodePointMapData::<GeneralCategory>::new()
        .iter_ranges_for_value(GeneralCategory::DecimalNumber)
        .take_while(|range| *range.start() <= u32::from(u16::MAX))
        .collect()
});

fn normalize_decimal_digits(value: &str) -> Cow<'_, str> {
    if value.is_ascii() {
        return Cow::Borrowed(value);
    }
    Cow::Owned(
        value
            .chars()
            .map(|c| {
                let code = u32::from(c);
                let index = DECIMAL_DIGIT_RANGES.partition_point(|range| *range.end() < code);
                match DECIMAL_DIGIT_RANGES.get(index) {
                    Some(range) if range.contains(&code) => {
                        char::from(b'0' + (code - range.start()) as u8)
                    }
                    _ => c,
                }
            })
            .collect(),
    )
}

// Spark first constructs a decimal literal, whose precision and scale must fit Decimal128.
fn partition_decimal(value: &str) -> Option<ScalarValue> {
    // Rust BigDecimal also accepts underscores in numeric literals.
    if value.contains('_') {
        return None;
    }
    let decimal = value.parse::<BigDecimal>().ok()?;
    let scale = decimal.fractional_digit_count();
    let precision = if scale < 0 {
        decimal.digits().checked_add(scale.unsigned_abs())?
    } else {
        decimal.digits().max(scale as u64)
    };
    if precision > 38 {
        return None;
    }
    let scale = scale.max(0);
    let (unscaled, _) = decimal.with_scale(scale).into_bigint_and_exponent();
    Some(ScalarValue::Decimal128(
        Some(unscaled.to_i128()?),
        precision as u8,
        scale as i8,
    ))
}

fn infer_partition_type(
    current: &DataType,
    value: &str,
    timestamp_type: &DataType,
    supports_time: bool,
    timestamp_parser: &TimestampParser,
) -> DataType {
    let normalized = normalize_decimal_digits(value);
    let inferred = if normalized.parse::<i32>().is_ok() {
        DataType::Int32
    } else if normalized.parse::<i64>().is_ok() {
        DataType::Int64
    } else if let Some(decimal @ ScalarValue::Decimal128(_, _, 0)) = partition_decimal(&normalized)
    {
        decimal.data_type()
    } else if java_double(value).is_some() {
        DataType::Float64
    } else if PARTITION_DATETIME.is_match(value) && value.parse::<NaiveDate>().is_ok() {
        DataType::Date32
    } else {
        let value = decoded(value);
        if PARTITION_DATETIME.is_match(&value)
            && value.contains(' ')
            && timestamp_parser.parse_unformatted(&value).is_ok()
        {
            timestamp_type.clone()
        } else if supports_time && PARTITION_TIME.is_match(&value) {
            DataType::Time64(TimeUnit::Microsecond)
        } else {
            DataType::Utf8
        }
    };
    match (current, &inferred) {
        (DataType::Null, _) => inferred,
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

/// Typed partition values indexed by the leaf directory of each file.
pub type BasePathPartitions = HashMap<String, Vec<ScalarValue>>;

#[derive(Default)]
pub(super) struct PartitionDiscovery {
    columns: Option<Vec<(String, DataType)>>,
    base: Option<String>,
    conflicting_bases: bool,
    directories: HashMap<String, Vec<String>>,
}

impl PartitionDiscovery {
    pub(super) fn observe(
        &mut self,
        root: &ListingTableUrl,
        file: &Path,
        options: Option<&BasePathOptions>,
    ) -> Result<bool> {
        let directory =
            options.map(|_| qualified_partition_path(root, parent_and_name(file.as_ref()).0));
        if let Some(values) = directory.as_ref().and_then(|dir| self.directories.get(dir)) {
            return Ok(!values.is_empty());
        }
        let Some(path) = partition_path(root, file, options)? else {
            return Ok(false);
        };
        let parts = path.columns;
        if let Some(options) = options {
            let base = qualified_partition_path(root, path.base);
            self.conflicting_bases |= !options.ignore_invalid_paths
                && self
                    .base
                    .as_ref()
                    .is_some_and(|b| !names_match(b, &base, false));
            self.base.get_or_insert(base);
            if let Some(directory) = directory {
                self.directories.insert(
                    directory,
                    parts.iter().map(|(_, value)| (*value).to_owned()).collect(),
                );
            }
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
            *ty = if let Some(options) = options
                && let Some(timestamp_type) = &options.timestamp_type
            {
                infer_partition_type(
                    ty,
                    value,
                    timestamp_type,
                    options.supports_time,
                    options.timestamp_parser(timestamp_type),
                )
            } else {
                DataType::Utf8
            };
        }
        Ok(true)
    }

    pub(super) fn has_partitions(&self) -> bool {
        self.columns
            .as_ref()
            .is_some_and(|columns| !columns.is_empty())
    }

    pub(super) fn fields(&self, nullable: bool) -> Result<Vec<FieldRef>> {
        if self.conflicting_bases && self.columns.is_some() {
            return plan_err!("Conflicting partition directory structures below basePath");
        }
        Ok(self
            .columns
            .iter()
            .flatten()
            .map(|(key, ty)| Arc::new(Field::new(key, ty.clone(), nullable)))
            .collect())
    }

    pub(super) fn resolve_values(
        self,
        options: &BasePathOptions,
        fields: &[FieldRef],
    ) -> Result<BasePathPartitions> {
        self.directories
            .into_iter()
            .filter(|(_, values)| !values.is_empty())
            .map(|(directory, values)| {
                let values = fields
                    .iter()
                    .zip(values)
                    .map(|(field, value)| cast_partition_value(options, &value, field))
                    .collect::<Result<_>>()?;
                Ok((directory, values))
            })
            .collect()
    }
}

fn cast_partition_value(
    options: &BasePathOptions,
    value: &str,
    field: &Field,
) -> Result<ScalarValue> {
    let ty = field.data_type();
    if value == HIVE_NULL || *ty == DataType::Null {
        return ScalarValue::try_from(ty);
    }
    let result = parse_partition_value(options, value, field);
    match result {
        Err(_)
            if !options.validate
                || (!options.ansi
                    && matches!(
                        ty,
                        DataType::Date32
                            | DataType::Timestamp(_, _)
                            | DataType::Time32(_)
                            | DataType::Time64(_)
                    )) =>
        {
            ScalarValue::try_from(ty)
        }
        result => result,
    }
}

fn parse_partition_value(
    options: &BasePathOptions,
    value: &str,
    field: &Field,
) -> Result<ScalarValue> {
    let ty = field.data_type();
    match ty {
        DataType::Float32 | DataType::Float64 => {
            let number = java_double(value).ok_or_else(|| {
                plan_datafusion_err!("invalid floating-point partition value: {value}")
            })?;
            ScalarValue::Float64(Some(number)).cast_to(ty)
        }
        DataType::Timestamp(_, zone) => {
            let micros = options
                .timestamp_parser(ty)
                .parse_unformatted(&decoded(value))?;
            ScalarValue::TimestampMicrosecond(Some(micros), zone.clone()).cast_to(ty)
        }
        DataType::Date32 => {
            let date = parse_date(value)
                .and_then(NaiveDate::try_from)
                .map_err(|_| plan_datafusion_err!("invalid date partition value: {value}"))?;
            Ok(ScalarValue::Date32(Some(Date32Type::from_naive_date(date))))
        }
        DataType::Boolean => boolean_option("boolean partition value", Some(value), false)
            .map(|v| ScalarValue::Boolean(Some(v))),
        DataType::Time32(_) | DataType::Time64(_) => {
            let nanos = parse_time_nanoseconds(&decoded(value))
                .ok_or_else(|| plan_datafusion_err!("invalid time partition value: {value}"))?;
            ScalarValue::Time64Nanosecond(Some(nanos)).cast_to(ty)
        }
        ty if ty.is_string() => ScalarValue::Utf8(Some(decoded(value).into_owned())).cast_to(ty),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => {
            // Released Spark versions parse an Int and narrow it for byte and short partitions.
            let integer = normalize_decimal_digits(value)
                .parse::<i32>()
                .map_err(|_| plan_datafusion_err!("invalid integer partition value: {value}"))?;
            Ok(match ty {
                DataType::Int8 => ScalarValue::Int8(Some(integer as i8)),
                DataType::Int16 => ScalarValue::Int16(Some(integer as i16)),
                _ => ScalarValue::Int32(Some(integer)),
            })
        }
        DataType::Int64 => normalize_decimal_digits(value)
            .parse::<i64>()
            .map(|v| ScalarValue::Int64(Some(v)))
            .map_err(|_| plan_datafusion_err!("invalid integer partition value: {value}")),
        DataType::Decimal128(_, _) => {
            let literal = partition_decimal(&normalize_decimal_digits(value))
                .ok_or_else(|| plan_datafusion_err!("invalid decimal partition value: {value}"))?;
            // A representable literal that overflows the declared precision becomes null.
            literal.cast_to(ty).or_else(|_| ScalarValue::try_from(ty))
        }
        DataType::Interval(IntervalUnit::YearMonth) | DataType::Duration(TimeUnit::Microsecond) => {
            let qualifier = match field.metadata().get(spec::SAIL_SPARK_INTERVAL_METADATA_KEY) {
                Some(metadata) => spec::SparkIntervalMetadata::from_json(metadata)
                    .map_err(|e| plan_datafusion_err!("{e}"))?,
                None if matches!(ty, DataType::Duration(_)) => {
                    spec::SparkIntervalMetadata::DayTime {
                        start_field: spec::DayTimeIntervalField::Day,
                        end_field: spec::DayTimeIntervalField::Second,
                    }
                }
                None => spec::SparkIntervalMetadata::YearMonth {
                    start_field: spec::YearMonthIntervalField::Year,
                    end_field: spec::YearMonthIntervalField::Month,
                },
            };
            match parse_interval_with_qualifier(&decoded(value), qualifier).map_err(|e| {
                plan_datafusion_err!("invalid interval partition value {value}: {e}")
            })? {
                IntervalValue::YearMonth { months, .. } => {
                    Ok(ScalarValue::IntervalYearMonth(Some(months)))
                }
                IntervalValue::Microsecond { microseconds, .. } => {
                    Ok(ScalarValue::DurationMicrosecond(Some(microseconds)))
                }
                _ => plan_err!("unexpected interval partition value: {value}"),
            }
        }
        DataType::Binary | DataType::BinaryView | DataType::LargeBinary => {
            ScalarValue::Binary(Some(value.as_bytes().to_vec())).cast_to(ty)
        }
        _ => plan_err!(
            "Unsupported partition type {ty} for {}: {value}",
            field.name()
        ),
    }
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
    if let Some(physical) = positional_schema {
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

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;

    use super::*;

    #[test]
    fn partition_discovery_keeps_object_stores_apart() -> Result<()> {
        let inputs = [
            vec![ListingTableUrl::parse("s3://bucket-a/table/")?],
            vec![ListingTableUrl::parse("s3://bucket-b/table/")?],
        ];
        let state = SessionContext::new().state();
        let options = resolve_base_path(&state, false, &[], &inputs, None)?;
        let mut discovery = PartitionDiscovery::default();
        let file = Path::from("table/p=1/part.parquet");
        for root in inputs.iter().flatten() {
            assert!(discovery.observe(root, &file, Some(&options))?);
        }
        assert!(matches!(
            discovery.fields(true),
            Err(datafusion_common::DataFusionError::Plan(message))
                if message.contains("Conflicting partition directory structures")
        ));
        Ok(())
    }
}
