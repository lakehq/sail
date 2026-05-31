use std::sync::Arc;

use chrono::prelude::*;
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::error::{DataFusionError, Result};
use datafusion_common::{exec_err, plan_err, ScalarValue};
use datafusion_expr::{ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_expr_common::signature::Volatility;
use sail_common::spec::{SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME};

use crate::functions_utils::make_scalar_function;
use crate::scalar::datetime::utils::spark_datetime_format_to_chrono_strftime;

#[cfg(test)]
const DEFAULT_SESSION_TIMEZONE: &str = "UTC";

/// UDF implementation of `to_csv`, similar to Spark's `to_csv` / `StructsToCsv`.
/// This function serializes a column of struct values into CSV strings.
///
/// Parameters:
/// - `args[0]`: a `StructArray` — each row is one struct to serialize.
/// - `args[1]` (optional): a `MapArray` of CSV options (e.g. `sep`, `timestampFormat`, `dateFormat`).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkToCsv {
    session_timezone: Arc<str>,
    signature: Signature,
}

/// Configuration options for the `to_csv` function.
#[derive(Debug)]
struct SparkToCsvOptions {
    sep: String,
    quote: char,
    escape: char,
    escape_quotes: bool,
    quote_all: bool,
    null_value: String,
    null_value_set: bool,
    empty_value: String,
    ignore_leading_whitespace: bool,
    ignore_trailing_whitespace: bool,
    timestamp_format: String,
    date_format: String,
}

impl SparkToCsvOptions {
    pub const SEP_OPTION: &'static str = "sep";
    pub const DELIMITER_OPTION: &'static str = "delimiter";
    pub const SEP_DEFAULT: &'static str = ",";
    pub const QUOTE_OPTION: &'static str = "quote";
    pub const QUOTE_DEFAULT: char = '"';
    pub const ESCAPE_OPTION: &'static str = "escape";
    pub const ESCAPE_DEFAULT: char = '\\';
    pub const ESCAPE_QUOTES_OPTION: &'static str = "escapeQuotes";
    pub const QUOTE_ALL_OPTION: &'static str = "quoteAll";
    pub const NULL_VALUE_OPTION: &'static str = "nullValue";
    pub const EMPTY_VALUE_OPTION: &'static str = "emptyValue";
    pub const EMPTY_VALUE_DEFAULT: &'static str = "\"\"";
    pub const IGNORE_LEADING_WHITESPACE_OPTION: &'static str = "ignoreLeadingWhiteSpace";
    pub const IGNORE_TRAILING_WHITESPACE_OPTION: &'static str = "ignoreTrailingWhiteSpace";
    pub const TIMESTAMP_FORMAT_OPTION: &'static str = "timestampFormat";
    pub const DATE_FORMAT_OPTION: &'static str = "dateFormat";

    // Default formats matching Spark's defaults
    pub const TIMESTAMP_FORMAT_DEFAULT: &'static str = "%Y-%m-%dT%H:%M:%S%.3f";
    pub const DATE_FORMAT_DEFAULT: &'static str = "%Y-%m-%d";

    /// Build `SparkToCsvOptions` from a DataFusion `MapArray` of key-value pairs.
    fn from_map(map: &MapArray) -> Result<Self> {
        let sep = find_key_value(map, Self::SEP_OPTION)
            .or_else(|| find_key_value(map, Self::DELIMITER_OPTION))
            .unwrap_or_else(|| Self::SEP_DEFAULT.to_string());

        let quote = parse_char_option(
            find_key_value(map, Self::QUOTE_OPTION).as_deref(),
            Self::QUOTE_OPTION,
            Self::QUOTE_DEFAULT,
        )?;
        let escape = parse_char_option(
            find_key_value(map, Self::ESCAPE_OPTION).as_deref(),
            Self::ESCAPE_OPTION,
            Self::ESCAPE_DEFAULT,
        )?;
        let escape_quotes = parse_bool_option(
            find_key_value(map, Self::ESCAPE_QUOTES_OPTION).as_deref(),
            Self::ESCAPE_QUOTES_OPTION,
            true,
        )?;
        let quote_all = parse_bool_option(
            find_key_value(map, Self::QUOTE_ALL_OPTION).as_deref(),
            Self::QUOTE_ALL_OPTION,
            false,
        )?;
        let null_value = find_key_value(map, Self::NULL_VALUE_OPTION);
        let null_value_set = null_value.is_some();
        let null_value = null_value.unwrap_or_default();
        let empty_value = find_key_value(map, Self::EMPTY_VALUE_OPTION)
            .unwrap_or_else(|| Self::EMPTY_VALUE_DEFAULT.to_string());
        let ignore_leading_whitespace = parse_bool_option(
            find_key_value(map, Self::IGNORE_LEADING_WHITESPACE_OPTION).as_deref(),
            Self::IGNORE_LEADING_WHITESPACE_OPTION,
            true,
        )?;
        let ignore_trailing_whitespace = parse_bool_option(
            find_key_value(map, Self::IGNORE_TRAILING_WHITESPACE_OPTION).as_deref(),
            Self::IGNORE_TRAILING_WHITESPACE_OPTION,
            true,
        )?;

        let timestamp_format = find_key_value(map, Self::TIMESTAMP_FORMAT_OPTION)
            .as_deref()
            .map(spark_datetime_format_to_chrono_strftime)
            .transpose()?
            .unwrap_or_else(|| Self::TIMESTAMP_FORMAT_DEFAULT.to_string());

        let date_format = find_key_value(map, Self::DATE_FORMAT_OPTION)
            .as_deref()
            .map(spark_datetime_format_to_chrono_strftime)
            .transpose()?
            .unwrap_or_else(|| Self::DATE_FORMAT_DEFAULT.to_string());

        Ok(Self {
            sep,
            quote,
            escape,
            escape_quotes,
            quote_all,
            null_value,
            null_value_set,
            empty_value,
            ignore_leading_whitespace,
            ignore_trailing_whitespace,
            timestamp_format,
            date_format,
        })
    }
}

impl Default for SparkToCsvOptions {
    fn default() -> Self {
        Self {
            sep: Self::SEP_DEFAULT.to_string(),
            quote: Self::QUOTE_DEFAULT,
            escape: Self::ESCAPE_DEFAULT,
            escape_quotes: true,
            quote_all: false,
            null_value: String::new(),
            null_value_set: false,
            empty_value: Self::EMPTY_VALUE_DEFAULT.to_string(),
            ignore_leading_whitespace: true,
            ignore_trailing_whitespace: true,
            timestamp_format: Self::TIMESTAMP_FORMAT_DEFAULT.to_string(),
            date_format: Self::DATE_FORMAT_DEFAULT.to_string(),
        }
    }
}

impl SparkToCsv {
    pub const TO_CSV_NAME: &'static str = "to_csv";

    /// Constructor for the UDF.
    ///
    /// `session_timezone` is the Spark session timezone (e.g. `"UTC"`, `"Asia/Shanghai"`).
    /// It is used to localize `TIMESTAMP` (LTZ) values when formatting to CSV,
    /// and for codec serialization in distributed mode.
    pub fn new(session_timezone: Arc<str>) -> Self {
        Self {
            session_timezone,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    pub fn session_timezone(&self) -> &str {
        &self.session_timezone
    }

    fn column_name(args: &[Expr]) -> String {
        let Some(input) = args.first() else {
            return format!("{}()", Self::TO_CSV_NAME);
        };
        format!("{}({})", Self::TO_CSV_NAME, input.schema_name())
    }
}

impl ScalarUDFImpl for SparkToCsv {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        Self::TO_CSV_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        Ok(Self::column_name(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let session_timezone = self.session_timezone.to_string();
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(
            move |inner_args| spark_to_csv_inner(inner_args, session_timezone.as_str()),
            vec![],
        )(&args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [DataType::Struct(_)] => Ok(vec![arg_types[0].clone()]),
            [DataType::Struct(_), DataType::Map(_, _)] => {
                Ok(vec![arg_types[0].clone(), arg_types[1].clone()])
            }
            _ => plan_err!(
                "`{}` function requires 1 or 2 arguments: a struct and an optional options map, got {:?}",
                Self::TO_CSV_NAME,
                arg_types
            ),
        }
    }
}

/// Core implementation of the `to_csv` function logic.
///
/// Iterates over each row of the input `StructArray`, serializes each field
/// value to a string, joins them with the configured separator, and returns
/// a `StringArray`.
///
/// # Parameters
/// - `args[0]`: `StructArray` — rows to serialize.
/// - `args[1]` (optional): `MapArray` — CSV options (sep, timestampFormat, dateFormat).
/// - `session_timezone`: the Spark session timezone, used to localize
///   `TIMESTAMP` (LTZ) values when formatting to CSV.
///
/// # Returns
/// A `StringArray` where each entry is the CSV representation of the struct row,
/// or `null` if the input row was null.
fn spark_to_csv_inner(args: &[ArrayRef], session_timezone: &str) -> Result<ArrayRef> {
    if args.is_empty() || args.len() > 2 {
        return exec_err!(
            "`{}` function requires 1 or 2 arguments, got {}",
            SparkToCsv::TO_CSV_NAME,
            args.len()
        );
    }

    let struct_array = args[0]
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "`{}` function requires a StructArray as first argument",
                SparkToCsv::TO_CSV_NAME
            ))
        })?;

    let options: SparkToCsvOptions = if let Some(opts) = args.get(1) {
        let map = opts.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "`{}` function requires a MapArray as second argument",
                SparkToCsv::TO_CSV_NAME
            ))
        })?;
        SparkToCsvOptions::from_map(map)?
    } else {
        SparkToCsvOptions::default()
    };

    let num_rows = struct_array.len();
    let fields = struct_array.fields();
    let columns = struct_array.columns();

    let mut output: Vec<Option<String>> = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        if struct_array.is_null(row_idx) {
            output.push(None);
            continue;
        }

        let mut parts: Vec<String> = Vec::with_capacity(fields.len());

        for (col_idx, field) in fields.iter().enumerate() {
            let col = &columns[col_idx];
            if col.is_null(row_idx) {
                parts.push(options.null_value.clone());
            } else {
                let value_str = format_field_to_csv(
                    col,
                    row_idx,
                    field.data_type(),
                    &options,
                    session_timezone,
                )?;
                parts.push(write_csv_field(&value_str, &options));
            }
        }

        output.push(Some(parts.join(&options.sep)));
    }

    Ok(Arc::new(StringArray::from(output)))
}

/// Extracts a timestamp value from an Arrow array at `row_idx` as microseconds,
/// then formats it as a UTC string using the configured `timestampFormat` option.
///
/// For TIMESTAMP (LTZ), the output is localized to the session timezone with an
/// ISO 8601 offset (e.g. `+08:00` or `Z` for UTC), matching Spark's behaviour.
/// For TIMESTAMP_NTZ, no timezone offset is appended.
fn format_timestamp_field(
    array: &ArrayRef,
    row_idx: usize,
    time_unit: &TimeUnit,
    tz_opt: &Option<Arc<str>>,
    options: &SparkToCsvOptions,
    session_timezone: &str,
) -> Result<String> {
    // Normalise every variant to microseconds for uniform handling.
    let micros = match time_unit {
        TimeUnit::Second => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Expected TimestampSecondArray".to_string())
                })?;
            arr.value(row_idx) * 1_000_000
        }
        TimeUnit::Millisecond => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Expected TimestampMillisecondArray".to_string())
                })?;
            arr.value(row_idx) * 1_000
        }
        TimeUnit::Microsecond => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Expected TimestampMicrosecondArray".to_string())
                })?;
            arr.value(row_idx)
        }
        TimeUnit::Nanosecond => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Expected TimestampNanosecondArray".to_string())
                })?;
            arr.value(row_idx) / 1_000
        }
    };

    let secs = micros.div_euclid(1_000_000);
    let nanos = (micros.rem_euclid(1_000_000) * 1_000) as u32;
    let is_default_format = options.timestamp_format == SparkToCsvOptions::TIMESTAMP_FORMAT_DEFAULT;

    if tz_opt.is_some() {
        // TIMESTAMP LTZ — localize to session timezone and emit offset
        let tz: Tz = session_timezone.parse().map_err(|e| {
            DataFusionError::Execution(format!(
                "Invalid session timezone '{session_timezone}': {e}"
            ))
        })?;
        let utc_dt = DateTime::<Utc>::from_timestamp(secs, nanos).ok_or_else(|| {
            DataFusionError::Execution(format!("Timestamp out of range: {micros}"))
        })?;
        let local_dt = utc_dt.with_timezone(&tz);
        if is_default_format {
            // Spark default: ISO 8601 with offset — Z for UTC, +HH:MM otherwise
            Ok(local_dt
                .format("%Y-%m-%dT%H:%M:%S%.3f%:z")
                .to_string()
                .replace("+00:00", "Z"))
        } else {
            // Custom timestampFormat — apply format, no offset appended
            Ok(local_dt.format(&options.timestamp_format).to_string())
        }
    } else {
        // TIMESTAMP_NTZ — no timezone, no offset suffix
        let naive = DateTime::from_timestamp(secs, nanos)
            .map(|dt| dt.naive_utc())
            .ok_or_else(|| {
                DataFusionError::Execution(format!("Timestamp out of range: {micros}"))
            })?;
        Ok(naive.format(&options.timestamp_format).to_string())
    }
}

/// Converts a single struct field cell from an Arrow array into its CSV string representation.
///
/// - `Timestamp` → formatted via [`format_timestamp_field`] using `timestampFormat` option.
/// -  Null cells are handled upstream in `spark_to_csv_inner` before this function is called.
/// - `Date32` / `Date64` → formatted using `dateFormat` option.
/// - Everything else → plain value string via `ScalarValue`.
fn format_field_to_csv(
    array: &ArrayRef,
    row_idx: usize,
    data_type: &DataType,
    options: &SparkToCsvOptions,
    session_timezone: &str,
) -> Result<String> {
    match data_type {
        DataType::Timestamp(time_unit, tz_opt) => {
            format_timestamp_field(array, row_idx, time_unit, tz_opt, options, session_timezone)
        }

        // --- Dates: format with user-specified or default dateFormat ---
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Execution("Expected Date32Array".to_string()))?;
            let days = arr.value(row_idx);
            // Arrow Date32 = signed days since Unix epoch (1970-01-01)
            // Negative values represent pre-epoch dates (e.g. -1 = 1969-12-31)
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|epoch| epoch.checked_add_signed(chrono::Duration::days(days as i64)))
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("Date32 value out of range: {days}"))
                })?;
            Ok(date.format(&options.date_format).to_string())
        }

        DataType::Date64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| DataFusionError::Execution("Expected Date64Array".to_string()))?;
            let millis = arr.value(row_idx);
            let secs = millis.div_euclid(1_000);
            let date = DateTime::<Utc>::from_timestamp(secs, 0)
                .map(|dt| dt.date_naive())
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("Date64 value out of range: {millis}"))
                })?;
            Ok(date.format(&options.date_format).to_string())
        }

        DataType::List(field) => {
            let arr = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| DataFusionError::Execution("Expected ListArray".to_string()))?;
            format_list_to_csv(
                &arr.value(row_idx),
                field.data_type(),
                options,
                session_timezone,
            )
        }

        DataType::LargeList(field) => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| DataFusionError::Execution("Expected LargeListArray".to_string()))?;
            format_list_to_csv(
                &arr.value(row_idx),
                field.data_type(),
                options,
                session_timezone,
            )
        }

        DataType::FixedSizeList(field, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Expected FixedSizeListArray".to_string())
                })?;
            format_list_to_csv(
                &arr.value(row_idx),
                field.data_type(),
                options,
                session_timezone,
            )
        }

        DataType::Map(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| DataFusionError::Execution("Expected MapArray".to_string()))?;
            format_map_to_csv(arr, row_idx, options, session_timezone)
        }

        DataType::Struct(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| DataFusionError::Execution("Expected StructArray".to_string()))?;
            format_struct_to_csv(arr, row_idx, options, session_timezone)
        }

        // --- All other types: use ScalarValue display ---
        _ => {
            let scalar = ScalarValue::try_from_array(array, row_idx)?;
            // Use ScalarValue::to_string alternatives that return the raw value
            // without type annotations (e.g. "42" not "Int32(42)")
            Ok(scalar_to_display_string(&scalar))
        }
    }
}

fn format_list_to_csv(
    values: &ArrayRef,
    element_type: &DataType,
    options: &SparkToCsvOptions,
    session_timezone: &str,
) -> Result<String> {
    let mut output = String::from("[");
    for i in 0..values.len() {
        if i > 0 {
            output.push(',');
        }
        if values.is_null(i) {
            append_nested_null(&mut output, options);
        } else {
            if i > 0 {
                output.push(' ');
            }
            output.push_str(&format_field_to_csv(
                values,
                i,
                element_type,
                options,
                session_timezone,
            )?);
        }
    }
    output.push(']');
    Ok(output)
}

fn format_map_to_csv(
    map_array: &MapArray,
    index: usize,
    options: &SparkToCsvOptions,
    session_timezone: &str,
) -> Result<String> {
    let entries = map_array.value(index);
    let struct_array = entries
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("Map entries should be StructArray".to_string())
        })?;
    let fields = struct_array.fields();
    let keys = struct_array.column(0);
    let values = struct_array.column(1);
    let key_type = fields[0].data_type();
    let value_type = fields[1].data_type();

    let mut output = String::from("{");
    for i in 0..keys.len() {
        if i > 0 {
            output.push_str(", ");
        }
        output.push_str(&format_field_to_csv(
            keys,
            i,
            key_type,
            options,
            session_timezone,
        )?);
        output.push_str(" ->");
        if values.is_null(i) {
            append_nested_null(&mut output, options);
        } else {
            output.push(' ');
            output.push_str(&format_field_to_csv(
                values,
                i,
                value_type,
                options,
                session_timezone,
            )?);
        }
    }
    output.push('}');
    Ok(output)
}

fn format_struct_to_csv(
    struct_array: &StructArray,
    index: usize,
    options: &SparkToCsvOptions,
    session_timezone: &str,
) -> Result<String> {
    let fields = struct_array.fields();
    let columns = struct_array.columns();
    let mut output = String::from("{");

    for (i, (field, column)) in fields.iter().zip(columns.iter()).enumerate() {
        if i > 0 {
            output.push(',');
        }
        if column.is_null(index) {
            append_nested_null(&mut output, options);
        } else {
            if i > 0 {
                output.push(' ');
            }
            output.push_str(&format_field_to_csv(
                column,
                index,
                field.data_type(),
                options,
                session_timezone,
            )?);
        }
    }
    output.push('}');
    Ok(output)
}

fn append_nested_null(output: &mut String, options: &SparkToCsvOptions) {
    if options.null_value_set {
        // Spark prefixes nested nullValue with a space, including first list/struct elements.
        output.push(' ');
        output.push_str(&options.null_value);
    }
}

/// Converts a `ScalarValue` to its plain display string (no type annotations).
///
/// This mirrors what Spark outputs: just the value, no quotes around strings,
/// no type prefix.
fn scalar_to_display_string(scalar: &ScalarValue) -> String {
    match scalar {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => s.clone(),
        ScalarValue::Boolean(Some(b)) => b.to_string(),
        ScalarValue::Int8(Some(v)) => v.to_string(),
        ScalarValue::Int16(Some(v)) => v.to_string(),
        ScalarValue::Int32(Some(v)) => v.to_string(),
        ScalarValue::Int64(Some(v)) => v.to_string(),
        ScalarValue::UInt8(Some(v)) => v.to_string(),
        ScalarValue::UInt16(Some(v)) => v.to_string(),
        ScalarValue::UInt32(Some(v)) => v.to_string(),
        ScalarValue::UInt64(Some(v)) => v.to_string(),
        ScalarValue::Float32(Some(v)) => format_float32(*v),
        ScalarValue::Float64(Some(v)) => format_float64(*v),
        ScalarValue::Binary(Some(v))
        | ScalarValue::LargeBinary(Some(v))
        | ScalarValue::BinaryView(Some(v))
        | ScalarValue::FixedSizeBinary(_, Some(v)) => format_binary(v),
        ScalarValue::Decimal128(Some(v), _, scale) => {
            // Format as fixed-point decimal, e.g. 999 with scale 2 → "9.99"
            if *scale == 0 {
                v.to_string()
            } else {
                let factor = 10i128.pow(*scale as u32);
                let abs_val = v.abs();
                let int_part = abs_val / factor;
                let frac_part = abs_val % factor;
                let sign = if *v < 0 { "-" } else { "" };
                format!(
                    "{sign}{int_part}.{frac_part:0>width$}",
                    width = *scale as usize
                )
            }
        }
        // Null of any type, or unhandled complex types (List, Map, Struct) returns empty string
        // Complex types are not natively supported in CSV serialization per Spark's behaviour
        _ => String::new(),
    }
}

fn format_binary(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut output = String::from("[");
    for (i, byte) in value.iter().enumerate() {
        if i > 0 {
            output.push(' ');
        }
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output.push(']');
    output
}

fn format_float32(value: f32) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value.is_sign_positive() {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        }
    } else {
        value.to_string()
    }
}

fn format_float64(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value.is_sign_positive() {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        }
    } else {
        value.to_string()
    }
}

// ---------------------------------------------------------------------------
// Helpers (same pattern as spark_from_csv.rs)
// ---------------------------------------------------------------------------

fn find_key_value(options: &MapArray, search_key: &str) -> Option<String> {
    let entries = options.entries();
    let keys = entries
        .column_by_name(SAIL_MAP_KEY_FIELD_NAME)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())?;
    let values = entries
        .column_by_name(SAIL_MAP_VALUE_FIELD_NAME)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())?;

    keys.iter()
        .enumerate()
        .find(|(_, k)| {
            k.as_deref()
                .is_some_and(|k| k.eq_ignore_ascii_case(search_key))
        })
        .and_then(|(i, _)| {
            // Return None if the value is null
            if values.is_null(i) {
                None
            } else {
                Some(values.value(i).to_string())
            }
        })
}

fn parse_char_option(value: Option<&str>, option_name: &str, default: char) -> Result<char> {
    match value {
        None => Ok(default),
        Some(value) => {
            let mut chars = value.chars();
            match (chars.next(), chars.next()) {
                (None, _) => Ok('\0'),
                (Some(ch), None) => Ok(ch),
                _ => exec_err!("CSV option `{option_name}` must be a single character"),
            }
        }
    }
}

fn parse_bool_option(value: Option<&str>, option_name: &str, default: bool) -> Result<bool> {
    match value {
        None => Ok(default),
        Some(value) if value.eq_ignore_ascii_case("true") => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") => Ok(false),
        Some(_) => exec_err!("CSV option `{option_name}` must be `true` or `false`"),
    }
}

/// Applies Spark CSV writer-style field preparation and quoting:
/// - leading/trailing whitespace is trimmed by default
/// - empty strings use `emptyValue`, which defaults to `""`
/// - fields are quoted for separator/newline, `quoteAll`, or quote characters when escaping is enabled
/// - quote characters are escaped after quoting, even when quoting was forced by another reason
/// - NULL fields are handled upstream and never reach this function
fn write_csv_field(value: &str, options: &SparkToCsvOptions) -> String {
    let mut value = value;
    if options.ignore_leading_whitespace {
        value = value.trim_start();
    }
    if options.ignore_trailing_whitespace {
        value = value.trim_end();
    }

    if value.is_empty() {
        return options.empty_value.clone();
    }

    let contains_quote = value.contains(options.quote);
    let needs_quoting = options.quote_all
        || value.contains(&options.sep)
        || value.contains('\n')
        || value.contains('\r')
        || (options.escape_quotes && contains_quote);

    if needs_quoting {
        let mut output = String::with_capacity(value.len() + 2);
        output.push(options.quote);
        for ch in value.chars() {
            if ch == options.quote {
                output.push(options.escape);
            }
            output.push(ch);
        }
        output.push(options.quote);
        output
    } else {
        value.to_string()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{
        BooleanArray, Date32Array, Decimal128Array, FixedSizeListArray, Float32Array, Float64Array,
        Int32Array, Int64Array, ListArray, StringArray, StructArray,
    };
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::{DataType, Field, Fields};

    use super::*;

    // ---------------------------------------------------------------------------
    // Helper: build a StructArray from columns, with optional row-level nulls.
    // ---------------------------------------------------------------------------
    fn make_struct_array(
        fields: Fields,
        columns: Vec<ArrayRef>,
        nulls: Option<Vec<bool>>,
    ) -> ArrayRef {
        Arc::new(StructArray::new(fields, columns, nulls.map(Into::into))) as ArrayRef
    }

    // ---------------------------------------------------------------------------
    // Basic correctness
    // ---------------------------------------------------------------------------

    /// named_struct('a', 1, 'b', 2) → "1,2"  (matches the Spark docs example exactly)
    #[test]
    fn test_to_csv_spark_docs_example() -> Result<()> {
        let fields = Fields::from(vec![
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Field::new("b", DataType::Int32, true)),
        ]);
        let col_a = Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef;
        let col_b = Arc::new(Int32Array::from(vec![Some(2)])) as ArrayRef;

        let struct_array = make_struct_array(fields, vec![col_a, col_b], None);
        let result = spark_to_csv_inner(&[struct_array], DEFAULT_SESSION_TIMEZONE)?;

        let output = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), "1,2");
        Ok(())
    }

    /// Basic struct with INT and STRING fields, including a null field value.
    #[test]
    fn test_to_csv_simple_struct() -> Result<()> {
        let fields = Fields::from(vec![
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Field::new("b", DataType::Utf8, true)),
        ]);
        let col_a = Arc::new(Int32Array::from(vec![Some(1), Some(2), None])) as ArrayRef;
        let col_b = Arc::new(StringArray::from(vec![
            Some("hello"),
            Some("world"),
            Some("x"),
        ])) as ArrayRef;

        let struct_array = make_struct_array(fields, vec![col_a, col_b], None);
        let result = spark_to_csv_inner(&[struct_array], DEFAULT_SESSION_TIMEZONE)?;

        let output = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), "1,hello");
        assert_eq!(output.value(1), "2,world");
        assert_eq!(output.value(2), ",x"); // null int → empty string
        Ok(())
    }

    /// Null struct rows produce null CSV output (not an empty string).
    #[test]
    fn test_to_csv_null_row() -> Result<()> {
        let fields = Fields::from(vec![Arc::new(Field::new("x", DataType::Int32, true))]);
        let col_x = Arc::new(Int32Array::from(vec![Some(42), Some(99)])) as ArrayRef;

        // row 0 valid, row 1 null at the struct level
        let struct_array = make_struct_array(fields, vec![col_x], Some(vec![true, false]));
        let result = spark_to_csv_inner(&[struct_array], DEFAULT_SESSION_TIMEZONE)?;

        let output = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), "42");
        assert!(output.is_null(1)); // struct-level null → CSV null, not ""
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Numeric types
    // ---------------------------------------------------------------------------

    /// Boolean values serialize as "true" / "false", null boolean → empty string.
    #[test]
    fn test_to_csv_boolean_values() -> Result<()> {
        let fields = Fields::from(vec![Arc::new(Field::new("flag", DataType::Boolean, true))]);
        let col = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef;

        let struct_array = make_struct_array(fields, vec![col], None);
        let result = spark_to_csv_inner(&[struct_array], DEFAULT_SESSION_TIMEZONE)?;

        let output = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), "true");
        assert_eq!(output.value(1), "false");
        assert_eq!(output.value(2), ""); // null → empty string
        Ok(())
    }

    /// Float64 and Int64 are serialised correctly.
    #[test]
    fn test_to_csv_float_and_long() -> Result<()> {
        let fields = Fields::from(vec![
            Arc::new(Field::new("score", DataType::Float64, true)),
            Arc::new(Field::new("count", DataType::Int64, true)),
        ]);
        let col_score = Arc::new(Float64Array::from(vec![Some(1.5), Some(0.0), None])) as ArrayRef;
        let col_count =
            Arc::new(Int64Array::from(vec![Some(1_000_000), Some(0), Some(-7)])) as ArrayRef;

        let struct_array = make_struct_array(fields, vec![col_score, col_count], None);
        let result = spark_to_csv_inner(&[struct_array], DEFAULT_SESSION_TIMEZONE)?;

        let output = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), "1.5,1000000");
        assert_eq!(output.value(1), "0,0");
        assert_eq!(output.value(2), ",-7"); // null float → empty string
        Ok(())
    }

    #[test]
    fn test_to_csv_float32_uses_float_display() -> Result<()> {
        let fields = Fields::from(vec![Arc::new(Field::new("value", DataType::Float32, true))]);
        let col = Arc::new(Float32Array::from(vec![
            Some(1.2_f32),
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
        ])) as ArrayRef;

        let struct_array = make_struct_array(fields, vec![col], None);
        let result = spark_to_csv_inner(&[struct_array], DEFAULT_SESSION_TIMEZONE)?;

        let output = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), "1.2");
        assert_eq!(output.value(1), "NaN");
        assert_eq!(output.value(2), "Infinity");
        assert_eq!(output.value(3), "-Infinity");
        Ok(())
    }

    #[test]
    fn test_to_csv_binary_display_string() {
        assert_eq!(
            scalar_to_display_string(&ScalarValue::Binary(Some(vec![0x61, 0x62, 0x63]))),
            "[61 62 63]"
        );
        assert_eq!(
            scalar_to_display_string(&ScalarValue::Binary(Some(vec![0x1a, 0xc0]))),
            "[1A C0]"
        );
    }

    /// DECIMAL(5,2) values are formatted as fixed-point strings (e.g. 999 → "9.99").
    #[test]
    fn test_to_csv_decimal_field() -> Result<()> {
        let fields = Fields::from(vec![Arc::new(Field::new(
            "price",
            DataType::Decimal128(5, 2),
            true,
        ))]);
        // Decimal128 stores the unscaled integer: 9.99 → 999, 12.34 → 1234
        let col = Arc::new(
            Decimal128Array::from(vec![Some(999i128), Some(1234i128), None])
                .with_precision_and_scale(5, 2)
                .unwrap(),
        ) as ArrayRef;

        let struct_array = make_struct_array(fields, vec![col], None);
        let result = spark_to_csv_inner(&[struct_array], DEFAULT_SESSION_TIMEZONE)?;

        let output = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), "9.99");
        assert_eq!(output.value(1), "12.34");
        assert_eq!(output.value(2), ""); // null decimal → empty string
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Timestamp handling
    // ---------------------------------------------------------------------------

    /// DECIMAL and TIMESTAMP together — mirrors test_from_csv_decimal_and_timestamp.
    #[test]
    fn test_to_csv_decimal_and_timestamp() -> Result<()> {
        let fields = Fields::from(vec![
            Arc::new(Field::new("price", DataType::Decimal128(5, 2), true)),
            Arc::new(Field::new(
                "created",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                true,
            )),
        ]);

        // 9.99 → 999 unscaled; 2023-01-01 00:00:00 UTC in micros
        let col_price = Arc::new(
            Decimal128Array::from(vec![Some(999i128), None])
                .with_precision_and_scale(5, 2)
                .unwrap(),
        ) as ArrayRef;
        let micros_2023: i64 = 1672531200 * 1_000_000; // 2023-01-01 00:00:00 UTC
        let col_ts = Arc::new(
            TimestampMicrosecondArray::from(vec![Some(micros_2023), Some(micros_2023)])
                .with_timezone("UTC"),
        ) as ArrayRef;

        let struct_array = make_struct_array(fields, vec![col_price, col_ts], None);
        let result = spark_to_csv_inner(&[struct_array], DEFAULT_SESSION_TIMEZONE)?;

        let output = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), "9.99,2023-01-01T00:00:00.000Z");
        assert_eq!(output.value(1), ",2023-01-01T00:00:00.000Z"); // null price → ""
        Ok(())
    }

    /// Timestamp default format: %Y-%m-%dT%H:%M:%S%.3f with UTC offset (ISO 8601),
    /// matching Spark's default `to_csv` timestamp serialization behaviour
    #[test]
    fn test_to_csv_timestamp_default_format() -> Result<()> {
        // 2015-08-26 00:00:00 UTC in microseconds
        let micros: i64 = 1440547200 * 1_000_000;
        let fields = Fields::from(vec![Arc::new(Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            true,
        ))]);
        let col_ts =
            Arc::new(TimestampMicrosecondArray::from(vec![Some(micros)]).with_timezone("UTC"))
                as ArrayRef;

        let struct_array = make_struct_array(fields, vec![col_ts], None);
        let result = spark_to_csv_inner(&[struct_array], DEFAULT_SESSION_TIMEZONE)?;

        let output = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), "2015-08-26T00:00:00.000Z");
        Ok(())
    }

    /// Session timezone shifts the formatted timestamp — mirrors
    /// test_from_csv_timestamp_uses_session_timezone.
    ///
    /// 1970-01-01 00:00:00 UTC = 1970-01-01 08:00:00 Asia/Shanghai (UTC+8).
    #[test]
    fn test_to_csv_timestamp_uses_session_timezone() -> Result<()> {
        let micros: i64 = 0; // Unix epoch in microseconds
        let fields = Fields::from(vec![Arc::new(Field::new(
            "created",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("Asia/Shanghai"))),
            true,
        ))]);
        let col_ts = Arc::new(
            TimestampMicrosecondArray::from(vec![Some(micros)]).with_timezone("Asia/Shanghai"),
        ) as ArrayRef;

        let struct_array = make_struct_array(fields, vec![col_ts], None);
        let result = spark_to_csv_inner(&[struct_array], "Asia/Shanghai")?;

        let output = result.as_any().downcast_ref::<StringArray>().unwrap();
        // UTC epoch localised to Asia/Shanghai is 08:00:00
        assert_eq!(output.value(0), "1970-01-01T08:00:00.000+08:00");
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Date handling
    // ---------------------------------------------------------------------------

    /// Date32 values are formatted with the default dateFormat (%Y-%m-%d).
    #[test]
    fn test_to_csv_date32_field() -> Result<()> {
        let fields = Fields::from(vec![Arc::new(Field::new("d", DataType::Date32, true))]);
        // Arrow Date32: days since 1970-01-01
        // 2015-08-26 = 16673 days after epoch
        let col = Arc::new(Date32Array::from(vec![Some(16673i32), None])) as ArrayRef;

        let struct_array = make_struct_array(fields, vec![col], None);
        let result = spark_to_csv_inner(&[struct_array], DEFAULT_SESSION_TIMEZONE)?;

        let output = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), "2015-08-26");
        assert_eq!(output.value(1), ""); // null date → empty string
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Options
    // ---------------------------------------------------------------------------

    /// Custom separator via the options map changes the CSV delimiter.
    /// This mirrors the `sep` / `delimiter` option from Spark.
    ///
    /// Tests the separator option by constructing `SparkToCsvOptions` directly.
    /// End-to-end option parsing via `MapArray` is covered by the BDD feature tests.
    #[test]
    fn test_to_csv_custom_separator() -> Result<()> {
        let fields = Fields::from(vec![
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Field::new("b", DataType::Int32, true)),
        ]);
        let col_a = Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef;
        let col_b = Arc::new(Int32Array::from(vec![Some(2)])) as ArrayRef;

        let struct_array = Arc::new(StructArray::new(fields, vec![col_a, col_b], None)) as ArrayRef;

        // Use the options struct directly with a pipe separator
        let options = SparkToCsvOptions {
            sep: "|".to_string(),
            ..SparkToCsvOptions::default()
        };

        let struct_arr = struct_array.as_any().downcast_ref::<StructArray>().unwrap();
        let fields = struct_arr.fields();
        let columns = struct_arr.columns();
        let parts: Vec<String> = fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                format_field_to_csv(
                    &columns[i],
                    0,
                    f.data_type(),
                    &options,
                    DEFAULT_SESSION_TIMEZONE,
                )
            })
            .collect::<Result<_>>()?;

        assert_eq!(parts.join(&options.sep), "1|2");
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Complex types (ARRAY, MAP, nested STRUCT)
    // ---------------------------------------------------------------------------

    /// ARRAY and MAP fields that are null serialize as empty strings —
    /// mirrors test_from_csv_schema_with_list_and_map.
    #[test]
    fn test_to_csv_null_array_and_map_fields() -> Result<()> {
        // Build a ListArray with one null entry
        let list_field = Arc::new(Field::new("item", DataType::Int32, true));
        let offsets = OffsetBuffer::new(vec![0i32, 0].into()); // one empty/null list
        let values = Arc::new(Int32Array::from(Vec::<Option<i32>>::new())) as ArrayRef;
        let list_array = Arc::new(ListArray::new(
            list_field.clone(),
            offsets,
            values,
            Some(vec![false].into()), // null bitmap: the single entry is null
        )) as ArrayRef;

        let fields = Fields::from(vec![
            Arc::new(Field::new("id", DataType::Int32, true)),
            Arc::new(Field::new("tags", DataType::List(list_field), true)),
        ]);
        let col_id = Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef;

        let struct_array = make_struct_array(fields, vec![col_id, list_array], None);
        let result = spark_to_csv_inner(&[struct_array], DEFAULT_SESSION_TIMEZONE)?;

        let output = result.as_any().downcast_ref::<StringArray>().unwrap();
        // id=1, tags=null → "1,"
        assert_eq!(output.value(0), "1,");
        Ok(())
    }

    #[test]
    fn test_to_csv_fixed_size_list_slice_uses_row_offset() -> Result<()> {
        let item_field = Arc::new(Field::new("item", DataType::Int32, true));
        let values =
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)])) as ArrayRef;
        let fixed_array = Arc::new(FixedSizeListArray::try_new(
            item_field.clone(),
            2,
            values,
            None,
        )?);
        let sliced = Arc::new(fixed_array.slice(1, 1)) as ArrayRef;

        let fields = Fields::from(vec![Arc::new(Field::new(
            "items",
            DataType::FixedSizeList(item_field, 2),
            true,
        ))]);
        let struct_array = make_struct_array(fields, vec![sliced], None);
        let result = spark_to_csv_inner(&[struct_array], DEFAULT_SESSION_TIMEZONE)?;

        let output = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), "\"[3, 4]\"");
        Ok(())
    }

    #[test]
    fn test_to_csv_nested_null_value_matches_spark_spacing() -> Result<()> {
        let options = SparkToCsvOptions {
            null_value: "-".to_string(),
            null_value_set: true,
            ..SparkToCsvOptions::default()
        };

        let values = Arc::new(Int32Array::from(vec![None, Some(1)])) as ArrayRef;
        assert_eq!(
            format_list_to_csv(
                &values,
                &DataType::Int32,
                &options,
                DEFAULT_SESSION_TIMEZONE,
            )?,
            "[ -, 1]"
        );

        let fields = Fields::from(vec![
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Field::new("b", DataType::Int32, true)),
        ]);
        let col_a = Arc::new(Int32Array::from(vec![None])) as ArrayRef;
        let col_b = Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef;
        let struct_array = StructArray::new(fields, vec![col_a, col_b], None);
        assert_eq!(
            format_struct_to_csv(&struct_array, 0, &options, DEFAULT_SESSION_TIMEZONE)?,
            "{ -, 1}"
        );
        Ok(())
    }

    /// Nested STRUCT fields that are null serialize as empty strings —
    /// mirrors test_from_csv_schema_nested_struct.
    #[test]
    fn test_to_csv_nested_struct_null() -> Result<()> {
        let inner_fields = Fields::from(vec![
            Arc::new(Field::new("city", DataType::Utf8, true)),
            Arc::new(Field::new("zip", DataType::Int32, true)),
        ]);
        let inner_city = Arc::new(StringArray::from(vec![Option::<&str>::None])) as ArrayRef;
        let inner_zip = Arc::new(Int32Array::from(vec![Option::<i32>::None])) as ArrayRef;

        // The inner struct itself is null
        let inner_struct = Arc::new(StructArray::new(
            inner_fields.clone(),
            vec![inner_city, inner_zip],
            Some(vec![false].into()), // null bitmap: the single row is null
        )) as ArrayRef;

        let outer_fields = Fields::from(vec![
            Arc::new(Field::new("id", DataType::Int32, true)),
            Arc::new(Field::new("addr", DataType::Struct(inner_fields), true)),
        ]);
        let col_id = Arc::new(Int32Array::from(vec![Some(42)])) as ArrayRef;

        let struct_array = make_struct_array(outer_fields, vec![col_id, inner_struct], None);
        let result = spark_to_csv_inner(&[struct_array], DEFAULT_SESSION_TIMEZONE)?;

        let output = result.as_any().downcast_ref::<StringArray>().unwrap();
        // id=42, addr=null struct → "42,"
        assert_eq!(output.value(0), "42,");
        Ok(())
    }

    /// Date64 values are formatted with dateFormat (similar to Date32).
    #[test]
    fn test_to_csv_date64_field() -> Result<()> {
        let fields = Fields::from(vec![Arc::new(Field::new("d", DataType::Date64, true))]);
        // Arrow Date64: milliseconds since epoch
        let millis: i64 = 1440547200 * 1_000; // 2015-08-26 in millis
        let col = Arc::new(Date64Array::from(vec![Some(millis), None])) as ArrayRef;

        let struct_array = make_struct_array(fields, vec![col], None);
        let result = spark_to_csv_inner(&[struct_array], DEFAULT_SESSION_TIMEZONE)?;

        let output = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), "2015-08-26");
        assert_eq!(output.value(1), "");
        Ok(())
    }

    /// All timestamp time_unit variants (Second, Millisecond, Microsecond, Nanosecond)
    /// are normalised to microseconds and formatted identically
    #[test]
    fn test_to_csv_timestamp_all_time_units() -> Result<()> {
        let secs: i64 = 1440547200; // 2015-08-26 00:00:00 UTC

        // --- Second ---
        let fields = Fields::from(vec![Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))),
            true,
        ))]);
        let col =
            Arc::new(TimestampSecondArray::from(vec![Some(secs)]).with_timezone("UTC")) as ArrayRef;
        let result = spark_to_csv_inner(
            &[make_struct_array(fields, vec![col], None)],
            DEFAULT_SESSION_TIMEZONE,
        )?;
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "2015-08-26T00:00:00.000Z"
        );

        // --- Millisecond ---
        let fields = Fields::from(vec![Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
            true,
        ))]);
        let col = Arc::new(
            TimestampMillisecondArray::from(vec![Some(secs * 1_000)]).with_timezone("UTC"),
        ) as ArrayRef;
        let result = spark_to_csv_inner(
            &[make_struct_array(fields, vec![col], None)],
            DEFAULT_SESSION_TIMEZONE,
        )?;
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "2015-08-26T00:00:00.000Z"
        );

        // --- Microsecond ---
        let fields = Fields::from(vec![Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            true,
        ))]);
        let col = Arc::new(
            TimestampMicrosecondArray::from(vec![Some(secs * 1_000_000)]).with_timezone("UTC"),
        ) as ArrayRef;
        let result = spark_to_csv_inner(
            &[make_struct_array(fields, vec![col], None)],
            DEFAULT_SESSION_TIMEZONE,
        )?;
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "2015-08-26T00:00:00.000Z"
        );

        // --- Nanosecond ---
        let fields = Fields::from(vec![Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
            true,
        ))]);
        let col = Arc::new(
            TimestampNanosecondArray::from(vec![Some(secs * 1_000_000_000)]).with_timezone("UTC"),
        ) as ArrayRef;
        let result = spark_to_csv_inner(
            &[make_struct_array(fields, vec![col], None)],
            DEFAULT_SESSION_TIMEZONE,
        )?;
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "2015-08-26T00:00:00.000Z"
        );

        Ok(())
    }

    /// Custom timestampFormat option changes the output format.
    #[test]
    fn test_to_csv_custom_timestamp_format() -> Result<()> {
        let micros: i64 = 1440547200 * 1_000_000; // 2015-08-26 00:00:00 UTC

        // Manually construct options with custom format
        let options = SparkToCsvOptions {
            timestamp_format: "%d/%m/%Y".to_string(), // dd/MM/yyyy format
            ..SparkToCsvOptions::default()
        };

        let fields = Fields::from(vec![Arc::new(Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            true,
        ))]);
        let col_ts =
            Arc::new(TimestampMicrosecondArray::from(vec![Some(micros)]).with_timezone("UTC"))
                as ArrayRef;

        let binding = make_struct_array(fields, vec![col_ts], None);
        let struct_arr = binding.as_any().downcast_ref::<StructArray>().unwrap();

        let field = &struct_arr.fields()[0];
        let result = format_field_to_csv(
            &struct_arr.columns()[0],
            0,
            field.data_type(),
            &options,
            DEFAULT_SESSION_TIMEZONE,
        )?;

        assert_eq!(result, "26/08/2015");
        Ok(())
    }

    /// Fields containing the separator are quoted per RFC4180, matches Spark behaviour
    #[test]
    fn test_to_csv_quoting_separator_in_field() -> Result<()> {
        let fields = Fields::from(vec![
            Arc::new(Field::new("a", DataType::Utf8, true)),
            Arc::new(Field::new("b", DataType::Int32, true)),
        ]);
        let col_a = Arc::new(StringArray::from(vec![Some("hello,world")])) as ArrayRef;
        let col_b = Arc::new(Int32Array::from(vec![Some(2)])) as ArrayRef;

        let struct_array = make_struct_array(fields, vec![col_a, col_b], None);
        let result = spark_to_csv_inner(&[struct_array], DEFAULT_SESSION_TIMEZONE)?;

        let output = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), "\"hello,world\",2");
        Ok(())
    }

    /// Fields containing double quotes are quoted and inner quotes escaped with backslash
    #[test]
    fn test_to_csv_quoting_double_quote_in_field() -> Result<()> {
        let fields = Fields::from(vec![
            Arc::new(Field::new("a", DataType::Utf8, true)),
            Arc::new(Field::new("b", DataType::Int32, true)),
        ]);
        let col_a = Arc::new(StringArray::from(vec![Some("say \"hello\"")])) as ArrayRef;
        let col_b = Arc::new(Int32Array::from(vec![Some(2)])) as ArrayRef;

        let struct_array = make_struct_array(fields, vec![col_a, col_b], None);
        let result = spark_to_csv_inner(&[struct_array], DEFAULT_SESSION_TIMEZONE)?;

        let output = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), "\"say \\\"hello\\\"\",2");
        Ok(())
    }

    #[test]
    fn test_to_csv_escape_quotes_false_still_escapes_when_quoted() {
        let options = SparkToCsvOptions {
            escape_quotes: false,
            ..SparkToCsvOptions::default()
        };
        assert_eq!(write_csv_field("say \"hello\"", &options), "say \"hello\"");
        assert_eq!(
            write_csv_field("hello, \"world\"", &options),
            "\"hello, \\\"world\\\"\""
        );

        let quote_all_options = SparkToCsvOptions {
            escape_quotes: false,
            quote_all: true,
            ..SparkToCsvOptions::default()
        };
        assert_eq!(
            write_csv_field("say \"hello\"", &quote_all_options),
            "\"say \\\"hello\\\"\""
        );
    }

    /// Empty string fields (not null) are always quoted, matches Spark behaviour
    #[test]
    fn test_to_csv_quoting_empty_string_field() -> Result<()> {
        let fields = Fields::from(vec![
            Arc::new(Field::new("a", DataType::Utf8, true)),
            Arc::new(Field::new("b", DataType::Int32, true)),
        ]);
        let col_a = Arc::new(StringArray::from(vec![Some("")])) as ArrayRef;
        let col_b = Arc::new(Int32Array::from(vec![Some(2)])) as ArrayRef;

        let struct_array = make_struct_array(fields, vec![col_a, col_b], None);
        let result = spark_to_csv_inner(&[struct_array], DEFAULT_SESSION_TIMEZONE)?;

        let output = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), "\"\",2");
        Ok(())
    }
}
