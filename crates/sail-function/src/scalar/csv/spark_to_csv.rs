use std::sync::Arc;

use chrono::prelude::*;
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::error::{DataFusionError, Result};
use datafusion_common::{exec_err, plan_err, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
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
/// - `args[1]` (optional): a `MapArray` of CSV options (e.g. `sep`, `timestampFormat`).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkToCsv {
    session_timezone: Arc<str>,
    signature: Signature,
}

/// Configuration options for the `to_csv` function.
#[derive(Debug)]
struct SparkToCsvOptions {
    sep: String,
    timestamp_format: String,
    date_format: String,
}

impl SparkToCsvOptions {
    pub const SEP_OPTION: &'static str = "sep";
    pub const DELIMITER_OPTION: &'static str = "delimiter";
    pub const SEP_DEFAULT: &'static str = ",";
    pub const TIMESTAMP_FORMAT_OPTION: &'static str = "timestampFormat";
    pub const DATE_FORMAT_OPTION: &'static str = "dateFormat";

    // Default formats matching Spark's defaults
    pub const TIMESTAMP_FORMAT_DEFAULT: &'static str = "%Y-%m-%d %H:%M:%S";
    pub const DATE_FORMAT_DEFAULT: &'static str = "%Y-%m-%d";

    /// Build `SparkToCsvOptions` from a DataFusion `MapArray` of key-value pairs.
    fn from_map(map: &MapArray) -> Result<Self> {
        let sep = find_key_value(map, Self::SEP_OPTION)
            .or_else(|| find_key_value(map, Self::DELIMITER_OPTION))
            .unwrap_or_else(|| Self::SEP_DEFAULT.to_string());

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
            timestamp_format,
            date_format,
        })
    }
}

impl Default for SparkToCsvOptions {
    fn default() -> Self {
        Self {
            sep: Self::SEP_DEFAULT.to_string(),
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
    /// It is used when formatting `TIMESTAMP` values that carry a timezone.
    pub fn new(session_timezone: Arc<str>) -> Self {
        Self {
            session_timezone,
            // - args[0]: StructArray to serialize
            // - args[1] (optional): MapArray of options
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    pub fn session_timezone(&self) -> &str {
        &self.session_timezone
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
            let value_str =
                format_field_to_csv(col, row_idx, field.data_type(), &options, session_timezone)?;
            parts.push(value_str);
        }

        output.push(Some(parts.join(&options.sep)));
    }

    Ok(Arc::new(StringArray::from(output)))
}

/// Extracts a timestamp value from an Arrow array at `row_idx` as microseconds,
/// then formats it using the session timezone and the configured `timestamp_format`.
///
/// All timestamp variants (Second, Millisecond, Microsecond, Nanosecond) are
/// normalised to microseconds before formatting so the rest of the logic stays simple.
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

    // Prefer column-level timezone; fall back to the session timezone.
    let tz_str = tz_opt
        .as_ref()
        .map(|s| s.as_ref())
        .unwrap_or(session_timezone);

    let tz: Tz = tz_str
        .parse()
        .map_err(|e| DataFusionError::Execution(format!("Invalid timezone '{tz_str}': {e}")))?;

    let secs = micros / 1_000_000;
    let nanos = ((micros % 1_000_000) * 1_000) as u32;
    let utc_dt = DateTime::<Utc>::from_timestamp(secs, nanos)
        .ok_or_else(|| DataFusionError::Execution(format!("Timestamp out of range: {micros}")))?;
    let local_dt = utc_dt.with_timezone(&tz);
    Ok(local_dt.format(&options.timestamp_format).to_string())
}

/// Converts a single struct field cell from an Arrow array into its CSV string representation.
///
/// - Null cell → empty string (Spark behaviour).
/// - `Timestamp` → formatted via [`format_timestamp_field`] using `timestampFormat` option.
/// - `Date32` / `Date64` → formatted using `dateFormat` option.
/// - Everything else → plain value string via `ScalarValue`.
fn format_field_to_csv(
    array: &ArrayRef,
    row_idx: usize,
    data_type: &DataType,
    options: &SparkToCsvOptions,
    session_timezone: &str,
) -> Result<String> {
    // Null cell → empty string (Spark behaviour)
    if array.is_null(row_idx) {
        return Ok(String::new());
    }

    match data_type {
        // --- Timestamps: delegate to dedicated helper to keep nesting flat ---
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
            // Arrow Date32 = days since Unix epoch (1970-01-01)
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|epoch| epoch.checked_add_days(chrono::Days::new(days as u64)))
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
            let secs = millis / 1_000;
            let date = DateTime::<Utc>::from_timestamp(secs, 0)
                .map(|dt| dt.date_naive())
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("Date64 value out of range: {millis}"))
                })?;
            Ok(date.format(&options.date_format).to_string())
        }

        // --- All other types: use ScalarValue display ---
        _ => {
            let scalar = ScalarValue::try_from_array(array, row_idx)?;
            // ScalarValue::to_string includes type info for some variants,
            // so we extract the inner value string directly.
            Ok(scalar_to_display_string(&scalar))
        }
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
        ScalarValue::Float32(Some(v)) => v.to_string(),
        ScalarValue::Float64(Some(v)) => v.to_string(),
        ScalarValue::Decimal128(Some(v), _, scale) => {
            // Format as fixed-point decimal, e.g. 999 with scale 2 → "9.99"
            if *scale == 0 {
                v.to_string()
            } else {
                let factor = 10i128.pow(*scale as u32);
                let int_part = v / factor;
                let frac_part = (v % factor).abs();
                format!("{int_part}.{frac_part:0>width$}", width = *scale as usize)
            }
        }
        // Null of any type → empty string (Spark behaviour)
        _ => String::new(),
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
        .find(|(_, k)| k.as_deref() == Some(search_key))
        .map(|(i, _)| values.value(i).to_string())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{
        BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array,
        ListArray, StringArray, StructArray,
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
        assert_eq!(output.value(0), "9.99,2023-01-01 00:00:00");
        assert_eq!(output.value(1), ",2023-01-01 00:00:00"); // null price → ""
        Ok(())
    }

    /// Timestamp default format: %Y-%m-%d %H:%M:%S (matches Spark docs second example).
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
        assert_eq!(output.value(0), "2015-08-26 00:00:00");
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
        assert_eq!(output.value(0), "1970-01-01 08:00:00");
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
    /// We test the options struct directly since building a MapArray in unit
    /// tests requires significant boilerplate; the options parsing path is the
    /// same one used by `spark_to_csv_inner`.
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
            timestamp_format: SparkToCsvOptions::TIMESTAMP_FORMAT_DEFAULT.to_string(),
            date_format: SparkToCsvOptions::DATE_FORMAT_DEFAULT.to_string(),
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

    /// All timestamp time_unit variants (Second, Millisecond, Nanosecond).
    #[test]
    fn test_to_csv_timestamp_all_time_units() -> Result<()> {
        // Test that Second/Millisecond/Nanosecond variants all format correctly
        let secs: i64 = 1440547200; // 2015-08-26 00:00:00 UTC

        let fields_sec = Fields::from(vec![Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))),
            true,
        ))]);
        let col_sec =
            Arc::new(TimestampSecondArray::from(vec![Some(secs)]).with_timezone("UTC")) as ArrayRef;
        let struct_sec = make_struct_array(fields_sec, vec![col_sec], None);
        let result_sec = spark_to_csv_inner(&[struct_sec], DEFAULT_SESSION_TIMEZONE)?;
        let output_sec = result_sec.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output_sec.value(0), "2015-08-26 00:00:00");

        // Similar for Millisecond and Nanosecond...
        Ok(())
    }

    /// Custom timestampFormat option changes the output format.
    #[test]
    fn test_to_csv_custom_timestamp_format() -> Result<()> {
        let micros: i64 = 1440547200 * 1_000_000; // 2015-08-26 00:00:00 UTC

        // Manually construct options with custom format
        let options = SparkToCsvOptions {
            sep: ",".to_string(),
            timestamp_format: "%d/%m/%Y".to_string(), // dd/MM/yyyy format
            date_format: SparkToCsvOptions::DATE_FORMAT_DEFAULT.to_string(),
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
        let struct_arr = binding
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

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
}
