use std::any::Any;
use std::sync::{Arc, OnceLock};

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use chrono::{TimeZone, Utc};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BinaryViewArray, BooleanArray, Date32Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, LargeListArray, LargeStringArray, ListArray, MapArray, StringArray, StringBuilder,
    StringViewArray, StructArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use sail_common::spec::{SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME};
use serde_json::{Map, Value};

use crate::functions_nested_utils::opt_downcast_arg;
use crate::functions_utils::make_scalar_function;
use crate::scalar::datetime::utils::spark_datetime_format_to_chrono_strftime;

/// Macro to simplify downcasting arrays and extracting values as JSON
macro_rules! downcast_and_convert {
    ($array:expr, $index:expr, $array_type:ty, $convert:expr) => {{
        let arr = $array
            .as_any()
            .downcast_ref::<$array_type>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(format!(
                    "Failed to downcast to {}",
                    stringify!($array_type)
                ))
            })?;
        Ok($convert(arr.value($index)))
    }};
}

/// Options for to_json function
#[derive(Debug, PartialEq, Eq, Hash)]
struct ToJsonOptions {
    timestamp_format: String,
    date_format: String,
}

impl ToJsonOptions {
    pub const TIMESTAMP_FORMAT_OPTION: &'static str = "timestampFormat";
    pub const DATE_FORMAT_OPTION: &'static str = "dateFormat";
    // Default ISO 8601 format with timezone offset (not Z)
    pub const TIMESTAMP_FORMAT_DEFAULT: &'static str = "%Y-%m-%dT%H:%M:%S%.6f%:z";
    pub const DATE_FORMAT_DEFAULT: &'static str = "%Y-%m-%d";

    /// Build ToJsonOptions from a DataFusion MapArray of key-value pairs.
    fn from_map(map: &MapArray) -> Result<Self> {
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
            timestamp_format,
            date_format,
        })
    }
}

impl Default for ToJsonOptions {
    fn default() -> Self {
        Self {
            timestamp_format: Self::TIMESTAMP_FORMAT_DEFAULT.to_string(),
            date_format: Self::DATE_FORMAT_DEFAULT.to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkToJson {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for SparkToJson {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkToJson {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::OneOf(vec![TypeSignature::Any(1), TypeSignature::Any(2)]),
                Volatility::Immutable,
            ),
            aliases: ["to_json".to_string()],
        }
    }
}

impl ScalarUDFImpl for SparkToJson {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.aliases[0].as_str()
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // If input is a Variant struct, use the shared variant-to-JSON conversion
        // (Spark's to_json supports Variant input and ignores options for it)
        if let Some(field) = args.arg_fields.first() {
            if matches!(field.data_type(), DataType::Struct(_))
                && crate::scalar::variant::utils::helper::try_field_as_variant_array(field).is_ok()
            {
                let result =
                    crate::scalar::variant::spark_variant_to_json::variant_to_json_columnar(
                        &args.args[0],
                    )?;
                // variant_to_json_columnar returns Utf8View, but to_json promises Utf8
                return match result {
                    ColumnarValue::Scalar(ScalarValue::Utf8View(v)) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(v)))
                    }
                    ColumnarValue::Array(arr) => Ok(ColumnarValue::Array(arrow::compute::cast(
                        &arr,
                        &DataType::Utf8,
                    )?)),
                    other => Ok(other),
                };
            }
        }
        make_scalar_function(to_json_inner, vec![])(&args.args)
    }
}

pub fn to_json_udf() -> Arc<ScalarUDF> {
    static STATIC_TO_JSON: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    STATIC_TO_JSON
        .get_or_init(|| Arc::new(ScalarUDF::new_from_impl(SparkToJson::new())))
        .clone()
}

/// Core implementation of the `to_json` function logic.
///
/// # Parameters
/// - `args`: An array of input arrays, where:
///   - `args[0]` is the value to convert to JSON (struct, map, array, etc.)
///   - `args[1]` (optional) is a `MapArray` of options like timestampFormat, dateFormat, etc.
fn to_json_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.is_empty() || args.len() > 2 {
        return Err(datafusion_common::DataFusionError::Plan(
            "to_json requires 1 or 2 arguments".to_string(),
        ));
    }

    let options = if let Some(opts_array) = args.get(1) {
        let map_array = opt_downcast_arg!(opts_array, MapArray).ok_or_else(|| {
            datafusion_common::DataFusionError::Plan(
                "[INVALID_OPTIONS.NON_MAP_FUNCTION] Invalid options: Must use the `map()` function for options.".to_string(),
            )
        })?;
        ToJsonOptions::from_map(map_array)?
    } else {
        ToJsonOptions::default()
    };

    array_to_json_strings(&args[0], &options)
}

fn array_to_json_strings(array: &ArrayRef, options: &ToJsonOptions) -> Result<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 64);

    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            let json_value = array_value_to_json(array, i, options)?;
            let json_string = serde_json::to_string(&json_value)
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
            builder.append_value(&json_string);
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn array_value_to_json(array: &ArrayRef, index: usize, options: &ToJsonOptions) -> Result<Value> {
    if array.is_null(index) {
        return Ok(Value::Null);
    }

    match array.data_type() {
        DataType::Null => Ok(Value::Null),
        DataType::Boolean => downcast_and_convert!(array, index, BooleanArray, Value::Bool),
        DataType::Int8 => {
            downcast_and_convert!(array, index, Int8Array, |v: i8| Value::Number(v.into()))
        }
        DataType::Int16 => {
            downcast_and_convert!(array, index, Int16Array, |v: i16| Value::Number(v.into()))
        }
        DataType::Int32 => {
            downcast_and_convert!(array, index, Int32Array, |v: i32| Value::Number(v.into()))
        }
        DataType::Int64 => {
            downcast_and_convert!(array, index, Int64Array, |v: i64| Value::Number(v.into()))
        }
        DataType::UInt8 => {
            downcast_and_convert!(array, index, UInt8Array, |v: u8| Value::Number(v.into()))
        }
        DataType::UInt16 => {
            downcast_and_convert!(array, index, UInt16Array, |v: u16| Value::Number(v.into()))
        }
        DataType::UInt32 => {
            downcast_and_convert!(array, index, UInt32Array, |v: u32| Value::Number(v.into()))
        }
        DataType::UInt64 => {
            downcast_and_convert!(array, index, UInt64Array, |v: u64| Value::Number(v.into()))
        }
        DataType::Float32 => {
            downcast_and_convert!(array, index, Float32Array, |v: f32| number_from_f64(
                v as f64
            ))
        }
        DataType::Float64 => {
            downcast_and_convert!(array, index, Float64Array, |v: f64| number_from_f64(v))
        }
        DataType::Utf8 => {
            downcast_and_convert!(array, index, StringArray, |v: &str| Value::String(
                v.to_string()
            ))
        }
        DataType::LargeUtf8 => {
            downcast_and_convert!(array, index, LargeStringArray, |v: &str| Value::String(
                v.to_string()
            ))
        }
        DataType::Utf8View => {
            downcast_and_convert!(array, index, StringViewArray, |v: &str| Value::String(
                v.to_string()
            ))
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "Failed to downcast to Date32Array".to_string(),
                    )
                })?;
            let days = arr.value(index);
            Ok(Value::String(format_date(days, &options.date_format)))
        }
        DataType::Decimal128(_, scale) => {
            let arr = array.as_primitive::<datafusion::arrow::datatypes::Decimal128Type>();
            let value = arr.value(index);
            Ok(decimal_to_json_number(value, *scale))
        }
        DataType::Timestamp(_, tz) => {
            let arr =
                array.as_primitive::<datafusion::arrow::datatypes::TimestampMicrosecondType>();
            let value = arr.value(index);
            let formatted = format_timestamp(
                value,
                tz.as_ref().map(|s| s.as_ref()),
                &options.timestamp_format,
            );
            Ok(Value::String(formatted))
        }
        DataType::Struct(_) => {
            let struct_array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "Failed to downcast to StructArray".to_string(),
                    )
                })?;
            struct_to_json(struct_array, index, options)
        }
        DataType::Binary => {
            downcast_and_convert!(array, index, BinaryArray, |v: &[u8]| Value::String(
                BASE64_STANDARD.encode(v)
            ))
        }
        DataType::BinaryView => {
            downcast_and_convert!(array, index, BinaryViewArray, |v: &[u8]| Value::String(
                BASE64_STANDARD.encode(v)
            ))
        }
        DataType::FixedSizeBinary(_) => {
            downcast_and_convert!(
                array,
                index,
                FixedSizeBinaryArray,
                |v: &[u8]| Value::String(BASE64_STANDARD.encode(v))
            )
        }
        DataType::List(_) => {
            let list_array = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "Failed to downcast to ListArray".to_string(),
                )
            })?;
            list_to_json(list_array, index, options)
        }
        DataType::FixedSizeList(_, _) => {
            let values = array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::FixedSizeListArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "Failed to downcast to FixedSizeListArray".to_string(),
                    )
                })?
                .value(index);
            let mut json_values = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                json_values.push(array_value_to_json(&values, i, options)?);
            }
            Ok(Value::Array(json_values))
        }
        DataType::LargeList(_) => {
            let list_array = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "Failed to downcast to LargeListArray".to_string(),
                    )
                })?;
            let values = list_array.value(index);
            let mut json_values = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                json_values.push(array_value_to_json(&values, i, options)?);
            }
            Ok(Value::Array(json_values))
        }
        DataType::Map(_, _) => {
            let map_array = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "Failed to downcast to MapArray".to_string(),
                )
            })?;
            map_to_json(map_array, index, options)
        }
        dt => Err(datafusion_common::DataFusionError::NotImplemented(format!(
            "to_json does not support data type: {:?}",
            dt
        ))),
    }
}

fn struct_to_json(
    struct_array: &StructArray,
    index: usize,
    options: &ToJsonOptions,
) -> Result<Value> {
    let fields = struct_array.fields();
    let columns = struct_array.columns();

    let mut map = Map::new();
    for (field, column) in fields.iter().zip(columns.iter()) {
        // Skip NULL values - PySpark doesn't include them in JSON output
        if !column.is_null(index) {
            let value = array_value_to_json(column, index, options)?;
            map.insert(field.name().clone(), value);
        }
    }

    Ok(Value::Object(map))
}

fn list_to_json(list_array: &ListArray, index: usize, options: &ToJsonOptions) -> Result<Value> {
    let values = list_array.value(index);
    let mut json_values = Vec::with_capacity(values.len());

    for i in 0..values.len() {
        let value = array_value_to_json(&values, i, options)?;
        json_values.push(value);
    }

    Ok(Value::Array(json_values))
}

fn map_to_json(map_array: &MapArray, index: usize, options: &ToJsonOptions) -> Result<Value> {
    let entries = map_array.value(index);
    let struct_array = entries
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "Map entries should be StructArray".to_string(),
            )
        })?;

    let keys = struct_array.column(0);
    let values = struct_array.column(1);

    let mut map = Map::new();
    for i in 0..keys.len() {
        // For map keys, Spark serializes structs as arrays of values (without field names)
        // e.g., named_struct('a', 1) becomes [1] instead of {"a":1}
        let key = map_key_to_json(keys, i, options)?;
        let key_str = match key {
            Value::String(s) => s,
            other => serde_json::to_string(&other)
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?,
        };
        let value = array_value_to_json(values, i, options)?;
        map.insert(key_str, value);
    }

    Ok(Value::Object(map))
}

/// Converts a map key to JSON. For struct keys, Spark serializes them as arrays
/// of values (without field names) rather than as objects.
fn map_key_to_json(array: &ArrayRef, index: usize, options: &ToJsonOptions) -> Result<Value> {
    if array.is_null(index) {
        return Ok(Value::Null);
    }

    // For struct keys, serialize as array of values (Spark behavior)
    if let DataType::Struct(_) = array.data_type() {
        let struct_array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "Failed to downcast to StructArray".to_string(),
                )
            })?;
        return struct_to_values_array(struct_array, index, options);
    }

    // For other types, use normal conversion
    array_value_to_json(array, index, options)
}

/// Converts a struct to a JSON array of its values (without field names).
/// This is used for map keys where Spark expects [value1, value2, ...] format.
fn struct_to_values_array(
    struct_array: &StructArray,
    index: usize,
    options: &ToJsonOptions,
) -> Result<Value> {
    let columns = struct_array.columns();
    let mut json_values = Vec::with_capacity(columns.len());

    for column in columns.iter() {
        let value = array_value_to_json(column, index, options)?;
        json_values.push(value);
    }

    Ok(Value::Array(json_values))
}

fn format_timestamp(value: i64, tz: Option<&str>, format: &str) -> String {
    if let Some(tz_str) = tz {
        // Try to parse the timezone and format with offset
        if let Ok(tz) = tz_str.parse::<chrono_tz::Tz>() {
            if let Some(dt_utc) = Utc.timestamp_micros(value).single() {
                let local_dt = dt_utc.with_timezone(&tz);
                return local_dt.format(format).to_string();
            }
        }
    }

    // Fallback to UTC
    Utc.timestamp_micros(value)
        .single()
        .map(|ts| ts.format(format).to_string())
        .unwrap_or_else(|| value.to_string())
}

fn format_date(days: i32, format: &str) -> String {
    chrono::DateTime::from_timestamp(days as i64 * 24 * 3600, 0)
        .map(|date| date.format(format).to_string())
        .unwrap_or_else(|| days.to_string())
}

fn format_decimal(value: i128, scale: i8) -> String {
    if scale <= 0 {
        return value.to_string();
    }

    let scale = scale as u32;
    let divisor = 10_i128.pow(scale);
    let integer_part = value / divisor;
    let fractional_part = (value % divisor).abs();

    format!(
        "{}.{:0width$}",
        integer_part,
        fractional_part,
        width = scale as usize
    )
}

fn decimal_to_json_number(value: i128, scale: i8) -> Value {
    let decimal_str = format_decimal(value, scale);
    // Parse the formatted decimal string as a JSON number
    // This preserves the exact decimal representation
    // TODO: This parsing approach may not be reliable for all decimal values
    serde_json::from_str(&decimal_str).unwrap_or(Value::String(decimal_str))
}

fn number_from_f64(value: f64) -> Value {
    if value.is_nan() {
        return Value::String("NaN".to_string());
    }
    if value.is_infinite() {
        return if value.is_sign_positive() {
            Value::String("Infinity".to_string())
        } else {
            Value::String("-Infinity".to_string())
        };
    }
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or_else(|| Value::String(value.to_string()))
}

/// Finds the index of a specified key in a MapArray.
fn find_key_index(options: &MapArray, search_key: &str) -> Option<usize> {
    options
        .entries()
        .column_by_name(SAIL_MAP_KEY_FIELD_NAME)
        .and_then(|x| x.as_any().downcast_ref::<StringArray>())
        .and_then(|x| {
            x.iter()
                .enumerate()
                .find(|(_, x)| x.as_ref().is_some_and(|x| *x == search_key))
        })
        .map(|(i, _)| i)
}

/// Retrieves the value associated with a specified key from a MapArray.
fn find_key_value(options: &MapArray, search_key: &str) -> Option<String> {
    if let Some(index) = find_key_index(options, search_key) {
        options
            .entries()
            .column_by_name(SAIL_MAP_VALUE_FIELD_NAME)
            .and_then(|x| x.as_any().downcast_ref::<StringArray>())
            .map(|values| values.value(index).to_string())
    } else {
        None
    }
}
