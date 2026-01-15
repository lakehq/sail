use std::any::Any;
use std::sync::{Arc, OnceLock};

use chrono::{TimeZone, Utc};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, LargeStringArray, ListArray, MapArray, StringArray,
    StringBuilder, StructArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use sail_common::spec::{SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME};
use serde_json::{Map, Value};

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
    // Default ISO 8601 format
    pub const TIMESTAMP_FORMAT_DEFAULT: &'static str = "%Y-%m-%dT%H:%M:%S%.6fZ";
    pub const DATE_FORMAT_DEFAULT: &'static str = "%Y-%m-%d";

    /// Build ToJsonOptions from a DataFusion MapArray of key-value pairs.
    fn from_map(map: &MapArray) -> Self {
        let timestamp_format = find_key_value(map, Self::TIMESTAMP_FORMAT_OPTION)
            .as_deref()
            .map(Self::convert_format)
            .unwrap_or_else(|| Self::TIMESTAMP_FORMAT_DEFAULT.to_string());

        let date_format = find_key_value(map, Self::DATE_FORMAT_OPTION)
            .as_deref()
            .map(Self::convert_format)
            .unwrap_or_else(|| Self::DATE_FORMAT_DEFAULT.to_string());

        Self {
            timestamp_format,
            date_format,
        }
    }

    /// Converts a Spark/Java-style format string (e.g., "yyyy-MM-dd")
    /// into a format compatible with the `chrono` crate (e.g., "%Y-%m-%d").
    fn convert_format(fmt: &str) -> String {
        fmt.replace("yyyy", "%Y")
            .replace("MM", "%m")
            .replace("dd", "%d")
            .replace("HH", "%H")
            .replace("mm", "%M")
            .replace("ss", "%S")
            .replace("SSS", "%.3f")
            .replace("SSSSSS", "%.6f")
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
pub struct ToJson {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for ToJson {
    fn default() -> Self {
        Self::new()
    }
}

impl ToJson {
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

impl ScalarUDFImpl for ToJson {
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
        if args.args.is_empty() || args.args.len() > 2 {
            return Err(datafusion_common::DataFusionError::Plan(
                "to_json requires 1 or 2 arguments".to_string(),
            ));
        }

        // Parse options from second argument if present
        let options = if args.args.len() == 2 {
            match &args.args[1] {
                ColumnarValue::Scalar(ScalarValue::Map(map_array)) => {
                    ToJsonOptions::from_map(map_array.as_ref())
                }
                ColumnarValue::Array(arr) => {
                    if let Some(map_array) = arr.as_any().downcast_ref::<MapArray>() {
                        ToJsonOptions::from_map(map_array)
                    } else {
                        ToJsonOptions::default()
                    }
                }
                _ => ToJsonOptions::default(),
            }
        } else {
            ToJsonOptions::default()
        };

        match &args.args[0] {
            ColumnarValue::Array(array) => {
                let result = array_to_json_strings(array, &options)?;
                Ok(ColumnarValue::Array(result))
            }
            ColumnarValue::Scalar(scalar) => {
                let json_value = scalar_to_json(scalar, &options)?;
                let json_string = serde_json::to_string(&json_value)
                    .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(json_string))))
            }
        }
    }
}

pub fn to_json_udf() -> Arc<ScalarUDF> {
    static STATIC_TO_JSON: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    STATIC_TO_JSON
        .get_or_init(|| Arc::new(ScalarUDF::new_from_impl(ToJson::new())))
        .clone()
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
        DataType::List(_) => {
            let list_array = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "Failed to downcast to ListArray".to_string(),
                )
            })?;
            list_to_json(list_array, index, options)
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
        let value = array_value_to_json(column, index, options)?;
        map.insert(field.name().clone(), value);
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
        let key = array_value_to_json(keys, i, options)?;
        let key_str = match key {
            Value::String(s) => s,
            other => other.to_string(),
        };
        let value = array_value_to_json(values, i, options)?;
        map.insert(key_str, value);
    }

    Ok(Value::Object(map))
}

/// Macro to simplify scalar to JSON conversion for numeric types
macro_rules! scalar_number {
    ($v:expr) => {
        Ok(Value::Number((*$v).into()))
    };
}

fn scalar_to_json(scalar: &ScalarValue, options: &ToJsonOptions) -> Result<Value> {
    match scalar {
        ScalarValue::Null => Ok(Value::Null),
        ScalarValue::Boolean(Some(v)) => Ok(Value::Bool(*v)),
        ScalarValue::Boolean(None) => Ok(Value::Null),
        ScalarValue::Int8(Some(v)) => scalar_number!(v),
        ScalarValue::Int8(None) => Ok(Value::Null),
        ScalarValue::Int16(Some(v)) => scalar_number!(v),
        ScalarValue::Int16(None) => Ok(Value::Null),
        ScalarValue::Int32(Some(v)) => scalar_number!(v),
        ScalarValue::Int32(None) => Ok(Value::Null),
        ScalarValue::Int64(Some(v)) => scalar_number!(v),
        ScalarValue::Int64(None) => Ok(Value::Null),
        ScalarValue::UInt8(Some(v)) => scalar_number!(v),
        ScalarValue::UInt8(None) => Ok(Value::Null),
        ScalarValue::UInt16(Some(v)) => scalar_number!(v),
        ScalarValue::UInt16(None) => Ok(Value::Null),
        ScalarValue::UInt32(Some(v)) => scalar_number!(v),
        ScalarValue::UInt32(None) => Ok(Value::Null),
        ScalarValue::UInt64(Some(v)) => scalar_number!(v),
        ScalarValue::UInt64(None) => Ok(Value::Null),
        ScalarValue::Float32(Some(v)) => Ok(number_from_f64(*v as f64)),
        ScalarValue::Float32(None) => Ok(Value::Null),
        ScalarValue::Float64(Some(v)) => Ok(number_from_f64(*v)),
        ScalarValue::Float64(None) => Ok(Value::Null),
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => Ok(Value::String(v.clone())),
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Utf8View(None) => {
            Ok(Value::Null)
        }
        ScalarValue::Date32(Some(days)) => {
            Ok(Value::String(format_date(*days, &options.date_format)))
        }
        ScalarValue::Date32(None) => Ok(Value::Null),
        ScalarValue::TimestampMicrosecond(Some(v), tz) => Ok(Value::String(format_timestamp(
            *v,
            tz.as_ref().map(|s| s.as_ref()),
            &options.timestamp_format,
        ))),
        ScalarValue::TimestampMicrosecond(None, _) => Ok(Value::Null),
        ScalarValue::Struct(struct_array) => {
            if struct_array.is_empty() {
                return Ok(Value::Null);
            }
            struct_to_json(struct_array.as_ref(), 0, options)
        }
        ScalarValue::List(list_array) => {
            if list_array.is_empty() {
                return Ok(Value::Array(vec![]));
            }
            let values = list_array.value(0);
            let mut json_values = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                let value = array_value_to_json(&values, i, options)?;
                json_values.push(value);
            }
            Ok(Value::Array(json_values))
        }
        ScalarValue::Map(map_array) => {
            if map_array.is_empty() {
                return Ok(Value::Object(Map::new()));
            }
            map_to_json(map_array.as_ref(), 0, options)
        }
        _ => Err(datafusion_common::DataFusionError::NotImplemented(format!(
            "to_json does not support scalar type: {:?}",
            scalar
        ))),
    }
}

fn format_timestamp(value: i64, _tz: Option<&str>, format: &str) -> String {
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

fn number_from_f64(value: f64) -> Value {
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
