use std::sync::Arc;

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-variant/blob/51e0d4be62d7675e9b7b56ed1c0b0a10ae4a28d7/src/variant_to_json.rs>
use arrow::array::timezone::Tz;
use arrow_schema::{ArrowError, DataType};
use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use parquet_variant::Variant;
use parquet_variant_compute::VariantArray;
use sail_common_datafusion::display::{spark_f32_to_string, spark_f64_to_string};
use sail_common_datafusion::formatter::{
    TimestampMicrosecondFormatter, TimestampNanosecondFormatter,
};

/// Renders a Variant to a JSON string the way Spark does, which `to_json_string`
/// from `parquet_variant_json::VariantToJson` (upstream) does not: it formats
/// `Float`/`Double` with Rust's `Display`, not Java's `Double.toString`/
/// `Float.toString`, so `1e10` round-trips as `10000000000` instead of Spark's
/// `1.0E10`, and `-0e0` loses its sign instead of staying `-0.0`. Everything
/// else here matches the upstream crate's `to_json` exactly (same formats,
/// same recursive structure) -- only the two float branches differ.
fn spark_variant_to_json_string(
    value: &Variant<'_, '_>,
) -> std::result::Result<String, ArrowError> {
    let mut buffer = String::new();
    write_variant_json(value, &mut buffer)?;
    Ok(buffer)
}

fn fmt_error(error: std::fmt::Error) -> ArrowError {
    ArrowError::InvalidArgumentError(format!("JSON encoding error: {error}"))
}

fn write_variant_json(
    value: &Variant<'_, '_>,
    buffer: &mut String,
) -> std::result::Result<(), ArrowError> {
    use std::fmt::Write;
    match value {
        Variant::Null => buffer.push_str("null"),
        Variant::BooleanTrue => buffer.push_str("true"),
        Variant::BooleanFalse => buffer.push_str("false"),
        Variant::Int8(i) => write!(buffer, "{i}").map_err(fmt_error)?,
        Variant::Int16(i) => write!(buffer, "{i}").map_err(fmt_error)?,
        Variant::Int32(i) => write!(buffer, "{i}").map_err(fmt_error)?,
        Variant::Int64(i) => write!(buffer, "{i}").map_err(fmt_error)?,
        Variant::Float(f) => buffer.push_str(&spark_f32_to_string(*f)),
        Variant::Double(f) => buffer.push_str(&spark_f64_to_string(*f)),
        Variant::Decimal4(decimal) => write!(buffer, "{decimal}").map_err(fmt_error)?,
        Variant::Decimal8(decimal) => write!(buffer, "{decimal}").map_err(fmt_error)?,
        Variant::Decimal16(decimal) => write!(buffer, "{decimal}").map_err(fmt_error)?,
        Variant::Date(date) => {
            write!(buffer, "\"{}\"", date.format("%Y-%m-%d")).map_err(fmt_error)?
        }
        Variant::TimestampMicros(ts) | Variant::TimestampNanos(ts) => {
            write!(buffer, "\"{}\"", ts.to_rfc3339()).map_err(fmt_error)?
        }
        Variant::TimestampNtzMicros(ts) => {
            write!(buffer, "\"{}\"", format_timestamp_ntz_string(ts, 6)).map_err(fmt_error)?
        }
        Variant::TimestampNtzNanos(ts) => {
            write!(buffer, "\"{}\"", format_timestamp_ntz_string(ts, 9)).map_err(fmt_error)?
        }
        Variant::Time(time) => {
            write!(buffer, "\"{}\"", format_time_ntz_string(time)).map_err(fmt_error)?
        }
        Variant::Binary(bytes) => {
            let base64_str =
                base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes);
            let json_str = serde_json::to_string(&base64_str).map_err(|e| {
                ArrowError::InvalidArgumentError(format!("JSON encoding error: {e}"))
            })?;
            buffer.push_str(&json_str);
        }
        Variant::String(s) => {
            let json_str = serde_json::to_string(s).map_err(|e| {
                ArrowError::InvalidArgumentError(format!("JSON encoding error: {e}"))
            })?;
            buffer.push_str(&json_str);
        }
        Variant::ShortString(s) => {
            let json_str = serde_json::to_string(s.as_str()).map_err(|e| {
                ArrowError::InvalidArgumentError(format!("JSON encoding error: {e}"))
            })?;
            buffer.push_str(&json_str);
        }
        Variant::Uuid(uuid) => write!(buffer, "\"{uuid}\"").map_err(fmt_error)?,
        Variant::Object(obj) => {
            buffer.push('{');
            for (i, (key, value)) in obj.iter().enumerate() {
                if i > 0 {
                    buffer.push(',');
                }
                let json_key = serde_json::to_string(key).map_err(|e| {
                    ArrowError::InvalidArgumentError(format!("JSON key encoding error: {e}"))
                })?;
                buffer.push_str(&json_key);
                buffer.push(':');
                write_variant_json(&value, buffer)?;
            }
            buffer.push('}');
        }
        Variant::List(arr) => {
            buffer.push('[');
            for (i, element) in arr.iter().enumerate() {
                if i > 0 {
                    buffer.push(',');
                }
                write_variant_json(&element, buffer)?;
            }
            buffer.push(']');
        }
    }
    Ok(())
}

fn format_timestamp_ntz_string(ts: &chrono::NaiveDateTime, precision: usize) -> String {
    use chrono::Timelike as _;
    let _ = ts.nanosecond();
    ts.format(&format!("%Y-%m-%dT%H:%M:%S%.{precision}f"))
        .to_string()
}

fn format_time_ntz_string(time: &chrono::NaiveTime) -> String {
    use chrono::Timelike as _;
    let base = time.format("%H:%M:%S");
    let micros = time.nanosecond() / 1000;
    if micros == 0 {
        format!("{base}.0")
    } else {
        let micros_str = format!("{micros:06}");
        let trimmed = micros_str.trim_end_matches('0');
        format!("{base}.{trimmed}")
    }
}

use crate::error::invalid_arg_count_exec_err;
use crate::scalar::variant::utils::helper::{try_field_as_variant_array, try_parse_string_scalar};

/// Converts a variant ColumnarValue to a Utf8View JSON string representation.
/// This is the shared logic used by both `variant_to_json` and `to_json` (for variant inputs).
pub fn variant_to_json_columnar(arg: &ColumnarValue) -> Result<ColumnarValue> {
    variant_to_string_columnar(arg, false, None)
}

fn variant_string(
    value: Variant<'_, '_>,
    cast: bool,
    timezone: Option<&Tz>,
) -> Result<Option<String>> {
    if cast {
        // VariantGet.cast:439 and :459: variant null becomes SQL NULL and
        // strings are unquoted. Compound values retain their JSON encoding.
        if value == Variant::Null {
            return Ok(None);
        }
        if let Some(value) = value.as_string() {
            return Ok(Some(value.to_owned()));
        }
        // VariantGet.cast delegates primitive values to Spark's typed Cast.
        // JSON uses different quoting, timestamp formatting and binary encoding.
        let primitive = match value {
            Variant::Date(value) => Some(value.format("%Y-%m-%d").to_string()),
            Variant::TimestampMicros(value) => {
                Some(TimestampMicrosecondFormatter(value.timestamp_micros(), timezone).to_string())
            }
            Variant::TimestampNtzMicros(value) => Some(
                TimestampMicrosecondFormatter(value.and_utc().timestamp_micros(), None).to_string(),
            ),
            Variant::TimestampNanos(value) => Some(
                TimestampNanosecondFormatter(
                    value
                        .timestamp_nanos_opt()
                        .ok_or_else(|| exec_datafusion_err!("variant timestamp is out of range"))?,
                    timezone,
                )
                .to_string(),
            ),
            Variant::TimestampNtzNanos(value) => Some(
                TimestampNanosecondFormatter(
                    value
                        .and_utc()
                        .timestamp_nanos_opt()
                        .ok_or_else(|| exec_datafusion_err!("variant timestamp is out of range"))?,
                    None,
                )
                .to_string(),
            ),
            Variant::Binary(value) => Some(
                std::str::from_utf8(value)
                    .map_err(|error| {
                        exec_datafusion_err!("invalid UTF-8 in variant binary: {error}")
                    })?
                    .to_owned(),
            ),
            _ => None,
        };
        if primitive.is_some() {
            return Ok(primitive);
        }
    }
    Ok(Some(spark_variant_to_json_string(&value)?))
}

fn variant_to_string_columnar(
    arg: &ColumnarValue,
    cast: bool,
    timezone: Option<&Tz>,
) -> Result<ColumnarValue> {
    match arg {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Null => Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(None))),
            ScalarValue::Struct(variant_array) => {
                let variant_array = VariantArray::try_new(variant_array.as_ref())?;
                if variant_array.is_empty() {
                    return exec_err!(
                        "Cannot convert empty VariantArray to JSON: the array must contain at least one element"
                    );
                }
                if variant_array.is_null(0) {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(None)))
                } else {
                    let v = variant_array.value(0);
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(
                        variant_string(v, cast, timezone)?,
                    )))
                }
            }
            _ => exec_err!("Unsupported data type: {}", scalar.data_type()),
        },
        ColumnarValue::Array(arr) => match arr.data_type() {
            DataType::Struct(_) => {
                let variant_array = VariantArray::try_new(arr.as_ref())?;
                let mut builder =
                    arrow::array::StringViewBuilder::with_capacity(variant_array.len());
                for variant in variant_array.iter() {
                    match variant {
                        Some(v) => builder.append_option(variant_string(v, cast, timezone)?),
                        None => builder.append_null(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            unsupported => exec_err!("Invalid data type: {unsupported}"),
        },
    }
}

/// Returns a JSON string from a VariantArray
///
/// ## Arguments
/// - expr: a DataType::Struct expression that represents a VariantArray
/// - options: an optional MAP (not yet implemented — currently rejected at runtime)
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkVariantToJsonUdf {
    signature: Signature,
    cast: bool,
}

impl SparkVariantToJsonUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            cast: false,
        }
    }

    pub fn new_cast() -> Self {
        Self {
            cast: true,
            ..Self::new()
        }
    }
}

impl Default for SparkVariantToJsonUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparkVariantToJsonUdf {
    fn name(&self) -> &str {
        if self.cast {
            "spark_variant_to_string"
        } else {
            "variant_to_json"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Spark: "If expr is a VARIANT, the options are ignored."
        // https://docs.databricks.com/en/sql/language-manual/functions/to_json.html

        let field = args
            .arg_fields
            .first()
            .ok_or_else(|| exec_datafusion_err!("missing argument field metadata"))?;

        try_field_as_variant_array(field.as_ref())?;

        let timezone = if self.cast {
            match args.args.get(1) {
                Some(ColumnarValue::Scalar(value)) => try_parse_string_scalar(value)?
                    .map(|value| value.parse::<Tz>())
                    .transpose()?,
                None => None,
                _ => return exec_err!("variant string cast timezone must be a constant string"),
            }
        } else {
            None
        };
        variant_to_string_columnar(&args.args[0], self.cast, timezone.as_ref())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() || arg_types.len() > 2 {
            return Err(invalid_arg_count_exec_err(
                "variant_to_json",
                (1, 2),
                arg_types.len(),
            ));
        }

        // Accept the variant type as-is (it's a Struct with extension type)
        // If there's a second argument (options MAP), accept it as-is too
        Ok(arg_types.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{Field, Fields};
    use parquet_variant_compute::{VariantArrayBuilder, VariantType};
    use serde_json::Value;

    use super::*;
    fn build_variant_array_from_json(value: &Value) -> Result<VariantArray> {
        use parquet_variant_json::JsonToVariant;

        let json_str = value.to_string();
        let mut builder = VariantArrayBuilder::new(1);
        builder.append_json(json_str.as_str())?;

        Ok(builder.build())
    }

    #[test]
    fn test_scalar_primitive() -> Result<()> {
        let expected_json = serde_json::json!("norm");
        let input = build_variant_array_from_json(&expected_json)?;

        let variant_input = ScalarValue::Struct(Arc::new(input.into()));

        let udf = SparkVariantToJsonUdf::default();
        let return_field = Arc::new(Field::new("result", DataType::Utf8View, true));
        let arg_field = Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
        );

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(variant_input)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args)?;

        let ColumnarValue::Scalar(ScalarValue::Utf8View(Some(j))) = result else {
            return exec_err!("expected valid json string");
        };

        assert_eq!(j.as_str(), r#""norm""#);
        Ok(())
    }

    #[test]
    fn test_variant_to_json_udf_scalar_complex() -> Result<()> {
        let expected_json = serde_json::json!({
            "name": "norm",
            "age": 50,
            "list": [false, true, ()]
        });

        let input = build_variant_array_from_json(&expected_json)?;

        let variant_input = ScalarValue::Struct(Arc::new(input.into()));

        let udf = SparkVariantToJsonUdf::default();

        let return_field = Arc::new(Field::new("result", DataType::Utf8View, true));
        let arg_field = Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
        );

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(variant_input)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args)?;

        let ColumnarValue::Scalar(ScalarValue::Utf8View(Some(j))) = result else {
            return exec_err!("expected valid json string");
        };

        let json: Value = serde_json::from_str(j.as_str())
            .map_err(|e| exec_datafusion_err!("failed to parse json: {}", e))?;
        assert_eq!(json, expected_json);
        Ok(())
    }

    #[test]
    fn test_scalar_null_returns_null() -> Result<()> {
        let udf = SparkVariantToJsonUdf::default();
        let return_field = Arc::new(Field::new("result", DataType::Utf8View, true));
        let arg_field = Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
        );

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Null)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args)?;
        let ColumnarValue::Scalar(ScalarValue::Utf8View(None)) = result else {
            return exec_err!("expected NULL Utf8View");
        };
        Ok(())
    }

    #[test]
    fn test_columnar_with_nulls() -> Result<()> {
        use arrow::array::{Array, ArrayRef, StringViewArray, StructArray};
        use parquet_variant_json::JsonToVariant;

        let mut builder = VariantArrayBuilder::new(3);
        builder.append_json(r#"{"a":1}"#)?;
        builder.append_null();
        builder.append_json(r#""hello""#)?;
        let arr: StructArray = builder.build().into();

        let udf = SparkVariantToJsonUdf::default();
        let return_field = Arc::new(Field::new("result", DataType::Utf8View, true));
        let arg_field = Arc::new(
            Field::new("input", DataType::Struct(Fields::empty()), true)
                .with_extension_type(VariantType),
        );

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(arr) as ArrayRef)],
            return_field,
            arg_fields: vec![arg_field],
            number_rows: Default::default(),
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args)?;
        let ColumnarValue::Array(arr) = result else {
            return exec_err!("expected Array");
        };
        let str_arr = arr
            .as_any()
            .downcast_ref::<StringViewArray>()
            .ok_or_else(|| exec_datafusion_err!("expected StringViewArray"))?;

        assert_eq!(str_arr.len(), 3);
        assert_eq!(str_arr.value(0), r#"{"a":1}"#);
        assert!(str_arr.is_null(1));
        assert_eq!(str_arr.value(2), r#""hello""#);
        Ok(())
    }
}
