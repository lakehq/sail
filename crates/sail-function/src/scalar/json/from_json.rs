use core::any::type_name;
use std::sync::Arc;

use chrono::prelude::*;
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::*;
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::*;
use datafusion::error::{DataFusionError, Result};
use datafusion_common::{exec_err, internal_err, plan_err, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_expr_common::signature::Volatility;
use sail_common::spec::{
    self, SAIL_LIST_FIELD_NAME, SAIL_MAP_FIELD_NAME, SAIL_MAP_KEY_FIELD_NAME,
    SAIL_MAP_VALUE_FIELD_NAME,
};
use sail_common_datafusion::utils::datetime::localize_with_fallback;
use sail_sql_analyzer::data_type::from_ast_data_type;
use sail_sql_analyzer::parser as sail_parser;
use serde_json::Value;

use crate::functions_nested_utils::*;
use crate::functions_utils::make_scalar_function;
use crate::scalar::datetime::utils::spark_datetime_format_to_chrono_strftime;

/// UDF implementation of `from_json`, similar to Spark's `from_json`.
/// This function parses a column of JSON strings using a specified schema
/// and returns a structured Arrow array (Struct, Array, or Map) with the parsed fields.
///
/// Parameters:
/// - The first input is a `StringArray` containing JSON-formatted values.
/// - The second input is a schema specifying the target type, either as a DDL string
///   (e.g., "a INT, b DOUBLE") or as a typed literal (Struct/Map/List).
/// - Optionally, a third input as a `MapArray` containing parsing options
///   (e.g., timestampFormat, dateFormat, mode).
///
/// [Credit] The core JSON-to-Arrow parsing logic is inspired by
/// [DataFusion Comet](https://github.com/apache/datafusion-comet/blob/main/native/spark-expr/src/json_funcs/from_json.rs),
/// licensed under the Apache License, Version 2.0.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkFromJson {
    session_timezone: Arc<str>,
    signature: Signature,
}

/// Configuration options for the `from_json` function.
#[derive(Debug)]
struct SparkFromJsonOptions {
    timestamp_format: String,
    date_format: String,
}

impl SparkFromJsonOptions {
    pub const TIMESTAMP_FORMAT_OPTION: &'static str = "timestampFormat";
    pub const DATE_FORMAT_OPTION: &'static str = "dateFormat";
    // Default formats matching Spark's behavior
    pub const TIMESTAMP_FORMAT_DEFAULT: &'static str = "%Y-%m-%d %H:%M:%S";
    pub const DATE_FORMAT_DEFAULT: &'static str = "%Y-%m-%d";

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

impl Default for SparkFromJsonOptions {
    fn default() -> Self {
        Self {
            timestamp_format: Self::TIMESTAMP_FORMAT_DEFAULT.to_string(),
            date_format: Self::DATE_FORMAT_DEFAULT.to_string(),
        }
    }
}

impl SparkFromJson {
    pub const FROM_JSON_NAME: &'static str = "from_json";

    pub fn new(session_timezone: Arc<str>) -> Self {
        Self {
            session_timezone,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    pub fn session_timezone(&self) -> &str {
        &self.session_timezone
    }
}

impl ScalarUDFImpl for SparkFromJson {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        Self::FROM_JSON_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Struct(Fields::empty()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let ReturnFieldArgs {
            scalar_arguments, ..
        } = args;
        let schema: &String = if let Some(schema) = scalar_arguments.get(1) {
            match schema {
                Some(ScalarValue::Utf8(Some(schema)))
                | Some(ScalarValue::LargeUtf8(Some(schema)))
                | Some(ScalarValue::Utf8View(Some(schema))) => Ok(schema),
                _ => internal_err!("Expected UTF-8 schema string"),
            }?
        } else {
            return plan_err!(
                "`{}` function requires 2 or 3 arguments, got {}",
                Self::FROM_JSON_NAME,
                scalar_arguments.len()
            );
        };

        let dt = parse_schema_to_data_type(schema, &self.session_timezone)?;
        Ok(Arc::new(Field::new(self.name(), dt, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let session_timezone = self.session_timezone.to_string();
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(
            move |inner_args| from_json_inner(inner_args, session_timezone.as_str()),
            vec![],
        )(&args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [DataType::Null | DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8] => {
                Ok(vec![DataType::Utf8, arg_types[1].clone()])
            }
            [DataType::Null | DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8, DataType::Map(_, _)] => {
                Ok(vec![
                    DataType::Utf8,
                    arg_types[1].clone(),
                    arg_types[2].clone(),
                ])
            }
            _ => plan_err!(
                "`{}` function requires 2 or 3 arguments, got {}",
                Self::FROM_JSON_NAME,
                arg_types.len()
            ),
        }
    }
}

/// Core implementation of the `from_json` function logic.
fn from_json_inner(args: &[ArrayRef], session_timezone: &str) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!(
            "`{}` function requires 2 or 3 arguments, got {}",
            SparkFromJson::FROM_JSON_NAME,
            args.len()
        );
    };

    let rows: &StringArray = downcast_arg!(&args[0], StringArray);
    let schema_str: &StringArray = downcast_arg!(&args[1], StringArray);
    let schema_str: &str = if schema_str.is_empty() || schema_str.is_null(0) {
        return exec_err!(
            "`{}` function requires a schema string, got an empty or null string",
            SparkFromJson::FROM_JSON_NAME
        );
    } else {
        let s = schema_str.value(0);
        if s.trim().is_empty() {
            return exec_err!(
                "`{}` function requires a schema string, got an empty string",
                SparkFromJson::FROM_JSON_NAME
            );
        }
        s
    };

    let options: SparkFromJsonOptions = if let Some(options) = args.get(2) {
        SparkFromJsonOptions::from_map(downcast_arg!(options, MapArray))?
    } else {
        SparkFromJsonOptions::default()
    };

    let target_type = parse_schema_to_data_type(schema_str, session_timezone)?;

    match &target_type {
        DataType::Struct(fields) => parse_json_to_struct(rows, fields, &options, session_timezone),
        DataType::List(field) => parse_json_to_list(rows, field, &options, session_timezone),
        DataType::Map(field, keys_sorted) => {
            parse_json_to_map(rows, field, *keys_sorted, &options, session_timezone)
        }
        other => exec_err!("Unsupported target schema type for from_json: {other:?}"),
    }
}

/// Parse JSON strings into a StructArray.
fn parse_json_to_struct(
    rows: &StringArray,
    fields: &Fields,
    options: &SparkFromJsonOptions,
    session_timezone: &str,
) -> Result<ArrayRef> {
    let num_rows = rows.len();
    let mut children_scalars: Vec<Vec<ScalarValue>> =
        vec![Vec::with_capacity(num_rows); fields.len()];
    let mut validity: Vec<bool> = Vec::with_capacity(num_rows);

    for i in 0..num_rows {
        if rows.is_null(i) {
            for (j, field) in fields.iter().enumerate() {
                children_scalars[j].push(ScalarValue::try_new_null(field.data_type())?);
            }
            validity.push(false);
        } else {
            let json_str = rows.value(i);
            match serde_json::from_str::<Value>(json_str) {
                Ok(Value::Object(obj)) => {
                    for (j, field) in fields.iter().enumerate() {
                        let field_value = obj.get(field.name());
                        children_scalars[j].push(json_value_to_scalar(
                            field_value,
                            field.data_type(),
                            options,
                            session_timezone,
                        )?);
                    }
                    validity.push(true);
                }
                _ => {
                    // Parse error or non-object → struct with null fields (PERMISSIVE mode)
                    for (j, field) in fields.iter().enumerate() {
                        children_scalars[j].push(ScalarValue::try_new_null(field.data_type())?);
                    }
                    validity.push(true);
                }
            }
        }
    }

    let children_arrays: Vec<ArrayRef> = children_scalars
        .into_iter()
        .map(|arr| ScalarValue::iter_to_array(arr))
        .collect::<Result<_>>()?;

    Ok(Arc::new(StructArray::new(
        fields.clone(),
        children_arrays,
        Some(validity.into()),
    )))
}

/// Parse JSON strings into a ListArray.
fn parse_json_to_list(
    rows: &StringArray,
    element_field: &FieldRef,
    options: &SparkFromJsonOptions,
    session_timezone: &str,
) -> Result<ArrayRef> {
    let num_rows = rows.len();
    let mut offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
    let mut all_values: Vec<ScalarValue> = Vec::new();
    let mut validity: Vec<bool> = Vec::with_capacity(num_rows);
    let mut current_offset: i32 = 0;
    offsets.push(current_offset);

    for i in 0..num_rows {
        if rows.is_null(i) {
            offsets.push(current_offset);
            validity.push(false);
        } else {
            let json_str = rows.value(i);
            match serde_json::from_str::<Value>(json_str) {
                Ok(Value::Array(arr)) => {
                    for item in &arr {
                        all_values.push(json_value_to_scalar(
                            Some(item),
                            element_field.data_type(),
                            options,
                            session_timezone,
                        )?);
                    }
                    current_offset += arr.len() as i32;
                    offsets.push(current_offset);
                    validity.push(true);
                }
                _ => {
                    offsets.push(current_offset);
                    validity.push(false);
                }
            }
        }
    }

    let values_array = if all_values.is_empty() {
        new_empty_array(element_field.data_type())
    } else {
        ScalarValue::iter_to_array(all_values)?
    };

    let offset_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets));
    Ok(Arc::new(ListArray::new(
        element_field.clone(),
        offset_buffer,
        values_array,
        Some(validity.into()),
    )))
}

/// Parse JSON strings into a MapArray.
fn parse_json_to_map(
    rows: &StringArray,
    map_field: &FieldRef,
    keys_sorted: bool,
    options: &SparkFromJsonOptions,
    session_timezone: &str,
) -> Result<ArrayRef> {
    let DataType::Struct(entry_fields) = map_field.data_type() else {
        return exec_err!("Expected struct entries field in Map type");
    };
    let key_field = &entry_fields[0];
    let value_field = &entry_fields[1];

    let num_rows = rows.len();
    let mut offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
    let mut all_keys: Vec<ScalarValue> = Vec::new();
    let mut all_values: Vec<ScalarValue> = Vec::new();
    let mut validity: Vec<bool> = Vec::with_capacity(num_rows);
    let mut current_offset: i32 = 0;
    offsets.push(current_offset);

    for i in 0..num_rows {
        if rows.is_null(i) {
            offsets.push(current_offset);
            validity.push(false);
        } else {
            let json_str = rows.value(i);
            match serde_json::from_str::<Value>(json_str) {
                Ok(Value::Object(obj)) => {
                    for (k, v) in &obj {
                        all_keys.push(json_value_to_scalar(
                            Some(&Value::String(k.clone())),
                            key_field.data_type(),
                            options,
                            session_timezone,
                        )?);
                        all_values.push(json_value_to_scalar(
                            Some(v),
                            value_field.data_type(),
                            options,
                            session_timezone,
                        )?);
                    }
                    current_offset += obj.len() as i32;
                    offsets.push(current_offset);
                    validity.push(true);
                }
                _ => {
                    offsets.push(current_offset);
                    validity.push(false);
                }
            }
        }
    }

    let keys_array = if all_keys.is_empty() {
        new_empty_array(key_field.data_type())
    } else {
        ScalarValue::iter_to_array(all_keys)?
    };

    let values_array = if all_values.is_empty() {
        new_empty_array(value_field.data_type())
    } else {
        ScalarValue::iter_to_array(all_values)?
    };

    let entries = StructArray::new(entry_fields.clone(), vec![keys_array, values_array], None);

    let offset_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets));
    Ok(Arc::new(MapArray::new(
        map_field.clone(),
        offset_buffer,
        entries,
        Some(validity.into()),
        keys_sorted,
    )))
}

/// Convert a JSON value to a ScalarValue of the target DataType.
fn json_value_to_scalar(
    value: Option<&Value>,
    data_type: &DataType,
    options: &SparkFromJsonOptions,
    session_timezone: &str,
) -> Result<ScalarValue> {
    match value {
        None | Some(Value::Null) => ScalarValue::try_new_null(data_type),
        Some(val) => match data_type {
            DataType::Boolean => match val {
                Value::Bool(b) => Ok(ScalarValue::Boolean(Some(*b))),
                _ => ScalarValue::try_new_null(data_type),
            },
            DataType::Int8 => match val {
                Value::Number(n) => Ok(ScalarValue::Int8(n.as_i64().map(|v| v as i8))),
                _ => ScalarValue::try_new_null(data_type),
            },
            DataType::Int16 => match val {
                Value::Number(n) => Ok(ScalarValue::Int16(n.as_i64().map(|v| v as i16))),
                _ => ScalarValue::try_new_null(data_type),
            },
            DataType::Int32 => match val {
                Value::Number(n) => Ok(ScalarValue::Int32(n.as_i64().map(|v| v as i32))),
                _ => ScalarValue::try_new_null(data_type),
            },
            DataType::Int64 => match val {
                Value::Number(n) => Ok(ScalarValue::Int64(n.as_i64())),
                _ => ScalarValue::try_new_null(data_type),
            },
            DataType::Float32 => match val {
                Value::Number(n) => Ok(ScalarValue::Float32(n.as_f64().map(|v| v as f32))),
                _ => ScalarValue::try_new_null(data_type),
            },
            DataType::Float64 => match val {
                Value::Number(n) => Ok(ScalarValue::Float64(n.as_f64())),
                _ => ScalarValue::try_new_null(data_type),
            },
            DataType::Utf8 => match val {
                Value::String(s) => Ok(ScalarValue::Utf8(Some(s.clone()))),
                other => Ok(ScalarValue::Utf8(Some(other.to_string()))),
            },
            DataType::LargeUtf8 => match val {
                Value::String(s) => Ok(ScalarValue::LargeUtf8(Some(s.clone()))),
                other => Ok(ScalarValue::LargeUtf8(Some(other.to_string()))),
            },
            DataType::Date32 => match val {
                Value::String(s) => parse_date32(s, options),
                _ => ScalarValue::try_new_null(data_type),
            },
            DataType::Timestamp(time_unit, tz) => match val {
                Value::String(s) => {
                    parse_timestamp(s, *time_unit, tz.clone(), options, session_timezone)
                }
                _ => ScalarValue::try_new_null(data_type),
            },
            DataType::Decimal128(precision, scale) => {
                let parse_decimal = |input: &str| -> Result<ScalarValue> {
                    let unscaled = parse_decimal_to_i128(input, *precision, *scale)?;
                    Ok(ScalarValue::Decimal128(Some(unscaled), *precision, *scale))
                };
                match val {
                    Value::Number(n) => parse_decimal(&n.to_string()),
                    Value::String(s) => parse_decimal(s),
                    _ => ScalarValue::try_new_null(data_type),
                }
            }
            DataType::Struct(fields) => match val {
                Value::Object(obj) => {
                    let arrays: Vec<ArrayRef> = fields
                        .iter()
                        .map(|field| {
                            let field_value = obj.get(field.name());
                            let scalar = json_value_to_scalar(
                                field_value,
                                field.data_type(),
                                options,
                                session_timezone,
                            )?;
                            scalar.to_array()
                        })
                        .collect::<Result<_>>()?;
                    Ok(ScalarValue::Struct(Arc::new(StructArray::new(
                        fields.clone(),
                        arrays,
                        None,
                    ))))
                }
                _ => ScalarValue::try_new_null(data_type),
            },
            DataType::List(element_field) => match val {
                Value::Array(arr) => {
                    let scalars: Vec<ScalarValue> = arr
                        .iter()
                        .map(|item| {
                            json_value_to_scalar(
                                Some(item),
                                element_field.data_type(),
                                options,
                                session_timezone,
                            )
                        })
                        .collect::<Result<_>>()?;
                    Ok(ScalarValue::List(ScalarValue::new_list(
                        scalars.as_slice(),
                        element_field.data_type(),
                        true,
                    )))
                }
                _ => ScalarValue::try_new_null(data_type),
            },
            DataType::Map(map_field, keys_sorted) => match val {
                Value::Object(obj) => {
                    let DataType::Struct(entry_fields) = map_field.data_type() else {
                        return exec_err!("Expected struct entries field in Map type");
                    };
                    let key_field = &entry_fields[0];
                    let value_field = &entry_fields[1];

                    let mut keys: Vec<ScalarValue> = Vec::with_capacity(obj.len());
                    let mut values: Vec<ScalarValue> = Vec::with_capacity(obj.len());
                    for (k, v) in obj {
                        keys.push(json_value_to_scalar(
                            Some(&Value::String(k.clone())),
                            key_field.data_type(),
                            options,
                            session_timezone,
                        )?);
                        values.push(json_value_to_scalar(
                            Some(v),
                            value_field.data_type(),
                            options,
                            session_timezone,
                        )?);
                    }
                    let keys_array = ScalarValue::iter_to_array(keys)?;
                    let values_array = ScalarValue::iter_to_array(values)?;
                    let keys_len = keys_array.len();
                    let struct_array = StructArray::try_new(
                        entry_fields.clone(),
                        vec![keys_array, values_array],
                        None,
                    )?;
                    let offsets = OffsetBuffer::new(vec![0, keys_len as i32].into());
                    let map_array = MapArray::try_new(
                        Arc::clone(map_field),
                        offsets,
                        struct_array,
                        None,
                        *keys_sorted,
                    )?;
                    Ok(ScalarValue::Map(Arc::new(map_array)))
                }
                _ => ScalarValue::try_new_null(data_type),
            },
            _ => ScalarValue::try_new_null(data_type),
        },
    }
}

/// Parse a decimal string into an unscaled `i128` value for `Decimal128(precision, scale)`.
///
/// Handles integer, fractional, and scientific notation inputs. Fractional digits are
/// padded or truncated (with rounding) to match the target scale. Validates that the
/// result fits within the declared precision.
fn parse_decimal_to_i128(input: &str, precision: u8, scale: i8) -> Result<i128> {
    let s = input.trim();
    if s.is_empty() {
        return Err(DataFusionError::Execution(
            "Failed to parse decimal: empty string".to_string(),
        ));
    }

    let (negative, rest) = if let Some(rest) = s.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = s.strip_prefix('+') {
        (false, rest)
    } else {
        (false, s)
    };

    // Split off scientific notation exponent (e.g. "1.5e3")
    let (mantissa, exponent) = if let Some((m, e)) = rest.split_once(['e', 'E']) {
        let exp: i32 = e.parse().map_err(|err| {
            DataFusionError::Execution(format!("Failed to parse decimal exponent: {err}"))
        })?;
        (m, exp)
    } else {
        (rest, 0)
    };

    let (int_part, frac_part) = if let Some((i, f)) = mantissa.split_once('.') {
        (i, f)
    } else {
        (mantissa, "")
    };

    if int_part.is_empty() && frac_part.is_empty() {
        return Err(DataFusionError::Execution(
            "Failed to parse decimal: missing digits".to_string(),
        ));
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(DataFusionError::Execution(format!(
            "Failed to parse decimal: invalid digits in '{input}'"
        )));
    }

    // effective_scale = number of fractional digits minus the exponent
    // e.g. "1.23e1" → frac_part.len()=2, exponent=1, effective_scale=1
    let effective_scale = frac_part.len() as i32 - exponent;
    let scale_delta = scale as i32 - effective_scale;

    // Combine integer and fractional digits into one coefficient
    let digits = format!("{int_part}{frac_part}");
    let digits = digits.trim_start_matches('0');
    let coefficient: i128 = if digits.is_empty() {
        0
    } else {
        digits.parse::<i128>().map_err(|err| {
            DataFusionError::Execution(format!("Failed to parse decimal coefficient: {err}"))
        })?
    };

    // Scale the coefficient to match the target scale
    let unscaled = if scale_delta >= 0 {
        coefficient.checked_mul(10_i128.checked_pow(scale_delta as u32).ok_or_else(|| {
            DataFusionError::Execution("Decimal scaling overflowed i128".to_string())
        })?)
    } else {
        let divisor = 10_i128.checked_pow((-scale_delta) as u32).ok_or_else(|| {
            DataFusionError::Execution("Decimal scaling overflowed i128".to_string())
        })?;
        let quotient = coefficient / divisor;
        let remainder = coefficient % divisor;
        // Round half-up
        if remainder
            .checked_mul(2)
            .map(|twice| twice >= divisor)
            .unwrap_or(true)
        {
            quotient.checked_add(1)
        } else {
            Some(quotient)
        }
    }
    .ok_or_else(|| DataFusionError::Execution("Decimal scaling overflowed i128".to_string()))?;

    let result = if negative {
        unscaled.checked_neg().ok_or_else(|| {
            DataFusionError::Execution("Decimal negation overflowed i128".to_string())
        })?
    } else {
        unscaled
    };

    // Validate precision: count digits in the absolute value
    let digit_count = if result == 0 {
        1u32
    } else {
        result.unsigned_abs().to_string().len() as u32
    };
    if digit_count > precision as u32 {
        return Err(DataFusionError::Execution(format!(
            "Decimal value '{input}' exceeds declared precision {precision} and scale {scale}"
        )));
    }

    Ok(result)
}

/// Parse a date string into a ScalarValue::Date32.
fn parse_date32(s: &str, options: &SparkFromJsonOptions) -> Result<ScalarValue> {
    let format = &options.date_format;
    let date = NaiveDate::parse_from_str(s, format).map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to parse date '{s}' with format '{format}': {e}"
        ))
    })?;
    let Some(epoch) = NaiveDate::from_ymd_opt(1970, 1, 1) else {
        return exec_err!("failed to construct Unix epoch date");
    };
    let days = (date - epoch).num_days() as i32;
    Ok(ScalarValue::Date32(Some(days)))
}

/// Parse a timestamp string into a ScalarValue::Timestamp.
fn parse_timestamp(
    s: &str,
    time_unit: TimeUnit,
    timezone: Option<Arc<str>>,
    options: &SparkFromJsonOptions,
    session_timezone: &str,
) -> Result<ScalarValue> {
    let format = &options.timestamp_format;

    let naive_datetime = if let Ok(datetime) = NaiveDateTime::parse_from_str(s, format) {
        datetime
    } else if let Ok(date) = NaiveDate::parse_from_str(s, format) {
        let Some(datetime) = date.and_hms_opt(0, 0, 0) else {
            return exec_err!("Failed to parse timestamp '{s}': invalid date");
        };
        datetime
    } else {
        return exec_err!("Failed to parse timestamp '{s}' with format '{format}'");
    };

    let utc_datetime = if let Some(tz) = timezone.as_ref() {
        let tz: Tz = tz
            .as_ref()
            .parse()
            .map_err(|e| DataFusionError::Execution(format!("Invalid timezone '{tz}': {e}")))?;
        localize_with_fallback(&tz, &naive_datetime)?
    } else {
        // Use session timezone when no explicit timezone is set
        let tz: Tz = session_timezone.parse().map_err(|e| {
            DataFusionError::Execution(format!(
                "Invalid session timezone '{session_timezone}': {e}"
            ))
        })?;
        localize_with_fallback(&tz, &naive_datetime)?
    };

    match time_unit {
        TimeUnit::Second => Ok(ScalarValue::TimestampSecond(
            Some(utc_datetime.timestamp()),
            timezone,
        )),
        TimeUnit::Millisecond => Ok(ScalarValue::TimestampMillisecond(
            Some(utc_datetime.timestamp_millis()),
            timezone,
        )),
        TimeUnit::Microsecond => Ok(ScalarValue::TimestampMicrosecond(
            Some(utc_datetime.timestamp_micros()),
            timezone,
        )),
        TimeUnit::Nanosecond => {
            let nanos = utc_datetime.timestamp_nanos_opt().ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Failed to parse timestamp '{s}': value out of range for nanoseconds"
                ))
            })?;
            Ok(ScalarValue::TimestampNanosecond(Some(nanos), timezone))
        }
    }
}

/// Parses a schema string into an Arrow DataType. The schema may be a bare field list
/// like "a INT, b DOUBLE" (interpreted as a STRUCT), or a full type string like
/// "ARRAY<INT>" or "MAP<STRING, INT>".
fn parse_schema_to_data_type(schema: &str, session_timezone: &str) -> Result<DataType> {
    let schema = schema.trim();

    // Try parsing as a full type first (STRUCT<...>, ARRAY<...>, MAP<...>).
    let ast = sail_parser::parse_data_type(schema).or_else(|_| {
        // If that fails, treat as a bare field list and wrap in STRUCT<...>.
        sail_parser::parse_data_type(&format!("STRUCT<{schema}>"))
    });

    let ast =
        ast.map_err(|e| DataFusionError::Plan(format!("Failed to parse schema '{schema}': {e}")))?;
    let spec_dt = from_ast_data_type(ast)
        .map_err(|e| DataFusionError::Plan(format!("Failed to analyze schema '{schema}': {e}")))?;
    spec_to_arrow_data_type(&spec_dt, session_timezone)
}

fn spec_to_arrow_data_type(dt: &spec::DataType, session_timezone: &str) -> Result<DataType> {
    use spec::DataType as SDT;

    fn to_time_unit(unit: &spec::TimeUnit) -> TimeUnit {
        match unit {
            spec::TimeUnit::Second => TimeUnit::Second,
            spec::TimeUnit::Millisecond => TimeUnit::Millisecond,
            spec::TimeUnit::Microsecond => TimeUnit::Microsecond,
            spec::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
        }
    }

    match dt {
        SDT::Null => Ok(DataType::Null),
        SDT::Boolean => Ok(DataType::Boolean),
        SDT::Int8 => Ok(DataType::Int8),
        SDT::Int16 => Ok(DataType::Int16),
        SDT::Int32 => Ok(DataType::Int32),
        SDT::Int64 => Ok(DataType::Int64),
        SDT::UInt8 => Ok(DataType::UInt8),
        SDT::UInt16 => Ok(DataType::UInt16),
        SDT::UInt32 => Ok(DataType::UInt32),
        SDT::UInt64 => Ok(DataType::UInt64),
        SDT::Float16 => Ok(DataType::Float16),
        SDT::Float32 => Ok(DataType::Float32),
        SDT::Float64 => Ok(DataType::Float64),
        SDT::Binary | SDT::ConfiguredBinary => Ok(DataType::Binary),
        SDT::FixedSizeBinary { size } => Ok(DataType::FixedSizeBinary(*size)),
        SDT::LargeBinary => Ok(DataType::LargeBinary),
        SDT::BinaryView => Ok(DataType::BinaryView),
        SDT::Utf8 | SDT::ConfiguredUtf8 { .. } => Ok(DataType::Utf8),
        SDT::LargeUtf8 => Ok(DataType::LargeUtf8),
        SDT::Utf8View => Ok(DataType::Utf8View),
        SDT::Date32 => Ok(DataType::Date32),
        SDT::Date64 => Ok(DataType::Date64),
        SDT::Timestamp {
            time_unit,
            timestamp_type,
        } => Ok(DataType::Timestamp(
            to_time_unit(time_unit),
            match timestamp_type {
                spec::TimestampType::Configured | spec::TimestampType::WithLocalTimeZone => {
                    Some(Arc::from(session_timezone))
                }
                spec::TimestampType::WithoutTimeZone => None,
            },
        )),
        SDT::Time32 { time_unit: u } => Ok(DataType::Time32(to_time_unit(u))),
        SDT::Time64 { time_unit: u } => Ok(DataType::Time64(to_time_unit(u))),
        SDT::Duration { time_unit: u } => Ok(DataType::Duration(to_time_unit(u))),
        SDT::Interval { interval_unit, .. } => Ok(DataType::Interval(match interval_unit {
            spec::IntervalUnit::YearMonth => IntervalUnit::YearMonth,
            spec::IntervalUnit::DayTime => IntervalUnit::DayTime,
            spec::IntervalUnit::MonthDayNano => IntervalUnit::MonthDayNano,
        })),
        SDT::Decimal128 { precision, scale } => Ok(DataType::Decimal128(*precision, *scale)),
        SDT::Decimal256 { precision, scale } => Ok(DataType::Decimal256(*precision, *scale)),
        SDT::List {
            data_type,
            nullable,
        } => Ok(DataType::List(Arc::new(Field::new(
            SAIL_LIST_FIELD_NAME,
            spec_to_arrow_data_type(data_type.as_ref(), session_timezone)?,
            *nullable,
        )))),
        SDT::FixedSizeList {
            data_type,
            nullable,
            length,
        } => Ok(DataType::FixedSizeList(
            Arc::new(Field::new(
                SAIL_LIST_FIELD_NAME,
                spec_to_arrow_data_type(data_type.as_ref(), session_timezone)?,
                *nullable,
            )),
            *length,
        )),
        SDT::LargeList {
            data_type,
            nullable,
        } => Ok(DataType::LargeList(Arc::new(Field::new(
            SAIL_LIST_FIELD_NAME,
            spec_to_arrow_data_type(data_type.as_ref(), session_timezone)?,
            *nullable,
        )))),
        SDT::Struct { fields } => {
            let mut out: Vec<Arc<Field>> = Vec::with_capacity(fields.len());
            for f in fields.iter() {
                out.push(Arc::new(Field::new(
                    f.name.clone(),
                    spec_to_arrow_data_type(&f.data_type, session_timezone)?,
                    f.nullable,
                )));
            }
            Ok(DataType::Struct(Fields::from(out)))
        }
        SDT::Map {
            key_type,
            value_type,
            value_type_nullable,
            keys_sorted,
        } => {
            let fields = Fields::from(vec![
                Arc::new(Field::new(
                    SAIL_MAP_KEY_FIELD_NAME,
                    spec_to_arrow_data_type(key_type.as_ref(), session_timezone)?,
                    false,
                )),
                Arc::new(Field::new(
                    SAIL_MAP_VALUE_FIELD_NAME,
                    spec_to_arrow_data_type(value_type.as_ref(), session_timezone)?,
                    *value_type_nullable,
                )),
            ]);
            Ok(DataType::Map(
                Arc::new(Field::new(
                    SAIL_MAP_FIELD_NAME,
                    DataType::Struct(fields),
                    false,
                )),
                *keys_sorted,
            ))
        }
        other => Err(DataFusionError::Plan(format!(
            "Unsupported data type in from_json schema: {other:?}"
        ))),
    }
}

/// Finds the index of a specified key in a `MapArray`.
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
            .and_then(|values| {
                if values.is_null(index) {
                    None
                } else {
                    Some(values.value(index).to_string())
                }
            })
    } else {
        None
    }
}
