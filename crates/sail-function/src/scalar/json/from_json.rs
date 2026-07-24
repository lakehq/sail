use std::sync::Arc;

use chrono::prelude::*;
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::*;
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::*;
use datafusion::error::{DataFusionError, Result};
use datafusion_common::{ScalarValue, exec_err, plan_err};
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
use crate::scalar::datetime::format::DateTimeFormat;

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
    timestamp_format: DateTimeFormat,
    date_format: DateTimeFormat,
}

impl SparkFromJsonOptions {
    pub const TIMESTAMP_FORMAT_OPTION: &'static str = "timestampFormat";
    pub const DATE_FORMAT_OPTION: &'static str = "dateFormat";
    // Default formats matching Spark's behavior (Java DateTimeFormatter patterns)
    pub const TIMESTAMP_FORMAT_DEFAULT: &'static str = "yyyy-MM-dd HH:mm:ss";
    pub const DATE_FORMAT_DEFAULT: &'static str = "yyyy-MM-dd";

    fn from_map(map: &MapArray) -> Result<Self> {
        let timestamp_format = find_key_value(map, Self::TIMESTAMP_FORMAT_OPTION)
            .as_deref()
            .map(DateTimeFormat::parse)
            .transpose()?
            .unwrap_or_else(|| {
                #[expect(clippy::expect_used)]
                DateTimeFormat::parse(Self::TIMESTAMP_FORMAT_DEFAULT)
                    .expect("default timestamp format should be valid")
            });

        let date_format = find_key_value(map, Self::DATE_FORMAT_OPTION)
            .as_deref()
            .map(DateTimeFormat::parse)
            .transpose()?
            .unwrap_or_else(|| {
                #[expect(clippy::expect_used)]
                DateTimeFormat::parse(Self::DATE_FORMAT_DEFAULT)
                    .expect("default date format should be valid")
            });

        Ok(Self {
            timestamp_format,
            date_format,
        })
    }
}

impl Default for SparkFromJsonOptions {
    #[expect(clippy::expect_used)]
    fn default() -> Self {
        Self {
            timestamp_format: DateTimeFormat::parse(Self::TIMESTAMP_FORMAT_DEFAULT)
                .expect("default timestamp format should be valid"),
            date_format: DateTimeFormat::parse(Self::DATE_FORMAT_DEFAULT)
                .expect("default date format should be valid"),
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
        let schema_str = if let Some(schema) = scalar_arguments.get(1) {
            match schema {
                Some(ScalarValue::Utf8(Some(s)))
                | Some(ScalarValue::LargeUtf8(Some(s)))
                | Some(ScalarValue::Utf8View(Some(s))) => s.as_str(),
                None | Some(_) => {
                    return plan_err!(
                        "`{}` function requires the schema argument to be a string literal",
                        Self::FROM_JSON_NAME
                    );
                }
            }
        } else {
            return plan_err!(
                "`{}` function requires 2 or 3 arguments, got {}",
                Self::FROM_JSON_NAME,
                scalar_arguments.len()
            );
        };

        let dt = parse_schema_to_data_type(schema_str, &self.session_timezone)?;
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
            [
                DataType::Null | DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
            ] => Ok(vec![DataType::Utf8, arg_types[1].clone()]),
            [
                DataType::Null | DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8,
                DataType::Map(_, _),
            ] => Ok(vec![
                DataType::Utf8,
                arg_types[1].clone(),
                arg_types[2].clone(),
            ]),
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

    let schema_data_type = parse_schema_to_data_type(schema_str, session_timezone)?;

    match &schema_data_type {
        DataType::Struct(_) | DataType::Map(_, _) | DataType::List(_) => {}
        other => {
            return exec_err!(
                "`{}` function doesn't support target schema type {}",
                SparkFromJson::FROM_JSON_NAME,
                other
            );
        }
    }

    parse_rows(rows, &schema_data_type, &options, session_timezone)
}

fn parse_rows(
    rows: &StringArray,
    schema: &DataType,
    options: &SparkFromJsonOptions,
    session_timezone: &str,
) -> Result<ArrayRef> {
    let mut builder = create_builder(schema, rows.len(), session_timezone)?;
    for i in 0..rows.len() {
        if rows.is_null(i) {
            append_to_builder(&mut builder, &Value::Null, options)?;
        } else {
            let json_str = rows.value(i);
            match serde_json::from_str::<serde_json::Value>(json_str) {
                Ok(value) if is_top_level_match(&builder, &value) => {
                    append_to_builder(&mut builder, &value, options)?;
                }
                Ok(value) if is_arr_and_obj_match(&builder, &value) => {
                    let arr_wrapped_struct = Value::Array(vec![value]);
                    append_to_builder(&mut builder, &arr_wrapped_struct, options)?;
                }
                // PERMISSIVE mode (only mode supported today):
                // - parsed JSON shape doesn't match top-level builder shape
                //   (e.g. struct schema with non-object JSON), OR
                // - JSON failed to parse entirely.
                // Struct → validity=true with null children.
                // List/Map → validity=false.
                _ => append_permissive_parse_error(&mut builder, options)?,
            }
        }
    }
    finish_builder(builder)
}

/// Returns true when the parsed JSON value matches the shape of the top-level
/// builder. The top-level kind guard in `from_json_inner` restricts builder kind
/// to Struct / List / Map, so other variants don't appear here.
fn is_top_level_match(builder: &FieldBuilder, value: &Value) -> bool {
    matches!(
        (builder, value),
        (FieldBuilder::Struct { .. }, Value::Object(_))
            | (FieldBuilder::List { .. }, Value::Array(_))
            | (FieldBuilder::Map { .. }, Value::Object(_))
    )
}

/// Returns true when parsed JSON is an Object and the builder is a List.
/// In this case the object should be wrapped in a list. e.g.
/// {"a": 1} -> [{"a": 1}]
fn is_arr_and_obj_match(builder: &FieldBuilder, value: &Value) -> bool {
    matches!(
        (builder, value),
        (FieldBuilder::List { .. }, Value::Object(_))
    )
}

/// [Credit] The core field builder logic is inspired by
/// [DataFusion Comet](https://github.com/apache/datafusion-comet/blob/main/native/spark-expr/src/json_funcs/from_json.rs),
/// licensed under the Apache License, Version 2.0.
#[derive(Debug)]
enum FieldBuilder {
    Boolean(BooleanBuilder),
    Date32(Date32Builder),
    Decimal128 {
        builder: Decimal128Builder,
        precision: u8,
        scale: i8,
    },
    Float32(Float32Builder),
    Float64(Float64Builder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    List {
        field: Arc<Field>,
        offsets: Vec<i32>,
        values: Box<FieldBuilder>,
        nulls: Vec<bool>,
    },
    LargeString(LargeStringBuilder),
    Map {
        field: Arc<Field>,
        keys: Box<FieldBuilder>,
        values: Box<FieldBuilder>,
        offsets: Vec<i32>,
        nulls: Vec<bool>,
        ordered: bool,
    },
    String(StringBuilder),
    Struct {
        fields: Fields,
        nested_builders: Vec<FieldBuilder>,
        nulls: Vec<bool>,
    },
    TimestampMicrosecond {
        builder: TimestampMicrosecondBuilder,
        tz: Arc<str>,
    },
    TimestampMillisecond {
        builder: TimestampMillisecondBuilder,
        tz: Arc<str>,
    },
    TimestampNanosecond {
        builder: TimestampNanosecondBuilder,
        tz: Arc<str>,
    },
    TimestampSecond {
        builder: TimestampSecondBuilder,
        tz: Arc<str>,
    },
    // Some data types aren't supported by spark's json implementation
    Unsupported {
        data_type: DataType,
        count: usize,
    },
}

fn create_builder(
    data_type: &DataType,
    capacity: usize,
    session_timezone: &str,
) -> Result<FieldBuilder> {
    match data_type {
        DataType::Boolean => Ok(FieldBuilder::Boolean(BooleanBuilder::with_capacity(
            capacity,
        ))),
        DataType::Date32 => Ok(FieldBuilder::Date32(Date32Builder::with_capacity(capacity))),
        DataType::Decimal128(precision, scale) => {
            let builder = Decimal128Builder::with_capacity(capacity)
                .with_precision_and_scale(*precision, *scale)?;
            Ok(FieldBuilder::Decimal128 {
                builder,
                precision: *precision,
                scale: *scale,
            })
        }
        DataType::Float32 => Ok(FieldBuilder::Float32(Float32Builder::with_capacity(
            capacity,
        ))),
        DataType::Float64 => Ok(FieldBuilder::Float64(Float64Builder::with_capacity(
            capacity,
        ))),
        DataType::Int8 => Ok(FieldBuilder::Int8(Int8Builder::with_capacity(capacity))),
        DataType::Int16 => Ok(FieldBuilder::Int16(Int16Builder::with_capacity(capacity))),
        DataType::Int32 => Ok(FieldBuilder::Int32(Int32Builder::with_capacity(capacity))),
        DataType::Int64 => Ok(FieldBuilder::Int64(Int64Builder::with_capacity(capacity))),
        DataType::List(field) => {
            let values = create_builder(field.data_type(), capacity, session_timezone)?;
            let mut offsets = Vec::with_capacity(capacity + 1);
            offsets.push(0);
            Ok(FieldBuilder::List {
                field: field.clone(),
                offsets,
                values: Box::new(values),
                nulls: Vec::with_capacity(capacity),
            })
        }
        DataType::LargeUtf8 => Ok(FieldBuilder::LargeString(
            LargeStringBuilder::with_capacity(capacity, 0),
        )),
        DataType::Map(field, ordered) => {
            let (keys_field, values_field) = match field.data_type() {
                DataType::Struct(fields) => Ok((fields[0].clone(), fields[1].clone())),
                other => exec_err!(
                    "Unreachable: `{}` function handled map field that should be a struct but is {:#?}",
                    SparkFromJson::FROM_JSON_NAME,
                    other
                ),
            }?;
            let keys_builder = create_builder(keys_field.data_type(), capacity, session_timezone)?;
            let values_builder =
                create_builder(values_field.data_type(), capacity, session_timezone)?;
            let mut offsets = Vec::with_capacity(capacity + 1);
            offsets.push(0);
            Ok(FieldBuilder::Map {
                field: field.clone(),
                keys: Box::new(keys_builder),
                values: Box::new(values_builder),
                offsets,
                nulls: Vec::with_capacity(capacity),
                ordered: *ordered,
            })
        }
        DataType::Struct(fields) => {
            let nested_builders = fields
                .iter()
                .map(|f| create_builder(f.data_type(), capacity, session_timezone))
                .collect::<Result<Vec<_>>>()?;
            Ok(FieldBuilder::Struct {
                fields: fields.clone(),
                nested_builders,
                nulls: Vec::with_capacity(capacity),
            })
        }
        DataType::Utf8 => Ok(FieldBuilder::String(StringBuilder::with_capacity(
            capacity, 0,
        ))),
        DataType::Timestamp(time_unit, tz) => {
            let resolved_tz = tz.clone().unwrap_or(Arc::from(session_timezone));
            match time_unit {
                TimeUnit::Microsecond => Ok(FieldBuilder::TimestampMicrosecond {
                    builder: TimestampMicrosecondBuilder::with_capacity(capacity)
                        .with_timezone_opt(tz.clone()),
                    tz: resolved_tz,
                }),
                TimeUnit::Millisecond => Ok(FieldBuilder::TimestampMillisecond {
                    builder: TimestampMillisecondBuilder::with_capacity(capacity)
                        .with_timezone_opt(tz.clone()),
                    tz: resolved_tz,
                }),
                TimeUnit::Nanosecond => Ok(FieldBuilder::TimestampNanosecond {
                    builder: TimestampNanosecondBuilder::with_capacity(capacity)
                        .with_timezone_opt(tz.clone()),
                    tz: resolved_tz,
                }),
                TimeUnit::Second => Ok(FieldBuilder::TimestampSecond {
                    builder: TimestampSecondBuilder::with_capacity(capacity)
                        .with_timezone_opt(tz.clone()),
                    tz: resolved_tz,
                }),
            }
        }
        _ => Ok(FieldBuilder::Unsupported {
            data_type: data_type.clone(),
            count: 0,
        }),
    }
}

fn append_to_builder(
    builder: &mut FieldBuilder,
    value: &Value,
    options: &SparkFromJsonOptions,
) -> Result<()> {
    match builder {
        FieldBuilder::Boolean(b) => match value {
            Value::Bool(bool) => b.append_value(*bool),
            _ => b.append_null(),
        },
        FieldBuilder::Date32(b) => match value {
            Value::String(string) => {
                let date32 = parse_date32(string, options)?;
                b.append_value(date32);
            }
            _ => b.append_null(),
        },
        FieldBuilder::Decimal128 {
            builder,
            precision,
            scale,
        } => {
            match value {
                Value::Number(n) => {
                    let decimal128 = parse_decimal_to_i128(&n.to_string(), *precision, *scale)?;
                    builder.append_value(decimal128);
                }
                Value::String(s) => {
                    let decimal128 = parse_decimal_to_i128(s, *precision, *scale)?;
                    builder.append_value(decimal128);
                }
                _ => builder.append_null(),
            };
        }
        FieldBuilder::Float32(b) => match value {
            Value::Number(num) => {
                if let Some(f) = num.as_f64() {
                    b.append_value(f as f32);
                } else {
                    b.append_null();
                }
            }
            _ => b.append_null(),
        },
        FieldBuilder::Float64(b) => match value {
            Value::Number(num) => {
                if let Some(f) = num.as_f64() {
                    b.append_value(f);
                } else {
                    b.append_null();
                }
            }
            _ => b.append_null(),
        },
        FieldBuilder::Int8(b) => match value {
            Value::Number(num) => {
                if let Some(n) = num.as_i64() {
                    b.append_value(n as i8);
                } else {
                    b.append_null();
                }
            }
            _ => b.append_null(),
        },
        FieldBuilder::Int16(b) => match value {
            Value::Number(num) => {
                if let Some(n) = num.as_i64() {
                    b.append_value(n as i16);
                } else {
                    b.append_null();
                }
            }
            _ => b.append_null(),
        },
        FieldBuilder::Int32(b) => match value {
            Value::Number(num) => {
                if let Some(n) = num.as_i64() {
                    b.append_value(n as i32);
                } else {
                    b.append_null();
                }
            }
            _ => b.append_null(),
        },
        FieldBuilder::Int64(b) => match value {
            Value::Number(num) => {
                if let Some(n) = num.as_i64() {
                    b.append_value(n);
                } else {
                    b.append_null();
                }
            }
            _ => b.append_null(),
        },
        // TODO: 1) verify string size 2) other Values can be converted to strings
        FieldBuilder::LargeString(b) => match value {
            Value::String(string) => b.append_value(string),
            Value::Number(num) => b.append_value(num.to_string()),
            Value::Bool(bool) => b.append_value(bool.to_string()),
            Value::Object(_) => b.append_value(value.to_string()),
            Value::Array(_) => b.append_value(value.to_string()),
            _ => b.append_null(),
        },
        FieldBuilder::List {
            offsets,
            values,
            nulls,
            ..
        } => match value {
            Value::Array(arr) => {
                nulls.push(true);
                for val in arr.iter() {
                    append_to_builder(values, val, options)?;
                }
                let curr_len = offsets.last().copied().unwrap_or(0) + arr.len() as i32;
                offsets.push(curr_len);
            }
            _ => {
                nulls.push(false);
                let curr = offsets.last().copied().unwrap_or(0);
                offsets.push(curr);
            }
        },
        FieldBuilder::Map {
            keys,
            values,
            nulls,
            offsets,
            ..
        } => {
            match value {
                Value::Object(obj) => {
                    // just push to struct builder
                    nulls.push(true);
                    for (k, v) in obj.iter() {
                        // map each pair to "key" and "value" - default field names
                        append_to_builder(keys, &Value::String(k.clone()), options)?;
                        append_to_builder(values, v, options)?;
                    }
                    let curr_len = offsets.last().copied().unwrap_or(0) + obj.len() as i32;
                    offsets.push(curr_len);
                }
                _ => {
                    nulls.push(false);
                    let curr = offsets.last().copied().unwrap_or(0);
                    offsets.push(curr);
                }
            }
        }
        FieldBuilder::String(b) => match value {
            Value::String(string) => b.append_value(string),
            Value::Number(num) => b.append_value(num.to_string()),
            Value::Bool(bool) => b.append_value(bool.to_string()),
            Value::Object(_) => b.append_value(value.to_string()),
            Value::Array(_) => b.append_value(value.to_string()),
            _ => b.append_null(),
        },
        FieldBuilder::Struct {
            fields,
            nested_builders,
            nulls,
        } => {
            match value {
                Value::Object(obj) => {
                    nulls.push(true);
                    for (field, nested_builder) in fields.iter().zip(nested_builders.iter_mut()) {
                        // if key not found return null
                        let val = if let Some(v) = obj.get(field.name()) {
                            v
                        } else {
                            &Value::Null
                        };
                        append_to_builder(nested_builder, val, options)?;
                    }
                }
                _ => {
                    nulls.push(false);
                    for nested_builder in nested_builders.iter_mut() {
                        append_to_builder(nested_builder, &Value::Null, options)?;
                    }
                }
            }
        }
        FieldBuilder::TimestampMicrosecond { builder, tz } => match value {
            Value::String(string) => {
                let utc_datetime = parse_timestamp(string, tz.clone(), options)?;
                if let Some(timestamp_microseconds) =
                    TimestampMicrosecondType::from_datetime(utc_datetime)
                {
                    builder.append_value(timestamp_microseconds);
                } else {
                    builder.append_null();
                }
            }
            _ => builder.append_null(),
        },
        FieldBuilder::TimestampMillisecond { builder, tz } => match value {
            Value::String(string) => {
                let utc_datetime = parse_timestamp(string, tz.clone(), options)?;
                if let Some(timestamp_milliseconds) =
                    TimestampMillisecondType::from_datetime(utc_datetime)
                {
                    builder.append_value(timestamp_milliseconds);
                } else {
                    builder.append_null();
                }
            }
            _ => builder.append_null(),
        },
        FieldBuilder::TimestampNanosecond { builder, tz } => match value {
            Value::String(string) => {
                let utc_datetime = parse_timestamp(string, tz.clone(), options)?;
                if let Some(timestamp_nanoseconds) =
                    TimestampNanosecondType::from_datetime(utc_datetime)
                {
                    builder.append_value(timestamp_nanoseconds);
                } else {
                    builder.append_null();
                }
            }
            _ => builder.append_null(),
        },
        FieldBuilder::TimestampSecond { builder, tz } => match value {
            Value::String(string) => {
                let utc_datetime = parse_timestamp(string, tz.clone(), options)?;
                if let Some(timestamp_second) = TimestampSecondType::from_datetime(utc_datetime) {
                    builder.append_value(timestamp_second);
                } else {
                    builder.append_null();
                }
            }
            _ => builder.append_null(),
        },
        FieldBuilder::Unsupported { count, .. } => *count += 1,
    };
    Ok(())
}

/// Handle PERMISSIVE MODE. When json fails to parse:
/// - struct: stays valid but each value gets null
/// - map: invalid, coerce to null
/// - list: invalid, coerce to null
fn append_permissive_parse_error(
    builder: &mut FieldBuilder,
    options: &SparkFromJsonOptions,
) -> Result<()> {
    match builder {
        FieldBuilder::Struct {
            nested_builders,
            nulls,
            ..
        } => {
            nulls.push(true);
            for nested_builder in nested_builders.iter_mut() {
                append_to_builder(nested_builder, &Value::Null, options)?;
            }
        }
        FieldBuilder::Map { offsets, nulls, .. } => {
            nulls.push(false);
            let curr = offsets.last().copied().unwrap_or(0);
            offsets.push(curr);
        }
        FieldBuilder::List { offsets, nulls, .. } => {
            nulls.push(false);
            let curr = offsets.last().copied().unwrap_or(0);
            offsets.push(curr);
        }
        other => unreachable!("this function shouldn't act on builder type: {other:#?}"),
    };
    Ok(())
}

fn finish_builder(builder: FieldBuilder) -> Result<ArrayRef> {
    match builder {
        FieldBuilder::Boolean(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Date32(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Decimal128 { mut builder, .. } => Ok(Arc::new(builder.finish())),
        FieldBuilder::Float32(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Float64(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::LargeString(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Int8(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Int16(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Int32(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Int64(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::List {
            field,
            offsets,
            values,
            nulls,
        } => {
            let deref_values = *values;
            let array_ref = finish_builder(deref_values)?;
            Ok(Arc::new(ListArray::new(
                field,
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                array_ref,
                Some(NullBuffer::from(nulls)),
            )))
        }
        FieldBuilder::Map {
            field,
            keys,
            values,
            offsets,
            nulls,
            ordered,
        } => {
            let finished_keys = finish_builder(*keys)?;
            let finished_values = finish_builder(*values)?;
            let fields = match field.data_type() {
                DataType::Struct(fields) => Ok(fields),
                other => exec_err!(
                    "Unreachable: `{}` function handled map field that should be a struct but is {:#?}",
                    SparkFromJson::FROM_JSON_NAME,
                    other
                ),
            }?;
            let struct_array =
                StructArray::new(fields.clone(), vec![finished_keys, finished_values], None);
            Ok(Arc::new(MapArray::new(
                field,
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                struct_array,
                Some(NullBuffer::from(nulls)),
                ordered,
            )))
        }
        FieldBuilder::String(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Struct {
            fields,
            nested_builders,
            nulls,
        } => {
            let arrays = nested_builders
                .into_iter()
                .map(finish_builder)
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::new(
                fields,
                arrays,
                Some(NullBuffer::from(nulls)),
            )))
        }
        FieldBuilder::TimestampMicrosecond { mut builder, .. } => Ok(Arc::new(builder.finish())),
        FieldBuilder::TimestampMillisecond { mut builder, .. } => Ok(Arc::new(builder.finish())),
        FieldBuilder::TimestampNanosecond { mut builder, .. } => Ok(Arc::new(builder.finish())),
        FieldBuilder::TimestampSecond { mut builder, .. } => Ok(Arc::new(builder.finish())),
        FieldBuilder::Unsupported { data_type, count } => Ok(new_null_array(&data_type, count)),
    }
}

/// Parse a decimal string into an unscaled `i128` value for `Decimal128(precision, scale)`.
///
/// Handles integer, fractional, and scientific notation inputs. Fractional digits are
/// padded or truncated (with rounding) to match the target scale. Validates that the
/// result fits within the declared precision.
fn parse_decimal_to_i128(
    input: &str,
    precision: u8,
    scale: i8,
) -> Result<<Decimal128Type as ArrowPrimitiveType>::Native> {
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

fn parse_date32(
    s: &str,
    options: &SparkFromJsonOptions,
) -> Result<<Date32Type as ArrowPrimitiveType>::Native> {
    let format = &options.date_format;
    let parsed = format.parse_datetime_value(s)?;
    Ok(Date32Type::from_naive_date(parsed.datetime.date()))
}

fn parse_timestamp(
    s: &str,
    timezone: Arc<str>,
    options: &SparkFromJsonOptions,
) -> Result<DateTime<Utc>> {
    let format = &options.timestamp_format;
    let parsed = format.parse_datetime_value(s)?;

    let datetime = if let Some(offset) = parsed.offset {
        parsed
            .datetime
            .and_local_timezone(offset)
            .single()
            .map(|x| x.to_utc())
            .ok_or_else(|| DataFusionError::Execution("cannot apply parsed offset".to_string()))?
    } else {
        let tz: Tz = timezone.as_ref().parse().map_err(|e| {
            DataFusionError::Execution(format!("Invalid timezone '{timezone}': {e}"))
        })?;
        localize_with_fallback(&tz, &parsed.datetime)?
    };

    Ok(datetime)
}

/// Parses a schema string into an Arrow DataType. The schema may be a bare field list
/// like "a INT, b DOUBLE" (interpreted as a STRUCT), or a full type string like
/// "ARRAY<INT>" or "MAP<STRING, INT>".
fn parse_schema_to_data_type(schema: &str, session_timezone: &str) -> Result<DataType> {
    let schema = schema.trim();

    // Try parsing as Spark JSON schema format (e.g., {"type":"struct","fields":[...]}).
    // PySpark serializes DataType objects to JSON format via schema.json().
    // For inputs that clearly look like JSON, return the JSON parsing result directly
    // so callers see the real error instead of a later DDL parsing failure.
    if schema.starts_with('{') || schema.starts_with('"') {
        return parse_spark_json_schema(schema, session_timezone);
    }

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

/// Parses a Spark JSON schema string into an Arrow DataType.
/// Spark serializes DataType objects to JSON format like:
/// - Simple types: `"integer"`, `"string"`, `"boolean"`
/// - Struct: `{"type":"struct","fields":[{"name":"a","type":"integer","nullable":true,"metadata":{}}]}`
/// - Array: `{"type":"array","elementType":"integer","containsNull":true}`
/// - Map: `{"type":"map","keyType":"string","valueType":"integer","valueContainsNull":true}`
fn parse_spark_json_schema(schema: &str, session_timezone: &str) -> Result<DataType> {
    let json: Value = serde_json::from_str(schema)
        .map_err(|e| DataFusionError::Plan(format!("Failed to parse JSON schema: {e}")))?;
    json_value_to_data_type(&json, session_timezone)
}

fn json_value_to_data_type(value: &Value, session_timezone: &str) -> Result<DataType> {
    match value {
        Value::String(s) => json_type_name_to_data_type(s, session_timezone),
        Value::Object(map) => {
            let type_str = map.get("type").and_then(|v| v.as_str()).ok_or_else(|| {
                DataFusionError::Plan("JSON schema object missing 'type' field".to_string())
            })?;
            match type_str {
                "struct" => {
                    let fields = map
                        .get("fields")
                        .and_then(|v| v.as_array())
                        .ok_or_else(|| {
                            DataFusionError::Plan("Struct type missing 'fields' array".to_string())
                        })?;
                    let mut arrow_fields = Vec::with_capacity(fields.len());
                    for field in fields {
                        let name = field.get("name").and_then(|v| v.as_str()).ok_or_else(|| {
                            DataFusionError::Plan("Struct field missing 'name'".to_string())
                        })?;
                        let nullable = field
                            .get("nullable")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(true);
                        let field_type = field.get("type").ok_or_else(|| {
                            DataFusionError::Plan("Struct field missing 'type'".to_string())
                        })?;
                        let dt = json_value_to_data_type(field_type, session_timezone)?;
                        arrow_fields.push(Arc::new(Field::new(name, dt, nullable)));
                    }
                    Ok(DataType::Struct(Fields::from(arrow_fields)))
                }
                "array" => {
                    let element_type = map.get("elementType").ok_or_else(|| {
                        DataFusionError::Plan("Array type missing 'elementType'".to_string())
                    })?;
                    let contains_null = map
                        .get("containsNull")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true);
                    let dt = json_value_to_data_type(element_type, session_timezone)?;
                    Ok(DataType::List(Arc::new(Field::new(
                        SAIL_LIST_FIELD_NAME,
                        dt,
                        contains_null,
                    ))))
                }
                "map" => {
                    let key_type = map.get("keyType").ok_or_else(|| {
                        DataFusionError::Plan("Map type missing 'keyType'".to_string())
                    })?;
                    let value_type = map.get("valueType").ok_or_else(|| {
                        DataFusionError::Plan("Map type missing 'valueType'".to_string())
                    })?;
                    let value_contains_null = map
                        .get("valueContainsNull")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true);
                    let key_dt = json_value_to_data_type(key_type, session_timezone)?;
                    let value_dt = json_value_to_data_type(value_type, session_timezone)?;
                    let fields = Fields::from(vec![
                        Arc::new(Field::new(SAIL_MAP_KEY_FIELD_NAME, key_dt, false)),
                        Arc::new(Field::new(
                            SAIL_MAP_VALUE_FIELD_NAME,
                            value_dt,
                            value_contains_null,
                        )),
                    ]);
                    Ok(DataType::Map(
                        Arc::new(Field::new(
                            SAIL_MAP_FIELD_NAME,
                            DataType::Struct(fields),
                            false,
                        )),
                        false,
                    ))
                }
                "udt" => {
                    let sql_type = map
                        .get("sqlType")
                        .or_else(|| map.get("sql_type"))
                        .ok_or_else(|| {
                            DataFusionError::Plan("UDT type missing 'sqlType' field".to_string())
                        })?;
                    json_value_to_data_type(sql_type, session_timezone)
                }
                other => {
                    // Some types might be expressed as {"type": "simple_name"}
                    json_type_name_to_data_type(other, session_timezone)
                }
            }
        }
        _ => Err(DataFusionError::Plan(format!(
            "Unexpected JSON schema value: {value}"
        ))),
    }
}

fn json_type_name_to_data_type(type_name: &str, session_timezone: &str) -> Result<DataType> {
    match type_name {
        "null" | "void" => Ok(DataType::Null),
        "boolean" => Ok(DataType::Boolean),
        "byte" | "tinyint" => Ok(DataType::Int8),
        "short" | "smallint" => Ok(DataType::Int16),
        "integer" | "int" => Ok(DataType::Int32),
        "long" | "bigint" => Ok(DataType::Int64),
        "float" => Ok(DataType::Float32),
        "double" => Ok(DataType::Float64),
        "decimal" => Ok(DataType::Decimal128(10, 0)),
        "string" => Ok(DataType::Utf8),
        "binary" => Ok(DataType::Binary),
        "date" => Ok(DataType::Date32),
        "timestamp" | "timestamp_ltz" => Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::from(session_timezone)),
        )),
        "timestamp_ntz" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        "interval" => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
        other => {
            if let Some(args) = other
                .strip_prefix("decimal(")
                .and_then(|x| x.strip_suffix(')'))
            {
                let mut parts = args.split(',').map(str::trim);
                if let (Some(precision), Some(scale), None) =
                    (parts.next(), parts.next(), parts.next())
                    && let (Ok(precision), Ok(scale)) =
                        (precision.parse::<u8>(), scale.parse::<i8>())
                {
                    datafusion::arrow::datatypes::validate_decimal_precision_and_scale::<
                        Decimal128Type,
                    >(precision, scale)
                    .map_err(|e| {
                        DataFusionError::Plan(format!(
                            "Invalid decimal precision/scale in '{other}': {e}"
                        ))
                    })?;
                    return Ok(DataType::Decimal128(precision, scale));
                }
            }
            if let Some(args) = other
                .strip_prefix("geometry(")
                .and_then(|x| x.strip_suffix(')'))
            {
                let srid = args.trim();
                if srid == "ANY" || srid.parse::<i32>().is_ok() {
                    return Ok(DataType::Binary);
                }
                return Err(DataFusionError::Plan(format!(
                    "Unsupported JSON schema type: '{other}'"
                )));
            }
            if let Some(args) = other
                .strip_prefix("geography(")
                .and_then(|x| x.strip_suffix(')'))
            {
                let mut parts = args.split(',').map(str::trim);
                if let (Some(srid), algorithm, None) = (parts.next(), parts.next(), parts.next()) {
                    let valid_srid = srid == "ANY" || srid.parse::<i32>().is_ok();
                    let valid_algorithm = algorithm.is_none_or(|x| x == "spherical");
                    if valid_srid && valid_algorithm {
                        return Ok(DataType::Binary);
                    }
                }
                return Err(DataFusionError::Plan(format!(
                    "Unsupported JSON schema type: '{other}'"
                )));
            }
            // Parameterized Spark JSON type strings such as "decimal(10,2)",
            // "time(0)", "char(10)", and "varchar(20)" use the same syntax
            // as Spark DDL type strings, so reuse the existing DDL analyzer.
            let ast = sail_parser::parse_data_type(other).map_err(|_| {
                DataFusionError::Plan(format!("Unsupported JSON schema type: '{other}'"))
            })?;
            let spec_dt = from_ast_data_type(ast).map_err(|e| {
                DataFusionError::Plan(format!("Failed to analyze JSON schema type '{other}': {e}"))
            })?;
            spec_to_arrow_data_type(&spec_dt, session_timezone)
        }
    }
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
        SDT::Interval { interval_unit, .. } => match interval_unit {
            spec::IntervalUnit::YearMonth => Ok(DataType::Interval(IntervalUnit::YearMonth)),
            spec::IntervalUnit::DayTime => Ok(DataType::Duration(TimeUnit::Microsecond)),
            spec::IntervalUnit::MonthDayNano => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
        },
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
        SDT::Geometry { .. } | SDT::Geography { .. } => Ok(DataType::Binary),
        SDT::Variant => Ok(DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("metadata", DataType::Binary, false)),
            Arc::new(Field::new("value", DataType::Binary, false)),
        ]))),
        SDT::UserDefined { sql_type, .. } => {
            spec_to_arrow_data_type(sql_type.as_ref(), session_timezone)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_spark_json_schema_string() -> Result<()> {
        let data_type = parse_schema_to_data_type(r#""integer""#, "UTC")?;
        assert_eq!(data_type, DataType::Int32);
        Ok(())
    }

    #[test]
    fn test_parse_struct_spark_json_schema_string() -> Result<()> {
        let schema = r#"{
            "type":"struct",
            "fields":[
                {"name":"a","type":"integer","nullable":true,"metadata":{}},
                {"name":"b","type":"string","nullable":false,"metadata":{}}
            ]
        }"#;

        let data_type = parse_schema_to_data_type(schema, "UTC")?;
        assert_eq!(
            data_type,
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("a", DataType::Int32, true)),
                Arc::new(Field::new("b", DataType::Utf8, false)),
            ]))
        );
        Ok(())
    }

    #[test]
    fn test_parse_array_spark_json_schema_string() -> Result<()> {
        let schema = r#"{"type":"array","elementType":"integer","containsNull":true}"#;

        let data_type = parse_schema_to_data_type(schema, "UTC")?;
        assert_eq!(
            data_type,
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
        );
        Ok(())
    }

    #[test]
    fn test_parse_map_spark_json_schema_string() -> Result<()> {
        let schema = r#"{
            "type":"map",
            "keyType":"string",
            "valueType":"integer",
            "valueContainsNull":true
        }"#;

        let data_type = parse_schema_to_data_type(schema, "UTC")?;
        assert_eq!(
            data_type,
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Arc::new(Field::new("key", DataType::Utf8, false)),
                        Arc::new(Field::new("value", DataType::Int32, true)),
                    ])),
                    false,
                )),
                false,
            )
        );
        Ok(())
    }

    #[test]
    fn test_parse_decimal_spark_json_schema_string() -> Result<()> {
        let simple_decimal = parse_schema_to_data_type(r#""decimal(10,2)""#, "UTC")?;
        assert_eq!(simple_decimal, DataType::Decimal128(10, 2));

        let struct_schema = r#"{
            "type":"struct",
            "fields":[
                {"name":"amount","type":"decimal(12,4)","nullable":false,"metadata":{}}
            ]
        }"#;
        let struct_decimal = parse_schema_to_data_type(struct_schema, "UTC")?;
        assert_eq!(
            struct_decimal,
            DataType::Struct(Fields::from(vec![Arc::new(Field::new(
                "amount",
                DataType::Decimal128(12, 4),
                false,
            ))]))
        );
        Ok(())
    }

    #[test]
    fn test_parse_bare_decimal_json_schema_string() -> Result<()> {
        let dt = parse_schema_to_data_type(r#""decimal""#, "UTC")?;
        assert_eq!(dt, DataType::Decimal128(10, 0));
        Ok(())
    }

    #[test]
    fn test_parse_invalid_decimal_precision_json_schema() {
        // precision > 38 should fail validation
        let result = parse_schema_to_data_type(r#""decimal(50,2)""#, "UTC");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_parameterized_spark_json_type_strings() -> Result<()> {
        assert_eq!(
            parse_schema_to_data_type(r#""decimal(10, 2)""#, "UTC")?,
            DataType::Decimal128(10, 2)
        );
        assert_eq!(
            parse_schema_to_data_type(r#""time""#, "UTC")?,
            DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(
            parse_schema_to_data_type(r#""time(0)""#, "UTC")?,
            DataType::Time32(TimeUnit::Second)
        );
        assert_eq!(
            parse_schema_to_data_type(r#""time(3)""#, "UTC")?,
            DataType::Time32(TimeUnit::Millisecond)
        );
        assert_eq!(
            parse_schema_to_data_type(r#""time(6)""#, "UTC")?,
            DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(
            parse_schema_to_data_type(r#""time(9)""#, "UTC")?,
            DataType::Time64(TimeUnit::Nanosecond)
        );
        assert_eq!(
            parse_schema_to_data_type(r#""char(10)""#, "UTC")?,
            DataType::Utf8
        );
        assert_eq!(
            parse_schema_to_data_type(r#""varchar(20)""#, "UTC")?,
            DataType::Utf8
        );
        Ok(())
    }

    #[test]
    fn test_parse_interval_spark_json_type_strings() -> Result<()> {
        assert_eq!(
            parse_schema_to_data_type(r#""interval""#, "UTC")?,
            DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert_eq!(
            parse_schema_to_data_type(r#""interval year""#, "UTC")?,
            DataType::Interval(IntervalUnit::YearMonth)
        );
        assert_eq!(
            parse_schema_to_data_type(r#""interval year to month""#, "UTC")?,
            DataType::Interval(IntervalUnit::YearMonth)
        );
        assert_eq!(
            parse_schema_to_data_type(r#""interval day""#, "UTC")?,
            DataType::Duration(TimeUnit::Microsecond)
        );
        assert_eq!(
            parse_schema_to_data_type(r#""interval day to second""#, "UTC")?,
            DataType::Duration(TimeUnit::Microsecond)
        );
        Ok(())
    }

    #[test]
    fn test_parse_variant_and_geospatial_spark_json_type_strings() -> Result<()> {
        assert_eq!(
            parse_schema_to_data_type(r#""variant""#, "UTC")?,
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("metadata", DataType::Binary, false)),
                Arc::new(Field::new("value", DataType::Binary, false)),
            ]))
        );
        assert_eq!(
            parse_schema_to_data_type(r#""geometry(4326)""#, "UTC")?,
            DataType::Binary
        );
        assert_eq!(
            parse_schema_to_data_type(r#""geometry(ANY)""#, "UTC")?,
            DataType::Binary
        );
        assert_eq!(
            parse_schema_to_data_type(r#""geography(4326, spherical)""#, "UTC")?,
            DataType::Binary
        );
        assert_eq!(
            parse_schema_to_data_type(r#""geography(ANY, spherical)""#, "UTC")?,
            DataType::Binary
        );
        Ok(())
    }

    #[test]
    fn test_parse_udt_spark_json_schema_uses_sql_type() -> Result<()> {
        let schema = r#"{
            "type":"udt",
            "pyClass":"example.PointUDT",
            "serializedClass":"abc",
            "sqlType":{
                "type":"struct",
                "fields":[
                    {"name":"x","type":"double","nullable":false,"metadata":{}},
                    {"name":"y","type":"double","nullable":false,"metadata":{}}
                ]
            }
        }"#;
        assert_eq!(
            parse_schema_to_data_type(schema, "UTC")?,
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("x", DataType::Float64, false)),
                Arc::new(Field::new("y", DataType::Float64, false)),
            ]))
        );
        Ok(())
    }

    #[test]
    fn test_parse_all_simple_json_types() -> Result<()> {
        assert_eq!(
            parse_schema_to_data_type(r#""null""#, "UTC")?,
            DataType::Null
        );
        assert_eq!(
            parse_schema_to_data_type(r#""boolean""#, "UTC")?,
            DataType::Boolean
        );
        assert_eq!(
            parse_schema_to_data_type(r#""byte""#, "UTC")?,
            DataType::Int8
        );
        assert_eq!(
            parse_schema_to_data_type(r#""tinyint""#, "UTC")?,
            DataType::Int8
        );
        assert_eq!(
            parse_schema_to_data_type(r#""short""#, "UTC")?,
            DataType::Int16
        );
        assert_eq!(
            parse_schema_to_data_type(r#""smallint""#, "UTC")?,
            DataType::Int16
        );
        assert_eq!(
            parse_schema_to_data_type(r#""int""#, "UTC")?,
            DataType::Int32
        );
        assert_eq!(
            parse_schema_to_data_type(r#""long""#, "UTC")?,
            DataType::Int64
        );
        assert_eq!(
            parse_schema_to_data_type(r#""bigint""#, "UTC")?,
            DataType::Int64
        );
        assert_eq!(
            parse_schema_to_data_type(r#""float""#, "UTC")?,
            DataType::Float32
        );
        assert_eq!(
            parse_schema_to_data_type(r#""double""#, "UTC")?,
            DataType::Float64
        );
        assert_eq!(
            parse_schema_to_data_type(r#""string""#, "UTC")?,
            DataType::Utf8
        );
        assert_eq!(
            parse_schema_to_data_type(r#""binary""#, "UTC")?,
            DataType::Binary
        );
        assert_eq!(
            parse_schema_to_data_type(r#""date""#, "UTC")?,
            DataType::Date32
        );
        assert_eq!(
            parse_schema_to_data_type(r#""timestamp_ntz""#, "UTC")?,
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        Ok(())
    }

    #[test]
    fn test_parse_timestamp_json_respects_timezone() -> Result<()> {
        let dt = parse_schema_to_data_type(r#""timestamp""#, "America/New_York")?;
        assert_eq!(
            dt,
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("America/New_York")))
        );

        let dt2 = parse_schema_to_data_type(r#""timestamp_ltz""#, "Europe/London")?;
        assert_eq!(
            dt2,
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("Europe/London")))
        );
        Ok(())
    }

    #[test]
    fn test_parse_unsupported_json_type_errors() {
        let result = parse_schema_to_data_type(r#""unsupported_type""#, "UTC");
        assert!(result.is_err());
        let err_msg = result
            .as_ref()
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(
            err_msg.contains("Unsupported JSON schema type"),
            "unexpected error message: {err_msg}"
        );
    }

    #[test]
    fn test_parse_invalid_json_schema_errors() {
        let result = parse_schema_to_data_type(r#"{"type":"struct""#, "UTC");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_nested_struct_json_schema() -> Result<()> {
        let schema = r#"{
            "type":"struct",
            "fields":[{
                "name":"outer",
                "type":{
                    "type":"struct",
                    "fields":[
                        {"name":"inner","type":"integer","nullable":true,"metadata":{}}
                    ]
                },
                "nullable":true,
                "metadata":{}
            }]
        }"#;
        let dt = parse_schema_to_data_type(schema, "UTC")?;
        assert_eq!(
            dt,
            DataType::Struct(Fields::from(vec![Arc::new(Field::new(
                "outer",
                DataType::Struct(Fields::from(vec![Arc::new(Field::new(
                    "inner",
                    DataType::Int32,
                    true,
                ))])),
                true,
            ))]))
        );
        Ok(())
    }

    #[test]
    fn test_parse_ddl_schema_fallback() -> Result<()> {
        // DDL-style schemas should still work
        let dt = parse_schema_to_data_type("a INT, b DOUBLE", "UTC")?;
        assert_eq!(
            dt,
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("a", DataType::Int32, true)),
                Arc::new(Field::new("b", DataType::Float64, true)),
            ]))
        );
        Ok(())
    }

    #[test]
    fn test_parse_ddl_array_schema() -> Result<()> {
        let dt = parse_schema_to_data_type("ARRAY<INT>", "UTC")?;
        assert!(matches!(dt, DataType::List(_)));
        Ok(())
    }

    #[test]
    fn test_parse_ddl_map_schema() -> Result<()> {
        let dt = parse_schema_to_data_type("MAP<STRING, INT>", "UTC")?;
        assert!(matches!(dt, DataType::Map(_, _)));
        Ok(())
    }
}
