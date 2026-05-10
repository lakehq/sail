use core::any::type_name;
use std::sync::Arc;

use arrow::buffer::NullBuffer;
use chrono::prelude::*;
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::*;
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::*;
use datafusion::error::{DataFusionError, Result};
use datafusion_common::{ScalarValue, exec_err, internal_err, not_impl_err, plan_err};
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
            move |inner_args| from_json_inner(inner_args, session_timezone.as_str()), vec![],)(&args)
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

    let schema_data_type = parse_schema_to_data_type(schema_str, session_timezone)?;

    parse_rows(rows, schema_data_type, options, session_timezone)
}

fn parse_rows(
    rows: &StringArray,
    schema: DataType,
    options: SparkFromJsonOptions,
    session_timezone: &str,
) -> Result<ArrayRef> {
    let mut builder = create_builder(schema, rows.len(), session_timezone)?;
    for i in 0..rows.len() {
        if rows.is_null(i) {
            append_to_builder(&mut builder, &Value::Null, &options, session_timezone)?;
        } else {
            let json_str = rows.value(i);
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(json_str) {
                append_to_builder(&mut builder, &value, &options, session_timezone)?;
            } else {
                // on invalid json handle PERMISSIVE mode
                // note other modes aren't handled yet
                append_permissive_parse_error(&mut builder, &options, session_timezone)?;
            }
        }
    }
    finish_builder(builder)
}

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
        offsets: Vec<i32>,
        struct_builder: Box<FieldBuilder>,
        nulls: Vec<bool>,
        ordered: bool,
    },
    String(StringBuilder),
    Struct {
        fields: Fields,
        nested_builders: Vec<FieldBuilder>,
        nulls: Vec<bool>,
    },
    Time32MillisecondBuilder(Time32MillisecondBuilder),
    Time32SecondBuilder(Time32SecondBuilder),
    Time64MicrosecondBuilder(Time64MicrosecondBuilder),
    Time64NanosecondBuilder(Time64NanosecondBuilder),
    // TODO: claude is there a better pattern to reduce writing `tz` 4 times?
    TimestampMicrosecondBuilder {
        builder: TimestampMicrosecondBuilder,
        tz: Arc<str>
    },
    TimestampMillisecondBuilder {
        builder: TimestampMillisecondBuilder,
        tz: Arc<str>
    },
    TimestampNanosecondBuilder {
        builder: TimestampNanosecondBuilder,
        tz: Arc<str>
    },
    TimestampSecondBuilder {
        builder: TimestampSecondBuilder,
        tz: Arc<str>
    },
}

fn create_builder(data_type: DataType, capacity: usize, session_timezone: &str) -> Result<FieldBuilder> {
    match data_type {
        DataType::Boolean => Ok(FieldBuilder::Boolean(BooleanBuilder::with_capacity(
            capacity,
        ))),
        DataType::Date32 => Ok(FieldBuilder::Date32(Date32Builder::with_capacity(capacity))),
        DataType::Decimal128(precision, scale) => {
            let builder = Decimal128Builder::with_capacity(capacity)
                .with_precision_and_scale(precision, scale)?;
            Ok(FieldBuilder::Decimal128 { builder, precision, scale })
        },
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
            let values = create_builder(field.data_type().clone(), capacity, session_timezone)?;
            let mut offsets = Vec::with_capacity(capacity);
            offsets.push(0);
            Ok(FieldBuilder::List {
                field,
                offsets,
                values: Box::new(values),
                nulls: Vec::with_capacity(capacity),
            })
        }
        DataType::LargeUtf8 => Ok(FieldBuilder::LargeString(LargeStringBuilder::with_capacity(
            capacity,
            capacity*16
        ))),
        DataType::Map(field, ordered) => {
            let struct_builder = create_builder(field.data_type().clone(), capacity, session_timezone)?;
            let mut offsets = Vec::with_capacity(capacity);
            offsets.push(0);
            Ok(FieldBuilder::Map {
                field,
                offsets,
                struct_builder: Box::new(struct_builder),
                nulls: Vec::with_capacity(capacity),
                ordered,
            })
        }
        DataType::Struct(fields) => {
            let nested_builders = fields
                .iter()
                .map(|f| create_builder(f.data_type().clone(), capacity, session_timezone))
                .collect::<Result<Vec<_>>>()?;
            Ok(FieldBuilder::Struct {
                fields: fields.clone(),
                nested_builders,
                nulls: Vec::with_capacity(capacity),
            })
        }
        DataType::Utf8 => Ok(FieldBuilder::String(StringBuilder::with_capacity(
            capacity,
            capacity*16,
        ))),
        DataType::Time32(time_unit) => {
            match time_unit {
                TimeUnit::Millisecond => Ok(FieldBuilder::Time32MillisecondBuilder(Time32MillisecondBuilder::with_capacity(capacity))),
                TimeUnit::Second => Ok(FieldBuilder::Time32SecondBuilder(Time32SecondBuilder::with_capacity(capacity))),
                _ => unreachable!()
            }
        },
        DataType::Time64(time_unit) => {
            match time_unit {
                TimeUnit::Microsecond => Ok(FieldBuilder::Time64MicrosecondBuilder(Time64MicrosecondBuilder::with_capacity(capacity))),
                TimeUnit::Nanosecond => Ok(FieldBuilder::Time64NanosecondBuilder(Time64NanosecondBuilder::with_capacity(capacity))),
                _ => unreachable!()
            }
        },
        DataType::Timestamp(time_unit, tz) => {
            let resolved_tz = tz.clone().unwrap_or(Arc::from(session_timezone));
            match time_unit {
                TimeUnit::Microsecond => {
                    Ok(FieldBuilder::TimestampMicrosecondBuilder {
                        builder: TimestampMicrosecondBuilder::with_capacity(capacity)
                            .with_timezone_opt(tz),
                        tz: resolved_tz
                    })
                },
                TimeUnit::Millisecond => {
                    Ok(FieldBuilder::TimestampMillisecondBuilder {
                        builder: TimestampMillisecondBuilder::with_capacity(capacity)
                            .with_timezone_opt(tz),
                        tz: resolved_tz
                    })
                },
                TimeUnit::Nanosecond => {
                    Ok(FieldBuilder::TimestampNanosecondBuilder {
                        builder: TimestampNanosecondBuilder::with_capacity(capacity)
                            .with_timezone_opt(tz),
                        tz: resolved_tz
                    })
                },
                TimeUnit::Second => {
                    Ok(FieldBuilder::TimestampSecondBuilder {
                        builder: TimestampSecondBuilder::with_capacity(capacity)
                            .with_timezone_opt(tz),
                        tz: resolved_tz
                    })
                },
            }
        },
        other => not_impl_err!(
            "`{}` function has unimplemented builder for data type {other:?}",
            SparkFromJson::FROM_JSON_NAME
        ),
    }
}

#[expect(clippy::unwrap_used)]
fn append_to_builder(
    builder: &mut FieldBuilder,
    value: &Value,
    options: &SparkFromJsonOptions,
    session_timezone: &str,
) -> Result<()> {
    // null value
    if let Value::Null = value {
        match builder {
            FieldBuilder::Boolean(b) => b.append_null(),
            FieldBuilder::Decimal128 { builder, .. } => builder.append_null(),
            FieldBuilder::Date32(b) => b.append_null(),
            FieldBuilder::Float32(b) => b.append_null(),
            FieldBuilder::Float64(b) => b.append_null(),
            FieldBuilder::LargeString(b) => b.append_null(),
            FieldBuilder::Int8(b) => b.append_null(),
            FieldBuilder::Int16(b) => b.append_null(),
            FieldBuilder::Int32(b) => b.append_null(),
            FieldBuilder::Int64(b) => b.append_null(),
            FieldBuilder::String(b) => b.append_null(),
            FieldBuilder::Time32MillisecondBuilder(b) => b.append_null(),
            FieldBuilder::Time32SecondBuilder(b) => b.append_null(),
            FieldBuilder::Time64MicrosecondBuilder(b) => b.append_null(),
            FieldBuilder::Time64NanosecondBuilder(b) => b.append_null(),
            FieldBuilder::TimestampMicrosecondBuilder{ builder, .. } => builder.append_null(),
            FieldBuilder::TimestampMillisecondBuilder{ builder, .. } => builder.append_null(),
            FieldBuilder::TimestampNanosecondBuilder{ builder, .. } => builder.append_null(),
            FieldBuilder::TimestampSecondBuilder{ builder, .. } => builder.append_null(),
            FieldBuilder::Struct {
                nested_builders,
                nulls,
                ..
            } => {
                nulls.push(false);
                for nested_builder in nested_builders.iter_mut() {
                    append_to_builder(nested_builder, value, options, session_timezone)?;
                }
            }
            FieldBuilder::Map { nulls, offsets, .. } => {
                nulls.push(false);
                let curr = *offsets.last().unwrap();
                offsets.push(curr);
            }
            FieldBuilder::List { nulls, offsets, .. } => {
                nulls.push(false);
                let curr = *offsets.last().unwrap();
                offsets.push(curr);
            }
        }
    // not null value
    } else {
        match (builder, value) {
            (FieldBuilder::Boolean(b), Value::Bool(bool)) => b.append_value(*bool),
            (FieldBuilder::Date32(b), Value::String(string)) => {
                let date32 = parse_date32(string, options)?;
                b.append_value(date32);
            }
            (FieldBuilder::Decimal128 { builder, precision, scale }, _) => {
                match value {
                    Value::Number(n) => {
                        let decimal128 = parse_decimal_to_i128(&n.to_string(), precision, scale)?;
                        builder.append_value(decimal128);
                    },
                    Value::String(s) => {
                        let decimal128 = parse_decimal_to_i128(s, precision, scale)?;
                        builder.append_value(decimal128);
                    },
                    _ => builder.append_null(),
                };
            }
            (FieldBuilder::Float32(b), Value::Number(num)) => {
                if let Some(float) = num.as_f64() {
                    if float >= f32::MIN as f64 && float <= f32::MAX as f64 {
                        b.append_value(float as f32);
                    } else {
                        b.append_null();
                    }
                } else {
                    b.append_null();
                }
            }
            (FieldBuilder::Float64(b), Value::Number(num)) => {
                if let Some(float) = num.as_f64() {
                    b.append_value(float);
                } else {
                    b.append_null();
                }
            }
            // TODO: 1) verify string size 2) other Values can be converted to strings
            (FieldBuilder::LargeString(b), Value::String(string)) => b.append_value(string),
            (FieldBuilder::Int8(b), Value::Number(num)) => {
                if let Some(n) = num.as_i64() {
                    if n >= i8::MIN as i64 && n <= i8::MAX as i64 {
                        b.append_value(n as i8);
                    } else {
                        b.append_null();
                    }
                }
            }
            (FieldBuilder::Int16(b), Value::Number(num)) => {
                if let Some(n) = num.as_i64() {
                    if n >= i16::MIN as i64 && n <= i16::MAX as i64 {
                        b.append_value(n as i16);
                    } else {
                        b.append_null();
                    }
                }
            }
            (FieldBuilder::Int32(b), Value::Number(num)) => {
                if let Some(n) = num.as_i64() {
                    if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                        b.append_value(n as i32);
                    } else {
                        b.append_null();
                    }
                } else {
                    b.append_null();
                }
            }
            (FieldBuilder::Int64(b), Value::Number(num)) => {
                if let Some(n) = num.as_i64() {
                    b.append_value(n);
                } else {
                    b.append_null();
                }
            }
            (FieldBuilder::String(b), Value::String(string)) => b.append_value(string),
            (
                FieldBuilder::Struct {
                    fields,
                    nested_builders,
                    nulls,
                },
                Value::Object(obj),
            ) => {
                nulls.push(true);
                for (field, nested_builder) in fields.iter().zip(nested_builders.iter_mut()) {
                    // if key not found return null
                    let val = if let Some(v) = obj.get(field.name()) {
                        v
                    } else {
                        &Value::Null
                    };
                    append_to_builder(nested_builder, val, options, session_timezone)?;
                }
            }
            (
                FieldBuilder::Map {
                    struct_builder,
                    nulls,
                    offsets,
                    ..
                },
                Value::Object(obj),
            ) => {
                // just push to struct builder
                nulls.push(true);
                for (k, v) in obj.iter() {
                    // map each pair to "key" and "value" - default field names
                    let entry = serde_json::json!({
                        "key": k,
                        "value": v
                    });
                    append_to_builder(struct_builder, &entry, options, session_timezone)?;
                }
                let curr_len = offsets.last().unwrap() + obj.len() as i32;
                offsets.push(curr_len);
            }
            (
                FieldBuilder::List {
                    offsets,
                    values,
                    nulls,
                    ..
                },
                Value::Array(arr),
            ) => {
                nulls.push(true);
                for val in arr.iter() {
                    append_to_builder(values, val, options, session_timezone)?;
                }
                let curr_len = offsets.last().unwrap() + arr.len() as i32;
                offsets.push(curr_len);
            }
            // it's valid to cast structs into a struct array
            (
                FieldBuilder::List {
                    offsets,
                    values: nested_builder,
                    nulls,
                    ..
                },
                Value::Object(_),
            ) => {
                nulls.push(true);
                append_to_builder(nested_builder, value, options, session_timezone)?;
                let curr_len = offsets.last().unwrap() + 1_i32; // only need to increment by 1
                offsets.push(curr_len);
            }
            // TODO: check spark parity - BDD tests want nulls
            (FieldBuilder::Time32MillisecondBuilder(b), _) => b.append_null(),
            (FieldBuilder::Time32SecondBuilder(b), _) => b.append_null(),
            (FieldBuilder::Time64MicrosecondBuilder(b), _) => b.append_null(),
            (FieldBuilder::Time64NanosecondBuilder(b), _) => b.append_null(),
            (FieldBuilder::TimestampMicrosecondBuilder{ builder, tz }, _) => {
                match value {
                    Value::String(string) => {
                        let naive_datetime = parse_timestamp(string, tz.clone(), options)?;
                        if let Some(timestamp_microseconds) = TimestampMicrosecondType::from_datetime(naive_datetime) {
                            builder.append_value(timestamp_microseconds);
                        } else {
                            builder.append_null();
                        }
                    },
                    _ => builder.append_null(),
                }
            }
            (FieldBuilder::TimestampMillisecondBuilder{ builder, tz }, _) => {
                match value {
                    Value::String(string) => {
                        let naive_datetime = parse_timestamp(string, tz.clone(), options)?;
                        if let Some(timestamp_milliseconds) = TimestampMillisecondType::from_datetime(naive_datetime) {
                            builder.append_value(timestamp_milliseconds);
                        } else {
                            builder.append_null();
                        }
                    },
                    _ => builder.append_null(),
                }
            }
            (FieldBuilder::TimestampNanosecondBuilder{ builder, tz }, _) => {
                match value {
                    Value::String(string) => {
                        let naive_datetime = parse_timestamp(string, tz.clone(), options)?;
                        if let Some(timestamp_nanoseconds) = TimestampNanosecondType::from_datetime(naive_datetime) {
                            builder.append_value(timestamp_nanoseconds);
                        } else {
                            builder.append_null();
                        }
                    },
                    _ => builder.append_null(),
                }
            }
            (FieldBuilder::TimestampSecondBuilder{ builder, tz }, _) => {
                match value {
                    Value::String(string) => {
                        let naive_datetime = parse_timestamp(string, tz.clone(), options)?;
                        if let Some(timestamp_second) = TimestampSecondType::from_datetime(naive_datetime) {
                            builder.append_value(timestamp_second);
                        } else {
                            builder.append_null();
                        }
                    },
                    _ => builder.append_null(),
                }
            }
            (other, other1) => {
                return plan_err!("Unsupported conversion of value {other1:?} to type {other:?}")
            }
        };
    }
    Ok(())
}

/// Handle PERMISSIVE MODE. When json fails to parse:
/// - struct: stays valid but each value gets null
/// - map: invalid, coerce to null
/// - list: invalid, coerce to null
fn append_permissive_parse_error(builder: &mut FieldBuilder, options: &SparkFromJsonOptions, session_timezone: &str) -> Result<()> {
    match builder {
        FieldBuilder::Struct { nested_builders, nulls, .. } => {
            nulls.push(true);
            for nested_builder in nested_builders.iter_mut() {
                append_to_builder(nested_builder, &Value::Null, options, session_timezone)?;
            }
        },
        FieldBuilder::Map { offsets, nulls, .. } => {
            nulls.push(false);
            let curr = *offsets.last().unwrap();
            offsets.push(curr);
        },
        FieldBuilder::List { offsets, nulls, .. } => {
            nulls.push(false);
            let curr = *offsets.last().unwrap();
            offsets.push(curr);
        },
        other => unreachable!("function shouldn't act on builder type: {other:#?}")
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
        FieldBuilder::String(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Time32MillisecondBuilder(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Time32SecondBuilder(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Time64MicrosecondBuilder(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Time64NanosecondBuilder(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::TimestampMicrosecondBuilder { mut builder, .. } => Ok(Arc::new(builder.finish())),
        FieldBuilder::TimestampMillisecondBuilder { mut builder, .. } => Ok(Arc::new(builder.finish())),
        FieldBuilder::TimestampNanosecondBuilder { mut builder, .. } => Ok(Arc::new(builder.finish())),
        FieldBuilder::TimestampSecondBuilder { mut builder, .. } => Ok(Arc::new(builder.finish())),
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
        FieldBuilder::Map {
            field,
            offsets,
            struct_builder,
            nulls,
            ordered,
        } => {
            let deref_struct_builder = *struct_builder;
            let array_ref = finish_builder(deref_struct_builder)?;
            let struct_array = downcast_array::<StructArray>(&*array_ref);
            Ok(Arc::new(MapArray::new(
                field,
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                struct_array.clone(),
                Some(NullBuffer::from(nulls)),
                ordered,
            )))
        }
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
    }
}

/// Parse a decimal string into an unscaled `i128` value for `Decimal128(precision, scale)`.
///
/// Handles integer, fractional, and scientific notation inputs. Fractional digits are
/// padded or truncated (with rounding) to match the target scale. Validates that the
/// result fits within the declared precision.
fn parse_decimal_to_i128(input: &str, precision: &u8, scale: &i8) -> Result<<Decimal128Type as ArrowPrimitiveType>::Native> {
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
    let scale_delta = scale.clone() as i32 - effective_scale;

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
    if digit_count > precision.clone() as u32 {
        return Err(DataFusionError::Execution(format!(
            "Decimal value '{input}' exceeds declared precision {precision} and scale {scale}"
        )));
    }

    Ok(result)
}

fn parse_date32(s: &str, options: &SparkFromJsonOptions) -> Result<<Date32Type as ArrowPrimitiveType>::Native> {
    let format = &options.date_format;
    let naive_date = NaiveDate::parse_from_str(s, &format).map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to parse date '{s}' with format '{format}': {e}"
        ))
    })?;
    Ok(Date32Type::from_naive_date(naive_date))
}

fn parse_timestamp(
    s: &str,
    timezone: Arc<str>,
    options: &SparkFromJsonOptions,
) -> Result<DateTime<Utc>> {
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
    let tz: Tz = timezone.as_ref()
        .parse()
        .map_err(|e| DataFusionError::Execution(format!("Invalid timezone '{timezone}': {e}")))?;
    localize_with_fallback(&tz, &naive_datetime)
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
