use core::any::type_name;
use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime};
use datafusion::arrow::array::{
    downcast_array, Array, ArrayRef, BooleanBuilder, Decimal128Builder, Float32Builder,
    Float64Builder, Int32Builder, Int64Builder, ListArray, MapArray, StringArray, StringBuilder,
    StructArray, TimestampMicrosecondBuilder,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, TimeUnit};
use datafusion_common::{plan_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::function::Hint;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;
use serde_json::Value;

use super::data_type::parse_spark_data_type;
use super::SailToArrayDataType;
use crate::functions_nested_utils::downcast_arg;

impl ScalarUDFImpl for SparkFromJson {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &"from_json"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    // return type is dynamic so is identified in `return_field_from_args`
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Struct(Fields::empty()))
    }

    fn return_field_from_args(&self, args: datafusion_expr::ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() < 2 || args.arg_fields.len() > 3 {
            return Err(datafusion_common::DataFusionError::Plan(format!(
                "from_json requires 2 or 3 arguments but got {}",
                args.arg_fields.len()
            )));
        };

        let schema_scalar_value = match args.scalar_arguments[1] {
            Some(value) => Ok(value),
            None => plan_err!(
                "Function {} got a non-literal schema argument which is not allowed",
                self.name()
            ),
        }?;
        let schema_data_type = args.arg_fields[1].data_type();

        match (schema_data_type, schema_scalar_value) {
            (
                DataType::Utf8,
                ScalarValue::Utf8(Some(utf8))
            ) | (
                DataType::LargeUtf8,
                ScalarValue::LargeUtf8(Some(utf8))
            )=> {
                let dtype = schema_str_to_data_type(utf8)?;
                Ok(Arc::new(Field::new(
                    "from_utf8",
                    dtype,
                    true // TODO: can we get nullable from here? maybe it's an option?
                )))
            },
            (DataType::Struct(_), ScalarValue::Struct(struct_array)) => {
                Ok(Arc::new(Field::new(
                    "struct",
                    schema_data_type.clone(),
                    struct_array.is_nullable()
                )))
            },
            (DataType::Map(_, _), ScalarValue::Map(map_array)) => {
                Ok(Arc::new(Field::new(
                    "map",
                    schema_data_type.clone(),
                    map_array.is_nullable()
                )))
            },
            (DataType::List(_), ScalarValue::List(list_array)) => {
                Ok(Arc::new(Field::new(
                    "list",
                    schema_data_type.clone(),
                    list_array.is_nullable()
                )))
            },
            (other, _) => plan_err!("Function {} expects a schema argument of literal datatype string, struct, map, or list. Instead got {other:?}", self.name())

        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let hints = vec![Hint::Pad, Hint::AcceptsSingular, Hint::AcceptsSingular];
        make_scalar_function(from_json_inner, hints)(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 || arg_types.len() > 3 {
            return Err(datafusion_common::DataFusionError::Plan(format!(
                "from_json requires 2 or 3 arguments but got {}",
                arg_types.len()
            )));
        };

        match arg_types {
            [DataType::Utf8 | DataType::LargeUtf8, DataType::Utf8 | DataType::Struct(_) | DataType::Map(_, _) | DataType::List(_)] => {
                Ok(vec![arg_types[0].clone(), arg_types[1].clone()])
            }
            [DataType::Utf8 | DataType::LargeUtf8, DataType::Utf8 | DataType::Struct(_) | DataType::Map(_, _) | DataType::List(_), DataType::Map(_, _)] => {
                Ok(vec![
                    arg_types[0].clone(),
                    arg_types[1].clone(),
                    arg_types[2].clone(),
                ])
            }
            other => {
                plan_err!(
                    "Unsupported datatypes for function `{}`: found {}, {}, {}",
                    self.name(),
                    other[0],
                    other[1],
                    other[2]
                )
            }
        }
    }
}

fn from_json_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return Err(datafusion_common::DataFusionError::Plan(format!(
            "from_json requires 2 or 3 arguments but got {}",
            args.len()
        )));
    };

    let rows: &StringArray = downcast_arg!(&args[0], StringArray);
    let schema = get_schema_data_type(args[1].clone())?;

    let options = if let Some(arr) = args.get(2) {
        let map_array = downcast_arg!(arr, MapArray);
        SparkFromJsonOptions::default().from_map(map_array)?
    } else {
        SparkFromJsonOptions::default()
    };

    parse_rows(rows, schema, options)
}

fn get_schema_data_type(schema_arg: ArrayRef) -> Result<DataType> {
    let as_any = schema_arg.as_any();
    if let Some(arr) = as_any.downcast_ref::<StructArray>() {
        Ok(arr.data_type().clone())
    } else if let Some(arr) = as_any.downcast_ref::<MapArray>() {
        Ok(arr.data_type().clone())
    } else if let Some(arr) = as_any.downcast_ref::<ListArray>() {
        Ok(arr.data_type().clone())
    } else if let Some(arr) = as_any.downcast_ref::<StringArray>() {
        let s = arr.value(0).to_string();
        schema_str_to_data_type(s.as_str())
    } else {
        plan_err!("Unsupported schema field type")
    }
}

fn schema_str_to_data_type(string: &str) -> Result<DataType> {
    let sail_dtype = parse_spark_data_type(string)?;
    let arrow_dtype = SailToArrayDataType.resolve_data_type(&sail_dtype)?;
    match arrow_dtype {
        DataType::Struct(_) | DataType::Map(_, _) | DataType::List(_) => Ok(arrow_dtype),
        other => plan_err!("Function `from_json` using unsupported schema type: {other:?}"),
    }
}

fn parse_rows(
    rows: &StringArray,
    schema: DataType,
    options: SparkFromJsonOptions,
) -> Result<ArrayRef> {
    let mut builder = create_builder(schema, rows.len())?;
    for i in 0..rows.len() {
        let json_str = rows.value(i);
        let value = serde_json::from_str::<serde_json::Value>(json_str)
            .map_err(|e| DataFusionError::Execution(format!("Unable to parse json: {e}")))?;
        append_to_builder(&mut builder, &value, &options)?;
    }
    finish_builder(builder)
}

#[derive(Debug)]
enum FieldBuilder {
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Decimal128 {
        builder: Decimal128Builder,
        scale: i8,
    },
    String(StringBuilder),
    Boolean(BooleanBuilder),
    TimestampMicrosecondBuilder(TimestampMicrosecondBuilder),
    Struct {
        fields: Fields,
        nested_builders: Vec<FieldBuilder>,
        nulls: Vec<bool>,
    },
    Map {
        field: Arc<Field>,
        offsets: Vec<i32>,
        struct_builder: Box<FieldBuilder>,
        nulls: Vec<bool>,
        ordered: bool,
    },
    List {
        field: Arc<Field>,
        offsets: Vec<i32>,
        values: Box<FieldBuilder>,
        nulls: Vec<bool>,
    },
}

// not all options supported yet
// https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option
#[derive(Debug)]
struct SparkFromJsonOptions {
    mode: ModeOptions,
    timestamp_format: String,
}

impl SparkFromJsonOptions {
    pub fn from_map(mut self, map_array: &MapArray) -> Result<Self> {
        let inner_struct = map_array.value(0);
        let keys = downcast_array::<StringArray>(inner_struct.column(0));
        let values = downcast_array::<StringArray>(inner_struct.column(1));
        for (key, value) in keys.iter().zip(values.iter()) {
            let (key, value) = match (key, value) {
                (Some(k), Some(v)) => (k, v),
                (_, _) => {
                    return Err(DataFusionError::Plan(
                        "Bad options most likely because len of keys != len of values".to_string(),
                    ))
                }
            };
            match key {
                "mode" => self.mode = ModeOptions::from_str(value.to_string())?,
                "timestampFormat" => {
                    self.timestamp_format = SparkFromJsonOptions::convert_format(value)
                }
                other => {
                    return plan_err!("Found unsupported option type when parsing options: {other}")
                }
            }
        }
        Ok(self)
    }

    fn convert_format(fmt: &str) -> String {
        fmt.replace("yyyy", "%Y")
            .replace("MM", "%m")
            .replace("dd", "%d")
            .replace("HH", "%H")
            .replace("mm", "%M")
            .replace("ss", "%S")
    }
}

impl Default for SparkFromJsonOptions {
    fn default() -> Self {
        Self {
            mode: Default::default(),
            timestamp_format: SparkFromJsonOptions::convert_format("yyyy-MM-ddTHH:mm:ss"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkFromJson {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for SparkFromJson {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkFromJson {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: ["from_json".to_string()],
        }
    }
}

#[derive(Debug, Default)]
enum ModeOptions {
    #[default]
    Permissive,
    FailFast,
    DropMalformed,
}

impl ModeOptions {
    fn from_str(value: String) -> Result<Self, DataFusionError> {
        match value.as_str() {
            "PERMISSIVE" => Ok(ModeOptions::Permissive),
            "FAILFAST" => Ok(ModeOptions::FailFast),
            "DROPMALFORMED" => Ok(ModeOptions::DropMalformed),
            other => plan_err!("Invalid mode option: {other}"),
        }
    }
}

fn create_builder(data_type: DataType, capacity: usize) -> Result<FieldBuilder> {
    match data_type {
        DataType::Utf8 => Ok(FieldBuilder::String(StringBuilder::with_capacity(
            capacity,
            capacity * 16,
        ))),
        DataType::Int32 => Ok(FieldBuilder::Int32(Int32Builder::with_capacity(capacity))),
        DataType::Int64 => Ok(FieldBuilder::Int64(Int64Builder::with_capacity(capacity))),
        DataType::Float32 => Ok(FieldBuilder::Float32(Float32Builder::with_capacity(
            capacity,
        ))),
        DataType::Float64 => Ok(FieldBuilder::Float64(Float64Builder::with_capacity(
            capacity,
        ))),
        DataType::Decimal128(precision, scale) => {
            let builder = Decimal128Builder::with_capacity(capacity)
                .with_precision_and_scale(precision, scale)?;
            Ok(FieldBuilder::Decimal128 { builder, scale })
        }
        DataType::Boolean => Ok(FieldBuilder::Boolean(BooleanBuilder::with_capacity(
            capacity,
        ))),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Ok(FieldBuilder::TimestampMicrosecondBuilder(
                TimestampMicrosecondBuilder::with_capacity(capacity),
            ))
        }
        DataType::Struct(fields) => {
            let nested_builders = fields
                .iter()
                .map(|f| create_builder(f.data_type().clone(), capacity))
                .collect::<Result<Vec<_>>>()?;
            Ok(FieldBuilder::Struct {
                fields: fields.clone(),
                nested_builders,
                nulls: Vec::with_capacity(capacity),
            })
        }
        DataType::Map(field, ordered) => {
            let struct_builder = create_builder(field.data_type().clone(), capacity)?;
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
        DataType::List(field) => {
            let values = create_builder(field.data_type().clone(), capacity)?;
            let mut offsets = Vec::with_capacity(capacity);
            offsets.push(0);
            Ok(FieldBuilder::List {
                field,
                offsets,
                values: Box::new(values),
                nulls: Vec::with_capacity(capacity),
            })
        }
        other => Err(DataFusionError::NotImplemented(format!(
            "Unimplemented builder for data type {other:?}"
        ))),
    }
}

fn append_to_builder(
    builder: &mut FieldBuilder,
    value: &Value,
    options: &SparkFromJsonOptions,
) -> Result<()> {
    // null value
    if let Value::Null = value {
        match builder {
            FieldBuilder::Int32(b) => b.append_null(),
            FieldBuilder::Int64(b) => b.append_null(),
            FieldBuilder::Float32(b) => b.append_null(),
            FieldBuilder::Float64(b) => b.append_null(),
            FieldBuilder::Decimal128 { builder, .. } => builder.append_null(),
            FieldBuilder::String(b) => b.append_null(),
            FieldBuilder::Boolean(b) => b.append_null(),
            FieldBuilder::TimestampMicrosecondBuilder(b) => b.append_null(),
            FieldBuilder::Struct {
                nested_builders,
                nulls,
                ..
            } => {
                nulls.push(false);
                for nested_builder in nested_builders.iter_mut() {
                    append_to_builder(nested_builder, value, options)?;
                }
            }
            FieldBuilder::Map {
                struct_builder,
                nulls,
                ..
            } => {
                nulls.push(false);
                append_to_builder(struct_builder, value, options)?;
            }
            FieldBuilder::List { values, nulls, .. } => {
                nulls.push(false);
                append_to_builder(values, value, options)?;
            }
        }
    // not null value
    } else {
        match (builder, value) {
            (FieldBuilder::Int32(b), Value::Number(num)) => {
                if let Some(n) = num.as_i64() {
                    if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                        b.append_value(n as i32);
                    } else {
                        b.append_null();
                    }
                }
            }
            (FieldBuilder::Int64(b), Value::Number(num)) => {
                if let Some(n) = num.as_i64() {
                    b.append_value(n);
                } else {
                    b.append_null();
                }
            }
            (FieldBuilder::Float32(b), Value::Number(num)) => {
                if let Some(float) = num.as_f64() {
                    if float >= f32::MIN as f64 && float <= f32::MAX as f64 {
                        b.append_value(float as f32);
                    } else {
                        b.append_null();
                    }
                }
            }
            (FieldBuilder::Float64(b), Value::Number(num)) => {
                if let Some(float) = num.as_f64() {
                    b.append_value(float);
                } else {
                    b.append_null();
                }
            }
            (FieldBuilder::Decimal128 { builder, scale, .. }, Value::Number(num)) => {
                let scale_factor = 10i128.pow(*scale as u32);
                if let Some(i) = num.as_i128() {
                    builder.append_value(i * scale_factor);
                } else if let Some(f) = num.as_f64() {
                    let decimal = (f * scale_factor as f64).round() as i128;
                    builder.append_value(decimal);
                } else {
                    builder.append_null();
                }
            }
            (FieldBuilder::TimestampMicrosecondBuilder(b), Value::String(string)) => {
                let ts = if let Ok(timestamp) =
                    NaiveDateTime::parse_from_str(string, &options.timestamp_format)
                {
                    timestamp
                } else if let Ok(date) =
                    NaiveDate::parse_from_str(string, &options.timestamp_format)
                {
                    date.and_hms_opt(0, 0, 0)
                        .expect("Unreachable: only fails on invalid hours/mins/secs")
                } else {
                    return Err(DataFusionError::Execution(format!(
                        "Timestamp error: can't parse {string:?} to {:?}",
                        options.timestamp_format
                    )));
                };

                let micro_seconds = ts.and_utc().timestamp_micros();
                b.append_value(micro_seconds);
            }
            (FieldBuilder::String(b), Value::String(string)) => b.append_value(string),
            (FieldBuilder::Boolean(b), Value::Bool(bool)) => b.append_value(*bool),
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
                    let val = if let Some(v) = obj.get(field.name()) {
                        v
                    } else {
                        let possible_keys = fields
                            .iter()
                            .map(|f| format!("{:?}", f.name()))
                            .collect::<Vec<_>>()
                            .join(", ");
                        return Err(DataFusionError::Execution(format!(
                            "Couldn't find the key {:?} out of specified keys: {possible_keys}",
                            field.name()
                        )));
                    };
                    append_to_builder(nested_builder, val, options)?;
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
                    append_to_builder(struct_builder, &entry, options)?;
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
                    append_to_builder(values, val, options)?;
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
                append_to_builder(nested_builder, value, options)?;
                let curr_len = offsets.last().unwrap() + 1 as i32; // only need to increment by 1
                offsets.push(curr_len);
            }
            (other, other1) => {
                return plan_err!("Unsupported conversion of value {other1:?} to type {other:?}")
            }
        };
    }

    Ok(())
}

fn finish_builder(builder: FieldBuilder) -> Result<ArrayRef> {
    match builder {
        FieldBuilder::Int32(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Int64(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Float32(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Float64(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Decimal128 { mut builder, .. } => Ok(Arc::new(builder.finish())),
        FieldBuilder::String(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Boolean(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::TimestampMicrosecondBuilder(mut b) => Ok(Arc::new(b.finish())),
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
