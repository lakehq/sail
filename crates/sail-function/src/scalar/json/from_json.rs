use core::any::type_name;
use std::fs::File;
use std::sync::{Arc};

use datafusion::arrow::array::MapFieldNames;
use datafusion_expr::function::Hint;
use sail_sql_analyzer::parser::parse_data_type;
use sail_sql_analyzer::data_type::from_ast_data_type;

use sail_common::spec;
use sail_common::spec::{
    SAIL_LIST_FIELD_NAME, SAIL_MAP_FIELD_NAME, SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME,
};

use datafusion::arrow::datatypes as adt;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion::arrow::{
    array::{
        Array,
        ArrayRef, MapArray, StringArray, StructArray,
        ListArray,
        Int32Builder, Int64Builder, Float32Builder,
        Float64Builder, Decimal32Builder, StringBuilder,
        TimestampMicrosecondBuilder,
    },
    datatypes::{
        DataType, Fields, Field, TimeUnit, FieldRef
    },
    buffer::{
        NullBuffer, OffsetBuffer, ScalarBuffer
    }
};
use datafusion_functions::utils::make_scalar_function;

use serde_json::Value;

use chrono::NaiveDate;

use std::collections::HashMap;

use crate::functions_nested_utils::downcast_arg;

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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Struct(Fields::empty()))
    }

    fn return_field_from_args(&self, args: datafusion_expr::ReturnFieldArgs) -> Result<FieldRef> {
        let schema_scalar_value = match args.scalar_arguments[1] {
            Some(value) => Ok(value),
            None => plan_err!("Function {} got a non-literal schema argument which is not allowed", self.name())
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
                let dtype = get_schema_data_type(utf8)?;
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
        match arg_types {
            [
                DataType::Utf8 | DataType::LargeUtf8,
                DataType::Utf8 | DataType::Struct(_) | DataType::Map(_, _) | DataType::List(_)
            ] => {
                Ok(vec![arg_types[0].clone(), arg_types[1].clone()])
            },
            [
                DataType::Utf8 | DataType::LargeUtf8,
                DataType::Utf8 | DataType::Struct(_) | DataType::Map(_, _) | DataType::List(_),
                DataType::Map(_, _)
            ] => {
                Ok(vec![arg_types[0].clone(), arg_types[1].clone(), arg_types[2].clone()])
            },
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
        return Err(datafusion_common::DataFusionError::Plan(
            format!("from_json requires 2 or 3 arguments but got {}", args.len())
        ));
    };

    let strings: &StringArray = downcast_arg!(&args[0], StringArray);
    let schema_array: &StringArray = downcast_arg!(&args[1], StringArray);
    let schema: &str = if schema_array.is_empty() {
        return exec_err!(
            "`{}` function requires a schema string, got an empty string",
            "from_json"
        )
    } else {
        schema_array.value(0)
    };
    let schema_fields = get_schema_as_fields(schema)?;
    let d = get_schema_data_type(schema)?;

    string_array_to_json_array(strings, schema_fields, None)
}

fn string_array_to_json_array(
    strings: &StringArray,
    schema_fields: Fields,
    _options: Option<&MapArray>,
) -> Result<ArrayRef> {
    let mut field_builders = create_field_builders(&schema_fields, strings.len())?;
    let mut struct_nulls = vec![true; strings.len()];
    for i in 0..strings.len() {
        let json_str = strings.value(i);
        let value = serde_json::from_str::<serde_json::Value>(json_str).unwrap();
        if let serde_json::Value::Object(obj) = value {
            struct_nulls[i] = true;
            for (field, builder) in schema_fields.iter().zip(field_builders.iter_mut()) {
                let field_value = obj.get(field.name());
                dbg!(&field, &builder);
                dbg!(&field_value);
                append_field_value(builder, field, field_value)?;
            }
        } else {
            struct_nulls[i] = false;
            append_null_to_all_builders(&mut field_builders);
        }
    }
    let field_arrays: Vec<ArrayRef> = field_builders
        .into_iter()
        .map(finish_builder)
        .collect::<Result<Vec<_>>>()?;
    let nulls = NullBuffer::from(struct_nulls);
    let struct_array = StructArray::new(schema_fields.clone(), field_arrays, Some(nulls));
    dbg!(&struct_array);
    Ok(Arc::new(struct_array))
}

#[derive(Debug)]
enum FieldBuilder {
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Decimal32(Decimal32Builder),
    String(StringBuilder),
    TimestampMicrosecondBuilder(TimestampMicrosecondBuilder),
    Struct {
        fields: Fields,
        builders: Vec<FieldBuilder>,
        null_buffer: Vec<bool>,
    },
    Map {
        field: Arc<Field>,
        offsets: Vec<i32>,
        entries: Box<FieldBuilder>, // should be struct
        null_buffer: Vec<bool>,
        ordered: bool,
    },
    List {
        field: Arc<Field>,
        offsets: Vec<i32>,
        builder: Box<FieldBuilder>,
        null_buffer: Vec<bool>,
    },
}

fn create_field_builders(fields: &Fields, capacity: usize) -> Result<Vec<FieldBuilder>> {
    fields
        .iter()
        .map(|field| match field.data_type() {
            DataType::Int32 => Ok(FieldBuilder::Int32(Int32Builder::with_capacity(capacity))),
            DataType::Int64 => Ok(FieldBuilder::Int64(Int64Builder::with_capacity(capacity))),
            DataType::Float32 => Ok(FieldBuilder::Float32(Float32Builder::with_capacity(capacity))),
            DataType::Float64 => Ok(FieldBuilder::Float64(Float64Builder::with_capacity(capacity))),
            DataType::Decimal32(_, _) => Ok(FieldBuilder::Decimal32(Decimal32Builder::with_capacity(capacity))),
            DataType::Utf8 => Ok(FieldBuilder::String(StringBuilder::with_capacity(capacity, capacity*16))),
            DataType::Timestamp(TimeUnit::Microsecond, _) =>
                Ok(FieldBuilder::TimestampMicrosecondBuilder(TimestampMicrosecondBuilder::with_capacity(capacity))),
            DataType::Struct(fields) => {
                let builders = create_field_builders(fields, capacity)?;
                Ok(FieldBuilder::Struct {
                    fields: fields.clone(),
                    builders: builders,
                    null_buffer: Vec::with_capacity(capacity)

                })
            },
            DataType::Map(struct_field, ordered) => { // field is a struct
                let tmp_fields = Fields::from(vec![struct_field.clone()]);
                let struct_builder = create_field_builders(&tmp_fields, capacity)?.pop().unwrap();
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);

                Ok(FieldBuilder::Map {
                    field: struct_field.clone(),
                    offsets: offsets,
                    entries: Box::new(struct_builder),
                    null_buffer: Vec::with_capacity(capacity),
                    ordered: ordered.clone()
                })
            }
            DataType::List(field) => {
                // TODO: allow passing in one field rather than Fields
                let builder = create_field_builders(&Fields::from(vec![field.clone()]), capacity)?.pop().unwrap();
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                Ok(FieldBuilder::List {
                    field: field.clone(),
                    offsets: offsets,
                    builder: Box::new(builder),
                    null_buffer: Vec::with_capacity(capacity)
                })
            },
            other => {
                return Err(DataFusionError::NotImplemented(format!("Unsupported json type: {:?}", other)))
            },
        })
        .collect()
}

fn append_field_value(
    builder: &mut FieldBuilder,
    field: &Field,
    serde_value: Option<&Value>
) -> Result<()> {
    let value = match serde_value {
        Some(Value::Null) | None => {
            match builder {
                FieldBuilder::Int32(builder) => builder.append_null(),
                FieldBuilder::Int64(builder) => builder.append_null(),
                FieldBuilder::Float32(builder) => builder.append_null(),
                FieldBuilder::Float64(builder) => builder.append_null(),
                FieldBuilder::Decimal32(builder) => builder.append_null(),
                FieldBuilder::String(builder) => builder.append_null(),
                FieldBuilder::TimestampMicrosecondBuilder(builder) => builder.append_null(),
                FieldBuilder::Struct {
                    builders: nested_builders,
                    null_buffer,
                    ..
                } => {
                    null_buffer.push(false);
                    append_null_to_all_builders(nested_builders)
                },
                FieldBuilder::Map {
                    entries,
                    null_buffer,
                    ..
                } => {
                    null_buffer.push(false);
                    append_null_to_all_builders(std::slice::from_mut(entries));
                },
                FieldBuilder::List {
					offsets,
					null_buffer,
                    ..
                } => {
                    null_buffer.push(false);
                    offsets.push(*offsets.last().unwrap());
                }
            }
            return Ok(());
        }
        Some(value) => value
    };

    match (builder, field.data_type()) {
        (FieldBuilder::Int32(b), DataType::Int32) => {
            if let Some(i) = value.as_i64() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    b.append_value(i as i32);
                } else {
                    b.append_null(); // Overflow
                }
            } else {
                b.append_null(); // Type mismatch
            }
        },
        (FieldBuilder::Int64(b), DataType::Int64) => {
            if let Some(i) = value.as_i64() {
                b.append_value(i);
            } else {
                b.append_null(); // Type mismatch
            }
        },
        (FieldBuilder::Float32(b), DataType::Float32) => {
            if let Some(f) = value.as_f64() {
                if f >= f32::MIN as f64 && f <= f32::MAX as f64 {
                    b.append_value(f as f32);
                } else {
                    b.append_null(); // Overflow
                }
            } else {
                b.append_null(); // Type mismatch
            }
        },
        (FieldBuilder::Float64(b), DataType::Float64) => {
            if let Some(f) = value.as_f64() {
                b.append_value(f);
            } else {
                b.append_null(); // Type mismatch
            }
        },
        (FieldBuilder::Decimal32(b), DataType::Decimal32(_, _)) => {
            if let Some(f) = value.as_i64() {
                b.append_value(f as i32);
            } else {
                b.append_null();
            }
        },
        (FieldBuilder::String(b), DataType::Utf8) => {
            if let Some(s) = value.as_str() {
                b.append_value(s);
            } else {
                b.append_null();
            }
        },
        (
            FieldBuilder::TimestampMicrosecondBuilder(b),
            DataType::Timestamp(TimeUnit::Microsecond, _)
        ) => {
            let s = value.as_str().unwrap();
            // TODO: hardcoded date parsing for test
            let micro_seconds = NaiveDate::parse_from_str(s, "%d/%m/%Y")
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_micros();
            dbg!(&micro_seconds);
            b.append_value(micro_seconds);
        },
        (
            FieldBuilder::Struct {
                fields: nested_fields,
                builders: nested_builders,
                null_buffer,
            },
            DataType::Struct(_)
        ) => {
            if let Some(obj) = value.as_object() {
                null_buffer.push(true);
                for (nested_field, nested_builder) in nested_fields.iter().zip(nested_builders.iter_mut()) {
                    let nested_value = obj.get(nested_field.name());
                    append_field_value(nested_builder, nested_field, nested_value)?;
                }
            } else {
                null_buffer.push(false);
                append_null_to_all_builders(nested_builders);
            }
        },
        (
            FieldBuilder::Map {
                field,
                entries: struct_builder,
                ..
            },
            DataType::Map(_, _),
        ) => {
            append_field_value(struct_builder, field, Some(value))?;
        },
        (
            FieldBuilder::List {
                field,
                offsets,
                builder,
                null_buffer
            },
            DataType::List(_)
        ) => {
            if let Some(arr) = value.as_array() {
                null_buffer.push(true);
                for val in arr {
                    append_field_value(builder, field, Some(val))?;
                };
                let last = *offsets.last().unwrap();
                offsets.push(last + arr.len() as i32);
            } else {
                null_buffer.push(false);
                let last = *offsets.last().unwrap();
                offsets.push(last);
            }
        }
        (_, other) => {
            return Err(DataFusionError::NotImplemented(format!("Invalid type in json: {:?}", other)));
        }
    }
    Ok(())
}

fn append_null_to_all_builders(builders: &mut [FieldBuilder]) {
    for builder in builders {
        match builder {
            FieldBuilder::Int32(b) => b.append_null(),
            FieldBuilder::Int64(b) => b.append_null(),
            FieldBuilder::Float32(b) => b.append_null(),
            FieldBuilder::Float64(b) => b.append_null(),
            FieldBuilder::Decimal32(b) => b.append_null(),
            FieldBuilder::String(b) => b.append_null(),
            FieldBuilder::TimestampMicrosecondBuilder(b) => b.append_null(),
            FieldBuilder::Struct {
                builders: nested_builder,
                null_buffer,
                ..
            } => {
                null_buffer.push(false);
                append_null_to_all_builders(nested_builder);
            },
            FieldBuilder::Map {
                entries,
                null_buffer,
                ..
            } => {
                null_buffer.push(false);
                append_null_to_all_builders(std::slice::from_mut(entries));
            }
            FieldBuilder::List {
				offsets,
				builder,
				null_buffer,
                ..
            } => {
                null_buffer.push(false);
                let last = *offsets.last().unwrap();
                offsets.push(last);
                append_null_to_all_builders(std::slice::from_mut(builder));
            }
        }
    }
}

fn finish_builder(builder: FieldBuilder) -> Result<ArrayRef> {
    Ok(
        match builder {
            FieldBuilder::Int32(mut b) => Arc::new(b.finish()),
            FieldBuilder::Int64(mut b) => Arc::new(b.finish()),
            FieldBuilder::Float32(mut b) => Arc::new(b.finish()),
            FieldBuilder::Float64(mut b) => Arc::new(b.finish()),
            FieldBuilder::Decimal32(mut b) => Arc::new(b.finish()),
            FieldBuilder::String(mut b) => Arc::new(b.finish()),
            FieldBuilder::TimestampMicrosecondBuilder(mut b) => Arc::new(b.finish()),
            FieldBuilder::Struct {
                fields,
                builders,
                null_buffer
            } => {
                dbg!("finishing struct");
                let nested_arrays: Vec<ArrayRef> = builders
                    .into_iter()
                    .map(finish_builder)
                    .collect::<Result<Vec<_>, DataFusionError>>()?;
                let null_buf = NullBuffer::from(null_buffer);
                dbg!(&fields);
                dbg!(&nested_arrays);
                dbg!(&null_buf);
                Arc::new(StructArray::new(fields, nested_arrays, Some(null_buf)))
            },
            FieldBuilder::Map {
                field,
                offsets,
                entries,
                null_buffer,
                ordered
            } => {
                dbg!("finishing map");
                let struct_array = finish_builder(*entries)?
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .clone();
                Arc::new(MapArray::new(
                    field,
                    OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    struct_array,
                    Some(NullBuffer::from(null_buffer)),
                    ordered
                ))
            }
            FieldBuilder::List {
                field,
                offsets,
                builder,
                null_buffer
            } => {
                let field_builder = *builder;
                let array_ref = finish_builder(field_builder)?;
                Arc::new(ListArray::new(
                    field,
                    OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    array_ref,
                    Some(NullBuffer::from(null_buffer)),
                ))
            }
        }
    )
}

fn get_schema_data_type(schema_str: &str) -> Result<DataType> {
    let ast_type = if let Ok(schema_struct) = parse_data_type(schema_str) {
        schema_struct
    } else {
        parse_data_type(format!("struct<{schema_str}>").as_str())
            .map_err(|_| DataFusionError::Plan("Could not convert str to struct".to_string()))?
    };
    let sail_dtype = from_ast_data_type(ast_type.clone())
        .map_err(|_| DataFusionError::Plan("Could not convert struct to sail type".to_string()))?;
    let arrow_dtype = SailToArrayDataType.resolve_data_type(&sail_dtype)?;
    match arrow_dtype {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Struct(_) | DataType::Map(_, _) | DataType::List(_) => Ok(arrow_dtype),
        other => plan_err!("Function `from_json` using unsupported schema type: {other:?}")
    }
}

fn get_schema_as_fields(schema_str: &str) -> Result<Fields> {
    let schema_struct = if let Ok(schema_struct) = parse_data_type(schema_str) {
        schema_struct
    } else {
        parse_data_type(format!("struct<{schema_str}>").as_str())
            .map_err(|e| DataFusionError::Plan("Could not convert str to struct".to_string()))?
    };
    //let schema_struct = match schema_expr {
    //    expr::Expr::Literal(ScalarValue::Utf8(Some(utf8)), _) => {
    //        let schema = utf8.as_str();
    //        if let Ok(dt) = parse_data_type(schema) {
    //            dt
    //        } else {
    //            parse_data_type(format!("struct<{schema}>").as_str())?
    //        }
    //    },
    //    expr::Expr::Literal(ScalarValue::Struct(struct_array), _) => return Ok(struct_array.fields().clone()),
    //    expr::Expr::Literal(ScalarValue::Map(_map_array), _) => return Err(PlanError::NotImplemented("Schemas from literal Map not implemented yet".to_string())),
    //    expr::Expr::Literal(ScalarValue::List(_list_array), _) => return Err(PlanError::NotImplemented("Schemas from literal Array not implemented yet".to_string())),
    //    other => return Err(PlanError::NotSupported(format!("Schema of type {other:?} not supported"))),
    //};
    let sail_dtype = from_ast_data_type(schema_struct.clone())
        .map_err(|e| DataFusionError::Plan("Could not convert struct to sail type".to_string()))?;
    let arrow_dtype = SailToArrayDataType.resolve_data_type(&sail_dtype)?;
    match arrow_dtype {
        DataType::Struct(fields) => Ok(fields),
        DataType::Map(field, _ordered) => Ok(Fields::from(vec![field])),
        DataType::List(field) => Ok(Fields::from(vec![field])),
        other => Err(DataFusionError::NotImplemented(format!("Not implemented {other:?}")))
    }
}

struct SailToArrayDataType;

impl SailToArrayDataType {
    //fn arrow_binary_type(&self, state: &mut PlanResolverState) -> adt::DataType {
    //    if self.config.arrow_use_large_var_types && state.config().arrow_allow_large_var_types {
    //        adt::DataType::LargeBinary
    //    } else {
    //        adt::DataType::Binary
    //    }
    //}

    //fn arrow_string_type(&self, state: &mut PlanResolverState) -> adt::DataType {
    //    if self.config.arrow_use_large_var_types && state.config().arrow_allow_large_var_types {
    //        adt::DataType::LargeUtf8
    //    } else {
    //        adt::DataType::Utf8
    //    }
    //}

    //pub fn resolve_data_type_for_plan(
    //    &self,
    //    data_type: &spec::DataType,
    //) -> PlanResult<adt::DataType> {
    //    let mut state = PlanResolverState::new();
    //    self.resolve_data_type(data_type, &mut state)
    //}

    /// References:
    ///   org.apache.spark.sql.util.ArrowUtils#toArrowType
    ///   org.apache.spark.sql.connect.common.DataTypeProtoConverter
    pub fn resolve_data_type(
        &self,
        data_type: &spec::DataType,
    ) -> Result<adt::DataType> {
        use spec::DataType;

        match data_type {
            DataType::Null => Ok(adt::DataType::Null),
            DataType::Boolean => Ok(adt::DataType::Boolean),
            DataType::Int8 => Ok(adt::DataType::Int8),
            DataType::Int16 => Ok(adt::DataType::Int16),
            DataType::Int32 => Ok(adt::DataType::Int32),
            DataType::Int64 => Ok(adt::DataType::Int64),
            DataType::UInt8 => Ok(adt::DataType::UInt8),
            DataType::UInt16 => Ok(adt::DataType::UInt16),
            DataType::UInt32 => Ok(adt::DataType::UInt32),
            DataType::UInt64 => Ok(adt::DataType::UInt64),
            DataType::Float16 => Ok(adt::DataType::Float16),
            DataType::Float32 => Ok(adt::DataType::Float32),
            DataType::Float64 => Ok(adt::DataType::Float64),
            DataType::Timestamp {
                time_unit,
                timestamp_type,
            } => Ok(adt::DataType::Timestamp(
                Self::resolve_time_unit(time_unit)?,
                self.resolve_timezone(timestamp_type)?,
            )),
            DataType::Date32 => Ok(adt::DataType::Date32),
            DataType::Date64 => Ok(adt::DataType::Date64),
            DataType::Time32 { time_unit } => {
                Ok(adt::DataType::Time32(Self::resolve_time_unit(time_unit)?))
            }
            DataType::Time64 { time_unit } => {
                Ok(adt::DataType::Time64(Self::resolve_time_unit(time_unit)?))
            }
            DataType::Duration { time_unit } => {
                Ok(adt::DataType::Duration(Self::resolve_time_unit(time_unit)?))
            }
            DataType::Interval {
                interval_unit,
                start_field: _,
                end_field: _,
            } => {
                // TODO: Currently `start_field` and `end_field` is lost in translation.
                //  This does not impact computation accuracy,
                //  This may affect the display string in the `data_type_to_simple_string` function.
                Ok(adt::DataType::Interval(Self::resolve_interval_unit(
                    interval_unit,
                )))
            }
            DataType::Binary => Ok(adt::DataType::Binary),
            DataType::FixedSizeBinary { size } => Ok(adt::DataType::FixedSizeBinary(*size)),
            DataType::LargeBinary => Ok(adt::DataType::LargeBinary),
            DataType::BinaryView => Ok(adt::DataType::BinaryView),
            DataType::Utf8 => Ok(adt::DataType::Utf8),
            DataType::LargeUtf8 => Ok(adt::DataType::LargeUtf8),
            DataType::Utf8View => Ok(adt::DataType::Utf8View),
            DataType::List {
                data_type,
                nullable,
            } => {
                let field = spec::Field {
                    name: SAIL_LIST_FIELD_NAME.to_string(),
                    data_type: data_type.as_ref().clone(),
                    nullable: *nullable,
                    metadata: vec![],
                };
                Ok(adt::DataType::List(Arc::new(
                    self.resolve_field(&field)?,
                )))
            }
            DataType::FixedSizeList {
                data_type,
                nullable,
                length,
            } => {
                let field = spec::Field {
                    name: SAIL_LIST_FIELD_NAME.to_string(),
                    data_type: data_type.as_ref().clone(),
                    nullable: *nullable,
                    metadata: vec![],
                };
                Ok(adt::DataType::FixedSizeList(
                    Arc::new(self.resolve_field(&field)?),
                    *length,
                ))
            }
            DataType::LargeList {
                data_type,
                nullable,
            } => {
                let field = spec::Field {
                    name: SAIL_LIST_FIELD_NAME.to_string(),
                    data_type: data_type.as_ref().clone(),
                    nullable: *nullable,
                    metadata: vec![],
                };
                Ok(adt::DataType::LargeList(Arc::new(
                    self.resolve_field(&field)?,
                )))
            }
            DataType::Struct { fields } => {
                Ok(adt::DataType::Struct(self.resolve_fields(fields)?))
            }
            DataType::Union {
                union_fields,
                union_mode,
            } => {
                // TODO: had to modify this, double check the work
                let (type_ids, fields): (Vec<i8>, Vec<Arc<Field>>) = union_fields
                    .iter()
                    .map(|(i, field)| Ok((*i, Arc::new(self.resolve_field(field)?))))
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .unzip();
                let union_fields = adt::UnionFields::try_new(type_ids, fields)?;
                Ok(adt::DataType::Union(
                    union_fields,
                    Self::resolve_union_mode(union_mode),
                ))
            }
            DataType::Dictionary {
                key_type,
                value_type,
            } => Ok(adt::DataType::Dictionary(
                Box::new(self.resolve_data_type(key_type)?),
                Box::new(self.resolve_data_type(value_type)?),
            )),
            DataType::Decimal128 { precision, scale } => {
                Ok(adt::DataType::Decimal128(*precision, *scale))
            }
            DataType::Decimal256 { precision, scale } => {
                Ok(adt::DataType::Decimal256(*precision, *scale))
            }
            DataType::Map {
                key_type,
                value_type,
                value_type_nullable,
                keys_sorted,
            } => {
                let fields = spec::Fields::from(vec![
                    spec::Field {
                        name: SAIL_MAP_KEY_FIELD_NAME.to_string(),
                        data_type: *key_type.clone(),
                        nullable: false,
                        metadata: vec![],
                    },
                    spec::Field {
                        name: SAIL_MAP_VALUE_FIELD_NAME.to_string(),
                        data_type: *value_type.clone(),
                        nullable: *value_type_nullable,
                        metadata: vec![],
                    },
                ]);
                Ok(adt::DataType::Map(
                    Arc::new(adt::Field::new(
                        SAIL_MAP_FIELD_NAME,
                        adt::DataType::Struct(self.resolve_fields(&fields)?),
                        false,
                    )),
                    *keys_sorted,
                ))
            }
            DataType::Geometry { srid: _ } => {
                // Geometry types are stored as Binary (WKB-encoded)
                // Extension type metadata is added at the Field level, not DataType level
                // See resolve_field() for metadata handling
                Ok(adt::DataType::Binary)
            }
            DataType::Geography {
                srid: _,
                algorithm: _,
            } => {
                // Geography types are stored as Binary (WKB-encoded)
                // Extension type metadata is added at the Field level, not DataType level
                // See resolve_field() for metadata handling
                Ok(adt::DataType::Binary)
            }
            DataType::ConfiguredUtf8 { utf8_type: _ } => {
                // FIXME: Currently `length` and `utf8_type` is lost in translation.
                //  This impacts accuracy if `spec::ConfiguredUtf8Type` is `VarChar` or `Char`.
                //Ok(self.arrow_string_type(state))
                Ok(adt::DataType::Utf8)
            }
            DataType::ConfiguredBinary => Err(DataFusionError::Internal("Conversion of ConfiguredBinary to Binary not supported".to_string())),
            DataType::UserDefined { .. } => Err(DataFusionError::Internal(
                "user defined data type should only exist in a field".to_string(),
            )),
        }
    }

    pub fn resolve_field(
        &self,
        field: &spec::Field,
    ) -> Result<adt::Field> {
        let spec::Field {
            name,
            data_type,
            nullable,
            metadata,
        } = field;
        let mut metadata: HashMap<_, _> = metadata
            .iter()
            .map(|(k, v)| (format!("metadata.{k}"), v.to_string()))
            .collect();
        let data_type = match data_type {
            spec::DataType::UserDefined {
                jvm_class,
                python_class,
                serialized_python_class,
                sql_type,
            } => {
                if let Some(jvm_class) = jvm_class {
                    metadata.insert("udt.jvm_class".to_string(), jvm_class.to_string());
                }
                if let Some(python_class) = python_class {
                    metadata.insert("udt.python_class".to_string(), python_class.to_string());
                }
                if let Some(serialized_python_class) = serialized_python_class {
                    metadata.insert(
                        "udt.serialized_python_class".to_string(),
                        serialized_python_class.to_string(),
                    );
                }
                sql_type
            }
            x => x,
        };
        Ok(
            adt::Field::new(name, self.resolve_data_type(data_type)?, *nullable)
                .with_metadata(metadata),
        )
    }

    pub fn resolve_fields(
        &self,
        fields: &spec::Fields,
    ) -> Result<adt::Fields> {
        let fields = fields
            .into_iter()
            .map(|f| self.resolve_field(f))
            .collect::<Result<Vec<_>>>()?;
        Ok(adt::Fields::from(fields))
    }

    #[allow(dead_code)]
    pub fn resolve_schema(
        &self,
        schema: spec::Schema,
    ) -> Result<adt::Schema> {
        let fields = self.resolve_fields(&schema.fields)?;
        Ok(adt::Schema::new(fields))
    }

    pub fn resolve_time_unit(time_unit: &spec::TimeUnit) -> Result<adt::TimeUnit> {
        match time_unit {
            spec::TimeUnit::Second => Ok(adt::TimeUnit::Second),
            spec::TimeUnit::Millisecond => Ok(adt::TimeUnit::Millisecond),
            spec::TimeUnit::Microsecond => Ok(adt::TimeUnit::Microsecond),
            spec::TimeUnit::Nanosecond => Ok(adt::TimeUnit::Nanosecond),
        }
    }

    pub fn resolve_interval_unit(interval_unit: &spec::IntervalUnit) -> adt::IntervalUnit {
        match interval_unit {
            spec::IntervalUnit::YearMonth => adt::IntervalUnit::YearMonth,
            spec::IntervalUnit::DayTime => adt::IntervalUnit::DayTime,
            spec::IntervalUnit::MonthDayNano => adt::IntervalUnit::MonthDayNano,
        }
    }

    pub fn resolve_union_mode(union_mode: &spec::UnionMode) -> adt::UnionMode {
        match union_mode {
            spec::UnionMode::Sparse => adt::UnionMode::Sparse,
            spec::UnionMode::Dense => adt::UnionMode::Dense,
        }
    }

    pub fn resolve_timezone(
        &self,
        _timestamp_type: &spec::TimestampType,
    ) -> Result<Option<Arc<str>>> {
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_tmp() {
        let json_str = r#"{
            "a": 1,
            "b": 2
        }"#;
        let strings = StringArray::from(vec![json_str, json_str]);
        let schema = get_schema_as_fields("map<string, int>").unwrap();
        string_array_to_json_array(&strings, schema, None).unwrap();
    }
}

