use core::any::type_name;
use std::sync::{Arc};

use datafusion_expr::function::Hint;
use sail_sql_analyzer::parser::parse_data_type;
use sail_sql_analyzer::data_type::from_ast_data_type;

use sail_common::spec::{
    SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME,
};

use datafusion_common::{DataFusionError, Result, ScalarValue, plan_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion::arrow::{
    array::{
        Array, ArrayRef, Float32Builder, Float64Builder, Int32Builder, Int64Builder, ListArray, MapArray, StringArray, StringBuilder, StructArray , TimestampMicrosecondBuilder
    }, buffer::{
        NullBuffer, OffsetBuffer, ScalarBuffer
    }, datatypes::{
        DataType, Field, FieldRef, Fields, TimeUnit
    }
};
use datafusion_functions::utils::make_scalar_function;

use serde_json::Value;


use crate::functions_nested_utils::downcast_arg;
use super::SailToArrayDataType;

enum FieldBuilder {
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    String(StringBuilder),
    TimestampMicrosecondBuilder(TimestampMicrosecondBuilder),
    Struct {
        fields: Fields,
        nested_builders: Vec<FieldBuilder>,
        nulls: Vec<bool>
    },
    Map {
        field: Arc<Field>,
        offsets: Vec<i32>,
        struct_builder: Box<FieldBuilder>,
        nulls: Vec<bool>,
        ordered: bool
    },
    List {
        field: Arc<Field>,
        offsets: Vec<i32>,
        values: Box<FieldBuilder>,
        nulls: Vec<bool>,
    },
}

#[derive(Debug, Default)]
enum ModeOptions {
    #[default]
    PERMISSIVE,
    FAILFAST,
    DROPMALFORMED
}

// https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option
#[derive(Debug)]
struct SparkFromJsonOptions {
    //time_zone: &'static str,
    //primitive_as_string: bool,
    //prefers_decimal: bool,
    //allow_comments: bool,
    //allow_unquoted_field_names: bool,
    //allow_single_quotes: bool,
    //allow_numeric_leading_zeros: bool,
    //allow_backslash_escaping_any_character: bool,
    mode: ModeOptions,
    //column_name_of_corrupted_record: &'static str,
    //date_format: &'static str,
    //timestamp_format: &'static str,
    //timestamp_ntz_format: &'static str,
    //enable_date_time_parsing_fallback: bool,
    //multi_line: bool,
    //allow_unquoted_control_chars: bool,
}

impl TryFrom<String> for ModeOptions {
    type Error = DataFusionError;

    fn try_from(value: String) -> Result<Self, DataFusionError> {
        match value.as_str() {
            "PERMISSIVE" => Ok(ModeOptions::PERMISSIVE),
            "FAILFAST" => Ok(ModeOptions::FAILFAST),
            "DROPMALFORMED" => Ok(ModeOptions::DROPMALFORMED),
            other => plan_err!("Invalid mode option: {other}")
        }
    }
}

impl SparkFromJsonOptions {
    pub fn from_map(map_array: &MapArray) -> Result<Self> {
        let mode = find_key_value(map_array, "mode")
            .unwrap_or(Default::default());
        Ok(Self {
            mode: ModeOptions::try_from(mode)?,
        })
    }
}

impl Default for SparkFromJsonOptions {
    fn default() -> Self {
        Self {
            mode: Default::default(),
        }
    }
}

/// Finds the index of a specified key in a `MapArray`.
///
/// This helper function locates the index of a given key within a `MapArray`,
/// where the keys are stored in a "key" column. It is useful for quickly identifying
/// the position of an option or setting within structured options data.
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

/// Retrieves the value associated with a specified key from a `MapArray`.
///
/// This function extracts the string value assigned to a given key within a `MapArray`,
/// leveraging the index found by `find_key_index`. It searches for the key in the "key"
/// column and returns the corresponding value from the "value" column if found.
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

#[allow(dead_code)]
fn from_json_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return Err(datafusion_common::DataFusionError::Plan(
            format!("from_json requires 2 or 3 arguments but got {}", args.len())
        ));
    };

    let rows: &StringArray = downcast_arg!(&args[0], StringArray);
    let schema = get_schema_data_type(args[1].clone())?;

    let options = if let Some(arr) = args.get(2) {
        let x = downcast_arg!(arr, MapArray);
        SparkFromJsonOptions::from_map(x)?
    } else {
        SparkFromJsonOptions::default()
    };

    parse_rows(rows, schema, options)
}

fn get_schema_data_type(schema_arg: ArrayRef) -> Result<DataType> {
    let as_any = schema_arg.as_any();
    if let Some(arr) = as_any.downcast_ref::<StringArray>() {
        let s = arr.value(0).to_string();
        schema_str_to_data_type(s.as_str())
    } else if let Some(arr) = as_any.downcast_ref::<StructArray>() {
        Ok(arr.data_type().clone())
    } else if let Some(arr) = as_any.downcast_ref::<MapArray>() {
        Ok(arr.data_type().clone())
    } else if let Some(arr) = as_any.downcast_ref::<ListArray>() {
        Ok(arr.data_type().clone())
    } else {
        plan_err!("Unsupported schema field type")
    }
}

fn schema_str_to_data_type(string: &str) -> Result<DataType> {
    let ast_type = if let Ok(schema_struct) = parse_data_type(string) {
        schema_struct
    } else {
        parse_data_type(format!("struct<{string}>").as_str())
            .map_err(|_| DataFusionError::Plan(format!("Schema string invalid so couldn't be parsed: {string}")))?
    };
    let sail_dtype = from_ast_data_type(ast_type.clone())
        .map_err(|_| DataFusionError::Plan("Could not convert struct to sail type".to_string()))?;
    let arrow_dtype = SailToArrayDataType.resolve_data_type(&sail_dtype)?;
    match arrow_dtype {
        DataType::Struct(_) | DataType::Map(_, _) | DataType::List(_) => Ok(arrow_dtype),
        other => plan_err!("Function `from_json` using unsupported schema type: {other:?}")
    }
}

fn parse_rows(rows: &StringArray, schema: DataType, options: SparkFromJsonOptions) -> Result<ArrayRef> {
    let mut builder = create_builder(schema, rows.len())?;
    for i in 0..rows.len() {
        let json_str = rows.value(i);
        let value = serde_json::from_str::<serde_json::Value>(json_str).unwrap();
        append_to_builder(&mut builder, &value, &options)?;
    }
    finish_builder(builder)
}

fn create_builder(data_type: DataType, capacity: usize) -> Result<FieldBuilder> {
    match data_type {
        DataType::Utf8 => Ok(FieldBuilder::String(StringBuilder::with_capacity(capacity, capacity*16))),
        DataType::Int32 => Ok(FieldBuilder::Int32(Int32Builder::with_capacity(capacity))),
        DataType::Int64 => Ok(FieldBuilder::Int64(Int64Builder::with_capacity(capacity))),
        DataType::Float32 => Ok(FieldBuilder::Float32(Float32Builder::with_capacity(capacity))),
        DataType::Float64 => Ok(FieldBuilder::Float64(Float64Builder::with_capacity(capacity))),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(FieldBuilder::TimestampMicrosecondBuilder(TimestampMicrosecondBuilder::with_capacity(capacity))),
        DataType::Struct(fields) => {
            let nested_builders = fields
                .iter()
                .map(|f| create_builder(f.data_type().clone(), capacity))
                .collect::<Result<Vec<_>>>()?;
            Ok(FieldBuilder::Struct {
                fields: fields.clone(),
                nested_builders,
                nulls: Vec::with_capacity(capacity)
            })
        },
        DataType::Map(field, ordered) => {
            let struct_builder = create_builder(field.data_type().clone(), capacity)?;
            let mut offsets = Vec::with_capacity(capacity);
            offsets.push(0);
            Ok(FieldBuilder::Map {
                field,
                offsets,
                struct_builder: Box::new(struct_builder),
                nulls: Vec::with_capacity(capacity),
                ordered
            })
        },
        DataType::List(field) => {
            let values = create_builder(field.data_type().clone(), capacity)?;
            let mut offsets = Vec::with_capacity(capacity);
            offsets.push(0);
            Ok(FieldBuilder::List {
                field,
			    offsets,
			    values: Box::new(values),
			    nulls: Vec::with_capacity(capacity)
            })
        },
        other => Err(DataFusionError::NotImplemented(format!("Unimplemented builder for data type {other:?}")))
    }
}

fn append_to_builder(
    builder: &mut FieldBuilder,
    value: &Value,
    options: &SparkFromJsonOptions
) -> Result<()> {
    // null value
    if let Value::Null = value {
        match builder {
            FieldBuilder::Int32(b) => b.append_null(),
            FieldBuilder::String(b) => b.append_null(),
            FieldBuilder::Struct {
                nested_builders,
                nulls,
                ..
            } => {
                nulls.push(false);
                for nested_builder in nested_builders.iter_mut() {
                    append_to_builder(nested_builder, value, options)?;
                }
            },
            FieldBuilder::Map { struct_builder, nulls, .. } => {
                nulls.push(false);
                append_to_builder(struct_builder, value, options)?;
            },
            FieldBuilder::List { values, nulls, .. } => {
                nulls.push(false);
                append_to_builder(values, value, options)?;
            },
            _ => unimplemented!("Not yet")
        }
    }

    // not null value
    match (builder, value) {
        (FieldBuilder::Int32(b), Value::Number(num)) => {
            if let Some(n) = num.as_i64() {
                if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                    b.append_value(n as i32);
                } else {
                    b.append_null();
                }
            }
        },
        (FieldBuilder::String(b), Value::String(string)) => {
            b.append_value(string);
        },
        (
            FieldBuilder::Struct {
                fields,
                nested_builders,
                nulls
            },
            Value::Object(obj)
        ) => {
            nulls.push(true);
            for (field, nested_builder) in fields.iter().zip(nested_builders.iter_mut()) {
                let val = obj.get(field.name()).unwrap();
                append_to_builder(nested_builder, val, options)?;
            }
        },
        (
            FieldBuilder::Map { struct_builder, nulls, offsets, .. },
            Value::Object(obj)
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
        },
        (
            FieldBuilder::List {
                offsets,
                values,
                nulls,
                ..
            },
            Value::Array(arr)
        ) => {
            nulls.push(true);
            for val in arr.iter() {
                append_to_builder(values, val, options)?;
            }
            let curr_len = offsets.last().unwrap() + arr.len() as i32;
            offsets.push(curr_len);
        },
        _ => return plan_err!("nah dog")
    };
    Ok(())
}

fn finish_builder(builder: FieldBuilder) -> Result<ArrayRef> {
    match builder {
        FieldBuilder::Int32(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Int64(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Float32(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Float64(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::String(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::TimestampMicrosecondBuilder(mut b) => Ok(Arc::new(b.finish())),
        FieldBuilder::Struct {
            fields,
            nested_builders,
            nulls
        } => {
            let arrays = nested_builders
                .into_iter()
                .map(finish_builder)
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::new(fields, arrays, Some(NullBuffer::from(nulls)))))
        },
        FieldBuilder::Map {
            field,
			offsets,
			struct_builder,
			nulls,
			ordered
        } => {
            let deref_struct_builder = *struct_builder;
            let array_ref = finish_builder(deref_struct_builder)?;
            let struct_array = array_ref
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            Ok(Arc::new(MapArray::new(
                field,
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                struct_array.clone(),
                Some(NullBuffer::from(nulls)),
                ordered
            )))
        },
        FieldBuilder::List { field, offsets, values, nulls } => {
            let deref_values = *values;
            let array_ref = finish_builder(deref_values)?;
            Ok(Arc::new(ListArray::new(
                field,
				OffsetBuffer::new(ScalarBuffer::from(offsets)),
				array_ref,
				Some(NullBuffer::from(nulls))
            )))
        },
        _ => plan_err!("Unsupported finish builder")
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_tmp1() {
        let json_str = r#"{
            "a": 1,
            "b": 2
        }"#;
        let strings = StringArray::from(vec![json_str, json_str]);
        let schema_arg = Arc::new(StringArray::from(vec!["map<string, int>"]));
        let schema_dtype = get_schema_data_type(schema_arg).unwrap();
        let x = parse_rows(&strings, schema_dtype, SparkFromJsonOptions::default()).unwrap();
        dbg!(x);
    }
}

