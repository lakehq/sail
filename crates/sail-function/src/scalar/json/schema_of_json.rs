use std::sync::Arc;

use datafusion::arrow::array::{
    downcast_array, Array, ArrayRef, MapArray, StringArray, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::{exec_err, plan_err, DataFusionError, Result};
use datafusion_expr::function::Hint;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_expr_common::signature::Volatility;
use datafusion_functions::downcast_arg;
use datafusion_functions::utils::make_scalar_function;
use serde_json::Value;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSchemaOfJson {
    signature: Signature,
}

impl Default for SparkSchemaOfJson {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSchemaOfJson {
    pub const SCHEMA_OF_JSON_NAME: &'static str = "schema_of_json";

    pub fn new() -> Self {
        SparkSchemaOfJson {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    fn validate_args_len<T>(args: &[T]) -> Result<()> {
        if args.is_empty() || args.len() > 2 {
            return plan_err!(
                "function `{}` expected 1 to 2 args but got {}",
                Self::SCHEMA_OF_JSON_NAME,
                args.len()
            );
        };
        Ok(())
    }

    fn validate_args_are_literal(cols: &[ColumnarValue]) -> Result<()> {
        if let Some(ColumnarValue::Array(_)) = cols.first() {
            return Err(DataFusionError::Execution(format!(
                "Expected a literal value for the first arg of `{}`, instead got a column",
                Self::SCHEMA_OF_JSON_NAME,
            )));
        }
        if let Some(ColumnarValue::Array(_)) = cols.get(1) {
            return Err(DataFusionError::Execution(format!(
                "Expected a literal value for the second arg of `{}`, instead got a column",
                Self::SCHEMA_OF_JSON_NAME,
            )));
        }
        Ok(())
    }

    fn validate_arg_types(arg_types: &[DataType]) -> Result<()> {
        match arg_types {
            [DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8] => Ok(()),
            [DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8, DataType::Map(map_field, _)] => {
                match map_field.data_type() {
                    DataType::Struct(fields) => {
                        let key = fields[0].clone();
                        let value = fields[1].clone();
                        if !key.data_type().is_string() || !value.data_type().is_string() {
                            return Err(DataFusionError::Plan(format!(
                                "For function `{}`, the options map keys/values should both be type string. Instead got key: {}, value: {}",
                                Self::SCHEMA_OF_JSON_NAME,
                                key.data_type(),
                                value.data_type(),
                            )));
                        }
                        Ok(())
                    }
                    _ => unreachable!(),
                }
            }
            _ => plan_err!(
                "For function `{:?}` found invalid arg types: {:?}",
                Self::SCHEMA_OF_JSON_NAME,
                arg_types
            ),
        }
    }
}

impl ScalarUDFImpl for SparkSchemaOfJson {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        Self::SCHEMA_OF_JSON_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Self::validate_args_len(arg_types)?;
        Self::validate_arg_types(arg_types)?;
        let mut coerce_to = vec![DataType::Utf8];
        if arg_types.len() > 1 {
            // Force map<k,v> → map<Utf8, Utf8>
            coerce_to.push(DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ));
        }
        // utf8, optional<map>
        Ok(coerce_to)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Self::validate_args_len(&args.args)?;
        Self::validate_args_are_literal(&args.args)?;
        let hints = vec![Hint::AcceptsSingular, Hint::AcceptsSingular];
        make_scalar_function(schema_of_json_inner, hints)(&args.args)
    }
}

fn schema_of_json_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    SparkSchemaOfJson::validate_args_len(args)?;
    let rows = downcast_arg!(&args[0], StringArray);
    let options = if let Some(arg) = args.get(1) {
        let map_array = downcast_arg!(arg, MapArray);
        SparkSchemaOfJsonOptions::default().map_to_options(map_array)?
    } else {
        SparkSchemaOfJsonOptions::default()
    };
    let type_ddl = if rows.is_empty() {
        return Err(DataFusionError::Execution(
            "No value passed into input".to_string(),
        ));
    } else if rows.value(0).is_empty() {
        "STRING".to_string()
    } else {
        infer_json_schema_type(rows.value(0), &options)?
    };
    Ok(Arc::new(StringArray::from(vec![type_ddl])))
}

fn infer_json_schema_type(
    json_string: &str,
    _options: &SparkSchemaOfJsonOptions,
) -> Result<String> {
    let value = serde_json::from_str::<serde_json::Value>(json_string)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    value_to_ddl_type(&value)
}

fn value_to_ddl_type(value: &Value) -> Result<String> {
    match value {
        Value::String(_) => Ok("STRING".to_string()),
        Value::Number(num) => {
            if num.is_f64() {
                Ok("DOUBLE".to_string())
            } else {
                Ok("BIGINT".to_string())
            }
        }
        Value::Bool(_) => Ok("BOOL".to_string()),
        Value::Object(map) => {
            let mut inner_k_v_ddl = Vec::new();
            for (k, v) in map.iter() {
                let ddl_type = value_to_ddl_type(v)?;
                inner_k_v_ddl.push(format!("{}: {}", k, ddl_type));
            }
            let inner_str = inner_k_v_ddl.join(", ");
            Ok(format!("STRUCT<{}>", inner_str))
        }
        Value::Array(arr) => {
            if arr.is_empty() {
                Ok("ARRAY<STRING>".to_string())
            } else {
                // TODO: evaluate all vals and pick broadest type
                let nested_type = value_to_ddl_type(&arr[0])?;
                Ok(format!("ARRAY<{nested_type}>"))
            }
        }
        other => exec_err!("Unsupported parsing of json type {other}"),
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

#[derive(Debug, Default)]
struct SparkSchemaOfJsonOptions {
    mode: ModeOptions,
    _allow_numeric_leading_zeros: bool,
}

impl SparkSchemaOfJsonOptions {
    pub fn map_to_options(mut self, map_array: &MapArray) -> Result<Self> {
        let inner_struct = map_array.value(0);
        // validate map is of type map<string, string>
        let (keys, values) = Self::get_keys_values_from_map(inner_struct)?;
        // match each k v pair
        for (key, value) in keys.iter().zip(values.iter()) {
            let (key, value) = Self::unwrap_or_key_value(key, value)?;
            match key {
                "mode" => self.mode = ModeOptions::from_str(value.to_string())?,
                "allowNumericLeadingZeros" => {
                    // TODO: extend serde/serde_json5 to support leading 0s
                    return Err(DataFusionError::NotImplemented(format!(
                        "`{}` currently doesn't support option allowNumericLeadingZeros",
                        SparkSchemaOfJson::SCHEMA_OF_JSON_NAME,
                    )));
                }
                other => {
                    return plan_err!("Found unsupported option type when parsing options: {other}")
                }
            }
        }
        Ok(self)
    }

    fn get_keys_values_from_map(inner_struct: StructArray) -> Result<(StringArray, StringArray)> {
        let (keys, values) = match inner_struct.data_type() {
            DataType::Struct(fields) => {
                let key_type = fields[0].data_type();
                let value_type = fields[1].data_type();
                if key_type == &DataType::Utf8 && value_type == &DataType::Utf8 {
                    let keys = downcast_array::<StringArray>(inner_struct.column(0));
                    let values = downcast_array::<StringArray>(inner_struct.column(1));
                    (keys, values)
                } else {
                    return Err(DataFusionError::Plan(format!(
                        "Expected options to be type map<string, string> but found key type {:?} and value type {:?}",
                        key_type,
                        value_type
                    )))
                }
            },
            other => {
                return Err(DataFusionError::Plan(format!(
                    "Should be unreachable: options should be a map with an inner struct but instead got {:?}",
                    other
                )))
            }
        };
        Ok((keys, values))
    }

    fn unwrap_or_key_value<'a>(
        key: Option<&'a str>,
        value: Option<&'a str>,
    ) -> Result<(&'a str, &'a str)> {
        match (key, value) {
            (Some(k), Some(v)) => Ok((k, v)),
            _ => Err(DataFusionError::Plan(format!(
                "Unexpected options key value pair: {:?}: {:?}",
                key, value
            ))),
        }
    }
}
