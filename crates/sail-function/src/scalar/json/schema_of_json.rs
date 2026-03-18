use std::{sync::Arc};

use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, function::Hint};
use datafusion_expr_common::signature::Volatility;
use datafusion::arrow::{array::{Array, ArrayRef, MapArray, StringArray, downcast_array}, datatypes::DataType};
use datafusion_common::{DataFusionError, Result, exec_err, plan_err};
use datafusion_functions::{downcast_arg, utils::make_scalar_function};
use serde_json::Value;
use regex::Regex;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSchemaOfJson {
    signature: Signature,
    aliases: [String; 1]
}

impl Default for SparkSchemaOfJson {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSchemaOfJson {
    pub fn new() -> Self {
        SparkSchemaOfJson {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: ["schema_of_json".to_string()]
        }
    }
}

impl ScalarUDFImpl for SparkSchemaOfJson {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &"schema_of_json"
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

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let hints = vec![Hint::AcceptsSingular, Hint::AcceptsSingular];
        make_scalar_function(schema_of_json_inner, hints)(&args.args)
    }
}

fn schema_of_json_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 1 || args.len() > 2 {
        return plan_err!("function `schema_of_json` expected 1 to 2 args but got {}", args.len())
    };
    let rows = downcast_arg!(&args[0], StringArray);
    let options = if let Some(arg) = args.get(1) {
        let map_array = downcast_arg!(arg, MapArray);
        SparkSchemaOfJsonOptions::default().map_to_options(map_array)?
    } else {
        SparkSchemaOfJsonOptions::default()
    };
    let type_ddl = infer_json_schema_type(rows.value(0), &options)?;
    Ok(Arc::new(StringArray::from(vec![type_ddl])))
}

fn infer_json_schema_type(json_string: &str, options: &SparkSchemaOfJsonOptions) -> Result<String> {
    let preprocessed = preprocess(json_string, options);
    let value = serde_json::from_str::<serde_json::Value>(&preprocessed)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    value_to_str(&value, options)
}

fn preprocess(string: &str, options: &SparkSchemaOfJsonOptions) -> String {
    if options.allow_numeric_leading_zeros {
        remove_leading_zeros(string.to_string())
    } else {
        string.to_string()
    }
}

fn remove_leading_zeros(string: String) -> String {
    let re = Regex::new(r"\b0+([1-9]\d*)").unwrap();
    re.replace_all(string.as_str(), "$1").to_string()
}

fn value_to_str(value: &Value, options: &SparkSchemaOfJsonOptions) -> Result<String> {
    match value {
        Value::String(_) => Ok("STRING".to_string()),
        Value::Number(_) => Ok("BIGINT".to_string()),
        Value::Bool(_) => Ok("BOOL".to_string()),
        Value::Object(map) => {
            let mut inner = Vec::new();
            for (k, v) in map.iter() {
                let val = value_to_str(v, options)?;
                let x = format!("{}: {}", k, val);
                inner.push(x);
            }
            let inner_str = inner.join(", ");
            Ok(format!("STRUCT<{}>", inner_str))
        },
        Value::Array(arr) => {
            let nested_val = value_to_str(&arr[0], options)?;
            Ok(format!("ARRAY<{nested_val}>"))
        },
        other => exec_err!("Unsupported parsing of json type {other}")
    }
}

#[derive(Debug, Default)]
enum ModeOptions {
    #[default]
    Permissive,
    FailFast,
    DropMalformed
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

#[derive(Debug)]
struct SparkSchemaOfJsonOptions {
    mode: ModeOptions,
    allow_numeric_leading_zeros: bool,
}

impl SparkSchemaOfJsonOptions {
    pub fn map_to_options(mut self, map_array: &MapArray) -> Result<Self> {
        let inner_struct = map_array.value(0);
        // validate map is of type map<string, string>
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
                        "Expections options to be type map<string, string> but found key type {:?} and value type {:?}",
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
        // Get each k/v pair
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
                "allowNumericLeadingZeros" => {
                    self.allow_numeric_leading_zeros = value.parse::<bool>()
                        .map_err(|e| DataFusionError::Plan(format!(
                            "Error parsing options: {key} of {value} can't be parsed to a float. Original error: {e}"
                        )))?
                },
                other => {
                    return plan_err!("Found unsupported option type when parsing options: {other}")
                }
            }
        }
        Ok(self)
    }
}

impl Default for SparkSchemaOfJsonOptions {
    fn default() -> Self {
        SparkSchemaOfJsonOptions {
            mode: ModeOptions::default(),
            allow_numeric_leading_zeros: false,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_tmp() {
        let s = r#"{"a": 01, "b": [1]}"#;
        let mut options = SparkSchemaOfJsonOptions::default();
        options.allow_numeric_leading_zeros = true;
        let o = infer_json_schema_type(s, &options);
        dbg!(o);
    }
}
