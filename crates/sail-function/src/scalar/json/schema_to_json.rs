use std::{fmt::format, sync::Arc};

use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, function::Hint};
use datafusion_expr_common::signature::Volatility;
use datafusion::arrow::{array::{ArrayRef, Datum, StringArray}, datatypes::DataType};
use datafusion_common::{DataFusionError, Result, exec_err, plan_err};
use datafusion_functions::{downcast_arg, utils::make_scalar_function};
use serde_json::Value;


#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkSchemaToJson {
    signature: Signature,
    aliases: [String; 1]
}

impl Default for SparkSchemaToJson {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSchemaToJson {
    pub fn new() -> Self {
        SparkSchemaToJson {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: ["schema_to_json".to_string()]
        }
    }
}

impl ScalarUDFImpl for SparkSchemaToJson {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &"schema_to_json"
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
        let hints = vec![Hint::Pad, Hint::AcceptsSingular];
        make_scalar_function(schema_to_json_inner, hints)(&args.args)
    }
}

fn schema_to_json_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 1 || args.len() > 2 {
        return plan_err!("function `schema_to_json` expected 1 to 2 args but got {}", args.len())
    };
    let rows = downcast_arg!(&args[0], StringArray);
    let type_ddl = infer_json_schema_type(rows.value(0))?;
    Ok(Arc::new(StringArray::from(vec![type_ddl])))
}

fn infer_json_schema_type(json_string: &str) -> Result<String> {
    let value = serde_json::from_str::<serde_json::Value>(json_string)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    value_to_str(&value)
}

fn value_to_str(value: &Value) -> Result<String> {
    match value {
        Value::String(_) => Ok("STRING".to_string()),
        Value::Number(_) => Ok("BIGINT".to_string()),
        Value::Bool(_) => Ok("BOOL".to_string()),
        Value::Object(map) => {
            let mut inner = Vec::new();
            for (k, v) in map.iter() {
                let val = value_to_str(v)?;
                let x = format!("{}: {}", k, val);
                inner.push(x);
            }
            let inner_str = inner.join(", ");
            Ok(format!("STRUCT<{}>", inner_str))
        },
        Value::Array(arr) => {
            let nested_val = value_to_str(&arr[0])?;
            Ok(format!("ARRAY<{nested_val}>"))
        },
        other => exec_err!("Unsupported parsing of json type {other}")
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_tmp() {
        let s = r#"{"a": "string", "b": [1]}"#;
        let o = infer_json_schema_type(s);
        dbg!(o);
    }
}
