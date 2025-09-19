use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringBuilder};
use arrow::datatypes::DataType;
use datafusion_common::cast::as_string_array;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use serde_json::Value;

use crate::extension::function::error_utils::invalid_arg_count_exec_err;
use crate::extension::function::functions_nested_utils::make_scalar_function;

#[derive(Debug)]
pub struct SparkParseJson {
    signature: Signature,
}

impl Default for SparkParseJson {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkParseJson {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkParseJson {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "parse_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(parse_json_kernel)(&args)
    }
}

fn parse_json_kernel(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [arr] = args else {
        return Err(invalid_arg_count_exec_err("parse_json", (1, 1), args.len()));
    };

    let input = as_string_array(arr)?;
    let len: usize = input.len();
    let mut builder = StringBuilder::with_capacity(len, 0);

    for i in 0..len {
        if input.is_null(i) {
            builder.append_null();
            continue;
        }

        let s = input.value(i);
        match serde_json::from_str::<Value>(s) {
            Ok(Value::Object(map)) => {
                let json = serde_json::to_string(&Value::Object(map)).map_err(|e| {
                    DataFusionError::Execution(format!("parse_json serialize: {e}"))
                })?;
                builder.append_value(json);
            }
            Ok(_) => {
                builder.append_null();
            }
            Err(_) => {
                builder.append_null();
            }
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, StringArray};
    use datafusion_common::cast::as_string_array;

    use super::*;

    fn run(arr: ArrayRef) -> Result<StringArray> {
        let out: ArrayRef = parse_json_kernel(&[arr])?;
        let sa: &StringArray = as_string_array(&out)?;
        Ok(sa.clone())
    }

    #[test]
    fn test_parse_json_obj_ok() -> Result<()> {
        use serde_json::Value;

        let input: ArrayRef = Arc::new(StringArray::from(vec![
            Some(r#"{"a":1,"b":true}"#),
            Some(r#"{ "x" : 10, "y" : [ false, 3.5 ] }"#),
            Some(r#"{"nested":{"k":"v"},"arr":[1,2]}"#),
        ]));

        let out = run(input)?;

        let eq_json = |i: usize, expected: &str| -> Result<()> {
            let got_v: Value = serde_json::from_str(out.value(i))
                .map_err(|e| DataFusionError::Execution(format!("parse got_v: {e}")))?;
            let exp_v: Value = serde_json::from_str(expected)
                .map_err(|e| DataFusionError::Execution(format!("parse expected: {e}")))?;
            assert_eq!(got_v, exp_v, "row {}: JSON distinct", i);
            Ok(())
        };

        eq_json(0, r#"{"a":1,"b":true}"#)?;
        eq_json(1, r#"{"x":10,"y":[false,3.5]}"#)?;
        eq_json(2, r#"{"nested":{"k":"v"},"arr":[1,2]}"#)?;
        Ok(())
    }

    #[test]
    fn test_parse_json_array_should_be_null() -> Result<()> {
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            Some(r#"[1,2,3]"#),
            Some(r#"[{"a":1},{"b":2}]"#),
        ]));

        let out = run(input)?;

        assert!(out.is_null(0));
        assert!(out.is_null(1));
        Ok(())
    }

    #[test]
    fn test_parse_json_non_object_roots_are_null() -> Result<()> {
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            Some("123"),
            Some("true"),
            Some("false"),
            Some("null"),
            Some(r#""hello""#),
        ]));

        let out = run(input)?;

        for i in 0..5 {
            assert!(out.is_null(i), "index {} should be NULL", i);
        }
        Ok(())
    }

    #[test]
    fn test_parse_json_invalid_and_null_inputs() -> Result<()> {
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            Some("not json"),
            Some("{malformatted"),
            None,
            Some(""),
        ]));

        let out = run(input)?;

        assert!(out.is_null(0));
        assert!(out.is_null(1));
        assert!(out.is_null(2));
        assert!(out.is_null(3));
        Ok(())
    }
}
