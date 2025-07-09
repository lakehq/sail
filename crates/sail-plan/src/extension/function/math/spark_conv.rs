use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_types_exec_err,
};

#[derive(Debug)]
pub struct SparkConv {
    signature: Signature,
}

impl Default for SparkConv {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkConv {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkConv {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_conv"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(DataType::Utf8) => Ok(DataType::Utf8),
            Some(DataType::Utf8View) => Ok(DataType::Utf8View),
            Some(DataType::LargeUtf8) => Ok(DataType::LargeUtf8),
            _ => Ok(DataType::Utf8),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        let [num, from_base, to_base] = args.as_slice() else {
            return Err(invalid_arg_count_exec_err("spark_conv", (3, 3), args.len()));
        };

        let num_str: Option<String> = match num {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Some(s.clone()),
            ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) => Some(s.to_string()),
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => Some(s.clone()),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(i))) => Some(i.to_string()),
            _ => None,
        };

        match (num_str, from_base, to_base) {
            (
                Some(num_str),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(from))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(to))),
            ) => {
                if *from < 2 || *from > 36 || *to < 2 || *to > 36 {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }
                match i64::from_str_radix(&num_str, *from as u32) {
                    Ok(n) => {
                        let result = if *to == 10 {
                            format!("{n}")
                        } else {
                            format!("{n:x}")
                        };
                        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
                    }
                    Err(_) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                }
            }
            _ => {
                let types = vec![num.data_type(), from_base.data_type(), to_base.data_type()];
                Err(unsupported_data_types_exec_err(
                    "spark_conv",
                    "(Utf8 | Utf8View | LargeUtf8 | Int32, Int32, Int32)",
                    &types,
                ))
            }
        }
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        let [input_type, from_base_type, to_base_type] = types else {
            return Err(invalid_arg_count_exec_err(
                "spark_conv",
                (3, 3),
                types.len(),
            ));
        };

        let valid_string: bool = matches!(
            input_type,
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Int32
        );
        let valid_from: bool = matches!(from_base_type, DataType::Int32);
        let valid_to: bool = matches!(to_base_type, DataType::Int32);

        if valid_string && valid_from && valid_to {
            Ok(vec![
                input_type.clone(),
                from_base_type.clone(),
                to_base_type.clone(),
            ])
        } else {
            Err(unsupported_data_types_exec_err(
                "spark_conv",
                "Utf8 | Utf8View | LargeUtf8 | Int32, Int32, Int32",
                types,
            ))
        }
    }
}
