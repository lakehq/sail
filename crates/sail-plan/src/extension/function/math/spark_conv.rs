use std::any::Any;
use std::sync::Arc;
use arrow::array::{as_string_array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::as_int32_array;
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

        if matches!(num, ColumnarValue::Array(_)) {
            return invoke_vectorized(num, from_base, to_base);
        }

        let num_str: Option<String> = match num {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Some(s.clone()),
            ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) => Some(s.to_string()),
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => Some(s.clone()),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(i))) => Some(i.to_string()),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(i))) => Some(i.to_string()),
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

fn invoke_vectorized(
    num: &ColumnarValue,
    from_base: &ColumnarValue,
    to_base: &ColumnarValue,
) -> Result<ColumnarValue> {
    let from = match from_base {
        ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => *v,
        _ => {
            return Err(unsupported_data_types_exec_err(
                "spark_conv",
                "(..., Int32, Int32)",
                &[num.data_type(), from_base.data_type(), to_base.data_type()],
            ));
        }
    };

    let to = match to_base {
        ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => *v,
        _ => {
            return Err(unsupported_data_types_exec_err(
                "spark_conv",
                "(..., Int32, Int32)",
                &[num.data_type(), from_base.data_type(), to_base.data_type()],
            ));
        }
    };

    let len = match num {
        ColumnarValue::Array(array) => array.len(),
        _ => {
            return Err(datafusion_common::DataFusionError::Execution(
                "Expected array input for spark_conv".to_string(),
            ));
        }
    };

    if from < 2 || from > 36 || to < 2 || to > 36 {
        return Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
            std::iter::repeat::<Option<String>>(None).take(len),
        ))));
    }
    let result = match num {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => {
                let string_array = as_string_array(array);
                string_array
                    .iter()
                    .map(|opt_s| {
                        opt_s.and_then(|s| {
                            i64::from_str_radix(s, from as u32).ok().map(|n| {
                                if to == 10 {
                                    n.to_string()
                                } else {
                                    format!("{:x}", n)
                                }
                            })
                        })
                    })
                    .collect::<StringArray>()
            }

            DataType::Int32 => {
                let int_array = as_int32_array(array)?;
                int_array
                    .iter()
                    .map(|opt_i| {
                        opt_i.map(|i| {
                            if to == 10 {
                                i.to_string()
                            } else {
                                format!("{:x}", i)
                            }
                        })
                    })
                    .collect::<StringArray>()
            }

            _ => {
                return Err(unsupported_data_types_exec_err(
                    "spark_conv",
                    "(Utf8 | Int32, Int32, Int32)",
                    &[
                        array.data_type().clone(),
                        from_base.data_type(),
                        to_base.data_type(),
                    ],
                ));
            }
        },
        _ => {
            return Err(unsupported_data_types_exec_err(
                "spark_conv",
                "(Utf8 | Int32, Int32, Int32)",
                &[num.data_type(), from_base.data_type(), to_base.data_type()],
            ));
        }
    };

    Ok(ColumnarValue::Array(Arc::new(result)))
}
