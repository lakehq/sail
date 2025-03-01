use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::try_binary;
use datafusion::arrow::datatypes::{DataType, Float64Type};
use datafusion::arrow::error::ArrowError;
use datafusion_common::{arrow_datafusion_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::extension::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err, unsupported_data_types_exec_err,
};

#[derive(Debug)]
pub struct SparkPmod {
    signature: Signature,
}

impl Default for SparkPmod {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkPmod {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkPmod {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_pmod"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(invalid_arg_count_exec_err("pmod", 2, args.args.len()));
        }
        match (&args.args[0], &args.args[1]) {
            (
                ColumnarValue::Scalar(ScalarValue::Float64(dividend)),
                ColumnarValue::Scalar(ScalarValue::Float64(divisor)),
            ) => {
                if let Some(divisor) = *divisor {
                    if divisor == 0.0f64 {
                        return Err(arrow_datafusion_err!(ArrowError::DivideByZero));
                    }
                    Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                        dividend.map(|x| x.rem_euclid(divisor)),
                    )))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)))
                }
            }
            (
                ColumnarValue::Array(dividend_array),
                ColumnarValue::Scalar(ScalarValue::Float64(divisor)),
            ) => {
                if let Some(divisor) = *divisor {
                    if divisor == 0.0f64 {
                        return Err(arrow_datafusion_err!(ArrowError::DivideByZero));
                    }
                    match dividend_array.data_type() {
                        DataType::Float64 => Ok(ColumnarValue::Array(Arc::new(
                            dividend_array
                                .as_primitive::<Float64Type>()
                                .unary::<_, Float64Type>(|dividend| dividend.rem_euclid(divisor)),
                        )
                            as ArrayRef)),
                        other => Err(unsupported_data_type_exec_err(
                            "pmod",
                            &DataType::Float64,
                            other,
                        )),
                    }
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)))
                }
            }
            (ColumnarValue::Array(dividend_array), ColumnarValue::Array(divisor_array)) => {
                match (dividend_array.data_type(), divisor_array.data_type()) {
                    (DataType::Float64, DataType::Float64) => {
                        let dividend_array = dividend_array.as_primitive::<Float64Type>();
                        let divisor_array = divisor_array.as_primitive::<Float64Type>();
                        Ok(ColumnarValue::Array(
                            Arc::new(try_binary::<_, _, _, Float64Type>(
                                dividend_array,
                                divisor_array,
                                |dividend, divisor| {
                                    if divisor == 0.0f64 {
                                        Err(ArrowError::DivideByZero)
                                    } else {
                                        Ok(dividend.rem_euclid(divisor))
                                    }
                                },
                            )?) as ArrayRef,
                        ))
                    }
                    (_dividend, _divisor) => Err(unsupported_data_types_exec_err(
                        "pmod",
                        &[DataType::Float64, DataType::Float64],
                        &[args.args[0].data_type(), args.args[1].data_type()],
                    )),
                }
            }
            (dividend, divisor) => Err(unsupported_data_types_exec_err(
                "pmod",
                &[DataType::Float64, DataType::Float64],
                &[dividend.data_type(), divisor.data_type()],
            )),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(invalid_arg_count_exec_err("pmod", 2, arg_types.len()));
        }
        if arg_types[0].is_numeric() && arg_types[1].is_numeric() {
            Ok(vec![DataType::Float64, DataType::Float64])
        } else {
            Err(unsupported_data_types_exec_err(
                "pmod",
                &[DataType::Float64, DataType::Float64],
                arg_types,
            ))
        }
    }
}
