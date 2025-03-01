use std::any::Any;
use std::cmp::{max, min};
use std::sync::Arc;

use crate::extension::function::error_utils::{
    generic_exec_err, invalid_arg_count_exec_err, unsupported_data_type_exec_err,
    unsupported_data_types_exec_err,
};
use crate::extension::function::math::round_decimal_base;
use datafusion::arrow::array::{ArrayRef, AsArray};
use datafusion::arrow::datatypes::{DataType, Float32Type, Float64Type};
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnInfo, ReturnTypeArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

#[derive(Debug)]
pub struct SparkCeil {
    signature: Signature,
}

impl Default for SparkCeil {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCeil {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkCeil {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "spark_ceil"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("`return_type` should not be called, call `return_type_from_args` instead")
    }

    fn return_type_from_args(&self, args: ReturnTypeArgs) -> Result<ReturnInfo> {
        ceil_floor_return_type_from_args("ceil", args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let arr: ArrayRef = match args[0].data_type() {
            DataType::Float64 => Arc::new(
                args[0]
                    .as_primitive::<Float64Type>()
                    .unary::<_, Float64Type>(|x: f64| f64::ceil(x)),
            ) as ArrayRef,
            DataType::Float32 => Arc::new(
                args[0]
                    .as_primitive::<Float32Type>()
                    .unary::<_, Float32Type>(|x: f32| f32::ceil(x)),
            ) as ArrayRef,
            _other => {
                return Err(datafusion_common::DataFusionError::Execution("meow".into()));
            }
        };

        Ok(ColumnarValue::Array(arr))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        ceil_floor_coerce_types("ceil", arg_types)
    }
}

fn ceil_floor_coerce_types(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() == 1 {
        if arg_types[0].is_numeric() {
            Ok(vec![arg_types[0].clone()])
        } else {
            Err(unsupported_data_types_exec_err(
                name,
                "Numeric Type",
                arg_types,
            ))
        }
    } else if arg_types.len() == 2 {
        if arg_types[0].is_numeric() && arg_types[1].is_integer() {
            let arg_type = match &arg_types[0] {
                DataType::UInt8 => DataType::Int16,
                DataType::UInt16 => DataType::Int32,
                DataType::UInt32 | DataType::UInt64 => DataType::Int64,
                other => other.clone(),
            };
            Ok(vec![arg_type, DataType::Int32])
        } else {
            Err(unsupported_data_types_exec_err(
                name,
                "Numeric Type for expr and Integer Type for target scale",
                arg_types,
            ))
        }
    } else {
        Err(invalid_arg_count_exec_err(name, (1, 2), arg_types.len()))
    }
}

fn ceil_floor_return_type_from_args(name: &str, args: ReturnTypeArgs) -> Result<ReturnInfo> {
    let arg_types = args.arg_types;
    let scalar_arguments = args.scalar_arguments;
    let return_type = if arg_types.len() == 1 {
        match &arg_types[0] {
            DataType::Decimal128(precision, scale) => {
                let (precision, scale) =
                    round_decimal_base(*precision as i32, *scale as i32, 0, true);
                Ok(DataType::Decimal128(precision, scale))
            }
            DataType::Decimal256(precision, scale) => {
                let (precision, scale) =
                    round_decimal_base(*precision as i32, *scale as i32, 0, false);
                Ok(DataType::Decimal256(precision, scale))
            }
            _ => Ok(DataType::Int64),
        }
    } else if arg_types.len() == 2 {
        if let Some(target_scale) = scalar_arguments[1] {
            let expr = &arg_types[0];
            let target_scale: i32 = match target_scale {
                ScalarValue::Int8(Some(v)) => Ok(*v as i32),
                ScalarValue::Int16(Some(v)) => Ok(*v as i32),
                ScalarValue::Int32(Some(v)) => Ok(*v),
                ScalarValue::Int64(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt8(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt16(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt32(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt64(Some(v)) => Ok(*v as i32),
                _ => Err(generic_exec_err(
                    name,
                    "Target scale must be Integer literal",
                )),
            }?;
            if target_scale < -38 {
                return Err(generic_exec_err(
                    name,
                    "Target scale must be greater than -38",
                ));
            }
            let (precision, scale, decimal_128) = match expr {
                DataType::UInt8 | DataType::Int8 => Ok((max(3, -target_scale + 1), 0, true)),
                DataType::UInt16 | DataType::Int16 => Ok((max(5, -target_scale + 1), 0, true)),
                DataType::UInt32 | DataType::Int32 => Ok((max(10, -target_scale + 1), 0, true)),
                DataType::UInt64 | DataType::Int64 => Ok((max(20, -target_scale + 1), 0, true)),
                DataType::Float32 => Ok((
                    max(14, -target_scale + 1),
                    min(7, max(0, target_scale)),
                    true,
                )),
                DataType::Float64 => Ok((
                    max(30, -target_scale + 1),
                    min(15, max(0, target_scale)),
                    true,
                )),
                DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                    let decimal_128 = matches!(expr, DataType::Decimal128(_, _));
                    let precision = *precision as i32;
                    let scale = *scale as i32;
                    Ok((
                        max(precision - scale + 1, -target_scale + 1),
                        min(scale, max(0, target_scale)),
                        decimal_128,
                    ))
                }
                _ => Err(unsupported_data_type_exec_err(
                    name,
                    "Numeric Type for expr",
                    expr,
                )),
            }?;
            let (precision, scale) =
                round_decimal_base(precision, scale, target_scale, decimal_128);
            if decimal_128 {
                Ok(DataType::Decimal128(precision, scale))
            } else {
                Ok(DataType::Decimal256(precision, scale))
            }
        } else {
            Err(generic_exec_err(
                name,
                "Target scale must be Integer literal",
            ))
        }
    } else {
        Err(invalid_arg_count_exec_err(name, (1, 2), arg_types.len()))
    }?;
    Ok(ReturnInfo::new_nullable(return_type))
}

// ADJUSTMENT (target_scale):
// spark-sql (default)> SELECT typeof(ceil(CAST(5 as TINYINT), 1));
// decimal(4,0)
//
// spark-sql (default)> SELECT typeof(ceil(CAST(5 as SMALLINT), 1));
// decimal(6,0)
//
// spark-sql (default)> SELECT typeof(ceil(CAST(5 as INT), 1));
// decimal(11,0)
//
// spark-sql (default)> SELECT typeof(ceil(CAST(5 as BIGINT), 1));
// decimal(21,0)
//
// spark-sql (default)> SELECT typeof(ceil(CAST(5 as FLOAT), 1));
// decimal(9,1)
//
// spark-sql (default)> SELECT typeof(ceil(CAST(5 as DOUBLE), 1));
// decimal(17,1)
//
// spark-sql (default)> SELECT typeof(ceil(CAST(5 AS DECIMAL(5, 2)), 1));
// decimal(5,1)
//
// NO ADJUSTMENT (no target_scale):
//
// spark-sql (default)> SELECT typeof(ceil(CAST(5 AS DECIMAL(5, 2)))); // DECIMAL(p - s + 1, 0)
// decimal(4,0)

// ADJUSTMENT (target_scale):
// spark-sql (default)> SELECT typeof(floor(CAST(5 as TINYINT), 1));
// decimal(4,0)
//
// spark-sql (default)> SELECT typeof(floor(CAST(5 as SMALLINT), 1));
// decimal(6,0)
//
// spark-sql (default)> SELECT typeof(floor(CAST(5 as INT), 1));
// decimal(11,0)
//
// spark-sql (default)> SELECT typeof(floor(CAST(5 as BIGINT), 1));
// decimal(21,0)
//
// spark-sql (default)> SELECT typeof(floor(CAST(5 as FLOAT), 1));
// decimal(9,1)
//
// spark-sql (default)> SELECT typeof(floor(CAST(5 as DOUBLE), 1));
// decimal(17,1)
//
// spark-sql (default)> SELECT typeof(floor(CAST(5 AS DECIMAL(5, 2)), 1));
// decimal(5,1)
//
// NO ADJUSTMENT (no target_scale):
//
// spark-sql (default)> SELECT typeof(floor(CAST(5 AS DECIMAL(5, 2))));
// decimal(4,0)
