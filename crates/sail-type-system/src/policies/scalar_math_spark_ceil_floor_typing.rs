//! Shared function type policies.

use std::sync::Arc;

use arrow_schema::{DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::ReturnFieldArgs;

use crate::error::{
    generic_exec_err, generic_internal_err, invalid_arg_count_exec_err,
    unsupported_data_type_exec_err, unsupported_data_types_exec_err,
};
use crate::policies::decimal_typing::round_decimal_base;

pub fn spark_ceil_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Err(generic_internal_err(
        "ceil",
        "`return_type` should not be called; `return_field_from_args` is used instead",
    ))
}

pub fn spark_ceil_return_field_from_args(args: ReturnFieldArgs) -> Result<FieldRef> {
    ceil_floor_return_type_from_args("ceil", args)
}

pub fn spark_ceil_coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    ceil_floor_coerce_types("ceil", arg_types)
}

pub fn spark_floor_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Err(generic_internal_err(
        "floor",
        "`return_type` should not be called; `return_field_from_args` is used instead",
    ))
}

pub fn spark_floor_return_field_from_args(args: ReturnFieldArgs) -> Result<FieldRef> {
    ceil_floor_return_type_from_args("floor", args)
}

pub fn spark_floor_coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    ceil_floor_coerce_types("floor", arg_types)
}

pub fn ceil_floor_coerce_types(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() == 1 {
        Ok(vec![ceil_floor_coerce_first_arg(name, &arg_types[0])?])
    } else if arg_types.len() == 2 {
        let first = ceil_floor_coerce_first_arg(name, &arg_types[0])?;
        if arg_types[1].is_integer() {
            Ok(vec![first, DataType::Int32])
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

pub fn ceil_floor_return_type_from_args(name: &str, args: ReturnFieldArgs) -> Result<FieldRef> {
    let arg_fields = args.arg_fields;
    let scalar_arguments = args.scalar_arguments;
    let return_type = if arg_fields.len() == 1 {
        match &arg_fields[0].data_type() {
            DataType::Decimal128(precision, scale) => {
                let (precision, scale) =
                    round_decimal_base(*precision as i32, *scale as i32, 0, true);
                Ok(DataType::Decimal128(precision, scale))
            }
            DataType::Decimal256(precision, scale) => {
                if *precision <= DECIMAL128_MAX_PRECISION && *scale <= DECIMAL128_MAX_SCALE {
                    let (precision, scale) =
                        round_decimal_base(*precision as i32, *scale as i32, 0, false);
                    Ok(DataType::Decimal128(precision, scale))
                } else {
                    Err(unsupported_data_type_exec_err(
                        name,
                        format!("Decimal Type must have precision <= {DECIMAL128_MAX_PRECISION} and scale <= {DECIMAL128_MAX_SCALE}").as_str(),
                        arg_fields[0].data_type(),
                    ))
                }
            }
            _ => Ok(DataType::Int64),
        }
    } else if arg_fields.len() == 2 {
        if let Some(target_scale) = scalar_arguments[1] {
            let expr = &arg_fields[0].data_type();
            let target_scale: i32 = match target_scale {
                ScalarValue::Int8(Some(v)) => Ok(*v as i32),
                ScalarValue::Int16(Some(v)) => Ok(*v as i32),
                ScalarValue::Int32(Some(v)) => Ok(*v),
                ScalarValue::Int64(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt8(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt16(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt32(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt64(Some(v)) => Ok(*v as i32),
                _ => Err(unsupported_data_type_exec_err(
                    name,
                    "Target scale must be Integer literal",
                    &target_scale.data_type(),
                )),
            }?;
            // Decimal128 can hold 10^37 (38 digits: 1 followed by 37 zeros) but not 10^38.
            // target_scale = -n produces a result that's a multiple of 10^n, so n ≤ 37.
            if target_scale < -37 {
                return Err(generic_exec_err(
                    name,
                    "Target scale must be greater than or equal to -37",
                ));
            }
            let (precision, scale) = match expr {
                DataType::Int8 => Ok((3, 0)),
                DataType::UInt8 | DataType::Int16 => Ok((5, 0)),
                DataType::UInt16 | DataType::Int32 => Ok((10, 0)),
                DataType::UInt32 | DataType::UInt64 | DataType::Int64 => Ok((20, 0)),
                DataType::Float32 => Ok((14, 7)),
                DataType::Float64 => Ok((30, 15)),
                DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                    if *precision <= DECIMAL128_MAX_PRECISION && *scale <= DECIMAL128_MAX_SCALE {
                        Ok((*precision as i32, *scale as i32))
                    } else {
                        Err(unsupported_data_type_exec_err(
                            name,
                            format!("Decimal Type must have precision <= {DECIMAL128_MAX_PRECISION} and scale <= {DECIMAL128_MAX_SCALE}").as_str(),
                            arg_fields[0].data_type(),
                        ))
                    }
                }
                _ => Err(unsupported_data_type_exec_err(
                    name,
                    "Numeric Type for expr",
                    expr,
                )),
            }?;
            let (precision, scale) = round_decimal_base(precision, scale, target_scale, true);
            Ok(DataType::Decimal128(precision, scale))
        } else {
            Err(generic_exec_err(
                name,
                "Target scale must be Integer literal, received: None",
            ))
        }
    } else {
        // Defensive: DataFusion may call `return_field_from_args` in paths
        // that bypass `coerce_types`. Surface a precise arity error.
        Err(invalid_arg_count_exec_err(name, (1, 2), arg_fields.len()))
    }?;
    Ok(Arc::new(Field::new(name.to_string(), return_type, true)))
}

pub fn ceil_floor_coerce_first_arg(name: &str, arg_type: &DataType) -> Result<DataType> {
    match arg_type {
        DataType::Null => Ok(DataType::Float64),
        DataType::UInt8 => Ok(DataType::Int16),
        DataType::UInt16 => Ok(DataType::Int32),
        DataType::UInt32 | DataType::UInt64 => Ok(DataType::Int64),
        DataType::Decimal256(precision, scale) => {
            if *precision <= DECIMAL128_MAX_PRECISION && *scale <= DECIMAL128_MAX_SCALE {
                Ok(DataType::Decimal128(*precision, *scale))
            } else {
                Err(unsupported_data_type_exec_err(
                    name,
                    format!("Decimal Type must have precision <= {DECIMAL128_MAX_PRECISION} and scale <= {DECIMAL128_MAX_SCALE}").as_str(),
                    arg_type,
                ))
            }
        }
        other if other.is_numeric() => Ok(other.clone()),
        other => Err(unsupported_data_type_exec_err(
            name,
            "First arg must be Numeric Type",
            other,
        )),
    }
}
