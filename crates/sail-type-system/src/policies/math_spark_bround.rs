//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::ReturnFieldArgs;

use crate::error::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err, unsupported_data_types_exec_err,
};

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn return_field_from_args(args: ReturnFieldArgs) -> Result<FieldRef> {
    let arg_types = args
        .arg_fields
        .iter()
        .map(|f| f.data_type().clone())
        .collect::<Vec<_>>();
    if !(1..=2).contains(&arg_types.len()) {
        return Err(invalid_arg_count_exec_err(
            "spark_bround",
            (1, 2),
            arg_types.len(),
        ));
    }
    let t = &arg_types[0];
    let data_type = match t {
        DataType::Float64
        | DataType::Float32
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => DataType::Float64,
        DataType::Int32 => DataType::Int32,
        DataType::Int64 => DataType::Int64,
        _ => {
            return Err(unsupported_data_type_exec_err(
                "spark_bround",
                "Float64, Float32, Decimal128, Decimal256, Int32, Int64",
                t,
            ));
        }
    };
    Ok(Arc::new(Field::new("spark_bround", data_type, true)))
}

pub fn coerce_types(types: &[DataType]) -> Result<Vec<DataType>> {
    // FIXME check if need delete coerce
    match types.len() {
        1 => {
            let x_type: &DataType = &types[0];
            let valid_x: bool = matches!(
                x_type,
                DataType::Float32
                    | DataType::Float64
                    | DataType::Decimal128(_, _)
                    | DataType::Decimal256(_, _)
                    | DataType::Int32
                    | DataType::Int64
            );

            if valid_x {
                Ok(vec![x_type.clone(), DataType::Int32])
            } else {
                Err(unsupported_data_types_exec_err(
                    "spark_bround",
                    "Float32 | Float64 | Decimal128 | Decimal256 | Int32 | Int64",
                    types,
                ))
            }
        }
        2 => {
            let x_type: &DataType = &types[0];
            let scale_type: &DataType = &types[1];

            let valid_x: bool = matches!(
                x_type,
                DataType::Float32
                    | DataType::Float64
                    | DataType::Decimal128(_, _)
                    | DataType::Decimal256(_, _)
                    | DataType::Int32
                    | DataType::Int64
            );
            let valid_scale: bool = matches!(scale_type, DataType::Int32);

            if valid_x && valid_scale {
                Ok(vec![x_type.clone(), scale_type.clone()])
            } else {
                Err(unsupported_data_types_exec_err(
                    "spark_bround",
                    "Float32 | Float64 | Decimal128 | Decimal256 | Int32 | Int64, Int32",
                    types,
                ))
            }
        }
        _ => Err(invalid_arg_count_exec_err(
            "spark_bround",
            (1, 2),
            types.len(),
        )),
    }
}
