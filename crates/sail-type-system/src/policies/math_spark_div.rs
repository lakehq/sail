//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::type_coercion::binary::BinaryTypeCoercer;
use datafusion_expr::{Operator, ReturnFieldArgs};

use crate::error::invalid_arg_count_exec_err;

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}

pub fn return_field_from_args(_args: ReturnFieldArgs) -> Result<FieldRef> {
    Ok(Arc::new(Field::new("spark_div", DataType::Int64, true)))
}

pub fn coerce_types(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 2 {
        return Err(invalid_arg_count_exec_err(
            "spark_div",
            (2, 2),
            arg_types.len(),
        ));
    }

    if matches!(
        (&arg_types[0], &arg_types[1]),
        (DataType::Interval(_), DataType::Interval(_))
    ) {
        return Ok(arg_types.to_vec());
    }

    // Resolve the common type first; decimal operands must not be truncated to BIGINT.
    let (left, right) = BinaryTypeCoercer::new(&arg_types[0], &Operator::Divide, &arg_types[1])
        .get_input_types()?;
    Ok([left, right]
        .into_iter()
        .map(|data_type| match data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 => DataType::Int64,
            data_type => data_type,
        })
        .collect())
}
