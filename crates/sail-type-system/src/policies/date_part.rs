use std::str::FromStr;
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::Result;
use datafusion_common::arrow::compute::kernels::cast_utils::IntervalUnit;
use datafusion_common::utils::take_function_args;
use datafusion_expr::{ReturnFieldArgs, ScalarUDFImpl};
pub fn return_field_from_args(
    inner: &dyn ScalarUDFImpl,
    args: ReturnFieldArgs,
) -> Result<FieldRef> {
    let [field, _] = take_function_args(inner.name(), args.scalar_arguments)?;

    field
        .and_then(|sv| sv.try_as_str())
        .flatten()
        .filter(|part| !part.is_empty())
        .filter(|part| {
            IntervalUnit::from_str(part).is_ok_and(|unit| matches!(unit, IntervalUnit::Second))
        })
        .map(|_| {
            Ok(Arc::new(Field::new(
                inner.name(),
                DataType::Decimal128(8, 6),
                true,
            )))
        })
        .unwrap_or_else(|| inner.return_field_from_args(args))
}
