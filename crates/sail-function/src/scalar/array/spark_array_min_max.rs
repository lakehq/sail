use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, GenericListArray, OffsetSizeTrait, as_large_list_array, as_list_array,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::functions_aggregate::min_max;
use datafusion_common::{Result, ScalarValue, exec_err, internal_err, plan_err};
use datafusion_expr::{
    Accumulator, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::functions_nested_utils::make_scalar_function;

enum ArrayOp {
    Min,
    Max,
}

fn element_return_field(name: &str, data_type: Option<&DataType>) -> Result<Field> {
    match data_type {
        Some(DataType::List(field) | DataType::LargeList(field)) => {
            Ok(field.as_ref().clone().with_nullable(true))
        }
        Some(DataType::Null) => Ok(Field::new(name, DataType::Null, true)),
        _ => plan_err!("{name} can only accept List or LargeList."),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayMin {
    signature: Signature,
}

impl Default for ArrayMin {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayMin {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayMin {
    fn name(&self) -> &str {
        "array_min"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `ArrayMin.nullable = true`, unconditional (an empty array yields NULL).
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/collectionOperations.scala#L2349>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(
            element_return_field("ArrayMin", args.arg_fields.first().map(|f| f.data_type()))?
                .with_name(self.name()),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args[0].data_type() == DataType::Null {
            return Ok(ColumnarValue::Scalar(ScalarValue::Null));
        }
        make_scalar_function(array_min_inner)(&args)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayMax {
    signature: Signature,
}

impl Default for ArrayMax {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayMax {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayMax {
    fn name(&self) -> &str {
        "array_max"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `ArrayMax.nullable = true`, unconditional (an empty array yields NULL).
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/collectionOperations.scala#L2422>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(
            element_return_field("ArrayMax", args.arg_fields.first().map(|f| f.data_type()))?
                .with_name(self.name()),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args[0].data_type() == DataType::Null {
            return Ok(ColumnarValue::Scalar(ScalarValue::Null));
        }
        make_scalar_function(array_max_inner)(&args)
    }
}

fn array_min_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_min needs one argument");
    }
    let result_values = match &args[0].data_type() {
        DataType::List(field) => {
            let array = as_list_array(&args[0]);
            let data_type = field.data_type();
            compute_min_max_values::<i32>(array, data_type, ArrayOp::Min)
        }
        DataType::LargeList(field) => {
            let array = as_large_list_array(&args[0]);
            let data_type = field.data_type();
            compute_min_max_values::<i64>(array, data_type, ArrayOp::Min)
        }
        _ => exec_err!(
            "array_min does not support type: {:?}",
            &args[0].data_type()
        ),
    }?;
    ScalarValue::iter_to_array(result_values)
}

fn array_max_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_max needs one argument");
    }
    let result_values = match &args[0].data_type() {
        DataType::List(field) => {
            let array = as_list_array(&args[0]);
            let data_type = field.data_type();
            compute_min_max_values::<i32>(array, data_type, ArrayOp::Max)
        }
        DataType::LargeList(field) => {
            let array = as_large_list_array(&args[0]);
            let data_type = field.data_type();
            compute_min_max_values::<i64>(array, data_type, ArrayOp::Max)
        }
        _ => exec_err!(
            "array_max does not support type: {:?}",
            &args[0].data_type()
        ),
    }?;
    ScalarValue::iter_to_array(result_values)
}

fn compute_min_max_values<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    data_type: &DataType,
    op: ArrayOp,
) -> Result<Vec<ScalarValue>>
where
    i64: TryInto<O>,
{
    let mut result_values = Vec::new();
    for i in 0..array.len() {
        let inner_array = array.value(i);
        let value = match op {
            ArrayOp::Min => {
                let mut min_accumulator = min_max::MinAccumulator::try_new(data_type)?;
                min_accumulator.update_batch(&[inner_array])?;
                min_accumulator.evaluate()?
            }
            ArrayOp::Max => {
                let mut max_accumulator = min_max::MaxAccumulator::try_new(data_type)?;
                max_accumulator.update_batch(&[inner_array])?;
                max_accumulator.evaluate()?
            }
        };
        result_values.push(value);
    }
    Ok(result_values)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn min_max_preserve_element_metadata() -> Result<()> {
        let element = Arc::new(
            Field::new("element", DataType::Int64, false)
                .with_metadata(HashMap::from([("logical_type".into(), "element".into())])),
        );
        for data_type in [
            DataType::List(Arc::clone(&element)),
            DataType::LargeList(Arc::clone(&element)),
        ] {
            let input = Arc::new(
                Field::new("array", data_type, false)
                    .with_metadata(HashMap::from([("logical_type".into(), "container".into())])),
            );
            for func in [&ArrayMin::new() as &dyn ScalarUDFImpl, &ArrayMax::new()] {
                let actual = func.return_field_from_args(ReturnFieldArgs {
                    arg_fields: &[Arc::clone(&input)],
                    scalar_arguments: &[None],
                })?;
                assert_eq!(
                    actual.as_ref(),
                    &element
                        .as_ref()
                        .clone()
                        .with_name(func.name())
                        .with_nullable(true)
                );
                assert!(func.return_type(&[input.data_type().clone()]).is_err());
            }
        }
        Ok(())
    }
}
