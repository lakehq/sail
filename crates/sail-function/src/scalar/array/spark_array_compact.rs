use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, GenericListArray, OffsetSizeTrait, as_large_list_array,
    as_list_array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common_datafusion::metadata::without_spark_user_metadata;

use crate::functions_nested_utils::make_scalar_function;
use crate::scalar::array::lambda_utils::coerce_single_list_arg;

/// `array_compact` removes all null values from the array.
/// This is the Spark-compatible implementation.
///
/// Returns null if the input array is null.
/// Returns an empty array if all elements are null.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArrayCompact {
    signature: Signature,
}

impl Default for SparkArrayCompact {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayCompact {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkArrayCompact {
    fn name(&self) -> &str {
        "spark_array_compact"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types == [DataType::Null] {
            return Ok(vec![DataType::new_list(DataType::Null, true)]);
        }
        let mut types = coerce_single_list_arg(self.name(), arg_types)?;
        // An unused NULL dictionary value still makes Arrow's dictionary nullable.
        // Decode dictionary elements before constructing the nonnullable result list.
        match &types[0] {
            DataType::List(field) | DataType::LargeList(field) => {
                if let DataType::Dictionary(_, values) = field.data_type() {
                    let field = Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_data_type(values.as_ref().clone()),
                    );
                    types[0] = if matches!(types[0], DataType::LargeList(_)) {
                        DataType::LargeList(field)
                    } else {
                        DataType::List(field)
                    };
                }
            }
            _ => {}
        }
        Ok(types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [input] = args.arg_fields else {
            return plan_err!("{} requires one list argument", self.name());
        };
        // Spark's ArrayCompact wraps its result in KnownNotContainsNull.
        let data_type = match input.data_type() {
            DataType::List(field) => {
                DataType::List(Arc::new(field.as_ref().clone().with_nullable(false)))
            }
            DataType::LargeList(field) => {
                DataType::LargeList(Arc::new(field.as_ref().clone().with_nullable(false)))
            }
            DataType::Null => DataType::Null,
            other => return plan_err!("{} expected a list, got {other}", self.name()),
        };
        let field = input
            .as_ref()
            .clone()
            .with_name(self.name())
            .with_data_type(data_type);
        Ok(Arc::new(without_spark_user_metadata(field)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args[0].data_type() == DataType::Null {
            return Ok(ColumnarValue::Scalar(ScalarValue::Null));
        }
        make_scalar_function(array_compact_inner)(&args)
    }
}

fn array_compact_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("spark_array_compact needs exactly one argument");
    }
    match args[0].data_type() {
        DataType::List(_) => {
            let list = as_list_array(&args[0]);
            array_compact_generic::<i32>(list)
        }
        DataType::LargeList(_) => {
            let list = as_large_list_array(&args[0]);
            array_compact_generic::<i64>(list)
        }
        other => exec_err!("spark_array_compact does not support type: {:?}", other),
    }
}

fn array_compact_generic<O: OffsetSizeTrait>(list: &GenericListArray<O>) -> Result<ArrayRef> {
    let offsets = list.offsets();
    let first = offsets[0].as_usize();
    let values = list
        .values()
        .slice(first, offsets[list.len()].as_usize() - first);
    let values_nulls = values.logical_nulls();

    // Build a boolean filter mask for the flattened values array.
    // We keep a value if its parent row is not null AND the value itself is not null.
    let mut keep: Vec<bool> = Vec::with_capacity(values.len());
    // New offsets after compaction: one entry per row, counting retained values.
    let mut new_offsets: Vec<O> = Vec::with_capacity(list.len() + 1);
    new_offsets.push(O::usize_as(0));
    let mut total_kept: usize = 0;

    for i in 0..list.len() {
        let start = offsets[i].as_usize() - first;
        let end = offsets[i + 1].as_usize() - first;

        if list.is_null(i) {
            // Null row: mark all values in the range as not kept.
            keep.extend(std::iter::repeat_n(false, end - start));
            // Null row: offset stays the same (empty slice).
            new_offsets.push(O::usize_as(total_kept));
        } else {
            // Non-null row: keep only non-null values.
            for j in start..end {
                let keep_value = values_nulls.as_ref().is_none_or(|nulls| nulls.is_valid(j));
                keep.push(keep_value);
                if keep_value {
                    total_kept += 1;
                }
            }
            new_offsets.push(O::usize_as(total_kept));
        }
    }

    let keep_mask = BooleanArray::from(keep);
    let new_values = compute::filter(values.as_ref(), &keep_mask)?;

    let field = match list.data_type() {
        DataType::List(f) | DataType::LargeList(f) => {
            Arc::new(f.as_ref().clone().with_nullable(false))
        }
        _ => unreachable!(),
    };

    Ok(Arc::new(GenericListArray::<O>::try_new(
        field,
        OffsetBuffer::new(new_offsets.into()),
        new_values,
        list.nulls().cloned(),
    )?))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::Field;

    use super::*;

    #[test]
    fn compact_preserves_fields_and_removes_element_nullability() -> Result<()> {
        let element = Field::new("element", DataType::Int32, true)
            .with_metadata(HashMap::from([("logical_type".into(), "element".into())]));
        for (list_type, output_type) in [
            (
                DataType::List as fn(FieldRef) -> DataType,
                DataType::List as fn(FieldRef) -> DataType,
            ),
            (DataType::LargeList, DataType::LargeList),
        ] {
            for nullable in [false, true] {
                let input = Field::new("array", list_type(Arc::new(element.clone())), nullable)
                    .with_metadata(HashMap::from([("logical_type".into(), "container".into())]));
                let actual = SparkArrayCompact::new().return_field_from_args(ReturnFieldArgs {
                    arg_fields: &[Arc::new(input.clone())],
                    scalar_arguments: &[None],
                })?;
                assert_eq!(
                    actual.as_ref(),
                    &input
                        .with_name("spark_array_compact")
                        .with_data_type(output_type(Arc::new(
                            element.clone().with_nullable(false)
                        )))
                );
            }
        }
        Ok(())
    }
}
