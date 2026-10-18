use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, GenericListArray, GenericListViewArray,
    MutableArrayData, OffsetSizeTrait, as_large_list_array, as_list_array, make_array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::functions_nested_utils::make_scalar_function;
use crate::scalar::array::{cast_list_result, list_view_to_list_type};

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
            DataType::ListView(field) => {
                DataType::ListView(Arc::new(field.as_ref().clone().with_nullable(false)))
            }
            DataType::LargeListView(field) => {
                DataType::LargeListView(Arc::new(field.as_ref().clone().with_nullable(false)))
            }
            DataType::Null => DataType::Null,
            other => return plan_err!("{} expected a list, got {other}", self.name()),
        };
        Ok(Arc::new(
            input
                .as_ref()
                .clone()
                .with_name(self.name())
                .with_data_type(list_view_to_list_type(&data_type)),
        ))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [
                DataType::Null
                | DataType::List(_)
                | DataType::LargeList(_)
                | DataType::ListView(_)
                | DataType::LargeListView(_),
            ] => Ok(arg_types.to_vec()),
            [DataType::FixedSizeList(field, _)] => Ok(vec![DataType::List(Arc::clone(field))]),
            _ => plan_err!("{} requires one list argument", self.name()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, return_field, ..
        } = args;
        if args[0].data_type() == DataType::Null {
            return Ok(ColumnarValue::Scalar(ScalarValue::Null));
        }
        cast_list_result(
            make_scalar_function(array_compact_inner)(&args)?,
            return_field.data_type(),
        )
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
        DataType::ListView(_) => array_compact_view::<i32>(args[0].as_list_view::<i32>()),
        DataType::LargeListView(_) => array_compact_view::<i64>(args[0].as_list_view::<i64>()),
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

fn array_compact_view<O: OffsetSizeTrait>(list: &GenericListViewArray<O>) -> Result<ArrayRef> {
    let values = list.values();
    let values_nulls = values.logical_nulls();
    let source = values.to_data();
    let mut output = MutableArrayData::new(vec![&source], false, 0);
    let mut offsets = Vec::with_capacity(list.len() + 1);
    offsets.push(O::usize_as(0));
    for i in 0..list.len() {
        if !list.is_null(i) {
            let start = list.value_offsets()[i].as_usize();
            let end = start + list.value_sizes()[i].as_usize();
            for j in start..end {
                if values_nulls.as_ref().is_none_or(|nulls| nulls.is_valid(j)) {
                    if output.len() == O::MAX_OFFSET {
                        return exec_err!("array_compact result exceeds the list offset range");
                    }
                    output.try_extend(0, j, j + 1)?;
                }
            }
        }
        offsets.push(O::usize_as(output.len()));
    }
    let field = match list.data_type() {
        DataType::ListView(field) | DataType::LargeListView(field) => {
            Arc::new(field.as_ref().clone().with_nullable(false))
        }
        _ => unreachable!(),
    };
    Ok(Arc::new(GenericListArray::<O>::try_new(
        field,
        OffsetBuffer::new(offsets.into()),
        make_array(output.freeze()),
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
            (DataType::ListView, DataType::List),
            (DataType::LargeListView, DataType::LargeList),
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
