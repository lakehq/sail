use std::sync::Arc;

use arrow::array::{NullArray, UInt64Array, downcast_run_array};
/// [Credit]: <https://github.com/apache/datafusion/blob/94d178ebe9674669b32ecd7896b5597f49e90791/datafusion/functions-nested/src/utils.rs>
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::datatypes::{
    DataType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use datafusion_common::cast::as_union_array;
use datafusion_common::scalar::get_dict_value;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err, internal_err};
use datafusion_expr::{ColumnarValue, LambdaArgument};

macro_rules! downcast_arg {
    ($ARG:expr_2021, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>().ok_or_else(|| {
            ::datafusion_common::DataFusionError::Internal(format!(
                "could not cast to {}",
                ::std::any::type_name::<$ARRAY_TYPE>()
            ))
        })?
    }};
}

macro_rules! opt_downcast_arg {
    ($ARG:expr_2021, $ARRAY_TYPE:ident) => {{ $ARG.as_any().downcast_ref::<$ARRAY_TYPE>() }};
}

pub(crate) use downcast_arg;
pub(crate) use opt_downcast_arg;

/// array function wrapper that differentiates between scalar (length 1) and array.
pub(crate) fn make_scalar_function<F>(
    inner: F,
) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue>
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef>,
{
    move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();

        let args = ColumnarValue::values_to_arrays(args)?;

        let result = inner(&args);

        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| try_from_array_preserving_fields(arr.as_ref(), 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }
}

// [Credit]: DataFusion 55.1.0, ScalarValue::try_from_array in datafusion/common/src/scalar/mod.rs.
// DataFusion rebuilds non-null list/list-view scalars with a builder that copies
// the child Field's name/nullability but drops its metadata (such as interval
// qualifiers). Slice all list variants to retain that Field, and route recursive
// dictionary/union/run-end conversions through this same function.
// Struct/Map already use slices upstream; leaf types have no child Fields.
// The enclosing column's Field is separate from Array and must be preserved by
// field inference, not by this value conversion.
fn try_from_array_preserving_fields(array: &dyn Array, index: usize) -> Result<ScalarValue> {
    if array.is_null(index) {
        return array.data_type().try_into();
    }

    Ok(match array.data_type() {
        DataType::List(_) => ScalarValue::List(Arc::new(array.as_list::<i32>().slice(index, 1))),
        DataType::LargeList(_) => {
            ScalarValue::LargeList(Arc::new(array.as_list::<i64>().slice(index, 1)))
        }
        DataType::FixedSizeList(_, _) => {
            ScalarValue::FixedSizeList(Arc::new(array.as_fixed_size_list().slice(index, 1)))
        }
        DataType::ListView(_) => {
            ScalarValue::ListView(Arc::new(array.as_list_view::<i32>().slice(index, 1)))
        }
        DataType::LargeListView(_) => {
            ScalarValue::LargeListView(Arc::new(array.as_list_view::<i64>().slice(index, 1)))
        }
        DataType::Dictionary(key_type, _) => {
            let (values_array, values_index) = match key_type.as_ref() {
                DataType::Int8 => get_dict_value::<Int8Type>(array, index)?,
                DataType::Int16 => get_dict_value::<Int16Type>(array, index)?,
                DataType::Int32 => get_dict_value::<Int32Type>(array, index)?,
                DataType::Int64 => get_dict_value::<Int64Type>(array, index)?,
                DataType::UInt8 => get_dict_value::<UInt8Type>(array, index)?,
                DataType::UInt16 => get_dict_value::<UInt16Type>(array, index)?,
                DataType::UInt32 => get_dict_value::<UInt32Type>(array, index)?,
                DataType::UInt64 => get_dict_value::<UInt64Type>(array, index)?,
                _ => return internal_err!("Invalid dictionary keys type: {}", key_type),
            };
            let value = match values_index {
                Some(values_index) => {
                    try_from_array_preserving_fields(values_array.as_ref(), values_index)
                }
                None => values_array.data_type().try_into(),
            }?;
            ScalarValue::Dictionary(key_type.clone(), Box::new(value))
        }
        DataType::RunEndEncoded(run_ends_field, value_field) => {
            // get_physical_index does not check the logical index bounds.
            if index >= array.len() {
                return exec_err!(
                    "Index {index} out of bounds for array of length {}",
                    array.len()
                );
            }
            let scalar = downcast_run_array!(
                array => {
                    let index = array.get_physical_index(index);
                    try_from_array_preserving_fields(array.values().as_ref(), index)?
                },
                dt => return internal_err!("Invalid run-ends type: {dt}")
            );
            ScalarValue::RunEndEncoded(
                Arc::clone(run_ends_field),
                Arc::clone(value_field),
                Box::new(scalar),
            )
        }
        DataType::Union(fields, mode) => {
            let array = as_union_array(array)?;
            let ti = array.type_id(index);
            let index = array.value_offset(index);
            let value = try_from_array_preserving_fields(array.child(ti).as_ref(), index)?;
            ScalarValue::Union(Some((ti, Box::new(value))), fields.clone(), *mode)
        }
        _ => ScalarValue::try_from_array(array, index)?,
    })
}

pub(crate) fn evaluate_lambda_rows(lambda: &LambdaArgument, rows: &[u64]) -> Result<ArrayRef> {
    let indices = UInt64Array::from(rows.to_vec());
    let dummy = || Ok(Arc::new(NullArray::new(rows.len())) as ArrayRef);

    lambda
        .evaluate(&[&dummy], |arrays| Ok(take_arrays(arrays, &indices, None)?))?
        .into_array(rows.len())
}

pub(crate) fn evaluate_lambdas_until_null(
    lambdas: &[&LambdaArgument],
    number_rows: usize,
) -> Result<(Vec<ArrayRef>, Vec<u64>)> {
    let row_count = u64::try_from(number_rows)
        .map_err(|_| exec_datafusion_err!("sequence row count does not fit in u64"))?;
    let mut active_rows = (0..row_count).collect::<Vec<_>>();
    let mut values = Vec::with_capacity(lambdas.len());

    for lambda in lambdas {
        if active_rows.is_empty() {
            break;
        }
        let value = evaluate_lambda_rows(lambda, &active_rows)?;
        let nulls = value.logical_nulls();
        let mut retained_positions = Vec::with_capacity(value.len());
        let mut retained_rows = Vec::with_capacity(value.len());
        for (position, row) in active_rows.iter().copied().enumerate() {
            if nulls.as_ref().is_none_or(|nulls| nulls.is_valid(position)) {
                retained_positions.push(
                    u64::try_from(position).map_err(|_| {
                        exec_datafusion_err!("sequence row index does not fit in u64")
                    })?,
                );
                retained_rows.push(row);
            }
        }

        values.push(value);
        if retained_rows.len() != active_rows.len() {
            let indices = UInt64Array::from(retained_positions);
            values = take_arrays(&values, &indices, None)?;
            active_rows = retained_rows;
        }
    }
    Ok((values, active_rows))
}

pub(crate) fn scatter_active_rows(
    value: ArrayRef,
    active_rows: &[u64],
    number_rows: usize,
) -> Result<ArrayRef> {
    if active_rows.len() == number_rows {
        return Ok(value);
    }

    let mut indices = vec![None::<u64>; number_rows];
    for (compact_index, row) in active_rows.iter().copied().enumerate() {
        let row = usize::try_from(row)
            .map_err(|_| exec_datafusion_err!("sequence row index does not fit in usize"))?;
        let compact_index = u64::try_from(compact_index)
            .map_err(|_| exec_datafusion_err!("sequence row index does not fit in u64"))?;
        let index = indices
            .get_mut(row)
            .ok_or_else(|| exec_datafusion_err!("sequence row index is out of bounds"))?;
        *index = Some(compact_index);
    }
    take_arrays(&[value], &UInt64Array::from(indices), None)?
        .pop()
        .ok_or_else(|| exec_datafusion_err!("sequence take returned no arrays"))
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        DictionaryArray, GenericListViewArray, Int8Array, Int16Array, Int32Array, ListArray,
        OffsetSizeTrait, PrimitiveArray, RunArray, UnionArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{
        ArrowDictionaryKeyType, ArrowNativeType, Field, Int32Type, RunEndIndexType, UnionFields,
    };

    use super::*;

    fn check_list_view_scalar<O: OffsetSizeTrait>() -> Result<()> {
        let field = Arc::new(
            Field::new("element", DataType::Int32, false)
                .with_metadata([("logical_type".into(), "retained".into())].into()),
        );
        // Unordered, overlapping spans, an empty row, and a NULL row with a hidden span.
        let array = GenericListViewArray::<O>::try_new(
            field,
            [4, 1, 0, 2, 1].map(O::usize_as).to_vec().into(),
            [1, 3, 0, 2, 2].map(O::usize_as).to_vec().into(),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
            Some(NullBuffer::from(vec![true, true, true, false, true])),
        )?;
        let expected = [
            Some(vec![50]),
            Some(vec![20, 30, 40]),
            Some(vec![]),
            None,
            Some(vec![20, 30]),
        ];
        for (row, expected) in expected.iter().enumerate() {
            let result = make_scalar_function(|_| Ok(Arc::new(array.slice(row, 1))))(&[
                ColumnarValue::Scalar(ScalarValue::Int32(Some(0))),
            ])?
            .into_array(2)?;
            assert_eq!(result.data_type(), array.data_type());
            let result = result.as_list_view::<O>();
            assert_eq!(result.len(), 2);
            for row in 0..result.len() {
                assert_eq!(result.is_null(row), expected.is_none());
                if let Some(expected) = expected {
                    assert_eq!(
                        result.value(row).as_primitive::<Int32Type>().values(),
                        expected
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn list_view_scalar_preserves_fields_and_values() -> Result<()> {
        check_list_view_scalar::<i32>()
    }

    #[test]
    fn large_list_view_scalar_preserves_fields_and_values() -> Result<()> {
        check_list_view_scalar::<i64>()
    }

    fn annotated_list() -> Result<ListArray> {
        Ok(ListArray::try_new(
            Arc::new(
                Field::new("item", DataType::Int32, false)
                    .with_metadata([("logical_type".into(), "retained".into())].into()),
            ),
            OffsetBuffer::new(vec![0, 2, 4, 6].into()),
            Arc::new(Int32Array::from(vec![10, 11, 90, 91, 12, 13])),
            Some(NullBuffer::from(vec![true, false, true])),
        )?)
    }

    fn check_wrapped_list(
        array: &dyn Array,
        index: usize,
        expected: &ListArray,
    ) -> Result<ScalarValue> {
        let scalar = try_from_array_preserving_fields(array, index)?;
        assert_eq!(scalar.data_type(), *array.data_type());
        let mut value = &scalar;
        loop {
            value = match value {
                ScalarValue::Dictionary(_, value)
                | ScalarValue::Union(Some((_, value)), _, _)
                | ScalarValue::RunEndEncoded(_, _, value) => value,
                _ => break,
            };
        }
        // Union/RunEndEncoded can retain their outer type while losing the child's fields.
        assert_eq!(value.data_type(), *expected.data_type());
        assert_eq!(value, &ScalarValue::List(Arc::new(expected.clone())));
        let broadcast = scalar.to_array_of_size(2)?;
        assert_eq!(broadcast.data_type(), array.data_type());
        assert_eq!(broadcast.len(), 2);
        if !scalar.is_null() {
            assert_eq!(
                try_from_array_preserving_fields(broadcast.as_ref(), 1)?,
                scalar
            );
        }
        Ok(scalar)
    }

    fn check_dictionary<K: ArrowDictionaryKeyType>() -> Result<()> {
        let list = annotated_list()?;
        let keys = PrimitiveArray::<K>::from_iter(
            [Some(2), Some(1), None, Some(0)].map(|x| x.map(K::Native::usize_as)),
        );
        let dictionary = DictionaryArray::<K>::try_new(keys, Arc::new(list.clone()))?;
        // Distinguish a valid key selecting a NULL list from a NULL key.
        for (index, row) in [2, 1, 1, 0].into_iter().enumerate() {
            check_wrapped_list(&dictionary, index, &list.slice(row, 1))?;
        }
        let empty = DictionaryArray::<K>::try_new(
            PrimitiveArray::<K>::from_iter([None]),
            Arc::new(list.slice(0, 0)),
        )?;
        check_wrapped_list(&empty, 0, &list.slice(1, 1))?;
        Ok(())
    }

    #[test]
    fn dictionary_scalars_preserve_nested_list_fields() -> Result<()> {
        check_dictionary::<Int8Type>()?;
        check_dictionary::<Int16Type>()?;
        check_dictionary::<Int32Type>()?;
        check_dictionary::<Int64Type>()?;
        check_dictionary::<UInt8Type>()?;
        check_dictionary::<UInt16Type>()?;
        check_dictionary::<UInt32Type>()?;
        check_dictionary::<UInt64Type>()
    }

    #[test]
    fn sliced_union_scalars_preserve_nested_list_fields() -> Result<()> {
        let list = annotated_list()?;
        let fields = UnionFields::try_new(
            [3],
            [Field::new("choice", list.data_type().clone(), true)
                .with_metadata([("branch".into(), "retained".into())].into())],
        )?;
        for dense in [false, true] {
            let union = UnionArray::try_new(
                fields.clone(),
                vec![3_i8, 3, 3].into(),
                dense.then(|| vec![2_i32, 1, 0].into()),
                vec![Arc::new(list.clone())],
            )?;
            let sliced = union.slice(1, 2);
            for index in 0..2 {
                let row = if dense { 1 - index } else { 1 + index };
                let scalar = check_wrapped_list(&sliced, index, &list.slice(row, 1))?;
                // A NULL child still belongs to the active union variant.
                assert!(matches!(scalar, ScalarValue::Union(Some((3, _)), _, _)));
            }
        }
        Ok(())
    }

    fn check_run_array<R: RunEndIndexType>() -> Result<()> {
        let list = annotated_list()?;
        let ends = PrimitiveArray::<R>::from_iter_values([2, 4, 7].map(R::Native::usize_as));
        let run = RunArray::<R>::try_new(&ends, &list)?;
        let data_type = DataType::RunEndEncoded(
            Arc::new(
                Field::new("ends", R::DATA_TYPE, false)
                    .with_metadata([("run".into(), "retained".into())].into()),
            ),
            Arc::new(
                Field::new("payload", list.data_type().clone(), true)
                    .with_metadata([("value".into(), "retained".into())].into()),
            ),
        );
        let run = RunArray::<R>::from(run.to_data().into_builder().data_type(data_type).build()?);
        let sliced = run.slice(1, 5);
        for (index, row) in [0, 1, 1, 2, 2].into_iter().enumerate() {
            check_wrapped_list(&sliced, index, &list.slice(row, 1))?;
        }
        assert!(try_from_array_preserving_fields(&sliced, sliced.len()).is_err());
        Ok(())
    }

    #[test]
    fn sliced_run_scalars_preserve_nested_list_fields() -> Result<()> {
        check_run_array::<Int16Type>()?;
        check_run_array::<Int32Type>()?;
        check_run_array::<Int64Type>()
    }

    #[test]
    fn composed_encodings_preserve_nested_list_fields() -> Result<()> {
        let list = annotated_list()?;
        let run = RunArray::<Int16Type>::try_new(&Int16Array::from(vec![2, 4, 7]), &list)?;
        let fields =
            UnionFields::try_new([3], [Field::new("choice", run.data_type().clone(), true)])?;
        let union = UnionArray::try_new(
            fields,
            vec![3_i8, 3, 3].into(),
            Some(vec![5_i32, 2, 1].into()),
            vec![Arc::new(run)],
        )?;
        let dictionary =
            DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![2, 0, 1]), Arc::new(union))?;
        for (index, row) in [0, 2, 1].into_iter().enumerate() {
            check_wrapped_list(&dictionary, index, &list.slice(row, 1))?;
        }
        Ok(())
    }
}
