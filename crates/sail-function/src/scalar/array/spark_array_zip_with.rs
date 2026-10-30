/// Spark-compatible `zip_with(left, right, lambda)` higher-order function.
///
/// The lambda receives one element from each array. Missing elements in the
/// shorter array are represented by typed nulls.
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, GenericListArray, LargeListArray, ListArray, OffsetSizeTrait, new_null_array,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::{cast, concat, take_arrays};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArrayZipWith {
    signature: HigherOrderSignature,
}

impl Default for SparkArrayZipWith {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayZipWith {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![
                    ValueOrLambda::Value(()),
                    ValueOrLambda::Value(()),
                    ValueOrLambda::Lambda(()),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl HigherOrderUDFImpl for SparkArrayZipWith {
    fn name(&self) -> &str {
        "zip_with"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let (left, right, _lambda) = zip_args(self.name(), fields)?;
        let left_element = list_element(self.name(), left, "first")?;
        let right_element = list_element(self.name(), right, "second")?;
        Ok(LambdaParametersProgress::Complete(vec![vec![
            Arc::new(left_element.as_ref().clone().with_nullable(true)),
            Arc::new(right_element.as_ref().clone().with_nullable(true)),
        ]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let (left, right, lambda) = zip_args(self.name(), args.arg_fields)?;
        list_element(self.name(), left, "first")?;
        list_element(self.name(), right, "second")?;
        let large_offsets =
            uses_large_offsets(left.data_type()) || uses_large_offsets(right.data_type());
        let element_field = Arc::new(Field::new(
            Field::LIST_FIELD_DEFAULT_NAME,
            lambda.data_type().clone(),
            lambda.is_nullable(),
        ));
        let return_type = if large_offsets {
            DataType::LargeList(element_field)
        } else {
            DataType::List(element_field)
        };
        Ok(Arc::new(Field::new(
            "",
            return_type,
            left.is_nullable() || right.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let (left, right, lambda) = zip_args(self.name(), &args.args)?;
        let left_array = left.to_array(args.number_rows)?;
        let right_array = right.to_array(args.number_rows)?;
        let (DataType::List(element_field) | DataType::LargeList(element_field)) =
            args.return_field.data_type()
        else {
            return exec_err!(
                "{} expected a list return type, got {}",
                self.name(),
                args.return_field
            );
        };
        let large_offsets = matches!(args.return_field.data_type(), DataType::LargeList(_));
        let left_rows = left_array.len();
        let right_rows = right_array.len();
        if left_rows != right_rows {
            return exec_err!(
                "{} expected arrays with the same row count, got {} and {}",
                self.name(),
                left_rows,
                right_rows
            );
        }
        let left_array = normalize_list_array(self.name(), &left_array, large_offsets)?;
        let right_array = normalize_list_array(self.name(), &right_array, large_offsets)?;
        let (left_values, right_values, offsets, nulls) = if large_offsets {
            let left_list = as_large_list_array(&left_array)?;
            let right_list = as_large_list_array(&right_array)?;
            build_zipped_values(left_list, right_list)?
        } else {
            let left_list = as_list_array(&left_array)?;
            let right_list = as_list_array(&right_array)?;
            build_zipped_values(left_list, right_list)?
        };
        let row_numbers = zip_row_numbers(&offsets);
        let left_param = || Ok(Arc::clone(&left_values));
        let right_param = || Ok(Arc::clone(&right_values));
        let params: [&dyn Fn() -> Result<ArrayRef>; 2] = [&left_param, &right_param];
        let output = lambda
            .evaluate(&params, |arrays| {
                Ok(take_arrays(arrays, &row_numbers, None)?)
            })?
            .into_array(left_values.len())?;
        let output = if output.data_type() == element_field.data_type() {
            output
        } else {
            cast(&output, element_field.data_type())?
        };
        if large_offsets {
            Ok(ColumnarValue::Array(Arc::new(LargeListArray::new(
                Arc::clone(element_field),
                offset_buffer::<i64>(&offsets)?,
                output,
                nulls,
            ))))
        } else {
            Ok(ColumnarValue::Array(Arc::new(ListArray::new(
                Arc::clone(element_field),
                offset_buffer::<i32>(&offsets)?,
                output,
                nulls,
            ))))
        }
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [left, right] = arg_types else {
            return plan_err!(
                "{} function requires 2 value arguments, got {}",
                self.name(),
                arg_types.len()
            );
        };
        let large_offsets = uses_large_offsets(left) || uses_large_offsets(right);
        Ok(vec![
            canonical_list_type(self.name(), left, large_offsets)?,
            canonical_list_type(self.name(), right, large_offsets)?,
        ])
    }
}

fn zip_args<'a, V: std::fmt::Debug, L: std::fmt::Debug>(
    name: &str,
    args: &'a [ValueOrLambda<V, L>],
) -> Result<(&'a V, &'a V, &'a L)> {
    let [left, right, lambda] = args else {
        return plan_err!("{name} expects two arrays followed by a lambda");
    };
    let ValueOrLambda::Value(left) = left else {
        return plan_err!("{name} expected a value as first argument");
    };
    let ValueOrLambda::Value(right) = right else {
        return plan_err!("{name} expected a value as second argument");
    };
    let ValueOrLambda::Lambda(lambda) = lambda else {
        return plan_err!("{name} expected a lambda as third argument");
    };
    Ok((left, right, lambda))
}

fn list_element(name: &str, field: &FieldRef, position: &str) -> Result<FieldRef> {
    list_element_from_type(name, field.data_type(), position)
}

fn list_element_from_type(name: &str, data_type: &DataType, position: &str) -> Result<FieldRef> {
    let element = match data_type {
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _)
        | DataType::ListView(element)
        | DataType::LargeListView(element) => element,
        _ => {
            return plan_err!("{name} expected a list as the {position} argument, got {data_type}");
        }
    };
    Ok(Arc::clone(element))
}

fn uses_large_offsets(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::LargeList(_) | DataType::LargeListView(_)
    )
}

fn canonical_list_type(name: &str, data_type: &DataType, large_offsets: bool) -> Result<DataType> {
    let element = list_element_from_type(name, data_type, "argument")?;
    Ok(if large_offsets {
        DataType::LargeList(element)
    } else {
        DataType::List(element)
    })
}

fn normalize_list_array(name: &str, array: &ArrayRef, large_offsets: bool) -> Result<ArrayRef> {
    let data_type = canonical_list_type(name, array.data_type(), large_offsets)?;
    if array.data_type() == &data_type {
        Ok(Arc::clone(array))
    } else {
        Ok(cast(array.as_ref(), &data_type)?)
    }
}

fn build_zipped_values<O>(
    left_list: &GenericListArray<O>,
    right_list: &GenericListArray<O>,
) -> Result<(ArrayRef, ArrayRef, Vec<usize>, Option<NullBuffer>)>
where
    O: OffsetSizeTrait + TryFrom<usize>,
    usize: TryFrom<O>,
{
    let left_values = left_list.values();
    let right_values = right_list.values();
    let mut offsets = vec![0usize];
    let row_count = left_list.offsets().len() - 1;
    let mut nulls = Vec::with_capacity(row_count);
    let mut left_parts = Vec::new();
    let mut right_parts = Vec::new();
    let mut total = 0usize;

    for row in 0..row_count {
        if left_list.nulls().is_some_and(|nulls| nulls.is_null(row))
            || right_list.nulls().is_some_and(|nulls| nulls.is_null(row))
        {
            nulls.push(false);
            offsets.push(total);
            continue;
        }
        nulls.push(true);
        let left_start = usize::try_from(left_list.offsets()[row]).map_err(|_| {
            datafusion_common::DataFusionError::Execution("invalid zip_with offset".to_owned())
        })?;
        let left_end = usize::try_from(left_list.offsets()[row + 1]).map_err(|_| {
            datafusion_common::DataFusionError::Execution("invalid zip_with offset".to_owned())
        })?;
        let right_start = usize::try_from(right_list.offsets()[row]).map_err(|_| {
            datafusion_common::DataFusionError::Execution("invalid zip_with offset".to_owned())
        })?;
        let right_end = usize::try_from(right_list.offsets()[row + 1]).map_err(|_| {
            datafusion_common::DataFusionError::Execution("invalid zip_with offset".to_owned())
        })?;
        let left_len = left_end.checked_sub(left_start).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution("invalid zip_with offset".to_owned())
        })?;
        let right_len = right_end.checked_sub(right_start).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution("invalid zip_with offset".to_owned())
        })?;
        let length = left_len.max(right_len);
        let left_part = left_values.slice(left_start, left_len);
        let right_part = right_values.slice(right_start, right_len);
        left_parts.push(pad_part(&left_part, length)?);
        right_parts.push(pad_part(&right_part, length)?);
        total = total.checked_add(length).ok_or_else(|| {
            datafusion_common::DataFusionError::Execution("zip_with array is too large".to_owned())
        })?;
        if total > O::MAX_OFFSET {
            return exec_err!("zip_with array is too large");
        }
        offsets.push(total);
    }

    let left_values = concat_or_empty(&left_parts, left_values.data_type())?;
    let right_values = concat_or_empty(&right_parts, right_values.data_type())?;
    Ok((
        left_values,
        right_values,
        offsets,
        Some(NullBuffer::from(nulls)),
    ))
}

fn pad_part(values: &ArrayRef, length: usize) -> Result<ArrayRef> {
    if values.len() == length {
        return Ok(Arc::clone(values));
    }
    let padding = new_null_array(values.data_type(), length - values.len());
    Ok(concat(&[values.as_ref(), padding.as_ref()])?)
}

fn concat_or_empty(parts: &[ArrayRef], data_type: &DataType) -> Result<ArrayRef> {
    if parts.is_empty() {
        return Ok(new_null_array(data_type, 0));
    }
    Ok(concat(
        &parts.iter().map(|part| part.as_ref()).collect::<Vec<_>>(),
    )?)
}

fn offset_buffer<O>(offsets: &[usize]) -> Result<OffsetBuffer<O>>
where
    O: OffsetSizeTrait + TryFrom<usize>,
{
    let offsets = offsets
        .iter()
        .copied()
        .map(|offset| {
            O::try_from(offset).map_err(|_| {
                datafusion_common::DataFusionError::Execution(
                    "zip_with array is too large".to_owned(),
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(OffsetBuffer::new(offsets.into()))
}

fn zip_row_numbers(offsets: &[usize]) -> datafusion::arrow::array::UInt32Array {
    let mut values = Vec::with_capacity(offsets.last().copied().unwrap_or_default());
    for row in 0..offsets.len() - 1 {
        values.extend(std::iter::repeat_n(
            row as u32,
            offsets[row + 1] - offsets[row],
        ));
    }
    datafusion::arrow::array::UInt32Array::from(values)
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::Int32Array;

    use super::*;

    fn list(values: Vec<Option<i32>>, lengths: Vec<usize>, nulls: Option<NullBuffer>) -> ListArray {
        ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::from_lengths(lengths),
            Arc::new(Int32Array::from(values)),
            nulls,
        )
    }

    #[test]
    fn pads_shorter_arrays_with_nulls() -> Result<()> {
        let left = list(vec![Some(1), Some(2), Some(3)], vec![2, 1], None);
        let right = list(vec![Some(10), Some(20), Some(30)], vec![3, 0], None);
        let (left_values, right_values, offsets, nulls) = build_zipped_values(&left, &right)?;

        assert_eq!(offsets.as_slice(), &[0, 3, 4]);
        assert_eq!(nulls.unwrap().iter().collect::<Vec<_>>(), vec![true, true]);
        assert_eq!(
            left_values.as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![Some(1), Some(2), None, Some(3)])
        );
        assert_eq!(
            right_values.as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![Some(10), Some(20), Some(30), None])
        );
        Ok(())
    }

    #[test]
    fn preserves_null_input_rows() -> Result<()> {
        let left = list(
            vec![Some(1), Some(2)],
            vec![2],
            Some(NullBuffer::from(vec![false])),
        );
        let right = list(vec![Some(10), Some(20)], vec![2], None);
        let (left_values, right_values, offsets, nulls) = build_zipped_values(&left, &right)?;

        assert!(left_values.is_empty());
        assert!(right_values.is_empty());
        assert_eq!(offsets.as_slice(), &[0, 0]);
        assert_eq!(nulls.unwrap().iter().collect::<Vec<_>>(), vec![false]);
        Ok(())
    }

    #[test]
    fn normalizes_supported_arrow_list_layouts() -> Result<()> {
        let element = Arc::new(Field::new_list_field(DataType::Int32, true));
        let layouts = [
            (
                DataType::List(Arc::clone(&element)),
                false,
                DataType::List(Arc::clone(&element)),
            ),
            (
                DataType::LargeList(Arc::clone(&element)),
                true,
                DataType::LargeList(Arc::clone(&element)),
            ),
            (
                DataType::FixedSizeList(Arc::clone(&element), 2),
                false,
                DataType::List(Arc::clone(&element)),
            ),
            (
                DataType::ListView(Arc::clone(&element)),
                false,
                DataType::List(Arc::clone(&element)),
            ),
            (
                DataType::LargeListView(Arc::clone(&element)),
                true,
                DataType::LargeList(Arc::clone(&element)),
            ),
        ];

        for (source_type, large_offsets, expected_type) in layouts {
            let source = new_null_array(&source_type, 1);
            let normalized = normalize_list_array("zip_with", &source, large_offsets)?;
            assert_eq!(normalized.data_type(), &expected_type);
        }
        Ok(())
    }
}
