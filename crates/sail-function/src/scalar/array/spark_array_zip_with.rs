/// Spark-compatible `zip_with(left, right, lambda)` higher-order function.
///
/// Spark semantics: merges two arrays element-wise using a lambda function.
/// The lambda receives `(left_element, right_element)` for each pair of elements.
/// If one array is longer than the other, the missing elements are passed as NULL.
/// The result is an array of the lambda's return type with length equal to the
/// maximum length of the two input arrays.
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, ListArray, new_null_array};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::{concat, take_arrays};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::as_list_array;
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
        Ok(Arc::new(Field::new(
            "",
            DataType::List(Arc::new(Field::new(
                Field::LIST_FIELD_DEFAULT_NAME,
                lambda.data_type().clone(),
                lambda.is_nullable(),
            ))),
            left.is_nullable() || right.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let (left, right, lambda) = zip_args(self.name(), &args.args)?;
        let left_array = left.to_array(args.number_rows)?;
        let right_array = right.to_array(args.number_rows)?;
        let left_list = as_list_array(&left_array)?;
        let right_list = as_list_array(&right_array)?;
        if left_list.len() != right_list.len() {
            return exec_err!(
                "{} expected arrays with the same row count, got {} and {}",
                self.name(),
                left_list.len(),
                right_list.len()
            );
        }

        let (left_values, right_values, offsets, nulls) =
            build_zipped_values(left_list, right_list)?;
        let row_numbers = zip_row_numbers(&offsets)?;
        let left_param = || Ok(Arc::clone(&left_values));
        let right_param = || Ok(Arc::clone(&right_values));
        let params: [&dyn Fn() -> Result<ArrayRef>; 2] = [&left_param, &right_param];
        let output = lambda
            .evaluate(&params, |arrays| {
                Ok(take_arrays(arrays, &row_numbers, None)?)
            })?
            .into_array(left_values.len())?;

        let DataType::List(element_field) = args.return_field.data_type() else {
            return exec_err!(
                "{} expected a list return type, got {}",
                self.name(),
                args.return_field
            );
        };
        let output = if output.data_type() == element_field.data_type() {
            output
        } else {
            datafusion::arrow::compute::cast(&output, element_field.data_type())?
        };
        Ok(ColumnarValue::Array(Arc::new(ListArray::new(
            Arc::clone(element_field),
            offsets,
            output,
            nulls,
        ))))
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return plan_err!(
                "{} function requires 2 value arguments, got {}",
                self.name(),
                arg_types.len()
            );
        }
        arg_types
            .iter()
            .map(|data_type| match data_type {
                DataType::List(_) => Ok(data_type.clone()),
                other => plan_err!("{} expected a list, got {other}", self.name()),
            })
            .collect()
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
    let DataType::List(element) = field.data_type() else {
        return plan_err!("{name} expected a list as the {position} argument, got {field}");
    };
    Ok(Arc::clone(element))
}

fn build_zipped_values(
    left_list: &ListArray,
    right_list: &ListArray,
) -> Result<(ArrayRef, ArrayRef, OffsetBuffer<i32>, Option<NullBuffer>)> {
    let left_values = left_list.values();
    let right_values = right_list.values();
    let mut offsets = vec![0i32];
    let mut nulls = Vec::with_capacity(left_list.len());
    let mut left_parts = Vec::new();
    let mut right_parts = Vec::new();
    let mut total = 0i32;

    for row in 0..left_list.len() {
        if left_list.is_null(row) || right_list.is_null(row) {
            nulls.push(false);
            offsets.push(total);
            continue;
        }
        nulls.push(true);
        let left_start = left_list.offsets()[row] as usize;
        let left_len = (left_list.offsets()[row + 1] - left_list.offsets()[row]) as usize;
        let right_start = right_list.offsets()[row] as usize;
        let right_len = (right_list.offsets()[row + 1] - right_list.offsets()[row]) as usize;
        let length = left_len.max(right_len);
        let left_part = left_values.slice(left_start, left_len);
        let right_part = right_values.slice(right_start, right_len);
        left_parts.push(pad_part(&left_part, length)?);
        right_parts.push(pad_part(&right_part, length)?);
        let length = i32::try_from(length).map_err(|_| {
            datafusion_common::DataFusionError::Execution("zip_with array is too large".to_string())
        })?;
        total += length;
        offsets.push(total);
    }

    let left_values = concat_or_empty(&left_parts, left_values.data_type())?;
    let right_values = concat_or_empty(&right_parts, right_values.data_type())?;
    Ok((
        left_values,
        right_values,
        OffsetBuffer::new(offsets.into()),
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

fn zip_row_numbers(offsets: &OffsetBuffer<i32>) -> Result<datafusion::arrow::array::UInt32Array> {
    let mut values = Vec::with_capacity(offsets.last().copied().unwrap_or_default() as usize);
    for row in 0..offsets.len() - 1 {
        values.extend(std::iter::repeat_n(
            row as u32,
            (offsets[row + 1] - offsets[row]) as usize,
        ));
    }
    Ok(datafusion::arrow::array::UInt32Array::from(values))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int32Array, UInt32Array};

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

        assert_eq!(offsets.as_ref(), &[0, 3, 4]);
        assert_eq!(nulls.unwrap().iter().collect::<Vec<_>>(), vec![true, true]);
        assert_eq!(
            left_values.as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![Some(1), Some(2), None, Some(3)])
        );
        assert_eq!(
            right_values.as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![Some(10), Some(20), Some(30), None])
        );
        assert_eq!(
            zip_row_numbers(&offsets)?,
            UInt32Array::from(vec![0, 0, 0, 1])
        );
        Ok(())
    }

    #[test]
    fn returns_null_for_null_input_array() -> Result<()> {
        let left = list(
            vec![Some(1), Some(2)],
            vec![2],
            Some(NullBuffer::from(vec![false])),
        );
        let right = list(vec![Some(10), Some(20)], vec![2], None);
        let (left_values, right_values, offsets, nulls) = build_zipped_values(&left, &right)?;

        assert!(left_values.is_empty());
        assert!(right_values.is_empty());
        assert_eq!(offsets.as_ref(), &[0, 0]);
        assert_eq!(nulls.unwrap().iter().collect::<Vec<_>>(), vec![false]);
        Ok(())
    }
}
