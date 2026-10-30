/// Spark-compatible `map_zip_with(map1, map2, lambda)` higher-order function.
///
/// The lambda receives `(key, left_value, right_value)` for the union of keys.
/// Missing values are represented by typed nulls.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, MapArray, StructArray, new_empty_array};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::{cast, take};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion_common::cast::as_map_array;
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};
use sail_common::spec::{SAIL_MAP_FIELD_NAME, SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMapZipWith {
    signature: HigherOrderSignature,
}

impl Default for SparkMapZipWith {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMapZipWith {
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

impl HigherOrderUDFImpl for SparkMapZipWith {
    fn name(&self) -> &str {
        "map_zip_with"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let (left, right, _lambda) = map_zip_args(self.name(), fields)?;
        let (left_key, left_value, _) = map_fields(self.name(), left)?;
        let (right_key, _right_value, _) = map_fields(self.name(), right)?;
        same_key_type(self.name(), &left_key, &right_key)?;
        Ok(LambdaParametersProgress::Complete(vec![vec![
            Arc::new(left_key.as_ref().clone().with_nullable(false)),
            Arc::new(left_value.as_ref().clone().with_nullable(true)),
            Arc::new(
                map_fields(self.name(), right)?
                    .1
                    .as_ref()
                    .clone()
                    .with_nullable(true),
            ),
        ]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let (left, right, lambda) = map_zip_args(self.name(), args.arg_fields)?;
        let (left_key, _, _) = map_fields(self.name(), left)?;
        let (right_key, _, _) = map_fields(self.name(), right)?;
        same_key_type(self.name(), &left_key, &right_key)?;
        let fields = Fields::from(vec![
            Field::new(SAIL_MAP_KEY_FIELD_NAME, left_key.data_type().clone(), false),
            Field::new(
                SAIL_MAP_VALUE_FIELD_NAME,
                lambda.data_type().clone(),
                lambda.is_nullable(),
            ),
        ]);
        Ok(Arc::new(Field::new(
            "",
            DataType::Map(
                Arc::new(Field::new(
                    SAIL_MAP_FIELD_NAME,
                    DataType::Struct(fields),
                    false,
                )),
                false,
            ),
            left.is_nullable() || right.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let (left, right, lambda) = map_zip_args(self.name(), &args.args)?;
        let left_array = left.to_array(args.number_rows)?;
        let right_array = right.to_array(args.number_rows)?;
        let left_map = as_map_array(&left_array)?;
        let right_map = as_map_array(&right_array)?;
        let inputs = build_lambda_inputs(
            self.name(),
            left_map,
            right_map,
            args.return_field.data_type(),
        )?;
        let key_param = || Ok(Arc::clone(&inputs.keys));
        let left_param = || Ok(Arc::clone(&inputs.left_values));
        let right_param = || Ok(Arc::clone(&inputs.right_values));
        let params: [&dyn Fn() -> Result<ArrayRef>; 3] = [&key_param, &left_param, &right_param];
        let output_values = lambda
            .evaluate(&params, |arrays| repeat_captures(arrays, &inputs.offsets))?
            .into_array(inputs.keys.len())?;

        let DataType::Map(entries_field, ordered) = args.return_field.data_type() else {
            return exec_err!(
                "{} expected map return type, got {}",
                self.name(),
                args.return_field
            );
        };
        let DataType::Struct(fields) = entries_field.data_type() else {
            return exec_err!("{} expected map entries struct", self.name());
        };
        let values = if output_values.data_type() == fields[1].data_type() {
            output_values
        } else {
            cast(&output_values, fields[1].data_type())?
        };
        let entries = StructArray::try_new(fields.clone(), vec![inputs.keys, values], None)?;
        Ok(ColumnarValue::Array(Arc::new(MapArray::try_new(
            Arc::clone(entries_field),
            inputs.offsets,
            entries,
            inputs.nulls,
            *ordered,
        )?)))
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [left, right] = arg_types else {
            return plan_err!(
                "{} function requires 2 value arguments, got {}",
                self.name(),
                arg_types.len()
            );
        };
        let (left_key, _, _) = map_fields_from_type(self.name(), left, "first")?;
        let (right_key, _, _) = map_fields_from_type(self.name(), right, "second")?;
        same_key_type(self.name(), &left_key, &right_key)?;
        Ok(vec![left.clone(), right.clone()])
    }
}

fn map_zip_args<'a, V: std::fmt::Debug, L: std::fmt::Debug>(
    name: &str,
    args: &'a [ValueOrLambda<V, L>],
) -> Result<(&'a V, &'a V, &'a L)> {
    let [left, right, lambda] = args else {
        return plan_err!("{name} expects two maps followed by a lambda");
    };
    let (ValueOrLambda::Value(left), ValueOrLambda::Value(right), ValueOrLambda::Lambda(lambda)) =
        (left, right, lambda)
    else {
        return plan_err!("{name} expects two maps followed by a lambda");
    };
    Ok((left, right, lambda))
}

fn map_fields(name: &str, field: &FieldRef) -> Result<(FieldRef, FieldRef, bool)> {
    map_fields_from_type(name, field.data_type(), "argument")
}

fn map_fields_from_type(
    name: &str,
    data_type: &DataType,
    position: &str,
) -> Result<(FieldRef, FieldRef, bool)> {
    let DataType::Map(entries, ordered) = data_type else {
        return plan_err!("{name} expected a map as the {position} argument, got {data_type}");
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return plan_err!(
            "{name} expected map entries struct, got {}",
            entries.data_type()
        );
    };
    let field_refs = fields.iter().collect::<Vec<_>>();
    let [key, value] = field_refs.as_slice() else {
        return plan_err!("{name} expected map entries to contain key and value fields");
    };
    Ok((Arc::clone(key), Arc::clone(value), *ordered))
}

fn same_key_type(name: &str, left: &FieldRef, right: &FieldRef) -> Result<()> {
    if left.data_type() != right.data_type() {
        return plan_err!(
            "{name} expected maps with the same key type, got {} and {}",
            left.data_type(),
            right.data_type()
        );
    }
    Ok(())
}

struct MapLambdaInputs {
    keys: ArrayRef,
    left_values: ArrayRef,
    right_values: ArrayRef,
    offsets: OffsetBuffer<i32>,
    nulls: Option<NullBuffer>,
}

fn build_lambda_inputs(
    name: &str,
    left: &MapArray,
    right: &MapArray,
    return_type: &DataType,
) -> Result<MapLambdaInputs> {
    let left_rows = left.offsets().len() - 1;
    let right_rows = right.offsets().len() - 1;
    if left_rows != right_rows {
        return exec_err!(
            "{name} expected map arguments with the same row count, got {} and {}",
            left_rows,
            right_rows
        );
    }
    let DataType::Map(entries_field, _) = return_type else {
        return exec_err!("{name} expected map return type, got {return_type}");
    };
    let DataType::Struct(return_fields) = entries_field.data_type() else {
        return exec_err!("{name} expected map return entries struct");
    };
    let key_type = return_fields[0].data_type().clone();
    let left_value_type = left.values().data_type().clone();
    let right_value_type = right.values().data_type().clone();
    let mut keys = Vec::new();
    let mut left_values = Vec::new();
    let mut right_values = Vec::new();
    let mut offsets = vec![0i32];
    let mut current = 0i32;
    let row_count = left.offsets().len() - 1;
    let mut nulls = Vec::with_capacity(row_count);

    for row in 0..row_count {
        if left.nulls().is_some_and(|nulls| nulls.is_null(row))
            || right.nulls().is_some_and(|nulls| nulls.is_null(row))
        {
            nulls.push(false);
            offsets.push(current);
            continue;
        }
        nulls.push(true);
        let left_start = left.offsets()[row] as usize;
        let left_end = left.offsets()[row + 1] as usize;
        let right_start = right.offsets()[row] as usize;
        let right_end = right.offsets()[row + 1] as usize;
        let mut seen = HashSet::new();
        let mut left_by_key = HashMap::new();
        let mut right_by_key = HashMap::new();
        let mut row_keys = Vec::new();

        for index in left_start..left_end {
            let key = ScalarValue::try_from_array(left.keys(), index)?.compacted();
            if seen.insert(key.clone()) {
                row_keys.push(key.clone());
                left_by_key.insert(key, index);
            }
        }
        for index in right_start..right_end {
            let key = ScalarValue::try_from_array(right.keys(), index)?.compacted();
            right_by_key.entry(key.clone()).or_insert(index);
            if seen.insert(key.clone()) {
                row_keys.push(key);
            }
        }

        for key in row_keys {
            let left_value = left_by_key
                .get(&key)
                .map(|index| ScalarValue::try_from_array(left.values(), *index))
                .transpose()?
                .unwrap_or(ScalarValue::try_new_null(&left_value_type)?);
            let right_value = right_by_key
                .get(&key)
                .map(|index| ScalarValue::try_from_array(right.values(), *index))
                .transpose()?
                .unwrap_or(ScalarValue::try_new_null(&right_value_type)?);
            keys.push(cast_scalar(key, &key_type)?);
            left_values.push(left_value);
            right_values.push(right_value);
            current = current.checked_add(1).ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "map_zip_with map is too large".to_owned(),
                )
            })?;
        }
        offsets.push(current);
    }

    Ok(MapLambdaInputs {
        keys: scalar_values_to_array(keys, &key_type)?,
        left_values: scalar_values_to_array(left_values, &left_value_type)?,
        right_values: scalar_values_to_array(right_values, &right_value_type)?,
        offsets: OffsetBuffer::new(offsets.into()),
        nulls: Some(NullBuffer::from(nulls)),
    })
}

fn scalar_values_to_array(values: Vec<ScalarValue>, data_type: &DataType) -> Result<ArrayRef> {
    if values.is_empty() {
        Ok(new_empty_array(data_type))
    } else {
        cast_array(ScalarValue::iter_to_array(values)?, data_type)
    }
}

fn cast_array(array: ArrayRef, data_type: &DataType) -> Result<ArrayRef> {
    if array.data_type() == data_type {
        Ok(array)
    } else {
        Ok(cast(&array, data_type)?)
    }
}

fn cast_scalar(value: ScalarValue, data_type: &DataType) -> Result<ScalarValue> {
    if value.data_type() == *data_type {
        Ok(value)
    } else {
        ScalarValue::try_from_array(
            &cast(&ScalarValue::iter_to_array(vec![value])?, data_type)?,
            0,
        )
    }
}

fn repeat_captures(arrays: &[ArrayRef], offsets: &OffsetBuffer<i32>) -> Result<Vec<ArrayRef>> {
    let indices = offsets_to_row_indices(offsets);
    arrays
        .iter()
        .map(|array| Ok(take(array, &indices, None)?))
        .collect()
}

fn offsets_to_row_indices(offsets: &OffsetBuffer<i32>) -> datafusion::arrow::array::UInt32Array {
    let total = offsets.last().copied().unwrap_or_default() as usize;
    let mut indices = Vec::with_capacity(total);
    for row in 0..offsets.len() - 1 {
        indices.extend(std::iter::repeat_n(
            row as u32,
            (offsets[row + 1] - offsets[row]) as usize,
        ));
    }
    datafusion::arrow::array::UInt32Array::from(indices)
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion_expr::HigherOrderReturnFieldArgs;

    use super::*;

    fn string_i32_map(keys: Vec<&str>, values: Vec<i32>, lengths: Vec<usize>) -> MapArray {
        let fields = Fields::from(vec![
            Field::new(SAIL_MAP_KEY_FIELD_NAME, DataType::Utf8, false),
            Field::new(SAIL_MAP_VALUE_FIELD_NAME, DataType::Int32, true),
        ]);
        let entries = StructArray::try_new(
            fields.clone(),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Int32Array::from(values)),
            ],
            None,
        )
        .unwrap();
        MapArray::try_new(
            Arc::new(Field::new(
                SAIL_MAP_FIELD_NAME,
                DataType::Struct(fields),
                false,
            )),
            OffsetBuffer::from_lengths(lengths),
            entries,
            None,
            false,
        )
        .unwrap()
    }

    fn return_type() -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                SAIL_MAP_FIELD_NAME,
                DataType::Struct(Fields::from(vec![
                    Field::new(SAIL_MAP_KEY_FIELD_NAME, DataType::Utf8, false),
                    Field::new(SAIL_MAP_VALUE_FIELD_NAME, DataType::Int32, true),
                ])),
                false,
            )),
            false,
        )
    }

    #[test]
    fn map_zip_with_does_not_claim_union_keys_are_sorted() -> Result<()> {
        let DataType::Map(entries, _) = return_type() else {
            return exec_err!("test map type is not a map");
        };
        let sorted_map = Arc::new(Field::new("map", DataType::Map(entries, true), true));
        let fields = [
            ValueOrLambda::Value(Arc::clone(&sorted_map)),
            ValueOrLambda::Value(Arc::clone(&sorted_map)),
            ValueOrLambda::Lambda(Arc::new(Field::new("", DataType::Int32, true))),
        ];
        let scalar_arguments = [None, None, None];
        let result = SparkMapZipWith::new().return_field_from_args(HigherOrderReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: &scalar_arguments,
        })?;
        let DataType::Map(_, sorted) = result.data_type() else {
            return exec_err!("map_zip_with return type is not a map");
        };
        assert!(!sorted);
        Ok(())
    }

    #[test]
    fn builds_union_inputs_with_typed_nulls() -> Result<()> {
        let left = string_i32_map(vec!["a", "b"], vec![1, 2], vec![2]);
        let right = string_i32_map(vec!["b", "c"], vec![3, 4], vec![2]);
        let inputs = build_lambda_inputs("map_zip_with", &left, &right, &return_type())?;

        assert_eq!(inputs.offsets.as_ref(), &[0, 3]);
        assert_eq!(inputs.nulls.unwrap().iter().collect::<Vec<_>>(), vec![true]);
        assert_eq!(
            inputs.keys.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec!["a", "b", "c"])
        );
        assert_eq!(
            inputs
                .left_values
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![Some(1), Some(2), None])
        );
        assert_eq!(
            inputs
                .right_values
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![None, Some(3), Some(4)])
        );
        Ok(())
    }

    #[test]
    fn preserves_left_key_order_then_appends_right_only_keys() -> Result<()> {
        let left = string_i32_map(vec!["a", "c"], vec![1, 3], vec![2]);
        let right = string_i32_map(vec!["b", "c"], vec![2, 4], vec![2]);
        let inputs = build_lambda_inputs("map_zip_with", &left, &right, &return_type())?;

        assert_eq!(
            inputs.keys.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec!["a", "c", "b"])
        );
        Ok(())
    }

    #[test]
    fn keeps_first_duplicate_key() -> Result<()> {
        let left = string_i32_map(vec!["a", "a"], vec![1, 99], vec![2]);
        let right = string_i32_map(vec!["a", "a"], vec![2, 88], vec![2]);
        let inputs = build_lambda_inputs("map_zip_with", &left, &right, &return_type())?;

        assert_eq!(inputs.offsets.as_ref(), &[0, 1]);
        assert_eq!(
            inputs.keys.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec!["a"])
        );
        assert_eq!(
            inputs
                .left_values
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![1])
        );
        assert_eq!(
            inputs
                .right_values
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![2])
        );
        Ok(())
    }
}
