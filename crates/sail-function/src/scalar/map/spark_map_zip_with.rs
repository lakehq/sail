/// Spark-compatible `map_zip_with(map1, map2, lambda)` higher-order function.
///
/// Spark semantics: the lambda receives `(key, left_value, right_value)` for the
/// union of keys in both maps. Missing side values are passed as NULL. If an
/// input map contains duplicated keys, only the first occurrence is passed to the
/// lambda.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, MapArray, StructArray};
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

type LambdaInputs = (
    ArrayRef,
    ArrayRef,
    ArrayRef,
    OffsetBuffer<i32>,
    Option<NullBuffer>,
);

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
        let (right_key, right_value, _) = map_fields(self.name(), right)?;
        if left_key.data_type() != right_key.data_type() {
            return plan_err!(
                "{} expected maps with the same key type, got {} and {}",
                self.name(),
                left_key.data_type(),
                right_key.data_type()
            );
        }
        Ok(LambdaParametersProgress::Complete(vec![vec![
            Arc::new(Field::new("", left_key.data_type().clone(), false)),
            Arc::new(Field::new("", left_value.data_type().clone(), true)),
            Arc::new(Field::new("", right_value.data_type().clone(), true)),
        ]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let (left, right, lambda) = map_zip_args(self.name(), args.arg_fields)?;
        let (left_key, _left_value, ordered) = map_fields(self.name(), left)?;
        let (right_key, _right_value, _) = map_fields(self.name(), right)?;
        if left_key.data_type() != right_key.data_type() {
            return plan_err!(
                "{} expected maps with the same key type, got {} and {}",
                self.name(),
                left_key.data_type(),
                right_key.data_type()
            );
        }

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
                ordered,
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
        let (keys, left_values, right_values, offsets, nulls) = build_lambda_inputs(
            self.name(),
            left_map,
            right_map,
            args.return_field.data_type(),
        )?;
        let num_entries = keys.len();

        let key_param = || Ok(Arc::clone(&keys));
        let left_param = || Ok(Arc::clone(&left_values));
        let right_param = || Ok(Arc::clone(&right_values));
        let params: [&dyn Fn() -> Result<ArrayRef>; 3] = [&key_param, &left_param, &right_param];
        let output_values = lambda
            .evaluate(&params, |arrays| repeat_captures(arrays, &offsets))?
            .into_array(num_entries)?;

        let DataType::Map(entries_field, ordered) = args.return_field.data_type() else {
            return exec_err!(
                "{} expected return_field to be a map, got {}",
                self.name(),
                args.return_field
            );
        };
        let DataType::Struct(fields) = entries_field.data_type() else {
            return exec_err!(
                "{} expected map entries to be a struct, got {}",
                self.name(),
                entries_field.data_type()
            );
        };
        let value_array = if output_values.data_type() == fields[1].data_type() {
            output_values
        } else {
            cast(&output_values, fields[1].data_type())?
        };
        let entries = StructArray::try_new(fields.clone(), vec![keys, value_array], None)?;
        Ok(ColumnarValue::Array(Arc::new(MapArray::try_new(
            Arc::clone(entries_field),
            offsets,
            entries,
            nulls,
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
        let DataType::Map(left_entries, _) = left else {
            return plan_err!(
                "{} expected a map as first argument, got {left}",
                self.name()
            );
        };
        let DataType::Map(right_entries, _) = right else {
            return plan_err!(
                "{} expected a map as second argument, got {right}",
                self.name()
            );
        };
        let (left_key, _, _) = map_fields_from_entries(self.name(), left_entries)?;
        let (right_key, _, _) = map_fields_from_entries(self.name(), right_entries)?;
        if left_key.data_type() != right_key.data_type() {
            return plan_err!(
                "{} expected maps with the same key type, got {} and {}",
                self.name(),
                left_key.data_type(),
                right_key.data_type()
            );
        }
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
    let DataType::Map(entries, ordered) = field.data_type() else {
        return plan_err!("{name} expected map argument, got {}", field.data_type());
    };
    let (key, value, _) = map_fields_from_entries(name, entries)?;
    Ok((key, value, *ordered))
}

fn map_fields_from_entries(name: &str, entries: &FieldRef) -> Result<(FieldRef, FieldRef, Fields)> {
    let DataType::Struct(fields) = entries.data_type() else {
        return plan_err!(
            "{name} expected map entries struct, got {}",
            entries.data_type()
        );
    };
    let fields_vec = fields.iter().cloned().collect::<Vec<_>>();
    let [key, value] = fields_vec.as_slice() else {
        return plan_err!("{name} expected map entries to contain key and value fields");
    };
    Ok((Arc::clone(key), Arc::clone(value), fields.clone()))
}

fn build_lambda_inputs(
    name: &str,
    left: &MapArray,
    right: &MapArray,
    return_type: &DataType,
) -> Result<LambdaInputs> {
    if left.len() != right.len() {
        return exec_err!(
            "{name} expected map arguments with the same row count, got {} and {}",
            left.len(),
            right.len()
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

    let mut key_values = Vec::new();
    let mut left_values = Vec::new();
    let mut right_values = Vec::new();
    let mut offsets = Vec::with_capacity(left.len() + 1);
    let mut current_offset = 0i32;
    offsets.push(current_offset);
    let mut nulls = Vec::with_capacity(left.len());

    for row in 0..left.len() {
        let row_is_null = left.is_null(row) || right.is_null(row);
        nulls.push(!row_is_null);
        if row_is_null {
            offsets.push(current_offset);
            continue;
        }

        let left_start = left.offsets()[row] as usize;
        let left_end = left.offsets()[row + 1] as usize;
        let right_start = right.offsets()[row] as usize;
        let right_end = right.offsets()[row + 1] as usize;
        let mut keys_seen = HashSet::new();
        let mut left_by_key = HashMap::new();
        let mut right_by_key = HashMap::new();
        let mut row_keys = Vec::new();

        for idx in left_start..left_end {
            let key = ScalarValue::try_from_array(left.keys(), idx)?.compacted();
            if keys_seen.insert(key.clone()) {
                row_keys.push(key.clone());
                left_by_key.insert(key, idx);
            }
        }
        for idx in right_start..right_end {
            let key = ScalarValue::try_from_array(right.keys(), idx)?.compacted();
            right_by_key.entry(key.clone()).or_insert(idx);
            if keys_seen.insert(key.clone()) {
                row_keys.push(key);
            }
        }

        for key in row_keys {
            let left_value = match left_by_key.get(&key) {
                Some(idx) => ScalarValue::try_from_array(left.values(), *idx)?,
                None => ScalarValue::try_new_null(&left_value_type)?,
            };
            let right_value = match right_by_key.get(&key) {
                Some(idx) => ScalarValue::try_from_array(right.values(), *idx)?,
                None => ScalarValue::try_new_null(&right_value_type)?,
            };
            key_values.push(cast_scalar(key, &key_type)?);
            left_values.push(left_value);
            right_values.push(right_value);
            current_offset += 1;
        }
        offsets.push(current_offset);
    }

    Ok((
        cast_array(ScalarValue::iter_to_array(key_values)?, &key_type)?,
        cast_array(ScalarValue::iter_to_array(left_values)?, &left_value_type)?,
        cast_array(ScalarValue::iter_to_array(right_values)?, &right_value_type)?,
        OffsetBuffer::new(offsets.into()),
        Some(NullBuffer::from(nulls)),
    ))
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
        Ok(ScalarValue::try_from_array(
            &cast(&ScalarValue::iter_to_array(vec![value])?, data_type)?,
            0,
        )?)
    }
}

fn repeat_captures(arrays: &[ArrayRef], offsets: &OffsetBuffer<i32>) -> Result<Vec<ArrayRef>> {
    let indices = offsets_to_row_indices(offsets)?;
    arrays
        .iter()
        .map(|array| Ok(take(array, &indices, None)?))
        .collect()
}

fn offsets_to_row_indices(
    offsets: &OffsetBuffer<i32>,
) -> Result<datafusion::arrow::array::UInt32Array> {
    let total = offsets.last().copied().unwrap_or(0) as usize;
    let mut indices = Vec::with_capacity(total);
    for row in 0..offsets.len() - 1 {
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        indices.extend(std::iter::repeat_n(row as u32, end - start));
    }
    Ok(datafusion::arrow::array::UInt32Array::from(indices))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::Fields;

    use super::*;

    fn string_i32_map(keys: Vec<&str>, values: Vec<i32>, lengths: Vec<usize>) -> Result<MapArray> {
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
        )?;
        Ok(MapArray::try_new(
            Arc::new(Field::new(
                SAIL_MAP_FIELD_NAME,
                DataType::Struct(fields),
                false,
            )),
            OffsetBuffer::from_lengths(lengths),
            entries,
            None,
            false,
        )?)
    }

    #[test]
    fn map_zip_with_builds_union_inputs() -> Result<()> {
        let left = string_i32_map(vec!["a", "b"], vec![1, 2], vec![2])?;
        let right = string_i32_map(vec!["b", "c"], vec![3, 4], vec![2])?;
        let return_type = DataType::Map(
            Arc::new(Field::new(
                SAIL_MAP_FIELD_NAME,
                DataType::Struct(Fields::from(vec![
                    Field::new(SAIL_MAP_KEY_FIELD_NAME, DataType::Utf8, false),
                    Field::new(SAIL_MAP_VALUE_FIELD_NAME, DataType::Int32, true),
                ])),
                false,
            )),
            false,
        );

        let (keys, left_values, right_values, offsets, nulls) =
            build_lambda_inputs("map_zip_with", &left, &right, &return_type)?;

        assert_eq!(offsets.as_ref(), &[0, 3]);
        let Some(nulls) = nulls else {
            return exec_err!("map_zip_with inputs must retain the row null buffer");
        };
        let Some(keys) = keys.as_any().downcast_ref::<StringArray>() else {
            return exec_err!("map_zip_with keys must be strings");
        };
        let Some(left_values) = left_values.as_any().downcast_ref::<Int32Array>() else {
            return exec_err!("map_zip_with left values must be Int32");
        };
        let Some(right_values) = right_values.as_any().downcast_ref::<Int32Array>() else {
            return exec_err!("map_zip_with right values must be Int32");
        };
        assert_eq!(nulls.iter().collect::<Vec<_>>(), vec![true]);
        assert_eq!(keys, &StringArray::from(vec!["a", "b", "c"]));
        assert_eq!(left_values, &Int32Array::from(vec![Some(1), Some(2), None]));
        assert_eq!(
            right_values,
            &Int32Array::from(vec![None, Some(3), Some(4)])
        );
        Ok(())
    }

    #[test]
    fn map_zip_with_uses_first_duplicate_key() -> Result<()> {
        let left = string_i32_map(vec!["a", "a"], vec![1, 99], vec![2])?;
        let right = string_i32_map(vec!["a", "a"], vec![2, 88], vec![2])?;
        let return_type = DataType::Map(
            Arc::new(Field::new(
                SAIL_MAP_FIELD_NAME,
                DataType::Struct(Fields::from(vec![
                    Field::new(SAIL_MAP_KEY_FIELD_NAME, DataType::Utf8, false),
                    Field::new(SAIL_MAP_VALUE_FIELD_NAME, DataType::Int32, true),
                ])),
                false,
            )),
            false,
        );

        let (keys, left_values, right_values, offsets, _nulls) =
            build_lambda_inputs("map_zip_with", &left, &right, &return_type)?;

        let Some(keys) = keys.as_any().downcast_ref::<StringArray>() else {
            return exec_err!("map_zip_with keys must be strings");
        };
        let Some(left_values) = left_values.as_any().downcast_ref::<Int32Array>() else {
            return exec_err!("map_zip_with left values must be Int32");
        };
        let Some(right_values) = right_values.as_any().downcast_ref::<Int32Array>() else {
            return exec_err!("map_zip_with right values must be Int32");
        };

        assert_eq!(offsets.as_ref(), &[0, 1]);
        assert_eq!(keys, &StringArray::from(vec!["a"]));
        assert_eq!(left_values, &Int32Array::from(vec![1]));
        assert_eq!(right_values, &Int32Array::from(vec![2]));
        Ok(())
    }
}
