/// Spark-compatible `map_zip_with(map1, map2, (k, v1, v2) -> ...)` higher-order function.
///
/// Spark semantics: see `MapZipWith` in
/// `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/higherOrderFunctions.scala`
/// — the result is null if either map is null. Otherwise it has the keys of the first map,
/// followed by the keys only in the second map, and the lambda receives null for the value
/// of a key that is missing from one of the maps.
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, MapArray, StructArray, UInt64Array, new_empty_array,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::{take, take_arrays};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};
use indexmap::IndexMap;

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

fn map_zip_with_args<'a, V: std::fmt::Debug, L: std::fmt::Debug>(
    name: &str,
    args: &'a [ValueOrLambda<V, L>],
) -> Result<(&'a V, &'a V, &'a L)> {
    match args {
        [
            ValueOrLambda::Value(left),
            ValueOrLambda::Value(right),
            ValueOrLambda::Lambda(lambda),
        ] => Ok((left, right, lambda)),
        _ => plan_err!("{name} expects two maps followed by a lambda, got {args:?}"),
    }
}

/// Returns the key and value fields of a map type.
fn entry_fields(name: &str, data_type: &DataType) -> Result<(FieldRef, FieldRef)> {
    let DataType::Map(entries, _) = data_type else {
        return plan_err!("{name} expected a map, got {data_type}");
    };
    match entries.data_type() {
        DataType::Struct(fields) if fields.len() == 2 => {
            Ok((Arc::clone(&fields[0]), Arc::clone(&fields[1])))
        }
        other => plan_err!("{name} expected map entries with a key and a value, got {other}"),
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
        let (left, right, _) = map_zip_with_args(self.name(), fields)?;
        let (key, left_value) = entry_fields(self.name(), left.data_type())?;
        let (_, right_value) = entry_fields(self.name(), right.data_type())?;
        Ok(LambdaParametersProgress::Complete(vec![vec![
            Arc::new(Field::new("", key.data_type().clone(), false)),
            Arc::new(Field::new("", left_value.data_type().clone(), true)),
            Arc::new(Field::new("", right_value.data_type().clone(), true)),
        ]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let (left, right, lambda) = map_zip_with_args(self.name(), args.arg_fields)?;
        let (key, _) = entry_fields(self.name(), left.data_type())?;
        let DataType::Map(entries, _) = left.data_type() else {
            return plan_err!("{} expected a map, got {}", self.name(), left.data_type());
        };
        let fields = Fields::from(vec![
            key.as_ref().clone(),
            Field::new("value", lambda.data_type().clone(), lambda.is_nullable()),
        ]);
        let entries = Field::new(entries.name(), DataType::Struct(fields), false);
        Ok(Arc::new(Field::new(
            "",
            DataType::Map(Arc::new(entries), false),
            left.is_nullable() || right.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let (left, right, lambda) = map_zip_with_args(self.name(), &args.args)?;
        let left = left.to_array(args.number_rows)?;
        let right = right.to_array(args.number_rows)?;
        let (Some(left), Some(right)) = (left.as_map_opt(), right.as_map_opt()) else {
            return exec_err!("{} expected two maps", self.name());
        };
        let DataType::Map(entries, _) = args.return_field.data_type() else {
            return exec_err!("{} expected a map return type", self.name());
        };
        let DataType::Struct(entry_fields) = entries.data_type() else {
            return exec_err!("{} expected map entries", self.name());
        };

        let mut keys = vec![];
        let mut left_indices = vec![];
        let mut right_indices = vec![];
        let mut rows = vec![];
        let mut lengths = Vec::with_capacity(args.number_rows);
        let mut valid = Vec::with_capacity(args.number_rows);
        let (left_offsets, right_offsets) = (left.value_offsets(), right.value_offsets());
        for row in 0..args.number_rows {
            if left.is_null(row) || right.is_null(row) {
                lengths.push(0);
                valid.push(false);
                continue;
            }
            // The keys of the first map come first, in order, as in Spark's `LinkedHashMap`.
            let mut entries = IndexMap::<ScalarValue, (Option<u64>, Option<u64>)>::new();
            for i in left_offsets[row] as usize..left_offsets[row + 1] as usize {
                let key = ScalarValue::try_from_array(left.keys(), i)?;
                entries.entry(key).or_default().0 = Some(i as u64);
            }
            for i in right_offsets[row] as usize..right_offsets[row + 1] as usize {
                let key = ScalarValue::try_from_array(right.keys(), i)?;
                entries.entry(key).or_default().1 = Some(i as u64);
            }
            lengths.push(entries.len());
            valid.push(true);
            for (key, (left_index, right_index)) in entries {
                keys.push(key);
                left_indices.push(left_index);
                right_indices.push(right_index);
                rows.push(row as u64);
            }
        }

        let keys = if keys.is_empty() {
            new_empty_array(entry_fields[0].data_type())
        } else {
            ScalarValue::iter_to_array(keys)?
        };
        let left_values = take(left.values(), &UInt64Array::from(left_indices), None)?;
        let right_values = take(right.values(), &UInt64Array::from(right_indices), None)?;
        let rows = UInt64Array::from(rows);
        let key_param = || Ok(Arc::clone(&keys));
        let left_param = || Ok(Arc::clone(&left_values));
        let right_param = || Ok(Arc::clone(&right_values));
        let values = lambda
            .evaluate(&[&key_param, &left_param, &right_param], |arrays| {
                // Repeat the captured columns of each row for each entry of the row.
                Ok(take_arrays(arrays, &rows, None)?)
            })?
            .into_array(rows.len())?;

        let entries_array = StructArray::try_new(entry_fields.clone(), vec![keys, values], None)?;
        Ok(ColumnarValue::Array(Arc::new(MapArray::try_new(
            Arc::clone(entries),
            OffsetBuffer::from_lengths(lengths),
            entries_array,
            Some(NullBuffer::from(valid)),
            false,
        )?) as ArrayRef))
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [left, right] = arg_types else {
            return plan_err!(
                "{} expects two map arguments, got {}",
                self.name(),
                arg_types.len()
            );
        };
        let (left_key, _) = entry_fields(self.name(), left)?;
        let (right_key, _) = entry_fields(self.name(), right)?;
        if left_key.data_type() != right_key.data_type() {
            return plan_err!(
                "{} expects maps with the same key type, got {} and {}",
                self.name(),
                left_key.data_type(),
                right_key.data_type()
            );
        }
        Ok(vec![left.clone(), right.clone()])
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::array::{
        Int32Array, Int32Builder, MapBuilder, RecordBatch, StringBuilder,
    };
    use datafusion_common::DFSchema;
    use datafusion_expr::execution_props::ExecutionProps;
    use datafusion_expr::expr::{HigherOrderFunction, LambdaVariable};
    use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
    use datafusion_expr::{Expr, HigherOrderUDF, col, lambda};
    use datafusion_physical_expr::create_physical_expr;

    use super::*;

    fn string_int_map(rows: Vec<Option<Vec<(&str, i32)>>>) -> Result<MapArray> {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        for row in rows {
            match row {
                Some(entries) => {
                    for (key, value) in entries {
                        builder.keys().append_value(key);
                        builder.values().append_value(value);
                    }
                    builder.append(true)?;
                }
                None => builder.append(false)?,
            }
        }
        Ok(builder.finish())
    }

    fn value(name: &str) -> Expr {
        Expr::LambdaVariable(LambdaVariable::new(
            name.to_string(),
            Some(Arc::new(Field::new(name, DataType::Int32, true))),
        ))
    }

    #[test]
    fn map_zip_with_merges_the_keys_of_both_maps() -> Result<()> {
        let left = string_int_map(vec![Some(vec![("a", 1), ("b", 2)]), None])?;
        let right = string_int_map(vec![Some(vec![("c", 30), ("a", 10)]), Some(vec![])])?;
        let schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("left", left.data_type().clone(), true),
                Field::new("right", right.data_type().clone(), true),
            ]
            .into(),
            HashMap::new(),
        )?;
        let func = Arc::new(HigherOrderUDF::new_from_impl(SparkMapZipWith::new()));
        let actual = create_physical_expr(
            &Expr::HigherOrderFunction(HigherOrderFunction::new(
                func,
                vec![
                    col("left"),
                    col("right"),
                    lambda(["k", "v1", "v2"], value("v2")),
                ],
            )),
            &schema,
            &ExecutionProps::new(),
            &PhysicalPlanningContext::default(),
        )?
        .evaluate(&RecordBatch::try_new(
            Arc::clone(schema.inner()),
            vec![Arc::new(left), Arc::new(right)],
        )?)?
        .into_array(2)?;
        let actual = actual.as_map();
        assert!(actual.is_null(1));
        let keys = actual.keys().as_string::<i32>().iter().collect::<Vec<_>>();
        assert_eq!(keys, vec![Some("a"), Some("b"), Some("c")]);
        let values = actual.values().as_any().downcast_ref::<Int32Array>();
        assert_eq!(
            values.map(|v| v.iter().collect::<Vec<_>>()),
            Some(vec![Some(10), None, Some(30)])
        );
        Ok(())
    }
}
