use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, ListArray, MapArray, StructArray, new_empty_array,
};
use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion_common::utils::{
    adjust_offsets_for_slice, list_values, list_values_row_number, remove_list_null_values,
};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, Expr, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};

use crate::scalar::array::lambda_utils::value_lambda_pair;
use crate::scalar::map::utils::map_deduplicate_keys;

/// Splits a map's entries struct into its key and value fields.
fn entry_fields(name: &str, entries: &FieldRef) -> Result<(FieldRef, FieldRef)> {
    let DataType::Struct(fields) = entries.data_type() else {
        return plan_err!("{name} expected key and value fields");
    };
    match fields.iter().collect::<Vec<_>>().as_slice() {
        [key, value] => Ok((Arc::clone(key), Arc::clone(value))),
        _ => plan_err!("{name} expected key and value fields"),
    }
}

/// Spark's `TransformKeys` (higherOrderFunctions.scala): rewrites every key
/// through the lambda, keeping each value and the map's entry order.
///
/// Two Spark behaviours come with rewriting keys, neither of which
/// [`super::spark_transform_values::SparkTransformValues`] has to deal with:
///
/// 1. A null key is rejected, because a map key cannot be null.
/// 2. Keys that collide after the rewrite follow `spark.sql.mapKeyDedupPolicy`:
///    `EXCEPTION` (the default) fails, `LAST_WIN` keeps the last value for the
///    key. The planner resolves the policy and stores it as `last_value_wins`,
///    the same way `SparkMapFromArrays` carries it.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTransformKeys {
    signature: HigherOrderSignature,
    last_value_wins: bool,
}

impl Default for SparkTransformKeys {
    fn default() -> Self {
        Self::new(false)
    }
}

impl SparkTransformKeys {
    pub fn new(last_value_wins: bool) -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
            last_value_wins,
        }
    }

    /// Whether duplicate keys keep the last value instead of failing. Used by
    /// the serialization codec to reconstruct the correct variant.
    pub fn is_last_value_wins(&self) -> bool {
        self.last_value_wins
    }
}

impl HigherOrderUDFImpl for SparkTransformKeys {
    fn name(&self) -> &str {
        "transform_keys"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn short_circuits(&self) -> bool {
        // The lambda is never evaluated for null or empty maps. In particular,
        // CSE must not hoist captured expressions out of it.
        true
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        let (map, lambdas) = args.split_first()?;
        Some((vec![map], lambdas.iter().collect()))
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let (map, _) = value_lambda_pair(self.name(), fields)?;
        let DataType::Map(entries, _) = map.data_type() else {
            return plan_err!("transform_keys expected a map, got {}", map.data_type());
        };
        let (key, value) = entry_fields(self.name(), entries)?;
        Ok(LambdaParametersProgress::Complete(vec![vec![key, value]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let (map, lambda) = value_lambda_pair(self.name(), args.arg_fields)?;
        let DataType::Map(entries, _) = map.data_type() else {
            return plan_err!("transform_keys expected a map, got {}", map.data_type());
        };
        let (key, value) = entry_fields(self.name(), entries)?;
        // Only the key type changes. A map key is never nullable: a null key
        // raises an error at evaluation time rather than widening the field.
        let key = Arc::new(Field::new(key.name(), lambda.data_type().clone(), false));
        let entries = Arc::new(Field::new(
            entries.name(),
            DataType::Struct(Fields::from(vec![key, value])),
            entries.is_nullable(),
        ));
        Ok(Arc::new(Field::new(
            "",
            // Rewritten keys are in no particular order, so the result can never
            // claim sorted keys even when the input map does.
            DataType::Map(entries, false),
            map.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let (map, lambda) = value_lambda_pair(self.name(), &args.args)?;
        let map_array = map.to_array(args.number_rows)?;
        if map_array.null_count() == map_array.len() {
            return Ok(ColumnarValue::Scalar(ScalarValue::try_new_null(
                args.return_type(),
            )?));
        }
        let map = map_array.as_map();
        let DataType::Map(input_entries, _) = map.data_type() else {
            return exec_err!("transform_keys expected a map");
        };
        let DataType::Map(entries, _) = args.return_field.data_type() else {
            return exec_err!(
                "transform_keys expected return_field to be a map, got {}",
                args.return_field
            );
        };
        let (result_key, _) = entry_fields(self.name(), entries)?;
        let entry_fields = match entries.data_type() {
            DataType::Struct(fields) => fields.clone(),
            other => return exec_err!("transform_keys expected entry fields, got {other}"),
        };

        // Reuse list flattening and capture expansion over the map's entries.
        // DataFusion clears hidden values beneath null lists, but not null maps;
        // clear them here so the lambda never evaluates those entries.
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::clone(input_entries),
            map.offsets().clone(),
            Arc::new(map.entries().clone()),
            map.nulls().cloned(),
        ));
        let list = if list.null_count() > 0 {
            remove_list_null_values(&list)?
        } else {
            list
        };
        let values = list_values(&list)?;
        let offsets = adjust_offsets_for_slice(list.as_list::<i32>());

        if values.is_empty() {
            // Includes a batch mixing empty and null maps. The lambda never
            // runs, but the result still carries the lambda's key type.
            let entry_values = StructArray::try_new(
                entry_fields,
                vec![
                    new_empty_array(result_key.data_type()),
                    new_empty_array(values.as_struct().column(1).data_type()),
                ],
                None,
            )?;
            return Ok(ColumnarValue::Array(Arc::new(MapArray::try_new(
                Arc::clone(entries),
                offsets,
                entry_values,
                map.nulls().cloned(),
                false,
            )?)));
        }

        let pairs = values.as_struct();
        let key = || Ok(Arc::clone(pairs.column(0)));
        let value = || Ok(Arc::clone(pairs.column(1)));
        let keys = lambda
            .evaluate(&[&key, &value], |arrays| {
                let indices = list_values_row_number(&list)?;
                Ok(take_arrays(arrays, &indices, None)?)
            })?
            .into_array(values.len())?;
        if keys.null_count() > 0 {
            return exec_err!("[NULL_MAP_KEY] Cannot use null as map key.");
        }
        let entry_values = Arc::clone(pairs.column(1));

        // Rewritten keys can collide even though the input keys were distinct.
        let nulls = map.nulls();
        let (keys, entry_values, offsets) = map_deduplicate_keys(
            &keys,
            &entry_values,
            &offsets,
            &offsets,
            nulls,
            nulls,
            self.last_value_wins,
        )?;
        let entry_values = StructArray::try_new(entry_fields, vec![keys, entry_values], None)?;
        Ok(ColumnarValue::Array(Arc::new(MapArray::try_new(
            Arc::clone(entries),
            offsets,
            entry_values,
            map.nulls().cloned(),
            false,
        )?)))
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [map @ DataType::Map(_, _)] => Ok(vec![map.clone()]),
            _ => plan_err!("transform_keys expects one map argument, got {arg_types:?}"),
        }
    }
}
