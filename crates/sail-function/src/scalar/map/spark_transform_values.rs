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

/// Spark's `TransformValues` (higherOrderFunctions.scala): rewrites every value
/// through the lambda, keeping each key and the map's entry order.
///
/// Unlike [`super::spark_map_filter::SparkMapFilter`], the result type differs
/// from the input type: the value type becomes the lambda's return type, so an
/// input whose lambda never runs still has to be rebuilt rather than returned
/// as is. Keys are untouched, so no duplicate-key resolution is needed.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTransformValues {
    signature: HigherOrderSignature,
}

impl Default for SparkTransformValues {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTransformValues {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
        }
    }
}

impl HigherOrderUDFImpl for SparkTransformValues {
    fn name(&self) -> &str {
        "transform_values"
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
            return plan_err!("transform_values expected a map, got {}", map.data_type());
        };
        let (key, value) = entry_fields(self.name(), entries)?;
        Ok(LambdaParametersProgress::Complete(vec![vec![key, value]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let (map, lambda) = value_lambda_pair(self.name(), args.arg_fields)?;
        let DataType::Map(entries, sorted) = map.data_type() else {
            return plan_err!("transform_values expected a map, got {}", map.data_type());
        };
        let (key, value) = entry_fields(self.name(), entries)?;
        // Only the value type changes: the lambda's return type and nullability
        // replace the input's, while the key field and the entries struct keep
        // their names so the result stays a well-formed Arrow map.
        let value = Arc::new(Field::new(
            value.name(),
            lambda.data_type().clone(),
            lambda.is_nullable(),
        ));
        let entries = Arc::new(Field::new(
            entries.name(),
            DataType::Struct(Fields::from(vec![key, value])),
            entries.is_nullable(),
        ));
        Ok(Arc::new(Field::new(
            "",
            DataType::Map(entries, *sorted),
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
        let DataType::Map(input_entries, sorted) = map.data_type() else {
            return exec_err!("transform_values expected a map");
        };
        let DataType::Map(entries, _) = args.return_field.data_type() else {
            return exec_err!(
                "transform_values expected return_field to be a map, got {}",
                args.return_field
            );
        };
        let (_, result_value) = entry_fields(self.name(), entries)?;

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
        let pairs = values.as_struct();

        let transformed = if values.is_empty() {
            // Includes a batch mixing empty and null maps. The lambda never
            // runs, but the result still carries the lambda's value type.
            new_empty_array(result_value.data_type())
        } else {
            let key = || Ok(Arc::clone(pairs.column(0)));
            let value = || Ok(Arc::clone(pairs.column(1)));
            lambda
                .evaluate(&[&key, &value], |arrays| {
                    let indices = list_values_row_number(&list)?;
                    Ok(take_arrays(arrays, &indices, None)?)
                })?
                .into_array(values.len())?
        };

        let keys = if values.is_empty() {
            new_empty_array(pairs.column(0).data_type())
        } else {
            Arc::clone(pairs.column(0))
        };
        let entry_values = StructArray::try_new(
            match entries.data_type() {
                DataType::Struct(fields) => fields.clone(),
                other => return exec_err!("transform_values expected entry fields, got {other}"),
            },
            vec![keys, transformed],
            None,
        )?;
        Ok(ColumnarValue::Array(Arc::new(MapArray::try_new(
            Arc::clone(entries),
            adjust_offsets_for_slice(list.as_list::<i32>()),
            entry_values,
            map.nulls().cloned(),
            *sorted,
        )?)))
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [map @ DataType::Map(_, _)] => Ok(vec![map.clone()]),
            _ => plan_err!("transform_values expects one map argument, got {arg_types:?}"),
        }
    }
}
