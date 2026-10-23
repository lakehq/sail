use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, ListArray, MapArray};
use datafusion::arrow::compute::{cast, take_arrays};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::utils::{
    adjust_offsets_for_slice, list_values, list_values_row_number, remove_list_null_values,
};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, Expr, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};

use crate::scalar::array::lambda_utils::value_lambda_pair;
use crate::scalar::array::spark_array_filter::filter_list_values;

/// Spark's MapFilter (higherOrderFunctions.scala): retain entries whose
/// predicate is true, preserving the input map type and nullability.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMapFilter {
    signature: HigherOrderSignature,
}

impl Default for SparkMapFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMapFilter {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
        }
    }
}

impl HigherOrderUDFImpl for SparkMapFilter {
    fn name(&self) -> &str {
        "map_filter"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn short_circuits(&self) -> bool {
        // The predicate is never evaluated for null or empty maps. In
        // particular, CSE must not hoist captured expressions out of it.
        true
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        let (map, predicates) = args.split_first()?;
        Some((vec![map], predicates.iter().collect()))
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        sail_type_system::higher_order::map_parameters(self.name(), fields)
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        sail_type_system::higher_order::map_result(
            self.name(),
            args,
            sail_type_system::higher_order::MapResult::Filter,
        )
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let (map, lambda) = value_lambda_pair(self.name(), &args.args)?;
        let map_array = map.to_array(args.number_rows)?;
        let map = map_array.as_map();
        let DataType::Map(field, sorted) = map.data_type() else {
            return exec_err!("map_filter expected a map");
        };

        // Reuse list filtering and capture expansion over the map's entries.
        // DataFusion clears hidden values beneath null lists, but not null maps;
        // clear them here so the predicate never evaluates those entries.
        let entries: ArrayRef = Arc::new(ListArray::new(
            Arc::clone(field),
            map.offsets().clone(),
            Arc::new(map.entries().clone()),
            map.nulls().cloned(),
        ));
        let entries = if entries.null_count() > 0 {
            remove_list_null_values(&entries)?
        } else {
            entries
        };
        let values = list_values(&entries)?;
        if values.is_empty() {
            // Includes a batch mixing empty and null maps. No predicate runs.
            return Ok(ColumnarValue::Array(map_array));
        }

        let pairs = values.as_struct();
        let key = || Ok(Arc::clone(pairs.column(0)));
        let value = || Ok(Arc::clone(pairs.column(1)));
        // TODO: Fix shared Boolean CASE short-circuit evaluation so skipped
        // branches cannot raise errors for map entries (see the Boolean CASE
        // predicate sail-bug test).
        let predicate = lambda
            .evaluate(&[&key, &value], |arrays| {
                let indices = list_values_row_number(&entries)?;
                Ok(take_arrays(arrays, &indices, None)?)
            })?
            .into_array(values.len())?;
        let predicate = if predicate.data_type() == &DataType::Null {
            cast(&predicate, &DataType::Boolean)?
        } else {
            predicate
        };
        let Some(predicate) = predicate.as_any().downcast_ref::<BooleanArray>() else {
            return exec_err!("map_filter lambda must return boolean");
        };
        let list = entries.as_list::<i32>();
        let (values, offsets) =
            filter_list_values(&values, predicate, &adjust_offsets_for_slice(list))?;
        Ok(ColumnarValue::Array(Arc::new(MapArray::try_new(
            Arc::clone(field),
            offsets,
            values.as_struct().clone(),
            map.nulls().cloned(),
            *sorted,
        )?)))
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [map @ DataType::Map(_, _)] => Ok(vec![map.clone()]),
            _ => plan_err!("map_filter expects one map argument, got {arg_types:?}"),
        }
    }
}
