/// Spark-compatible `zip_with(left, right, (x, y) -> ...)` higher-order function.
///
/// Spark semantics: see `ZipWith` in
/// `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/higherOrderFunctions.scala`
/// — the result is null if either array is null. Otherwise the shorter array is padded
/// with nulls to the length of the longer one, and the lambda is applied to each pair of
/// elements, so both lambda parameters are nullable.
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, ListArray, UInt64Array};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::{take, take_arrays};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
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

fn zip_with_args<'a, V: std::fmt::Debug, L: std::fmt::Debug>(
    name: &str,
    args: &'a [ValueOrLambda<V, L>],
) -> Result<(&'a V, &'a V, &'a L)> {
    match args {
        [
            ValueOrLambda::Value(left),
            ValueOrLambda::Value(right),
            ValueOrLambda::Lambda(lambda),
        ] => Ok((left, right, lambda)),
        _ => plan_err!("{name} expects two arrays followed by a lambda, got {args:?}"),
    }
}

fn element_type<'a>(name: &str, data_type: &'a DataType) -> Result<&'a DataType> {
    match data_type {
        DataType::List(field) => Ok(field.data_type()),
        other => plan_err!("{name} expected an array, got {other}"),
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
        let (left, right, _) = zip_with_args(self.name(), fields)?;
        let left = element_type(self.name(), left.data_type())?;
        let right = element_type(self.name(), right.data_type())?;
        Ok(LambdaParametersProgress::Complete(vec![vec![
            Arc::new(Field::new("", left.clone(), true)),
            Arc::new(Field::new("", right.clone(), true)),
        ]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let (left, right, lambda) = zip_with_args(self.name(), args.arg_fields)?;
        let field = Arc::new(Field::new(
            Field::LIST_FIELD_DEFAULT_NAME,
            lambda.data_type().clone(),
            lambda.is_nullable(),
        ));
        Ok(Arc::new(Field::new(
            "",
            DataType::List(field),
            left.is_nullable() || right.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let (left, right, lambda) = zip_with_args(self.name(), &args.args)?;
        let left = left.to_array(args.number_rows)?;
        let right = right.to_array(args.number_rows)?;
        let (Some(left), Some(right)) = (left.as_list_opt::<i32>(), right.as_list_opt::<i32>())
        else {
            return exec_err!("{} expected two arrays", self.name());
        };
        let DataType::List(field) = args.return_field.data_type() else {
            return exec_err!("{} expected an array return type", self.name());
        };

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
            let left_start = left_offsets[row] as usize;
            let left_len = left_offsets[row + 1] as usize - left_start;
            let right_start = right_offsets[row] as usize;
            let right_len = right_offsets[row + 1] as usize - right_start;
            let len = left_len.max(right_len);
            for i in 0..len {
                left_indices.push((i < left_len).then(|| (left_start + i) as u64));
                right_indices.push((i < right_len).then(|| (right_start + i) as u64));
                rows.push(row as u64);
            }
            lengths.push(len);
            valid.push(true);
        }

        // The value offsets are positions in the values of the arrays, even for sliced arrays.
        let left_values = take(left.values(), &UInt64Array::from(left_indices), None)?;
        let right_values = take(right.values(), &UInt64Array::from(right_indices), None)?;
        let rows = UInt64Array::from(rows);
        let left_param = || Ok(Arc::clone(&left_values));
        let right_param = || Ok(Arc::clone(&right_values));
        let values = lambda
            .evaluate(&[&left_param, &right_param], |arrays| {
                // Repeat the captured columns of each row for each element of the row.
                Ok(take_arrays(arrays, &rows, None)?)
            })?
            .into_array(rows.len())?;

        Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
            Arc::clone(field),
            OffsetBuffer::from_lengths(lengths),
            values,
            Some(NullBuffer::from(valid)),
        )?) as ArrayRef))
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [left, right] = arg_types else {
            return plan_err!(
                "{} expects two array arguments, got {}",
                self.name(),
                arg_types.len()
            );
        };
        [left, right]
            .into_iter()
            .map(|data_type| match data_type {
                DataType::List(_) => Ok(data_type.clone()),
                DataType::LargeList(field)
                | DataType::ListView(field)
                | DataType::LargeListView(field)
                | DataType::FixedSizeList(field, _) => Ok(DataType::List(Arc::clone(field))),
                other => plan_err!("{} expected an array argument, got {other}", self.name()),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion_common::DFSchema;
    use datafusion_expr::execution_props::ExecutionProps;
    use datafusion_expr::expr::{HigherOrderFunction, LambdaVariable};
    use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
    use datafusion_expr::{Expr, HigherOrderUDF, col, lambda};
    use datafusion_physical_expr::create_physical_expr;

    use super::*;

    fn int_list(rows: Vec<Option<Vec<Option<i32>>>>) -> ListArray {
        ListArray::from_iter_primitive::<datafusion::arrow::datatypes::Int32Type, _, _>(rows)
    }

    fn variable(name: &str) -> Expr {
        Expr::LambdaVariable(LambdaVariable::new(
            name.to_string(),
            Some(Arc::new(Field::new(name, DataType::Int32, true))),
        ))
    }

    fn zip_with(left: ListArray, right: ListArray, body: Expr) -> Result<ArrayRef> {
        let schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("left", left.data_type().clone(), true),
                Field::new("right", right.data_type().clone(), true),
            ]
            .into(),
            HashMap::new(),
        )?;
        let func = Arc::new(HigherOrderUDF::new_from_impl(SparkArrayZipWith::new()));
        let rows = left.len();
        create_physical_expr(
            &Expr::HigherOrderFunction(HigherOrderFunction::new(
                func,
                vec![col("left"), col("right"), lambda(["x", "y"], body)],
            )),
            &schema,
            &ExecutionProps::new(),
            &PhysicalPlanningContext::default(),
        )?
        .evaluate(&RecordBatch::try_new(
            Arc::clone(schema.inner()),
            vec![Arc::new(left), Arc::new(right)],
        )?)?
        .into_array(rows)
    }

    #[test]
    fn zip_with_pads_the_shorter_array_with_nulls() -> Result<()> {
        let left = int_list(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
            Some(vec![]),
        ]);
        let right = int_list(vec![
            Some(vec![Some(10)]),
            Some(vec![Some(1)]),
            Some(vec![]),
        ]);
        let actual = zip_with(left, right, variable("x") + variable("y"))?;
        let actual = actual.as_list::<i32>();
        let expected = int_list(vec![Some(vec![Some(11), None, None]), None, Some(vec![])]);
        assert_eq!(
            actual.iter().collect::<Vec<_>>(),
            expected.iter().collect::<Vec<_>>()
        );
        Ok(())
    }

    #[test]
    fn zip_with_lambda_using_only_the_second_parameter() -> Result<()> {
        let left = int_list(vec![Some(vec![Some(1), Some(2)])]);
        let right = int_list(vec![Some(vec![Some(7), Some(8), Some(9)])]);
        let actual = zip_with(left, right, variable("y"))?;
        let values = actual.as_list::<i32>().value(0);
        let values = values.as_any().downcast_ref::<Int32Array>();
        assert_eq!(
            values.map(|v| v.iter().collect::<Vec<_>>()),
            Some(vec![Some(7), Some(8), Some(9)])
        );
        Ok(())
    }
}
