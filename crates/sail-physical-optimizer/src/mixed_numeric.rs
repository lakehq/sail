use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::ScalarUDF;
use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::filter::{FilterExec, FilterExecBuilder};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use sail_function::scalar::mixed_numeric::MixedNumeric;

use crate::correlation_cast::same_properties;

/// Specialize execution after aggregate combination and native predicate pushdown.
pub(crate) fn specialize(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let config = Arc::new(config.clone());
    plan.transform_up(|plan| rewrite_plan(plan, &config))
        .map(|result| result.data)
}

fn rewrite_plan(
    plan: Arc<dyn ExecutionPlan>,
    config: &Arc<ConfigOptions>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let result: Arc<dyn ExecutionPlan> = if let Some(projection) =
        plan.downcast_ref::<ProjectionExec>()
    {
        let schema = projection.input().schema();
        let mut changed = false;
        let expressions = projection
            .expr()
            .iter()
            .map(|value| {
                let result = rewrite_expr(Arc::clone(&value.expr), &schema, config)?;
                changed |= result.transformed;
                Ok(ProjectionExpr::new(result.data, value.alias.clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        if !changed {
            return Ok(Transformed::no(plan));
        }
        Arc::new(ProjectionExec::try_new_with_schema_metadata(
            expressions,
            Arc::clone(projection.input()),
            &projection.schema(),
        )?)
    } else if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        let result = rewrite_expr(
            Arc::clone(filter.predicate()),
            &filter.input().schema(),
            config,
        )?;
        if !result.transformed {
            return Ok(Transformed::no(plan));
        }
        Arc::new(
            FilterExecBuilder::from(filter)
                .with_predicate(result.data)
                .build()?,
        )
    } else if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
        if !matches!(
            aggregate.mode(),
            AggregateMode::Partial | AggregateMode::Single | AggregateMode::SinglePartitioned
        ) {
            return Ok(Transformed::no(plan));
        }
        let schema = aggregate.input().schema();
        let mut changed = false;
        let expressions = aggregate
            .aggr_expr()
            .iter()
            .map(|value| {
                if value.is_reversed() {
                    return Ok(Arc::clone(value));
                }
                let mut changed_argument = false;
                let arguments = value
                    .expressions()
                    .into_iter()
                    .map(|argument| {
                        let result = rewrite_expr(Arc::clone(&argument), &schema, config)?;
                        // DataFusion retains cached argument fields in with_new_expressions.
                        if result.data.return_field(&schema)? != argument.return_field(&schema)? {
                            return Ok(argument);
                        }
                        changed_argument |= result.transformed;
                        Ok(result.data)
                    })
                    .collect::<Result<Vec<_>>>()?;
                if !changed_argument {
                    return Ok(Arc::clone(value));
                }
                let ordering = value
                    .order_bys()
                    .iter()
                    .map(|sort| Arc::clone(&sort.expr))
                    .collect();
                let Some(result) = value.with_new_expressions(arguments, ordering) else {
                    return Ok(Arc::clone(value));
                };
                changed = true;
                Ok(Arc::new(result))
            })
            .collect::<Result<Vec<_>>>()?;
        if !changed {
            return Ok(Transformed::no(plan));
        }
        Arc::new(aggregate.with_new_aggr_exprs(expressions))
    } else {
        return Ok(Transformed::no(plan));
    };
    if result.schema() != plan.schema() || !same_properties(plan.properties(), result.properties())
    {
        return Ok(Transformed::no(plan));
    }
    Ok(Transformed::yes(result))
}

fn rewrite_expr(
    expr: Arc<dyn PhysicalExpr>,
    schema: &Schema,
    config: &Arc<ConfigOptions>,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    expr.transform_up(|expr| {
        let Some(binary) = expr.downcast_ref::<BinaryExpr>() else {
            return Ok(Transformed::no(expr));
        };
        let Some(function) = MixedNumeric::new(*binary.op()) else {
            return Ok(Transformed::no(expr));
        };
        if expr.data_type(schema)? != DataType::Float64 {
            return Ok(Transformed::no(expr));
        }
        let Some((left, left_float)) = input(binary.left(), schema)? else {
            return Ok(Transformed::no(expr));
        };
        let Some((right, right_float)) = input(binary.right(), schema)? else {
            return Ok(Transformed::no(expr));
        };
        if left_float == right_float {
            return Ok(Transformed::no(expr));
        }
        let field = expr.return_field(schema)?;
        // The existing scalar-function wire field does not serialize metadata.
        if !field.metadata().is_empty() {
            return Ok(Transformed::no(expr));
        }
        let function = Arc::new(ScalarUDF::from(function));
        let result: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
            function.name(),
            Arc::clone(&function),
            vec![left, right],
            field,
            Arc::clone(config),
        ));
        Ok(Transformed::yes(result))
    })
}

fn input(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Option<(Arc<dyn PhysicalExpr>, bool)>> {
    if let Some(cast) = expr.downcast_ref::<CastExpr>() {
        if cast.cast_type() != &DataType::Float64
            || cast.cast_options().safe
            || cast.cast_options().format_options
                != datafusion::common::format::DEFAULT_FORMAT_OPTIONS
        {
            return Ok(None);
        }
        return Ok(match cast.expr().data_type(schema)? {
            DataType::Float32 => Some((Arc::clone(cast.expr()), true)),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Some((Arc::clone(cast.expr()), false)),
            _ => None,
        });
    }
    if let Some(literal) = expr.downcast_ref::<Literal>() {
        if matches!(literal.value(), datafusion::common::ScalarValue::Float64(value)
            if value.is_none_or(f64::is_finite))
        {
            return Ok(Some((Arc::clone(expr), false)));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, Float32Array, Float64Array, Int32Array};
    use datafusion::arrow::datatypes::Field;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::{ScalarValue, internal_err};
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::aggregates::PhysicalGroupBy;
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;

    fn source(nullable: bool) -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![
            Field::new("kf", DataType::Float32, nullable),
            Field::new("g", DataType::Int32, nullable),
        ]))))
    }

    fn arithmetic(target: DataType) -> Arc<dyn PhysicalExpr> {
        let left: Arc<dyn PhysicalExpr> = Arc::new(Column::new("kf", 0));
        let left = if target == DataType::Float64 {
            Arc::new(CastExpr::new(left, target.clone(), None)) as Arc<dyn PhysicalExpr>
        } else {
            left
        };
        let right: Arc<dyn PhysicalExpr> =
            Arc::new(CastExpr::new(Arc::new(Column::new("g", 1)), target, None));
        Arc::new(BinaryExpr::new(left, Operator::Plus, right))
    }

    #[test]
    fn projection_preserves_fields_and_uses_double_arithmetic() -> Result<()> {
        for nullable in [false, true] {
            let input = source(nullable);
            let schema = input.schema();
            let original = arithmetic(DataType::Float64);
            let plan: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
                vec![ProjectionExpr::new(Arc::clone(&original), "value")],
                input,
            )?);
            let result = specialize(Arc::clone(&plan), &ConfigOptions::default())?;
            assert_eq!(plan.schema(), result.schema());
            let Some(projection) = result.downcast_ref::<ProjectionExec>() else {
                return internal_err!("expected projection");
            };
            let expression = &projection.expr()[0].expr;
            assert!(expression.is::<ScalarFunctionExpr>());
            assert_eq!(
                expression.return_field(&schema)?,
                original.return_field(&schema)?
            );
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Float32Array::from(vec![
                        Some(16_777_216.0),
                        (!nullable).then_some(2.0),
                    ])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![16_777_217, 3])) as ArrayRef,
                ],
            )?;
            let actual = expression.evaluate(&batch)?.into_array(2)?;
            let expected = Float64Array::from(vec![Some(33_554_433.0), (!nullable).then_some(5.0)]);
            assert_eq!(
                actual.as_ref(),
                &expected as &dyn datafusion::arrow::array::Array
            );
        }
        Ok(())
    }

    #[test]
    fn filter_keeps_its_configuration_and_predicate_result() -> Result<()> {
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            arithmetic(DataType::Float64),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Float64(Some(100.0)))),
        ));
        let filter = FilterExec::try_new(Arc::clone(&predicate), source(false))?;
        let filter = FilterExecBuilder::from(&filter)
            .with_default_selectivity(37)
            .with_batch_size(123)
            .with_fetch(Some(7))
            .apply_projection(Some(vec![0]))?
            .build()?;
        let plan: Arc<dyn ExecutionPlan> = Arc::new(filter);
        let result = specialize(Arc::clone(&plan), &ConfigOptions::default())?;
        let Some(filter) = result.downcast_ref::<FilterExec>() else {
            return internal_err!("expected filter");
        };
        assert_eq!(plan.schema(), result.schema());
        assert_eq!(filter.default_selectivity(), 37);
        assert_eq!(filter.batch_size(), 123);
        assert_eq!(filter.fetch(), Some(7));
        let Some(binary) = filter.predicate().downcast_ref::<BinaryExpr>() else {
            return internal_err!("expected comparison");
        };
        assert!(binary.left().is::<ScalarFunctionExpr>());
        let batch = RecordBatch::try_new(
            filter.input().schema(),
            vec![
                Arc::new(Float32Array::from(vec![99.0, 100.0])) as ArrayRef,
                Arc::new(Int32Array::from(vec![1, 1])) as ArrayRef,
            ],
        )?;
        assert_eq!(
            predicate.evaluate(&batch)?.into_array(2)?.as_ref(),
            filter.predicate().evaluate(&batch)?.into_array(2)?.as_ref()
        );
        Ok(())
    }

    #[test]
    fn raw_sum_arguments_fuse_without_changing_aggregate_fields() -> Result<()> {
        let input = source(false);
        let schema = input.schema();
        let function = Arc::new(
            AggregateExprBuilder::new(sum_udaf(), vec![arithmetic(DataType::Float64)])
                .schema(Arc::clone(&schema))
                .alias("total")
                .build()?,
        );
        let plan: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![]),
            vec![function],
            vec![None],
            input,
            schema,
        )?);
        let result = specialize(Arc::clone(&plan), &ConfigOptions::default())?;
        let Some(aggregate) = result.downcast_ref::<AggregateExec>() else {
            return internal_err!("expected aggregate");
        };
        assert_eq!(plan.schema(), result.schema());
        assert!(aggregate.aggr_expr()[0].expressions()[0].is::<ScalarFunctionExpr>());
        assert!(matches!(aggregate.mode(), AggregateMode::Single));
        Ok(())
    }

    #[test]
    fn unsigned_integer_widening_is_specialized() -> Result<()> {
        for integer in [
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ] {
            let schema = Schema::new(vec![
                Field::new("k", DataType::Float32, true),
                Field::new("g", integer, true),
            ]);
            let expression: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                Arc::new(CastExpr::new(
                    Arc::new(Column::new("k", 0)),
                    DataType::Float64,
                    None,
                )),
                Operator::Plus,
                Arc::new(CastExpr::new(
                    Arc::new(Column::new("g", 1)),
                    DataType::Float64,
                    None,
                )),
            ));
            let result = rewrite_expr(Arc::clone(&expression), &schema, &Arc::default())?;
            assert!(result.transformed);
            assert_eq!(
                expression.return_field(&schema)?,
                result.data.return_field(&schema)?
            );
        }
        Ok(())
    }

    #[test]
    fn native_float_arithmetic_is_unchanged() -> Result<()> {
        let input = source(false);
        let original = arithmetic(DataType::Float32);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
            vec![ProjectionExpr::new(Arc::clone(&original), "value")],
            input,
        )?);
        let result = specialize(Arc::clone(&plan), &ConfigOptions::default())?;
        assert!(Arc::ptr_eq(&plan, &result));
        assert_eq!(result.schema().field(0).data_type(), &DataType::Float32);
        Ok(())
    }
}
