use datafusion::optimizer::simplify_expressions::SimplifyExpressions as DataFusionSimplifyExpressions;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator, lit};

/// Extend the existing simplification pass with exact numeric widening comparisons.
#[derive(Debug, Default)]
pub struct SimplifyExpressions(DataFusionSimplifyExpressions);

impl OptimizerRule for SimplifyExpressions {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Folding first exposes constants written as casts or constant expressions.
        self.0.rewrite(plan, config)?.transform_data(|plan| {
            let mut candidate = false;
            plan.apply_expressions(|expr| {
                candidate = expr.exists(|expr| {
                    let Expr::BinaryExpr(binary) = expr else {
                        return Ok(false);
                    };
                    let pair = |value: &Expr, literal: &Expr| {
                        matches!(value, Expr::Cast(_) | Expr::TryCast(_))
                            && matches!(
                                literal,
                                Expr::Literal(ScalarValue::Float32(_) | ScalarValue::Float64(_), _)
                            )
                    };
                    Ok(pair(&binary.left, &binary.right) || pair(&binary.right, &binary.left))
                })?;
                Ok(if candidate {
                    TreeNodeRecursion::Stop
                } else {
                    TreeNodeRecursion::Continue
                })
            })?;
            if !candidate {
                return Ok(Transformed::no(plan));
            }
            let schema = match &plan {
                LogicalPlan::TableScan(scan) => DFSchema::try_from_qualified_schema(
                    scan.table_name.clone(),
                    &scan.source.schema(),
                )?,
                _ => merge_schema(&plan.inputs()),
            };
            let names = NamePreserver::new(&plan);
            let rewrite = |expr: Expr| {
                let name = names.save(&expr);
                expr.transform_up(|expr| unwrap_comparison(expr, &schema))
                    .map(|result| result.update_data(|expr| name.restore(expr)))
            };
            plan.map_expressions(|expr| {
                if matches!(expr, Expr::GroupingSet(_)) {
                    expr.map_children(rewrite)
                } else {
                    rewrite(expr)
                }
            })
        })
    }
}

fn unwrap_comparison(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Expr::BinaryExpr(binary) = &expr else {
        return Ok(Transformed::no(expr));
    };
    if !matches!(
        binary.op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    ) {
        return Ok(Transformed::no(expr));
    }
    let operands = unwrap_pair(&binary.left, &binary.right, schema).or_else(|| {
        unwrap_pair(&binary.right, &binary.left, schema).map(|(value, literal)| (literal, value))
    });
    match operands {
        Some((left, right)) => Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            binary.op,
            Box::new(right),
        )))),
        None => Ok(Transformed::no(expr)),
    }
}

fn unwrap_pair(cast: &Expr, literal: &Expr, schema: &DFSchema) -> Option<(Expr, Expr)> {
    use datafusion::arrow::datatypes::DataType;

    let (value, target) = match cast {
        Expr::Cast(cast) => (&cast.expr, cast.field.data_type()),
        Expr::TryCast(cast) => {
            // Physical TRY_CAST is nullable even for total casts of nonnullable inputs.
            if !cast.expr.nullable(schema).ok()? {
                return None;
            }
            (&cast.expr, cast.field.data_type())
        }
        _ => return None,
    };
    let Expr::Literal(literal, _) = literal else {
        return None;
    };
    let source = value.get_type(schema).ok()?;
    // Spark's UnwrapCastInBinaryComparison excludes INT->FLOAT and BIGINT->FLOAT/DOUBLE.
    if !matches!(
        (&source, target),
        (DataType::Int8 | DataType::Int16, DataType::Float32)
            | (
                DataType::Int8 | DataType::Int16 | DataType::Int32,
                DataType::Float64
            )
            | (DataType::Float32, DataType::Float64)
    ) || literal.data_type() != *target
    {
        return None;
    }
    // NaN widening can quiet signaling payloads, so a round trip is insufficient
    // to prove that moving a NaN literal preserves its comparison class.
    if !matches!(literal, ScalarValue::Float32(Some(x)) if x.is_finite())
        && !matches!(literal, ScalarValue::Float64(Some(x)) if x.is_finite())
    {
        return None;
    }
    let narrowed = literal.cast_to(&source).ok()?;
    let roundtrip = narrowed.cast_to(target).ok()?;
    let exact = match (literal, roundtrip) {
        (ScalarValue::Float32(Some(a)), ScalarValue::Float32(Some(b))) => {
            a.to_bits() == b.to_bits()
        }
        (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => {
            a.to_bits() == b.to_bits()
        }
        _ => false,
    };
    exact.then(|| (value.as_ref().clone(), lit(narrowed)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Float32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::create_physical_expr;
    use datafusion_expr::execution_props::ExecutionProps;
    use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
    use datafusion_expr::{cast, col, try_cast};

    use super::*;

    fn schema(data_type: DataType) -> Result<DFSchema> {
        DFSchema::try_from(Schema::new(vec![Field::new("x", data_type, true)]))
    }

    #[test]
    fn exact_float_widening_comparisons_preserve_every_operator_and_nulls() -> Result<()> {
        let schema = schema(DataType::Float32)?;
        let raw: ArrayRef = Arc::new(Float32Array::from(vec![
            None,
            Some(f32::NEG_INFINITY),
            Some(f32::from_bits(0xff80_0001)),
            Some(-f32::MAX),
            Some(-1.5),
            Some(-0.0),
            Some(0.0),
            Some(f32::from_bits(1)),
            Some(1.5),
            Some(f32::MAX),
            Some(f32::INFINITY),
            Some(f32::from_bits(0x7f80_0001)),
            Some(f32::from_bits(0x7fc0_0001)),
            Some(f32::from_bits(0x7fc0_0002)),
        ]));
        let batch = RecordBatch::try_new(Arc::new(schema.as_arrow().clone()), vec![raw])?;
        for literal in [
            0.0_f64,
            -0.0,
            1.5,
            -1.5,
            f64::from(f32::from_bits(1)),
            f64::from(f32::MAX),
        ] {
            for op in [
                Operator::Eq,
                Operator::NotEq,
                Operator::Lt,
                Operator::LtEq,
                Operator::Gt,
                Operator::GtEq,
                Operator::IsDistinctFrom,
                Operator::IsNotDistinctFrom,
            ] {
                for reversed in [false, true] {
                    for value in [
                        cast(col("x"), DataType::Float64),
                        try_cast(col("x"), DataType::Float64),
                    ] {
                        let (left, right) = if reversed {
                            (lit(literal), value)
                        } else {
                            (value, lit(literal))
                        };
                        let original =
                            Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)));
                        let optimized = unwrap_comparison(original.clone(), &schema)?;
                        assert!(optimized.transformed, "{original:?}");
                        let evaluate = |expr: &Expr| -> Result<ArrayRef> {
                            create_physical_expr(
                                expr,
                                &schema,
                                &ExecutionProps::new(),
                                &PhysicalPlanningContext::default(),
                            )?
                            .evaluate(&batch)?
                            .into_array(batch.num_rows())
                        };
                        let original_values = evaluate(&original)?;
                        let optimized_values = evaluate(&optimized.data)?;
                        assert_eq!(
                            original_values.as_ref(),
                            optimized_values.as_ref(),
                            "{original:?}"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn exact_integral_widening_literals_and_ineligible_casts() -> Result<()> {
        for (from, to, good, bad) in [
            (
                DataType::Int8,
                DataType::Float32,
                lit(127.0_f32),
                lit(128.0_f32),
            ),
            (
                DataType::Int16,
                DataType::Float32,
                lit(32767.0_f32),
                lit(32768.0_f32),
            ),
            (
                DataType::Int8,
                DataType::Float64,
                lit(-128.0_f64),
                lit(-129.0_f64),
            ),
            (
                DataType::Int16,
                DataType::Float64,
                lit(-32768.0_f64),
                lit(-32769.0_f64),
            ),
            (
                DataType::Int32,
                DataType::Float64,
                lit(f64::from(i32::MAX)),
                lit(f64::from(i32::MAX) + 1.0),
            ),
        ] {
            let schema = schema(from)?;
            let value = cast(col("x"), to);
            assert!(unwrap_pair(&value, &good, &schema).is_some());
            assert!(unwrap_pair(&value, &bad, &schema).is_none());
            let fractional = match good {
                Expr::Literal(ScalarValue::Float32(_), _) => lit(1.5_f32),
                _ => lit(1.5_f64),
            };
            assert!(unwrap_pair(&value, &fractional, &schema).is_none());
        }
        for (from, to) in [
            (DataType::Int32, DataType::Float32),
            (DataType::Int64, DataType::Float32),
            (DataType::Int64, DataType::Float64),
            (DataType::Float64, DataType::Float32),
        ] {
            let schema = schema(from)?;
            let literal = if to == DataType::Float32 {
                lit(1.0_f32)
            } else {
                lit(1.0_f64)
            };
            assert!(unwrap_pair(&cast(col("x"), to), &literal, &schema).is_none());
        }
        let schema = schema(DataType::Float32)?;
        for literal in [
            1.1_f64,
            f64::from(f32::MAX) * 2.0,
            f64::from(f32::from_bits(1)) / 2.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            assert!(
                unwrap_pair(&cast(col("x"), DataType::Float64), &lit(literal), &schema).is_none()
            );
        }
        assert!(
            unwrap_pair(
                &cast(col("x"), DataType::Float64),
                &lit(ScalarValue::Float64(None)),
                &schema,
            )
            .is_none()
        );
        Ok(())
    }
    #[test]
    fn nonnullable_try_cast_comparisons_keep_their_physical_schema() -> Result<()> {
        let schema =
            DFSchema::try_from(Schema::new(vec![Field::new("x", DataType::Float32, false)]))?;
        let original = try_cast(col("x"), DataType::Float64).gt(lit(1.5_f64));
        let optimized = unwrap_comparison(original.clone(), &schema)?;
        assert!(!optimized.transformed);
        let physical = |expr: &Expr| {
            create_physical_expr(
                expr,
                &schema,
                &ExecutionProps::new(),
                &PhysicalPlanningContext::default(),
            )
        };
        assert!(physical(&original)?.nullable(schema.as_arrow())?);
        assert!(physical(&optimized.data)?.nullable(schema.as_arrow())?);
        let ordinary = cast(col("x"), DataType::Float64).gt(lit(1.5_f64));
        let ordinary = unwrap_comparison(ordinary, &schema)?;
        assert!(ordinary.transformed);
        assert!(!physical(&ordinary.data)?.nullable(schema.as_arrow())?);
        Ok(())
    }
}
