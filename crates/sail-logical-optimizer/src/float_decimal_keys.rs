// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::expr::{BinaryExpr, Cast};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::{Expr, LogicalPlan, Operator};
use sail_function::scalar::conversion::float_to_decimal::FloatToDecimal;

/// Restore the plain cast for strict FLOAT or DOUBLE to DECIMAL correlation keys of
/// `IN` and `EXISTS` subqueries that native decorrelation cannot pull up.
#[derive(Debug)]
pub struct RestorePredicateSubqueryCastKeys;

impl OptimizerRule for RestorePredicateSubqueryCastKeys {
    fn name(&self) -> &str {
        "restore_predicate_subquery_cast_keys"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !matches!(plan, LogicalPlan::Filter(_)) {
            return Ok(Transformed::no(plan));
        }
        let names = NamePreserver::new(&plan);
        plan.map_expressions(|expression| {
            let name = names.save(&expression);
            expression
                .transform_up(|expression| match expression {
                    Expr::Exists(mut exists) => {
                        let result = restore_cast_keys(exists.subquery.subquery.as_ref())?;
                        if result.transformed {
                            exists.subquery.subquery = Arc::new(result.data);
                        }
                        Ok(Transformed::new_transformed(
                            Expr::Exists(exists),
                            result.transformed,
                        ))
                    }
                    Expr::InSubquery(mut in_subquery) => {
                        let result = restore_cast_keys(in_subquery.subquery.subquery.as_ref())?;
                        if result.transformed {
                            in_subquery.subquery.subquery = Arc::new(result.data);
                        }
                        Ok(Transformed::new_transformed(
                            Expr::InSubquery(in_subquery),
                            result.transformed,
                        ))
                    }
                    _ => Ok(Transformed::no(expression)),
                })
                .map(|result| result.update_data(|expression| name.restore(expression)))
        })
    }
}

/// Sail plans a strict FLOAT or DOUBLE to DECIMAL cast as a function that rounds like
/// Spark, but native decorrelation pulls a correlated equality above an aggregate with
/// aggregate functions only for a column or a cast of one. Where such a key is left
/// below that aggregate, fall back to the plain cast, as before the function, so that
/// the subquery is still decorrelated.
pub(crate) fn restore_cast_keys(plan: &LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    restore_below(plan.clone(), false)
}

fn restore_below(plan: LogicalPlan, below_aggregate: bool) -> Result<Transformed<LogicalPlan>> {
    // An aggregate without aggregate functions acts as a distinct, and native
    // decorrelation pulls any correlated filter above it.
    let below = below_aggregate
        || matches!(&plan, LogicalPlan::Aggregate(aggregate) if !aggregate.aggr_expr.is_empty());
    plan.map_children(|child| restore_below(child, below))?
        .transform_data(|plan| match plan {
            LogicalPlan::Filter(mut filter) if below_aggregate => {
                let predicate = filter.predicate.transform_up(restore_cast_key)?;
                filter.predicate = predicate.data;
                Ok(Transformed::new_transformed(
                    LogicalPlan::Filter(filter),
                    predicate.transformed,
                ))
            }
            _ => Ok(Transformed::no(plan)),
        })
}

fn restore_cast_key(expression: Expr) -> Result<Transformed<Expr>> {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expression else {
        return Ok(Transformed::no(expression));
    };
    let outer = |expression: &Expr| expression.contains_outer() && !expression.any_column_refs();
    let (left, right, transformed) = match (op, strict_cast(&left), strict_cast(&right)) {
        (Operator::Eq, Some(cast), _) if outer(&right) => (cast, *right, true),
        (Operator::Eq, _, Some(cast)) if outer(&left) => (*left, cast, true),
        _ => (*left, *right, false),
    };
    Ok(Transformed::new_transformed(
        Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right))),
        transformed,
    ))
}

/// The plain cast of a column that a strict FLOAT or DOUBLE to DECIMAL function replaces.
fn strict_cast(expression: &Expr) -> Option<Expr> {
    let Expr::ScalarFunction(function) = expression else {
        return None;
    };
    let decimal = function
        .func
        .inner()
        .downcast_ref::<FloatToDecimal>()
        .filter(|function| !function.null_on_overflow())?;
    match function.args.as_slice() {
        [column @ Expr::Column(_)] => Some(Expr::Cast(Cast::new(
            Box::new(column.clone()),
            DataType::Decimal128(decimal.precision(), decimal.scale()),
        ))),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::functions_aggregate::expr_fn::max;
    use datafusion_common::tree_node::TreeNodeRecursion;
    use datafusion_expr::{LogicalPlanBuilder, ScalarUDF, cast, col, out_ref_col};

    use super::*;

    fn scan() -> Result<LogicalPlanBuilder> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Float64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        LogicalPlanBuilder::scan(
            "i",
            provider_as_source(Arc::new(EmptyTable::new(schema))),
            None,
        )
    }

    fn key(null_on_overflow: bool) -> Expr {
        ScalarUDF::from(FloatToDecimal::new(10, 2, null_on_overflow))
            .call(vec![col("d")])
            .eq(out_ref_col(DataType::Decimal128(10, 2), "k"))
    }

    fn filters(plan: &LogicalPlan) -> Result<Vec<Expr>> {
        let mut predicates = vec![];
        plan.apply(|plan| {
            if let LogicalPlan::Filter(filter) = plan {
                predicates.push(filter.predicate.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(predicates)
    }

    #[test]
    fn restores_the_cast_for_a_strict_key_below_an_aggregate() -> Result<()> {
        let plan = scan()?
            .filter(key(false))?
            .aggregate(Vec::<Expr>::new(), vec![max(col("v"))])?
            .build()?;
        let result = restore_cast_keys(&plan)?;
        assert!(result.transformed);
        assert_eq!(
            filters(&result.data)?,
            vec![
                cast(col("i.d"), DataType::Decimal128(10, 2))
                    .eq(out_ref_col(DataType::Decimal128(10, 2), "k"))
            ]
        );
        Ok(())
    }

    #[test]
    fn keeps_the_function_where_native_decorrelation_can_pull_it_up() -> Result<()> {
        let without_aggregate = scan()?.filter(key(false))?.build()?;
        let above_aggregate = scan()?
            .aggregate(vec![col("d")], vec![max(col("v"))])?
            .filter(key(false))?
            .build()?;
        let null_on_overflow = scan()?
            .filter(key(true))?
            .aggregate(Vec::<Expr>::new(), vec![max(col("v"))])?
            .build()?;
        let distinct = scan()?
            .filter(key(false))?
            .aggregate(vec![col("v")], Vec::<Expr>::new())?
            .build()?;
        for plan in [
            without_aggregate,
            above_aggregate,
            null_on_overflow,
            distinct,
        ] {
            let result = restore_cast_keys(&plan)?;
            assert!(!result.transformed);
            assert_eq!(result.data, plan);
        }
        Ok(())
    }
}
