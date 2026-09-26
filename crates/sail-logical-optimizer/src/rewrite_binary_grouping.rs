use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_expr::{
    Aggregate, Expr, ExprSchemable, LogicalPlan, Operator, Projection, binary_expr, lit,
};

/// Rewrites binary grouping keys to LargeBinary while preserving the output schema.
#[derive(Debug, Default)]
pub struct RewriteBinaryGrouping;

impl OptimizerRule for RewriteBinaryGrouping {
    fn name(&self) -> &str {
        "rewrite_binary_grouping"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Aggregate(aggregate) = plan else {
            return Ok(Transformed::no(plan));
        };

        let mut changed = false;
        let group_expr = aggregate
            .group_expr
            .iter()
            .cloned()
            .map(|expr| {
                let schema = aggregate.input.schema();
                let result = if matches!(expr, Expr::GroupingSet(_)) {
                    expr.map_children(|expr| binary_grouping_expr(expr, schema))?
                } else {
                    binary_grouping_expr(expr, schema)?
                };
                changed |= result.transformed;
                Ok(result.data)
            })
            .collect::<Result<Vec<_>>>()?;
        if !changed {
            return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
        }

        // Binary grouping accumulates all distinct bytes in one i32-offset buffer.
        // LargeBinary widens the offsets, and repartition's take copies selected
        // bytes instead of retaining BinaryView backing buffers.
        let schema = aggregate.schema;
        let aggregate = Arc::new(LogicalPlan::Aggregate(Aggregate::try_new(
            aggregate.input,
            group_expr,
            aggregate.aggr_expr,
        )?));
        let projection = schema
            .columns()
            .into_iter()
            .zip(schema.fields())
            .map(|(column, field)| {
                let expr = Expr::Column(column.clone());
                if field.data_type() == &DataType::Binary {
                    // Rebase this slice's offsets and isolate its values before casting.
                    // The view round trip drops excess capacity retained by concatenation.
                    let expr = if expr.get_type(aggregate.schema())? == DataType::LargeBinary {
                        binary_expr(
                            expr,
                            Operator::StringConcat,
                            lit(ScalarValue::LargeBinary(Some(vec![]))),
                        )
                        .cast_to(&DataType::BinaryView, aggregate.schema())?
                    } else {
                        expr
                    };
                    Ok(expr
                        .cast_to(&DataType::Binary, aggregate.schema())?
                        .alias_qualified_with_metadata(
                            column.relation,
                            column.name,
                            Some(field.metadata().into()),
                        ))
                } else {
                    Ok(expr)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Transformed::yes(LogicalPlan::Projection(
            Projection::try_new_with_schema(projection, aggregate, schema)?,
        )))
    }
}

fn binary_grouping_expr(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let (qualifier, field) = expr.to_field(schema)?;
    if field.data_type() != &DataType::Binary {
        return Ok(Transformed::no(expr));
    }
    let expr = expr
        .unalias()
        .cast_to(&DataType::LargeBinary, schema)?
        .alias_qualified_with_metadata(qualifier, field.name(), Some(field.metadata().into()));
    Ok(Transformed::yes(expr))
}
