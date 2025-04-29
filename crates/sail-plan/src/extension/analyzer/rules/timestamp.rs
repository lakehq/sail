use std::ops::Div;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::optimizer::AnalyzerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{lit, Cast, Expr, ExprSchemable, LogicalPlan, TryCast};

#[derive(Debug, Default)]
pub struct TimestampTypeCast {}

impl AnalyzerRule for TimestampTypeCast {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| {
            // We need the schema to get the data type of the expression.
            // The following logic is seen in a few DataFusion logical analyzers and optimizers.
            // Ideally, there should be a utility function for this.
            let mut schema = merge_schema(&plan.inputs());

            if let LogicalPlan::TableScan(ts) = &plan {
                let source_schema = DFSchema::try_from_qualified_schema(
                    ts.table_name.clone(),
                    &ts.source.schema(),
                )?;
                schema.merge(&source_schema);
            }

            // TODO: Do we need to take care of the outer schema for subquery expressions?

            let mut rewriter = TimestampTypeCastRewriter { schema: &schema };
            plan.map_expressions(|e| e.rewrite(&mut rewriter))
        })
        .map(|x| x.data)
    }

    fn name(&self) -> &str {
        "timestamp_type_cast"
    }
}

struct TimestampTypeCastRewriter<'a> {
    schema: &'a DFSchema,
}

fn is_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::UInt8
            | DataType::Int16
            | DataType::UInt16
            | DataType::Int32
            | DataType::UInt32
            | DataType::Int64
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _),
    )
}

fn scale_timestamp(expr: Box<Expr>, time_unit: &TimeUnit) -> Expr {
    let expr = Expr::Cast(Cast {
        expr,
        data_type: DataType::Int64,
    });
    match time_unit {
        TimeUnit::Second => expr,
        TimeUnit::Millisecond => expr.div(lit(1_000i64)),
        TimeUnit::Microsecond => expr.div(lit(1_000_000i64)),
        TimeUnit::Nanosecond => expr.div(lit(1_000_000_000i64)),
    }
}

impl TreeNodeRewriter for TimestampTypeCastRewriter<'_> {
    type Node = Expr;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        match node {
            Expr::Cast(Cast {
                expr,
                data_type: target_type,
            }) => {
                let source_type = expr.get_type(self.schema)?;
                let cast_to_numeric = is_numeric_type(&target_type);
                match source_type {
                    DataType::Timestamp(time_unit, _) if cast_to_numeric => {
                        Ok(Transformed::yes(Expr::Cast(Cast {
                            expr: Box::new(scale_timestamp(expr, &time_unit)),
                            data_type: target_type,
                        })))
                    }
                    _ => Ok(Transformed::no(Expr::Cast(Cast {
                        expr,
                        data_type: target_type,
                    }))),
                }
            }
            Expr::TryCast(TryCast {
                expr,
                data_type: target_type,
            }) => {
                let source_type = expr.get_type(self.schema)?;
                let cast_to_numeric = is_numeric_type(&target_type);
                match source_type {
                    DataType::Timestamp(time_unit, _) if cast_to_numeric => {
                        Ok(Transformed::yes(Expr::TryCast(TryCast {
                            expr: Box::new(scale_timestamp(expr, &time_unit)),
                            data_type: target_type,
                        })))
                    }
                    _ => Ok(Transformed::no(Expr::TryCast(TryCast {
                        expr,
                        data_type: target_type,
                    }))),
                }
            }
            _ => Ok(Transformed::no(node)),
        }
    }
}
