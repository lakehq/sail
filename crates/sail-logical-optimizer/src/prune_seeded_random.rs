use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::tree_node::Transformed;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_expr::{Extension, LogicalPlan, Sort, UserDefinedLogicalNodeCore};
use sail_logical_plan::seeded_random::SeededRandomNode;
use sail_logical_plan::sort::{RequiredSortNode, SortWithinPartitionsNode};

use crate::eliminate_sorts::is_order_irrelevant;

/// Removes the columns of a [`SeededRandomNode`] that are not used, and the node itself if
/// none of its columns are used, as Spark's `ColumnPruning` removes unused nondeterministic
/// expressions. This lets filters be pushed down to the input. The rule runs after
/// `optimize_projections`, which removes the input columns that are no longer used in the
/// next optimizer pass.
///
/// When the node is removed below an aggregate that does not depend on the order of its
/// input, the sort that the node required of its input is no longer required, so that it can
/// be eliminated as Spark does.
#[derive(Debug, Default)]
pub struct PruneSeededRandomColumns;

impl OptimizerRule for PruneSeededRandomColumns {
    fn name(&self) -> &str {
        "prune_seeded_random_columns"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        prune_seeded_random_columns(plan)
    }
}

/// Removes the unused columns of a [`SeededRandomNode`] below a projection or an aggregate.
/// The columns used are found from the projection or the aggregate, and the operators between
/// it and the node that pass the columns of the node through.
fn prune_seeded_random_columns(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if !matches!(plan, LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_)) {
        return Ok(Transformed::no(plan));
    }
    let mut chain = vec![&plan];
    loop {
        let [input] = chain[chain.len() - 1].inputs()[..] else {
            return Ok(Transformed::no(plan));
        };
        match input {
            LogicalPlan::Filter(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Repartition(_) => chain.push(input),
            // The projections below an aggregate are followed to find the sort that is no
            // longer required.
            LogicalPlan::Projection(_) if matches!(plan, LogicalPlan::Aggregate(_)) => {
                chain.push(input)
            }
            LogicalPlan::Extension(extension)
                if extension.node.as_any().is::<SeededRandomNode>() =>
            {
                break;
            }
            _ => return Ok(Transformed::no(plan)),
        }
    }
    let mut used = HashSet::new();
    for node in &chain {
        for expr in node.expressions() {
            used.extend(expr.column_refs().into_iter().map(|c| c.name.clone()));
        }
    }
    // Nothing above the aggregate depends on the order of the input of the node.
    let unrequire = match &plan {
        LogicalPlan::Aggregate(aggregate) => {
            is_order_irrelevant(aggregate)?
                && chain[1..].iter().all(|node| {
                    matches!(
                        node,
                        LogicalPlan::Projection(_)
                            | LogicalPlan::Filter(_)
                            | LogicalPlan::SubqueryAlias(_)
                            | LogicalPlan::Repartition(_)
                    )
                })
        }
        _ => false,
    };
    prune(plan, &used, unrequire)
}

/// Rebuilds the plan down to the node, without the columns of the node that are not used.
fn prune(
    plan: LogicalPlan,
    used: &HashSet<String>,
    unrequire: bool,
) -> Result<Transformed<LogicalPlan>> {
    if let LogicalPlan::Extension(extension) = &plan
        && let Some(node) = extension.node.as_any().downcast_ref::<SeededRandomNode>()
    {
        let columns = node
            .columns()
            .iter()
            .filter(|column| used.contains(&column.name))
            .cloned()
            .collect::<Vec<_>>();
        if columns.len() == node.columns().len() {
            return Ok(Transformed::no(plan));
        }
        let input = Arc::clone(node.input());
        return Ok(Transformed::yes(if columns.is_empty() && unrequire {
            unrequire_sort(Arc::unwrap_or_clone(input))
        } else if columns.is_empty() {
            Arc::unwrap_or_clone(input)
        } else {
            LogicalPlan::Extension(Extension {
                node: Arc::new(SeededRandomNode::try_new(input, columns)?),
            })
        }));
    }
    let [input] = plan.inputs()[..] else {
        return Ok(Transformed::no(plan));
    };
    let input = prune(input.clone(), used, unrequire)?;
    if !input.transformed {
        return Ok(Transformed::no(plan));
    }
    let exprs = plan.expressions();
    Ok(Transformed::yes(
        plan.with_new_exprs(exprs, vec![input.data])?,
    ))
}

/// Reverts the [`RequiredSortNode`] below the operators that pass the input order through
/// to the sort that it was created from.
fn unrequire_sort(plan: LogicalPlan) -> LogicalPlan {
    if matches!(
        plan,
        LogicalPlan::SubqueryAlias(_) | LogicalPlan::Projection(_) | LogicalPlan::Filter(_)
    ) {
        let [input] = plan.inputs()[..] else {
            return plan;
        };
        let input = unrequire_sort(input.clone());
        let exprs = plan.expressions();
        return plan.with_new_exprs(exprs, vec![input]).unwrap_or(plan);
    }
    let LogicalPlan::Extension(extension) = &plan else {
        return plan;
    };
    let Some(sort) = extension.node.as_any().downcast_ref::<RequiredSortNode>() else {
        return plan;
    };
    let [input] = sort.inputs()[..] else {
        return plan;
    };
    let input = Arc::new(input.clone());
    if sort.preserve_partitioning() {
        LogicalPlan::Extension(Extension {
            node: Arc::new(SortWithinPartitionsNode::new(
                input,
                sort.sort_expr().to_vec(),
                sort.fetch(),
            )),
        })
    } else {
        LogicalPlan::Sort(Sort {
            expr: sort.sort_expr().to_vec(),
            input,
            fetch: sort.fetch(),
        })
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::optimizer::optimize_projections::OptimizeProjections;
    use datafusion::optimizer::push_down_filter::PushDownFilter;
    use datafusion::optimizer::{Optimizer, OptimizerContext};
    use datafusion_expr::{LogicalPlanBuilder, ScalarUDF, col, lit};
    use sail_function::scalar::math::partition_random::PartitionSeed;
    use sail_function::scalar::math::random::Random;
    use sail_logical_plan::seeded_random::SeededRandomColumn;

    use super::*;

    /// Returns the optimized plan of
    /// `SELECT <columns> FROM (SELECT a, part, rand(42) AS r FROM t) WHERE part = 1`.
    fn optimize(columns: &[&str]) -> Result<String> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("part", DataType::Int64, false),
        ]);
        let scan = LogicalPlanBuilder::scan(
            "t",
            provider_as_source(Arc::new(EmptyTable::new(Arc::new(schema)))),
            None,
        )?
        .build()?;
        let node = SeededRandomNode::try_new(
            Arc::new(scan),
            vec![SeededRandomColumn {
                name: "r".to_string(),
                expr: ScalarUDF::from(Random::new()).call(vec![lit(42_i64)]),
                seed: PartitionSeed::Offset,
            }],
        )?;
        let plan = LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
        .project(vec![col("a"), col("part"), col("r")])?
        .filter(col("part").eq(lit(1_i64)))?
        .project(columns.iter().map(|x| col(*x)))?
        .build()?;
        let optimizer = Optimizer::with_rules(vec![
            Arc::new(OptimizeProjections::new()),
            Arc::new(PruneSeededRandomColumns),
            Arc::new(PushDownFilter::new()),
        ]);
        let plan = optimizer.optimize(plan, &OptimizerContext::new(), |_, _| {})?;
        Ok(plan.display_indent().to_string())
    }

    #[test]
    fn test_unused_seeded_random_column_is_pruned() -> Result<()> {
        let display = optimize(&["a"])?;
        assert!(!display.contains("SeededRandom"), "{display}");
        let filter = display.find("Filter").unwrap_or(usize::MAX);
        assert!(filter < display.find("TableScan").unwrap_or(0), "{display}");
        assert!(!display[filter..].contains("Projection"), "{display}");
        Ok(())
    }

    #[test]
    fn test_used_seeded_random_column_is_kept() -> Result<()> {
        let display = optimize(&["a", "r"])?;
        let position = |name: &str| display.find(name);
        assert!(position("SeededRandom").is_some(), "{display}");
        assert!(position("Filter") < position("SeededRandom"), "{display}");
        Ok(())
    }
}
