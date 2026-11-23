// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_physical_expr::Distribution;
use sail_physical_plan::key_distribution::KeyDistributionExec;
use sail_physical_plan::repartition::ExplicitRepartitionExec;

/// Removes the key distributions that are not needed or not wanted.
///
/// The rows are distributed by the keys unless the input of the distribution
/// has a single partition, is already partitioned by the keys, or has the
/// partitions of an explicit repartition or coalesce of the query.
///
/// This is decided on the plan that the other rules have optimized, so that
/// the partitions are the ones that are executed. The rules add repartitions
/// below filters, computed projections and aggregates, for example, and
/// the partitions of the query do not reach the distribution then. The explicit
/// repartitions are rewritten after this rule, since they cannot be told from
/// other repartitions afterwards.
#[derive(Debug, Default)]
pub struct KeepExplicitPartitions;

impl PhysicalOptimizerRule for KeepExplicitPartitions {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            let Some(distribution) = plan.downcast_ref::<KeyDistributionExec>() else {
                return Ok(Transformed::no(plan));
            };
            let input = distribution.input();
            let partitioning = input.output_partitioning();
            let required = Distribution::KeyPartitioned(distribution.keys().to_vec());
            if partitioning.partition_count() <= 1
                || partitioning
                    .satisfaction(&required, input.equivalence_properties(), false)
                    .is_satisfied()
                || has_explicit_partitions(input)
            {
                Ok(Transformed::yes(Arc::clone(input)))
            } else {
                Ok(Transformed::no(plan))
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "KeepExplicitPartitions"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Returns whether the partitions of the plan are the partitions
/// that the query asks for with a repartition or a coalesce.
fn has_explicit_partitions(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let mut plan = plan;
    loop {
        if plan.is::<ExplicitRepartitionExec>() {
            return true;
        }
        // The partitions are kept by the plans that compute each
        // output partition from the input partition of the same index.
        let partitions = plan.output_partitioning().partition_count();
        match plan.children().as_slice() {
            [child]
                if !plan.is::<RepartitionExec>()
                    && child.output_partitioning().partition_count() == partitions =>
            {
                plan = child
            }
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, lit};
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
    use datafusion_physical_expr::{Partitioning, PhysicalExpr};

    use super::*;
    use crate::{PhysicalOptimizerOptions, get_physical_optimizers};

    const TARGET_PARTITIONS: usize = 8;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("p", DataType::Int64, true),
            Field::new("o", DataType::Int64, true),
        ]))
    }

    fn column(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, index))
    }

    /// A source whose partitions have more rows than a batch, so that
    /// the optimizer repartitions its rows for the plans that benefit from it.
    fn source(partitions: usize) -> Result<Arc<dyn ExecutionPlan>> {
        let values: ArrayRef = Arc::new(Int64Array::from_iter_values(0..10_000));
        let batch = RecordBatch::try_new(schema(), vec![Arc::clone(&values), values])?;
        let partitions = vec![vec![batch]; partitions];
        Ok(MemorySourceConfig::try_new_exec(
            &partitions,
            schema(),
            None,
        )?)
    }

    fn explicit(partitioning: Partitioning) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ExplicitRepartitionExec::new(
            source(4)?,
            partitioning,
        )))
    }

    fn projection(input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let sum: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(column("o", 1), Operator::Plus, lit(1_i64)));
        Ok(Arc::new(ProjectionExec::try_new(
            vec![
                ProjectionExpr {
                    expr: column("p", 0),
                    alias: "p".to_string(),
                },
                ProjectionExpr {
                    expr: sum,
                    alias: "o".to_string(),
                },
            ],
            input,
        )?))
    }

    fn filter(input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let predicate: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(column("o", 1), Operator::GtEq, lit(0_i64)));
        Ok(Arc::new(FilterExec::try_new(predicate, input)?))
    }

    /// The distinct values of the columns, which the plan has in the order of the schema.
    fn aggregate(
        input: Arc<dyn ExecutionPlan>,
        columns: &[&str],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_schema = input.schema();
        let group_by = |schema: &Schema| -> Result<PhysicalGroupBy> {
            let exprs = columns
                .iter()
                .map(|name| Ok((column(name, schema.index_of(name)?), name.to_string())))
                .collect::<Result<Vec<_>>>()?;
            Ok(PhysicalGroupBy::new_single(exprs))
        };
        let partial: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by(&input_schema)?,
            vec![],
            vec![],
            input,
            Arc::clone(&input_schema),
        )?);
        let schema = partial.schema();
        Ok(Arc::new(AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            group_by(&schema)?,
            vec![],
            vec![],
            partial,
            input_schema,
        )?))
    }

    /// Returns the number of partitions that the query keeps for a consumer that
    /// prefers its input to be distributed by the column `p`, or nothing
    /// when the rows are distributed by the column.
    fn kept_partitions(input: Arc<dyn ExecutionPlan>) -> Result<Option<usize>> {
        let keys = vec![column("p", input.schema().index_of("p")?)];
        let mut plan: Arc<dyn ExecutionPlan> =
            Arc::new(KeyDistributionExec::new(input, keys, TARGET_PARTITIONS));
        let mut config = ConfigOptions::default();
        config.execution.target_partitions = TARGET_PARTITIONS;
        for rule in get_physical_optimizers(PhysicalOptimizerOptions::default()) {
            plan = rule.optimize(plan, &config)?;
        }
        if plan.is::<KeyDistributionExec>() {
            assert_eq!(
                plan.output_partitioning().partition_count(),
                TARGET_PARTITIONS
            );
            Ok(None)
        } else {
            Ok(Some(plan.output_partitioning().partition_count()))
        }
    }

    #[test]
    fn test_columns_computed_from_distributed_rows_keep_partitions_of_query() -> Result<()> {
        // The Parquet write computes columns from the rows that it has distributed.
        // The optimizer repartitions the input of computed columns that has fewer
        // partitions than the target, which the distribution never has, so the
        // partitions of the query are kept when the distribution is removed.
        let input = explicit(Partitioning::RoundRobinBatch(3))?;
        let keys = vec![column("p", 0)];
        let distribution = Arc::new(KeyDistributionExec::new(input, keys, TARGET_PARTITIONS));
        let mut plan = projection(distribution)?;
        let mut config = ConfigOptions::default();
        config.execution.target_partitions = TARGET_PARTITIONS;
        for rule in get_physical_optimizers(PhysicalOptimizerOptions::default()) {
            plan = rule.optimize(plan, &config)?;
        }
        assert!(plan.is::<ProjectionExec>());
        assert!(!plan.children()[0].is::<KeyDistributionExec>());
        assert_eq!(plan.output_partitioning().partition_count(), 3);
        Ok(())
    }

    #[test]
    fn test_key_distribution_of_input_without_explicit_partitions() -> Result<()> {
        assert_eq!(kept_partitions(source(4)?)?, None);
        assert_eq!(kept_partitions(source(32)?)?, None);
        assert_eq!(kept_partitions(projection(source(4)?)?)?, None);
        assert_eq!(kept_partitions(filter(source(4)?)?)?, None);
        assert_eq!(kept_partitions(aggregate(source(4)?, &["p", "o"])?)?, None);
        // A single partition has all rows of every key.
        assert_eq!(kept_partitions(source(1)?)?, Some(1));
        // The groups of the aggregate are distributed by the column.
        assert_eq!(
            kept_partitions(aggregate(source(4)?, &["p"])?)?,
            Some(TARGET_PARTITIONS)
        );
        Ok(())
    }

    #[test]
    fn test_key_distribution_of_input_with_explicit_partitions() -> Result<()> {
        let p = || column("p", 0);
        let o = || column("o", 1);
        // The partitioning of the query and its number of partitions.
        let cases = || {
            [
                (Partitioning::RoundRobinBatch(3), 3),
                (Partitioning::Hash(vec![o()], 3), 3),
                (Partitioning::Hash(vec![p()], 3), 3),
                (Partitioning::UnknownPartitioning(2), 2),
                (Partitioning::UnknownPartitioning(1), 1),
            ]
        };
        // The partitions that the query asks for are kept.
        for (partitioning, partitions) in cases() {
            let name = partitioning.to_string();
            assert_eq!(
                kept_partitions(explicit(partitioning)?)?,
                Some(partitions),
                "{name}"
            );
        }
        // The optimizer repartitions the rows below a computed projection and
        // below a filter, since the query asks for fewer partitions than there
        // are threads. The rows are then distributed by the column.
        for (partitioning, _) in cases() {
            let name = partitioning.to_string();
            let plan = projection(explicit(partitioning.clone())?)?;
            assert_eq!(kept_partitions(plan)?, None, "{name}");
            let plan = filter(explicit(partitioning)?)?;
            assert_eq!(kept_partitions(plan)?, None, "{name}");
        }
        // An aggregate distributes the rows by its groups.
        for (partitioning, _) in cases() {
            let name = partitioning.to_string();
            let plan = aggregate(explicit(partitioning)?, &["p", "o"])?;
            assert_eq!(kept_partitions(plan)?, None, "{name}");
        }
        // The partitions of the query are kept below a computed projection and
        // a filter when the query asks for at least as many partitions as there
        // are threads, since the optimizer does not repartition the rows then.
        let partitioning = Partitioning::RoundRobinBatch(2 * TARGET_PARTITIONS);
        let partitions = Some(2 * TARGET_PARTITIONS);
        assert_eq!(
            kept_partitions(explicit(partitioning.clone())?)?,
            partitions
        );
        assert_eq!(
            kept_partitions(projection(explicit(partitioning.clone())?)?)?,
            partitions
        );
        assert_eq!(
            kept_partitions(filter(explicit(partitioning)?)?)?,
            partitions
        );
        Ok(())
    }
}
