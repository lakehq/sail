use std::sync::Arc;

use datafusion_common::tree_node::TreeNode;
use datafusion_expr::{Expr, Extension, Filter, LogicalPlan, LogicalPlanBuilder};
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::misc::monotonically_increasing_id::SparkMonotonicallyIncreasingId;
use sail_logical_plan::partition_filter::PartitionFilterNode;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_filter(
        &self,
        input: spec::QueryPlan,
        condition: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let output_schema = Arc::clone(input.schema());
        let (predicate, input) = self
            .resolve_expressions_with_missing_inputs(vec![condition], input, state)
            .await?;
        let filter = self.rewrite_filter(input, predicate.one()?, false, state)?;
        Self::restore_missing_input_output(filter, output_schema)
    }

    pub(super) fn rewrite_filter(
        &self,
        input: LogicalPlan,
        predicate: Expr,
        having: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let stateful = predicate.exists(|expr| {
            Ok(matches!(expr, Expr::ScalarFunction(function)
                if function.func.inner().is::<SparkMonotonicallyIncreasingId>()))
        })?;
        let (input, predicate) = if stateful {
            let input = Self::require_input_sort_inner(input)?.0;
            // Keep predicates at their original input while allowing the normal
            // Filter analyzer and subquery rewrites to run inside this boundary.
            let input = LogicalPlan::Extension(Extension {
                node: Arc::new(PartitionFilterNode::new(Arc::new(input))),
            });
            (input, predicate)
        } else {
            let (input, predicates) =
                self.rewrite_partition_expressions(input, vec![predicate], state)?;
            (input, predicates.one()?)
        };
        let filter = if having {
            LogicalPlanBuilder::from(input).having(predicate)?.build()?
        } else {
            LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(input))?)
        };
        if stateful {
            Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(PartitionFilterNode::new(Arc::new(filter))),
            }))
        } else {
            Ok(filter)
        }
    }
}
