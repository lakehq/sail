use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema};
use datafusion_expr::{Expr, Filter, LogicalPlan, Projection};
use sail_common::spec;
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::repartition::ExplicitRepartitionNode;
use sail_logical_plan::sort::{RequiredSortNode, SortWithinPartitionsNode};
use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;

use crate::error::{PlanError, PlanResult};
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
        // Most predicates only reference the input's output. Resolving them against it
        // first avoids building the descendant schema, which grows with the chain depth.
        // An outer reference may instead belong to a descendant, so it takes the full path.
        if let Ok(predicate) = self
            .resolve_expression(condition.clone(), &output_schema, state)
            .await
            && !predicate.exists(|expr| Ok(matches!(expr, Expr::OuterReferenceColumn(..))))?
        {
            return Ok(LogicalPlan::Filter(Filter::try_new(
                predicate,
                Arc::new(input),
            )?));
        }
        let mut schemas = vec![Arc::clone(&output_schema)];
        let mut plan = &input;
        while let Some(child) = Self::filter_missing_input_child(plan, state) {
            schemas.push(Arc::clone(child.schema()));
            plan = child;
        }

        // Type inference needs all reachable columns, while name resolution must
        // prefer the nearest output for each attribute independently. In particular,
        // retrying the entire predicate on a child loses projected aliases.
        let mut columns = HashSet::new();
        let fields = schemas
            .iter()
            .flat_map(|schema| schema.iter())
            .filter(|(qualifier, field)| {
                columns.insert(Column::new(qualifier.cloned(), field.name()))
            })
            .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
            .collect();
        let schema = Arc::new(DFSchema::new_with_metadata(
            fields,
            output_schema.metadata().clone(),
        )?);
        let predicate = {
            let mut schema_count = schemas.len();
            let mut first_error = None;
            let mut scope = state.enter_filter_scope(Arc::clone(&schema), schemas);
            loop {
                match self
                    .resolve_expression(condition.clone(), &schema, scope.state())
                    .await
                {
                    Ok(predicate) => break predicate,
                    Err(error) => {
                        let remaining = scope
                            .state()
                            .get_filter_schemas(&schema)
                            .map_or(0, |schemas| schemas.len());
                        if remaining >= schema_count {
                            return Err(first_error.unwrap_or(error));
                        }
                        // Discard bindings from the failed descendant and deeper
                        // outputs, retaining earlier outputs and outer references.
                        // Each retry removes at least one name-resolution schema.
                        first_error.get_or_insert(error);
                        schema_count = remaining;
                    }
                }
            }
        };
        let mut columns = predicate.column_refs();
        predicate.apply(|expr| {
            let subquery = match expr {
                Expr::Exists(exists) => &exists.subquery,
                Expr::InSubquery(in_subquery) => &in_subquery.subquery,
                Expr::ScalarSubquery(subquery) => subquery,
                _ => return Ok(TreeNodeRecursion::Continue),
            };
            // Tuple IN is lowered to EXISTS with its left-hand columns represented
            // as outer references, which column_refs() deliberately excludes.
            // Nested lateral joins also advertise references to their own inputs;
            // only references reachable from this filter need to be recovered here.
            // TODO: Support nested tuple IN values that already reference an enclosing
            // query; their scope must survive EXISTS lowering and decorrelation.
            for expr in &subquery.outer_ref_columns {
                if let Expr::OuterReferenceColumn(_, column) = expr
                    && schema.has_column(column)
                {
                    columns.insert(column);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let input = Self::add_filter_missing_inputs(input, &columns, state)?;
        let restore_output = input.schema() != &output_schema;
        let filter = LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(input))?);
        if restore_output {
            Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                output_schema
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .collect(),
                Arc::new(filter),
                output_schema,
            )?))
        } else {
            Ok(filter)
        }
    }

    /// Only cross operators that can carry additional columns without changing
    /// their semantics. Spark also stops at aliases and multi-input operators.
    fn filter_missing_input_child<'a>(
        plan: &'a LogicalPlan,
        state: &PlanResolverState,
    ) -> Option<&'a LogicalPlan> {
        if state.is_filter_input_boundary(plan) {
            return None;
        }
        let transparent = match plan {
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::Unnest(_) => true,
            LogicalPlan::Extension(extension) => {
                let node = extension.node.as_any();
                node.is::<ExplicitRepartitionNode>()
                    || node.is::<SortWithinPartitionsNode>()
                    || node.is::<RequiredSortNode>()
                    || node.is::<MonotonicIdNode>()
                    || node.is::<SparkPartitionIdNode>()
            }
            // TODO: Spark DataFrame distinct uses Deduplicate and can carry missing
            // filter attributes. Sail's Distinct lowering cannot do so without
            // changing its deduplication keys; preserve those keys before supporting it.
            // The same applies to dropDuplicates with a subset, which Sail lowers to DistinctOn.
            // TODO: Spark's LateralJoin is a unary node over its left input, so a filter can
            // recover attributes removed from that input. Sail lowers it to a Join instead.
            _ => false,
        };
        transparent
            .then(|| plan.inputs().first().copied())
            .flatten()
    }

    fn add_filter_missing_inputs(
        plan: LogicalPlan,
        columns: &HashSet<&Column>,
        state: &PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let missing = columns
            .iter()
            .copied()
            .filter(|column| !plan.schema().has_column(column))
            .collect::<HashSet<_>>();
        if missing.is_empty() {
            return Ok(plan);
        }
        let child = Self::filter_missing_input_child(&plan, state)
            .ok_or_else(|| PlanError::internal("missing filter input at resolution boundary"))?;
        let child = Self::add_filter_missing_inputs(child.clone(), &missing, state)?;
        if let LogicalPlan::Projection(mut projection) = plan {
            projection.expr.extend(
                child
                    .schema()
                    .columns()
                    .into_iter()
                    .filter(|column| missing.contains(column))
                    .map(Expr::Column),
            );
            Ok(LogicalPlan::Projection(Projection::try_new(
                projection.expr,
                Arc::new(child),
            )?))
        } else {
            let expressions = if matches!(plan, LogicalPlan::Unnest(_)) {
                vec![]
            } else {
                plan.expressions()
            };
            Ok(plan.with_new_exprs(expressions, vec![child])?)
        }
    }
}
