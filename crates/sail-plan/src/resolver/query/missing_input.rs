use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, DFSchemaRef};
use datafusion_expr::{Expr, LogicalPlan, Projection};
use sail_common::spec;
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::repartition::ExplicitRepartitionNode;
use sail_logical_plan::sort::{RequiredSortNode, SortWithinPartitionsNode};
use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    /// Resolves expressions against the input, recovering attributes that the input
    /// removed but one of its descendants outputs, like Spark's `resolveExprsAndAddMissingAttrs`.
    /// Returns the resolved expressions and the input extended with the recovered attributes.
    pub(super) async fn resolve_expressions_with_missing_inputs(
        &self,
        expressions: Vec<spec::Expr>,
        input: LogicalPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<(Vec<Expr>, LogicalPlan)> {
        let (resolved, schema) = self
            .resolve_missing_input_expressions(expressions, &input, false, state)
            .await?;
        if Arc::ptr_eq(&schema, input.schema()) {
            return Ok((resolved, input));
        }
        let mut columns = resolved
            .iter()
            .flat_map(|expr| expr.column_refs())
            .collect::<HashSet<_>>();
        for expr in &resolved {
            expr.apply(|expr| {
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
        }
        let input = Self::add_missing_inputs(&input, &columns, state)?
            .ok_or_else(|| PlanError::internal("missing input at resolution boundary"))?;
        Ok((resolved, input))
    }

    /// Resolves each reference against the nearest reachable output. The combined
    /// schema is only for type checking, and does not extend the input plan.
    pub(super) async fn resolve_missing_input_expressions(
        &self,
        expressions: Vec<spec::Expr>,
        input: &LogicalPlan,
        resolve_aggregate_inputs: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<(Vec<Expr>, DFSchemaRef)> {
        let output_schema = Arc::clone(input.schema());
        // Most predicates only reference the input's output. Resolving them against it
        // first avoids building the descendant schema, which grows with the chain depth.
        // Subquery filters need descendant resolution before outer references. Avoid
        // resolving them speculatively: nested correlated filters would otherwise
        // resolve the entire subquery tree twice at each level.
        if state.get_outer_query_schema().is_none()
            && let Ok(resolved) = self
                .resolve_expressions(expressions.clone(), &output_schema, state)
                .await
            && !Self::has_outer_reference(&resolved)?
        {
            return Ok((resolved, output_schema));
        }
        let mut schemas = vec![Arc::clone(&output_schema)];
        let mut plan = input;
        while let Some(child) = Self::missing_input_child(plan, state) {
            schemas.push(Arc::clone(child.schema()));
            plan = child;
        }
        // Sorts can contain grouping expressions and aggregate arguments that are
        // not in the aggregate output. Rebase them before recovering inputs.
        if resolve_aggregate_inputs
            && !state.is_missing_input_boundary(plan)
            && let LogicalPlan::Aggregate(aggregate) = plan
        {
            schemas.push(Arc::clone(aggregate.input.schema()));
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
        // Spark resolves each expression independently, so discarding the bindings to an
        // output for one expression does not affect the others.
        let mut resolved = Vec::with_capacity(expressions.len());
        for expression in expressions {
            let mut schema_count = schemas.len();
            let mut first_error = None;
            let mut scope = state.enter_missing_input_scope(Arc::clone(&schema), schemas.clone());
            // TODO: Resolve ordinary references before lambda bodies, as Spark does, so
            // retrying a failed lambda body retains higher-order arguments and references
            // recovered elsewhere in the predicate.
            let expr = loop {
                match self
                    .resolve_expression(expression.clone(), &schema, scope.state())
                    .await
                {
                    Ok(expr) => break expr,
                    Err(error) => {
                        let remaining = scope
                            .state()
                            .get_missing_input_schemas(&schema)
                            .map_or(0, |schemas| schemas.len());
                        if remaining >= schema_count {
                            return Err(first_error.unwrap_or(error));
                        }
                        // Discard bindings from the failed descendant output, retaining
                        // the other outputs and outer references.
                        // Each retry removes at least one name-resolution schema.
                        first_error.get_or_insert(error);
                        schema_count = remaining;
                    }
                }
            };
            resolved.push(expr);
        }
        Ok((resolved, schema))
    }

    fn has_outer_reference(expressions: &[Expr]) -> PlanResult<bool> {
        for expr in expressions {
            if expr.exists(|expr| Ok(matches!(expr, Expr::OuterReferenceColumn(..))))? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Projects away the attributes recovered for an operator, as Spark does with
    /// `Project(child.output, ...)` above the operator.
    pub(super) fn restore_missing_input_output(
        plan: LogicalPlan,
        output_schema: DFSchemaRef,
    ) -> PlanResult<LogicalPlan> {
        if plan.schema() == &output_schema {
            return Ok(plan);
        }
        Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
            output_schema
                .columns()
                .into_iter()
                .map(Expr::Column)
                .collect(),
            Arc::new(plan),
            output_schema,
        )?))
    }

    /// Only cross operators that can carry additional columns without changing
    /// their semantics. Spark also stops at aliases and multi-input operators.
    pub(in crate::resolver) fn missing_input_child<'a>(
        plan: &'a LogicalPlan,
        state: &PlanResolverState,
    ) -> Option<&'a LogicalPlan> {
        if state.is_missing_input_boundary(plan) {
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
            // attributes. Sail's Distinct lowering cannot do so without
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

    /// Adds the columns to every operator between the plan's output and the descendant that
    /// outputs them, like Spark's `resolveExprsAndAddMissingAttrs`. Returns `None` if an
    /// operator in between cannot carry them.
    pub(super) fn add_missing_inputs(
        plan: &LogicalPlan,
        columns: &HashSet<&Column>,
        state: &PlanResolverState,
    ) -> PlanResult<Option<LogicalPlan>> {
        let missing = columns
            .iter()
            .copied()
            .filter(|column| !plan.schema().has_column(column))
            .collect::<HashSet<_>>();
        if missing.is_empty() {
            return Ok(Some(plan.clone()));
        }
        let Some(child) = Self::missing_input_child(plan, state) else {
            return Ok(None);
        };
        let Some(child) = Self::add_missing_inputs(child, &missing, state)? else {
            return Ok(None);
        };
        if let LogicalPlan::Projection(projection) = plan {
            let mut expr = projection.expr.clone();
            expr.extend(
                child
                    .schema()
                    .columns()
                    .into_iter()
                    .filter(|column| missing.contains(column))
                    .map(Expr::Column),
            );
            Ok(Some(LogicalPlan::Projection(Projection::try_new(
                expr,
                Arc::new(child),
            )?)))
        } else {
            let expressions = if matches!(plan, LogicalPlan::Unnest(_)) {
                vec![]
            } else {
                plan.expressions()
            };
            Ok(Some(plan.with_new_exprs(expressions, vec![child])?))
        }
    }
}
