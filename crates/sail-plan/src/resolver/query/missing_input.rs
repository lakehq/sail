use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, DFSchemaRef};
use datafusion_expr::{Distinct, Expr, LogicalPlan, Projection};
use sail_common::spec;
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::repartition::ExplicitRepartitionNode;
use sail_logical_plan::sort::{RequiredSortNode, SortWithinPartitionsNode};
use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

/// The resolution context of expressions that can recover missing inputs.
#[derive(Debug)]
pub(in crate::resolver) struct MissingInputResolution {
    /// The combined schema that the expressions are type-checked against.
    schema: DFSchemaRef,
    /// The operator's own input schema.
    local_schema: DFSchemaRef,
    /// The outputs that names resolve against, nearest first.
    schemas: Vec<DFSchemaRef>,
    /// Whether the expressions are sort keys. Sorts can discard even their own output.
    resolve_sort_inputs: bool,
}

impl MissingInputResolution {
    fn new(schema: DFSchemaRef, schemas: Vec<DFSchemaRef>, resolve_sort_inputs: bool) -> Self {
        let local_schema = schemas
            .first()
            .cloned()
            .unwrap_or_else(|| Arc::clone(&schema));
        Self {
            schema,
            local_schema,
            schemas,
            resolve_sort_inputs,
        }
    }

    /// Returns whether `schema` is the type-checking schema of this resolution.
    pub(in crate::resolver) fn applies_to(&self, schema: &DFSchemaRef) -> bool {
        Arc::ptr_eq(&self.schema, schema)
    }

    pub(in crate::resolver) fn schemas(&self) -> &[DFSchemaRef] {
        &self.schemas
    }

    /// Discards the bindings to one output when resolution against it fails.
    /// Sorts can discard their own output; other operators can only discard descendants.
    /// Returns whether the output is discarded.
    pub(in crate::resolver) fn discard(&mut self, index: usize) -> bool {
        if (index > 0 || self.resolve_sort_inputs) && index < self.schemas.len() {
            self.schemas.remove(index);
            return true;
        }
        false
    }

    /// Discards the bindings to one output and all deeper outputs.
    pub(in crate::resolver) fn discard_from(&mut self, index: usize) {
        if index > 0 || self.resolve_sort_inputs {
            self.schemas.truncate(index);
        }
    }
}

/// An output whose descendants cannot participate in missing-reference resolution.
/// The output is paired with its input so that a pass-through projection that
/// reproduces the output over a wider input (e.g. a join) is not a boundary.
#[derive(Debug)]
struct MissingInputBoundary {
    output: DFSchemaRef,
    input: Option<DFSchemaRef>,
}

/// The outputs whose descendants cannot participate in missing-reference resolution.
#[derive(Debug, Default)]
pub(in crate::resolver) struct MissingInputBoundaries {
    boundaries: Vec<MissingInputBoundary>,
}

impl MissingInputBoundaries {
    /// Registers the plan as a missing-input boundary.
    pub(super) fn register(&mut self, plan: &LogicalPlan) {
        let mut plan = plan;
        // Empty outputs can share a schema with unrelated plans. Stop recovery at
        // the first nonempty input instead, whose field IDs distinguish the boundary.
        while plan.schema().fields().is_empty() {
            let Some(child) = self.child(plan) else {
                return;
            };
            if !child.schema().fields().is_empty() {
                break;
            }
            plan = child;
        }
        self.boundaries.push(MissingInputBoundary {
            output: Arc::clone(plan.schema()),
            input: plan
                .inputs()
                .first()
                .map(|input| Arc::clone(input.schema())),
        });
    }

    /// Returns whether the plan is a registered boundary.
    fn contains(&self, plan: &LogicalPlan) -> bool {
        // Rewriters can rebuild schemas with different types or nullability.
        // Retain the original schemas cheaply, but compare only column identities.
        let matches = |left: &DFSchemaRef, right: &DFSchemaRef| {
            Arc::ptr_eq(left, right)
                || (left.fields().len() == right.fields().len()
                    && left
                        .iter()
                        .map(|(qualifier, field)| (qualifier, field.name()))
                        .eq(right
                            .iter()
                            .map(|(qualifier, field)| (qualifier, field.name()))))
        };
        self.boundaries.iter().any(|boundary| {
            matches(&boundary.output, plan.schema())
                && match (&boundary.input, plan.inputs().first()) {
                    (Some(schema), Some(child)) => matches(schema, child.schema()),
                    (None, None) => true,
                    _ => false,
                }
        })
    }

    /// Only cross operators that can carry additional columns without changing
    /// their semantics. Spark also stops at aliases and multi-input operators.
    fn child<'a>(&self, plan: &'a LogicalPlan) -> Option<&'a LogicalPlan> {
        if self.contains(plan) {
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
}

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
        let mut columns = HashSet::new();
        for expr in &resolved {
            expr.apply(|expr| {
                let subquery = match expr {
                    Expr::Column(column) => {
                        columns.insert(column);
                        return Ok(TreeNodeRecursion::Continue);
                    }
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
        resolve_sort_inputs: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<(Vec<Expr>, DFSchemaRef)> {
        let output_schema = Arc::clone(input.schema());
        // Most predicates only reference the input's output. Resolving them against it
        // first avoids building the descendant schema, which grows with the chain depth.
        // Subquery filters need descendant resolution before outer references. Avoid
        // resolving them speculatively: nested correlated filters would otherwise
        // resolve the entire subquery tree twice at each level.
        // A predicate without subqueries costs at most one more cheap pass.
        let in_subquery = state.get_outer_query_schema().is_some();
        let mut local = Vec::with_capacity(expressions.len());
        for expression in &expressions {
            if in_subquery && !Self::is_subquery_free(expression) {
                break;
            }
            match self
                .resolve_expression(expression.clone(), &output_schema, state)
                .await
            {
                Ok(expr) if !Self::has_outer_reference(std::slice::from_ref(&expr))? => {
                    local.push(expr);
                }
                // Stop at the first missing input rather than speculatively
                // resolving every remaining expression against a schema that
                // may be missing most of their references.
                _ => break,
            }
        }
        if local.len() == expressions.len() {
            return Ok((local, output_schema));
        }
        let mut schemas = vec![Arc::clone(&output_schema)];
        let mut plan = input;
        while let Some(child) = Self::missing_input_child(plan, state) {
            schemas.push(Arc::clone(child.schema()));
            plan = child;
        }
        if resolve_sort_inputs && !state.missing_input_boundaries().contains(plan) {
            match plan {
                // Sorts can contain grouping expressions and aggregate arguments that are
                // not in the aggregate output. Rebase them before recovering inputs.
                LogicalPlan::Aggregate(aggregate) => {
                    schemas.push(Arc::clone(aggregate.input.schema()));
                }
                // Spark's projected attributes keep their qualifiers, so a sort over DISTINCT
                // can reference `t.a` for a projected `t.a`. Sail's projection renames them,
                // so resolve such references against the attributes that it passes through.
                // The logical plan builder adds them below the DISTINCT, which keeps the
                // distinct rows unchanged since they duplicate projected columns.
                LogicalPlan::Distinct(Distinct::All(input)) => {
                    if let LogicalPlan::Projection(projection) = input.as_ref()
                        && !state.missing_input_boundaries().contains(input)
                    {
                        schemas.push(Self::projected_attributes(projection, state)?);
                    }
                }
                _ => {}
            }
        }

        // Type inference needs all reachable columns, while name resolution must
        // prefer the nearest output for each attribute independently. In particular,
        // retrying the entire predicate on a child loses projected aliases.
        let schema = if let [schema] = schemas.as_slice() {
            // Without reachable descendants (e.g. most subquery filters), the output
            // has every column. Reuse it so that no input needs to be recovered.
            Arc::clone(schema)
        } else {
            // The combined schema can hold every descendant's fields. Hash them with
            // DataFusion's faster hasher rather than SipHash.
            let mut columns = datafusion_common::HashSet::new();
            let fields = schemas
                .iter()
                .flat_map(|schema| schema.iter())
                .filter(|(qualifier, field)| columns.insert((*qualifier, field.name())))
                .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
                .collect();
            Arc::new(DFSchema::new_with_metadata(
                fields,
                output_schema.metadata().clone(),
            )?)
        };
        // Spark resolves each expression independently, so discarding the bindings to an
        // output for one expression does not affect the others.
        // Keep the successfully resolved prefix: one missing sort or partitioning
        // key must not force preceding visible keys to resolve again.
        let mut resolved = local;
        for expression in expressions.into_iter().skip(resolved.len()) {
            let mut schema_count = schemas.len();
            let mut first_error = None;
            let mut scope = state.enter_missing_input_scope(MissingInputResolution::new(
                Arc::clone(&schema),
                schemas.clone(),
                resolve_sort_inputs,
            ));
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
                            .missing_input(&schema)
                            .map_or(0, |input| input.schemas().len());
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

    /// Returns the operator's own input schema when `schema` is the type-checking schema
    /// for missing-input resolution. Expansions such as `*` only see this schema.
    pub(in crate::resolver) fn local_schema(
        schema: &DFSchemaRef,
        state: &PlanResolverState,
    ) -> DFSchemaRef {
        state.missing_input(schema).map_or_else(
            || Arc::clone(schema),
            |input| Arc::clone(&input.local_schema),
        )
    }

    /// Returns the input attributes that the projection outputs without renaming them,
    /// which keep their qualifiers in Spark (unlike aliases and computed columns).
    fn projected_attributes(
        projection: &Projection,
        state: &PlanResolverState,
    ) -> PlanResult<DFSchemaRef> {
        let name = |id: &str| state.get_field_info(id).ok().map(|info| info.name());
        let input = projection.input.schema();
        let mut seen = HashSet::new();
        let fields = projection
            .expr
            .iter()
            .zip(projection.schema.fields())
            .filter_map(|(expr, field)| {
                let Expr::Alias(alias) = expr else {
                    return None;
                };
                let Expr::Column(column) = alias.expr.as_ref() else {
                    return None;
                };
                let (qualifier, source) = input.qualified_field_from_column(column).ok()?;
                (name(field.name())? == name(source.name())?
                    && seen.insert((qualifier, source.name())))
                .then(|| (qualifier.cloned(), Arc::clone(source)))
            })
            .collect();
        Ok(Arc::new(DFSchema::new_with_metadata(
            fields,
            input.metadata().clone(),
        )?))
    }

    /// Returns whether the expression cannot contain a subquery. Expressions that
    /// are not listed here are conservatively assumed to contain one.
    fn is_subquery_free(expr: &spec::Expr) -> bool {
        match expr {
            spec::Expr::Literal(_) | spec::Expr::UnresolvedAttribute { .. } => true,
            spec::Expr::UnresolvedFunction(function) => {
                function.filter.is_none()
                    && function.order_by.is_none()
                    && function.named_arguments.is_empty()
                    && function.arguments.iter().all(Self::is_subquery_free)
            }
            spec::Expr::Alias { expr, .. }
            | spec::Expr::Cast { expr, .. }
            | spec::Expr::IsFalse(expr)
            | spec::Expr::IsNotFalse(expr)
            | spec::Expr::IsTrue(expr)
            | spec::Expr::IsNotTrue(expr)
            | spec::Expr::IsNull(expr)
            | spec::Expr::IsNotNull(expr)
            | spec::Expr::IsUnknown(expr)
            | spec::Expr::IsNotUnknown(expr) => Self::is_subquery_free(expr),
            spec::Expr::SortOrder(sort) => Self::is_subquery_free(&sort.child),
            spec::Expr::UnresolvedExtractValue { child, extraction } => {
                Self::is_subquery_free(child) && Self::is_subquery_free(extraction)
            }
            spec::Expr::IsDistinctFrom { left, right }
            | spec::Expr::IsNotDistinctFrom { left, right } => {
                Self::is_subquery_free(left) && Self::is_subquery_free(right)
            }
            spec::Expr::Between {
                expr, low, high, ..
            } => {
                Self::is_subquery_free(expr)
                    && Self::is_subquery_free(low)
                    && Self::is_subquery_free(high)
            }
            spec::Expr::InList { expr, list, .. } => {
                Self::is_subquery_free(expr) && list.iter().all(Self::is_subquery_free)
            }
            _ => false,
        }
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

    /// Returns the input that missing-reference resolution can cross into from the plan.
    pub(super) fn missing_input_child<'a>(
        plan: &'a LogicalPlan,
        state: &PlanResolverState,
    ) -> Option<&'a LogicalPlan> {
        state.missing_input_boundaries().child(plan)
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
            let mut fields = child
                .schema()
                .functional_dependencies()
                .is_empty()
                .then(|| {
                    projection
                        .schema
                        .iter()
                        .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
                        .collect::<Vec<_>>()
                });
            for (qualifier, field) in child.schema().iter() {
                let column = Column::new(qualifier.cloned(), field.name());
                if missing.contains(&column) {
                    expr.push(Expr::Column(column));
                    if let Some(fields) = &mut fields {
                        fields.push((qualifier.cloned(), Arc::clone(field)));
                    }
                }
            }
            // Carrying extra columns leaves the original projection's fields unchanged.
            // Reuse them instead of resolving every expression against the wider child
            // again, which is quadratic in the width for column projections.
            // Let DataFusion recompute dependencies when the input has any to propagate.
            let projection = if let Some(fields) = fields {
                let schema = Arc::new(DFSchema::new_with_metadata(
                    fields,
                    child.schema().metadata().clone(),
                )?);
                Projection::try_new_with_schema(expr, Arc::new(child), schema)?
            } else {
                Projection::try_new(expr, Arc::new(child))?
            };
            Ok(Some(LogicalPlan::Projection(projection)))
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
