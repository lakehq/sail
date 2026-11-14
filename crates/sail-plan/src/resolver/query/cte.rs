use std::any::Any;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::RecordBatch;
use datafusion::physical_plan::collect;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_expr::{Expr, LogicalPlan, SubqueryAlias};
use sail_common::spec;
use sail_common_datafusion::input_file::is_input_file_metadata_function;
use sail_data_source::reuse::{is_python_source, is_reusable_source};
use sail_function::scalar::misc::scalar_subquery_result::ScalarSubqueryResult;
use sail_logical_plan::map_partitions::MapPartitionsNode;
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::range::RangeNode;
use sail_logical_plan::repartition::ExplicitRepartitionNode;
use sail_logical_plan::seeded_random::SeededRandomNode;
use sail_logical_plan::sort::{RequiredSortNode, SortWithinPartitionsNode};
use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;
use sail_python_udf::udf::pyspark_udtf::PySparkUDTF;

use crate::error::PlanResult;
use crate::plan_with_shared_ctes;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;
use crate::scalar_subquery_key::is_volatile_call;
use crate::shared_cte::{
    CteDefinition, CteReference, contains_reference, inline_deterministic_references,
    inline_references, project_in_references, renew_cte_definitions,
};

/// Whether a named relation is a CTE definition or a parameter view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::resolver) enum CteKind {
    /// A CTE defined by a `WITH` clause.
    Definition,
    /// A DataFrame argument of `spark.sql`, referenced by its view name.
    ParameterView,
}

/// A CTE definition or a parameter view, with the attribute identities of its output.
#[derive(Debug)]
pub(in crate::resolver) struct CteInfo {
    pub(super) plan: Arc<LogicalPlan>,
    /// Whether this is a CTE definition or a parameter view.
    kind: CteKind,
    /// The attribute identity of each output field.
    origins: Vec<usize>,
    /// The DataFrame plan IDs bound to the output attributes when the CTE is defined.
    bindings: datafusion_common::HashMap<usize, datafusion_common::HashSet<i64>>,
    /// How the references to the CTE can share one evaluation of it, if they can.
    sharing: Option<CteSharing>,
}

/// The state of a CTE definition whose references may share one evaluation of it.
#[derive(Debug)]
struct CteSharing {
    /// The position of the definition among the CTE definitions of the query.
    index: usize,
    name: datafusion_common::TableReference,
    /// Whether the definition is inside a subquery, where it can refer to outer columns.
    nested: bool,
    /// Whether the references can share one evaluation of the definition, as
    /// `Some(volatile)`, decided when the CTE is first referenced.
    shareable: OnceLock<Option<bool>>,
    /// The shareable definition.
    definition: OnceLock<Arc<CteDefinition>>,
}

impl CteInfo {
    pub(in crate::resolver) fn try_new(
        plan: LogicalPlan,
        kind: CteKind,
        state: &PlanResolverState,
    ) -> PlanResult<Self> {
        let mut origins = Vec::with_capacity(plan.schema().fields().len());
        let mut bindings = datafusion_common::HashMap::new();
        for field in plan.schema().fields() {
            let info = state.get_field_info(field.name())?;
            origins.push(info.origin());
            let plan_ids = info.plan_ids().collect::<datafusion_common::HashSet<_>>();
            if !plan_ids.is_empty() {
                bindings.insert(info.origin(), plan_ids);
            }
        }
        Ok(Self {
            plan: Arc::new(plan),
            kind,
            origins,
            bindings,
            sharing: None,
        })
    }

    /// Lets the repeated references to the CTE definition share one evaluation of it.
    fn with_sharing(
        mut self,
        name: datafusion_common::TableReference,
        state: &mut PlanResolverState,
    ) -> Self {
        self.sharing = Some(CteSharing {
            index: state.next_cte_definition_index(),
            name,
            nested: state.get_outer_query_schema().is_some(),
            shareable: OnceLock::new(),
            definition: OnceLock::new(),
        });
        self
    }

    /// Returns a scan of the CTE definition for a reference, or `None` if the reference
    /// must evaluate the definition itself. The first reference to a definition that is
    /// not volatile is the definition itself, which becomes a scan only if the definition
    /// is referenced again, so that a CTE that is referenced once is resolved as if it was
    /// written in place. The first reference to a volatile definition is a scan, so that
    /// no rewrite of the plan can separate it from the other references.
    pub(super) fn scan(
        &self,
        first: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<LogicalPlan>> {
        let Some(sharing) = &self.sharing else {
            return Ok(None);
        };
        let shareable = match sharing.shareable.get() {
            Some(shareable) => *shareable,
            None => {
                let shareable = shareable_definition(&self.plan, sharing.nested)?;
                *sharing.shareable.get_or_init(|| shareable)
            }
        };
        let Some(volatile) = shareable else {
            return Ok(None);
        };
        if first && !volatile {
            return Ok(None);
        }
        let definition = sharing.definition.get_or_init(|| {
            let definition = Arc::new(CteDefinition::new(
                sharing.index,
                sharing.name.clone(),
                Arc::clone(&self.plan),
                volatile,
            ));
            state.register_shared_cte_definition(Arc::clone(&definition));
            definition
        });
        Ok(Some(definition.scan()?))
    }

    /// Spark renews repeated CTERelationRef and parameter-view attributes
    /// separately. Identity projections preserve the original attribute IDs.
    pub(super) fn renew_reference(
        &self,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<Vec<String>>> {
        let origins = match self.kind {
            CteKind::Definition => state.cte_reference_origins_mut(),
            CteKind::ParameterView => state.parameter_view_origins_mut(),
        };
        if !self.origins.iter().any(|origin| origins.contains(origin)) {
            // SQL outputs may acquire a DataFrame plan ID only after this query
            // finishes. Their repeated CTE references already need fresh identities.
            origins.extend(self.origins.iter().copied());
            return Ok(None);
        }
        let mut renewed = datafusion_common::HashMap::with_capacity(self.origins.len());
        let mut names = Vec::with_capacity(self.origins.len());
        for (field, &original) in self.plan.schema().fields().iter().zip(&self.origins) {
            let info = state.get_field_info(field.name())?;
            let (name, hidden) = (info.name().to_string(), info.is_hidden());
            // Spark's CTERelationRef.newInstance preserves duplicate attributes
            // within one output while giving the reference fresh identities.
            let origin = *renewed
                .entry(original)
                .or_insert_with(|| state.next_origin());
            let plan_ids = match self.kind {
                CteKind::Definition => datafusion_common::HashSet::new(),
                CteKind::ParameterView => self.bindings.get(&original).cloned().unwrap_or_default(),
            };
            names.push(state.register_field_with_origin(name, hidden, origin, plan_ids));
            if self.kind == CteKind::ParameterView {
                state.parameter_view_origins_mut().insert(origin);
            }
        }
        Ok(Some(names))
    }
}

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_with_ctes(
        &self,
        input: spec::QueryPlan,
        recursive: bool,
        ctes: Vec<(spec::Identifier, spec::QueryPlan)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        // Deduplicate CTEs - keep the last occurrence of each name (shadowing behavior)
        // This matches Spark's behavior where later CTEs can shadow earlier ones
        let mut seen_names: HashSet<spec::Identifier> = HashSet::new();
        let ctes: Vec<_> = ctes
            .into_iter()
            .rev() // Reverse to keep last occurrence
            .filter(|(name, _)| seen_names.insert(name.clone()))
            .collect::<Vec<_>>()
            .into_iter()
            .rev() // Reverse back to original order
            .collect();
        let mut scope = state.enter_cte_scope();
        let state = scope.state();
        for (name, query) in ctes.into_iter() {
            let reference = self.resolve_table_reference(&spec::ObjectName::bare(name.clone()))?;
            let plan = if recursive {
                self.resolve_recursive_query_plan(query, state).await?
            } else {
                self.resolve_query_plan(query, state).await?
            };
            let plan = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                Arc::new(plan),
                reference.clone(),
            )?);
            let share = self.share_ctes && !recursive;
            // A scan of the definition hides its parameters from the query.
            let plan = if share {
                state.bind_param_values(plan)?
            } else {
                plan
            };
            let cte = CteInfo::try_new(plan, CteKind::Definition, state)?;
            let cte = if share {
                cte.with_sharing(reference.clone(), state)
            } else {
                cte
            };
            state.insert_cte_info(reference, cte);
        }
        let plan = self.resolve_query_plan(input, state).await?;
        // Spark's `WithCTE` also has the CTE definitions as children, so
        // missing-reference recovery resolves only against the query output.
        Self::restore_cte_output_bindings(&plan, state)?;
        state.register_missing_input_boundary(&plan);
        Ok(plan)
    }

    /// A Union is a plan-ID lookup leaf, but WithCTE also exposes its definitions.
    /// Restore only bindings whose original attributes survive in the output.
    fn restore_cte_output_bindings(
        plan: &LogicalPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<()> {
        let definitions = state
            .ctes()
            .filter(|cte| cte.kind == CteKind::Definition && !cte.bindings.is_empty())
            .cloned()
            .collect::<Vec<_>>();
        if definitions.is_empty() {
            return Ok(());
        }
        for field in plan.schema().fields() {
            let origin = state.get_field_info(field.name())?.origin();
            for cte in &definitions {
                for &plan_id in cte.bindings.get(&origin).into_iter().flatten() {
                    state.register_plan_id_for_field(field.name(), plan_id)?;
                }
            }
        }
        Ok(())
    }

    /// Turns the first reference to each shared CTE definition in the plan into a scan
    /// like its repeated references. The first reference is resolved as the definition
    /// itself, so a CTE that is referenced once is resolved as if it was written in
    /// place. A first reference that the resolver has rewritten since remains as it is.
    pub(in crate::resolver) fn scan_shared_cte_references(
        plan: LogicalPlan,
        state: &PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let definitions = state.shared_cte_definitions();
        if definitions.is_empty() {
            return Ok(plan);
        }
        let first_references = definitions
            .iter()
            .filter_map(|definition| match definition.resolved().as_ref() {
                LogicalPlan::SubqueryAlias(alias) => {
                    Some((Arc::as_ptr(&alias.input), Arc::clone(definition)))
                }
                _ => None,
            })
            .collect::<datafusion_common::HashMap<_, _>>();
        Ok(plan
            .transform_down_with_subqueries(|plan| {
                if let LogicalPlan::SubqueryAlias(alias) = &plan
                    && let Some(definition) = first_references.get(&Arc::as_ptr(&alias.input))
                {
                    return Ok(Transformed::new(
                        definition.scan()?,
                        true,
                        TreeNodeRecursion::Jump,
                    ));
                }
                Ok(Transformed::no(plan))
            })
            .data()?)
    }

    /// Turns the first reference to each shared CTE definition into a scan, in the plan
    /// and in the definitions, which get the same treatment of order-sensitive
    /// aggregates as the plan.
    pub(in crate::resolver) fn scan_shared_cte_definitions(
        plan: LogicalPlan,
        state: &PlanResolverState,
        preserve_order_sensitive_aggregate_sorts: bool,
    ) -> PlanResult<LogicalPlan> {
        for definition in state.shared_cte_definitions() {
            let LogicalPlan::SubqueryAlias(mut alias) = definition.resolved().as_ref().clone()
            else {
                continue;
            };
            alias.input = Arc::new(Self::scan_shared_cte_references(
                Arc::unwrap_or_clone(alias.input),
                state,
            )?);
            let plan = LogicalPlan::SubqueryAlias(alias);
            definition.set_plan(if preserve_order_sensitive_aggregate_sorts {
                Self::preserve_order_sensitive_aggregate_sorts(plan)?
            } else {
                plan
            });
        }
        let plan = Self::scan_shared_cte_references(plan, state)?;
        Ok(project_in_references(plan)?)
    }

    /// Returns the plan of a stored view in which the references to shared CTE definitions
    /// refer to copies of the definitions, which the query evaluates with its own CTEs.
    pub(in crate::resolver) fn renew_shared_cte_definitions(
        &self,
        plan: LogicalPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut definitions = vec![];
        let plan = renew_cte_definitions(
            plan,
            &mut || state.next_cte_definition_index(),
            &mut |definition| definitions.push(definition),
        )?;
        for definition in definitions {
            state.register_shared_cte_definition(definition);
        }
        Ok(plan)
    }

    /// Replaces the references to shared CTE definitions by the definitions, for a plan
    /// that is not optimized with its shared CTEs.
    pub(in crate::resolver) fn inline_shared_cte_references(
        plan: LogicalPlan,
    ) -> PlanResult<LogicalPlan> {
        Ok(inline_references(plan, &|_| true)?)
    }

    /// Executes a plan whose output the resolver needs. Each volatile CTE that the plan
    /// refers to is evaluated once for all its references in the plan, as in a query.
    pub(in crate::resolver) fn execute_auxiliary_plan(
        &self,
        plan: LogicalPlan,
    ) -> Pin<Box<dyn Future<Output = PlanResult<Vec<RecordBatch>>> + Send + '_>> {
        // Keep auxiliary execution out of the recursive resolver futures, including
        // the ordinary planning path used when there are no shared references.
        Box::pin(async move {
            let plan = inline_deterministic_references(plan)?;
            if !contains_reference(&plan)? {
                return Ok(self.ctx.execute_logical_plan(plan).await?.collect().await?);
            }
            // The plan refers to copies of the definitions, so that planning it leaves the
            // definitions as they are for the query.
            let mut index = 0;
            let plan = renew_cte_definitions(
                plan,
                &mut || {
                    index += 1;
                    index - 1
                },
                &mut |_| {},
            )?;
            let session_state = self.ctx.state();
            let (_, plan) = plan_with_shared_ctes(&session_state, plan).await?;
            Ok(collect(plan, session_state.task_ctx()).await?)
        })
    }
}

/// Returns whether the references to the CTE definition can share one evaluation of it,
/// as `Some(volatile)`. A volatile definition is evaluated once for all its references,
/// since each evaluation can return different rows.
///
/// Each reference evaluates the definition itself if the definition refers to outer
/// columns, or, unless the definition is volatile, if it reads the files or the
/// partitions of its input, calls an external service, depends on the evaluation of a
/// correlated subquery, if a source is read once for each reference, if its consumers can
/// depend on the order of its rows, or if it only projects and filters the rows of one
/// source, which is not worth keeping for another reference.
fn shareable_definition(plan: &Arc<LogicalPlan>, nested: bool) -> PlanResult<Option<bool>> {
    let mut input = plan.as_ref();
    let mut subquery = false;
    loop {
        input.apply_expressions(|expr| {
            subquery |= expr.exists(|expr| {
                Ok(matches!(
                    expr,
                    Expr::ScalarSubquery(_)
                        | Expr::Exists(_)
                        | Expr::InSubquery(_)
                        | Expr::SetComparison(_)
                ))
            })?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        input = match input {
            LogicalPlan::Projection(projection) => &projection.input,
            LogicalPlan::Filter(filter) => &filter.input,
            LogicalPlan::SubqueryAlias(alias) => &alias.input,
            _ => break,
        };
    }
    let trivial = input.inputs().is_empty() && !subquery;
    // Like Spark, only the operators of the definition can refer to the outer query.
    // A subquery in the definition refers to the definition's own columns, since a
    // query can only refer to the columns of its immediate outer query.
    let mut shareable = !(nested && plan.exists(|plan| Ok(plan.contains_outer_reference()))?);
    let mut volatile = false;
    // Whether each reference evaluates the definition itself unless it is volatile.
    let mut per_reference = false;
    plan.apply_with_subqueries(|plan| {
        shareable = shareable
            && match plan {
                LogicalPlan::TableScan(scan) => {
                    let python = is_python_source(&scan.source);
                    per_reference |= python;
                    python
                        || CteReference::try_from_scan(scan).is_some()
                        || is_reusable_source(&scan.source)
                }
                LogicalPlan::Extension(extension) => {
                    let node = extension.node.as_any();
                    // The output of these nodes depends on the partitions of their input.
                    let partitioned = node.is::<SeededRandomNode>()
                        || node.is::<MonotonicIdNode>()
                        || node.is::<SparkPartitionIdNode>();
                    let udtf = is_nondeterministic_udtf(node);
                    volatile |= partitioned || udtf;
                    // Each reference evaluates these nodes itself, unless the definition is
                    // volatile and must be evaluated once.
                    let per_reference_node = (node.is::<MapPartitionsNode>() && !udtf)
                        || node.is::<ExplicitRepartitionNode>();
                    per_reference |= per_reference_node;
                    partitioned
                        || udtf
                        || per_reference_node
                        || node.is::<RangeNode>()
                        || node.is::<RequiredSortNode>()
                        || node.is::<SortWithinPartitionsNode>()
                }
                LogicalPlan::RecursiveQuery(_) => false,
                _ => true,
            };
        if shareable {
            plan.apply_expressions(|expr| {
                expr.apply(|expr| {
                    if is_volatile_call(expr) {
                        if is_evaluated_once_call(expr) {
                            volatile = true;
                        } else {
                            per_reference = true;
                        }
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })?;
        }
        Ok(if shareable {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    })?;
    if !shareable {
        return Ok(None);
    }
    if volatile {
        return Ok(Some(true));
    }
    if per_reference || trivial {
        return Ok(None);
    }
    let (_, sorted, _) = PlanResolver::require_input_sort_inner(plan.as_ref().clone())?;
    Ok((!sorted).then_some(false))
}

/// Returns whether the volatile call returns different values for each evaluation, which
/// Spark evaluates once for all the references to a CTE. The other volatile calls depend
/// on the input files or on the evaluation of a correlated subquery, or call an external
/// service, and do not make a definition volatile by themselves.
fn is_evaluated_once_call(expr: &Expr) -> bool {
    match expr {
        Expr::ScalarFunction(function) => {
            function.func.as_async().is_none()
                && !function.func.inner().is::<ScalarSubqueryResult>()
                && !is_input_file_metadata_function(&function.func)
        }
        _ => true,
    }
}

/// Returns whether the node calls a nondeterministic Python UDTF, which Spark evaluates
/// once for all the references to a CTE.
fn is_nondeterministic_udtf(node: &dyn Any) -> bool {
    node.downcast_ref::<MapPartitionsNode>()
        .and_then(|node| (node.udf().as_ref() as &dyn Any).downcast_ref::<PySparkUDTF>())
        .is_some_and(|udtf| !udtf.deterministic())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::DataType;
    use datafusion::functions::math::expr_fn::random;
    use datafusion_common::Spans;
    use datafusion_expr::{Expr, LogicalPlanBuilder, Subquery, col, lit, out_ref_col};

    use super::shareable_definition;
    use crate::error::PlanResult;

    fn values(alias: &str) -> PlanResult<LogicalPlanBuilder> {
        Ok(LogicalPlanBuilder::values(vec![vec![lit(1i64)]])?.alias(alias)?)
    }

    #[test]
    fn test_nested_definition_with_outer_reference() -> PlanResult<()> {
        // A subquery in the definition that refers to a column of the definition.
        let subquery = values("b")?
            .project(vec![out_ref_col(DataType::Int64, "v.column1")])?
            .build()?;
        let subquery = Expr::ScalarSubquery(Subquery {
            subquery: Arc::new(subquery),
            outer_ref_columns: vec![out_ref_col(DataType::Int64, "v.column1")],
            spans: Spans::new(),
        });
        let plan = values("v")?
            .project(vec![
                col("v.column1"),
                random().alias("r"),
                subquery.alias("s"),
            ])?
            .build()?;
        assert_eq!(shareable_definition(&Arc::new(plan), true)?, Some(true));

        // An operator of the definition that refers to a column of the outer query.
        let plan = values("v")?
            .project(vec![
                col("v.column1"),
                random().alias("r"),
                out_ref_col(DataType::Int64, "o.id").alias("o"),
            ])?
            .build()?;
        assert_eq!(shareable_definition(&Arc::new(plan), true)?, None);
        Ok(())
    }
}
