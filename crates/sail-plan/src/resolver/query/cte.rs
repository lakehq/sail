use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_expr::{LogicalPlan, SubqueryAlias};
use sail_common::spec;
use sail_data_source::reuse::is_reusable_source;
use sail_logical_plan::range::RangeNode;
use sail_logical_plan::sort::{RequiredSortNode, SortWithinPartitionsNode};

use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;
use crate::scalar_subquery_key::is_volatile_call;
use crate::shared_cte::{CteDefinition, CteReference, inline_references};

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
    /// The shareable definition, decided when the CTE is referenced again.
    definition: OnceLock<Option<Arc<CteDefinition>>>,
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
            definition: OnceLock::new(),
        });
        self
    }

    /// Returns a scan of the CTE definition for a repeated reference, or `None` if each
    /// reference must evaluate the definition itself.
    pub(super) fn scan(&self, state: &mut PlanResolverState) -> PlanResult<Option<LogicalPlan>> {
        let Some(sharing) = &self.sharing else {
            return Ok(None);
        };
        let definition = match sharing.definition.get() {
            Some(definition) => definition,
            None => {
                let definition = if is_shareable_definition(&self.plan, sharing.nested)? {
                    let definition = Arc::new(CteDefinition::new(
                        sharing.index,
                        sharing.name.clone(),
                        Arc::clone(&self.plan),
                    ));
                    state.register_shared_cte_definition(Arc::clone(&definition));
                    Some(definition)
                } else {
                    None
                };
                sharing.definition.get_or_init(|| definition)
            }
        };
        Ok(definition
            .as_ref()
            .map(|definition| definition.scan())
            .transpose()?)
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
        Self::scan_shared_cte_references(plan, state)
    }

    /// Replaces the references to shared CTE definitions by the definitions, for a plan
    /// that is not optimized with its shared CTEs.
    pub(in crate::resolver) fn inline_shared_cte_references(
        plan: LogicalPlan,
    ) -> PlanResult<LogicalPlan> {
        Ok(inline_references(plan, &|_| true)?)
    }
}

/// Returns whether the references to the CTE definition can share one evaluation of it.
///
/// Each reference evaluates the definition itself if the definition refers to outer
/// columns, if its output depends on when or in which partitions it is evaluated, if a
/// source is read once for each reference, or if its consumers can depend on the order
/// of its rows.
fn is_shareable_definition(plan: &Arc<LogicalPlan>, nested: bool) -> PlanResult<bool> {
    let mut shareable = true;
    plan.apply_with_subqueries(|plan| {
        shareable = !(nested && plan.contains_outer_reference())
            && match plan {
                LogicalPlan::TableScan(scan) => {
                    CteReference::try_from_scan(scan).is_some() || is_reusable_source(&scan.source)
                }
                LogicalPlan::Extension(extension) => {
                    let node = extension.node.as_any();
                    node.is::<RangeNode>()
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
                        shareable = false;
                        return Ok(TreeNodeRecursion::Stop);
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
        return Ok(false);
    }
    let (_, sorted, _) = PlanResolver::require_input_sort_inner(plan.as_ref().clone())?;
    Ok(!sorted)
}
