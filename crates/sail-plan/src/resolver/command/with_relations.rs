use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion_expr::{LogicalPlan, SubqueryAlias};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    /// Resolves a command `WithRelations` node by storing references in state, then resolving
    /// the root command. This is the command analogue of `resolve_query_with_relations`.
    ///
    /// This is used when a command (e.g., `MERGE INTO`) references DataFrames passed as named
    /// arguments via `spark.sql("MERGE INTO t USING {df}", df=dataframe)`. PySpark wraps the
    /// DataFrame in a `SubqueryAlias` within a `WithRelations` node whose root is the command.
    #[async_recursion]
    pub(super) async fn resolve_command_with_relations(
        &self,
        root: spec::CommandPlan,
        references: Vec<spec::QueryPlan>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_with_relations_scope();
        let state = scope.state();
        // Enter a CTE scope so that DataFrame aliases registered below do not leak
        // into the outer command scope.
        let mut scope = state.enter_cte_scope();
        let state = scope.state();
        for ref_plan in references {
            // If the reference is a SubqueryAlias, register it as a named table so that
            // SQL commands can reference the DataFrame by its alias name.
            // This enables the `spark.sql("MERGE INTO t USING {df}", df=dataframe)` pattern where
            // PySpark replaces `{df}` with a generated temp view name and wraps the DataFrame
            // in a SubqueryAlias within a WithRelations node.
            if let spec::QueryNode::SubqueryAlias {
                ref input,
                ref alias,
                ref qualifier,
            } = ref_plan.node
            {
                let resolved = self.resolve_query_plan((**input).clone(), state).await?;
                let table_ref = self.resolve_table_reference(
                    &spec::ObjectName::from(qualifier.clone()).child(alias.clone()),
                )?;
                let aliased = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                    Arc::new(resolved),
                    table_ref.clone(),
                )?);
                state.insert_cte(table_ref, aliased);
            }
            let plan_id = ref_plan
                .plan_id
                .ok_or_else(|| PlanError::invalid("subquery reference missing plan_id"))?;
            if state.insert_subquery_reference(plan_id, ref_plan).is_some() {
                return Err(PlanError::invalid(format!(
                    "duplicate subquery reference for plan_id {}",
                    plan_id
                )));
            }
        }
        self.resolve_command_plan(root, scope.state()).await
    }
}
