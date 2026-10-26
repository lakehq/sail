use std::sync::Arc;

use sail_common::spec;

use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::resolver::{CteDefinition, PlanResolver};

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_command_with_relations(
        &mut self,
        root: spec::CommandPlan,
        references: Vec<spec::QueryPlan>,
    ) -> PlanResult<CommandKind> {
        for reference in references {
            let id = reference
                .plan_id
                .ok_or_else(|| PlanError::invalid("subquery reference missing plan_id"))?;
            if let spec::QueryNode::SubqueryAlias { alias, .. } = &reference.node {
                let plan = self.resolve_query_plan(reference.clone()).await?;
                self.ctes.insert(
                    self.name_key(alias.as_ref()),
                    Arc::new(CteDefinition {
                        plan,
                        parameter: true,
                    }),
                );
            }
            if self.relations.insert(id, reference).is_some() {
                return Err(PlanError::invalid(format!(
                    "duplicate subquery reference for plan_id {id}"
                )));
            }
        }
        self.resolve_command_plan(root).await
    }
}
