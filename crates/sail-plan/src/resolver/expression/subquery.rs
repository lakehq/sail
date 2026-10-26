use sail_common::spec;

use crate::analyzer::model::*;
use crate::error::PlanResult;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_expression_subquery(
        &mut self,
        query: spec::QueryPlan,
        schema: &QuerySchema,
    ) -> PlanResult<Query> {
        let depth = self.outer.len();
        let recovery = self.recovery.take();
        if let Some(scope) = recovery {
            self.outer_recovery.insert(depth, scope);
        }
        self.outer.push(schema.clone());
        let aliases = std::mem::take(&mut self.expression_aliases);
        let replacements = std::mem::take(&mut self.expression_replacements);
        let result = self.resolve_query_plan(query).await;
        self.expression_aliases = aliases;
        self.expression_replacements = replacements;
        self.outer.pop();
        self.recovery = self.outer_recovery.remove(&depth);
        self.visible_output(result?)
    }
}
