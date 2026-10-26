use std::sync::Arc;

use sail_common::spec;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_query_with_ctes(
        &mut self,
        input: spec::QueryPlan,
        recursive: bool,
        ctes: Vec<(spec::Identifier, spec::QueryPlan)>,
    ) -> PlanResult<Query> {
        Ok({
            if recursive
                && ctes
                    .iter()
                    .any(|(_, plan)| matches!(plan.node, spec::QueryNode::SetOperation { .. }))
            {
                return Err(PlanError::unsupported("recursive CTE resolution"));
            }
            let previous = self.ctes.clone();
            for (name, plan) in ctes {
                let key = self.name_key(name.as_ref());
                let plan = self.resolve_query_plan(plan).await?;
                let definition = crate::resolver::CteDefinition {
                    plan,
                    parameter: false,
                };
                self.ctes.insert(key, Arc::new(definition));
            }
            let result = self.resolve_query_plan(input).await;
            let mut input = result?;
            let mut schema = input.schema.into_resolved("CTE output")?;
            for attribute in &mut schema.attributes {
                for definition in self.ctes.values().filter(|d| !d.parameter) {
                    for source in &definition
                        .plan
                        .schema
                        .resolved("CTE definition")?
                        .attributes
                    {
                        if source.origin == attribute.origin {
                            for id in &source.plan_ids {
                                if !attribute.plan_ids.contains(id) {
                                    attribute.plan_ids.push(*id);
                                }
                            }
                        }
                    }
                }
            }
            self.ctes = previous;
            input.schema = Deferred::Resolved(schema);
            Query {
                schema: input.schema.clone(),
                kind: Deferred::Resolved(QueryKind::Scope {
                    input: Box::new(input),
                }),
            }
        })
    }
}
