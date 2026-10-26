//! Resolve query and command requests through the same native model.
use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::PlanResult;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_plan(
        &mut self,
        plan: AnalyzerPlan,
    ) -> PlanResult<AnalyzerPlan> {
        Ok(match plan {
            AnalyzerPlan::Query(query) => AnalyzerPlan::Query(self.complete_query(query).await?),
            AnalyzerPlan::Command(command) => {
                AnalyzerPlan::Command(self.resolve_command(command).await?)
            }
        })
    }

    pub(in crate::resolver) async fn complete_query(&mut self, query: Query) -> PlanResult<Query> {
        let query = self.resolve_query(query).await?;
        let query = crate::analyzer::row_identity::materialize(query, &mut self.context.ids)?;
        let query = crate::analyzer::scalar_iterator::materialize(query, &mut self.context.ids)?;
        crate::analyzer::ordering::preserve(query)
    }

    #[async_recursion::async_recursion]
    async fn resolve_command(&mut self, command: Command) -> PlanResult<Command> {
        let mut kind = match command.kind {
            Deferred::Unresolved(request) => {
                return Ok(self.resolve_command_plan(*request).await?.into());
            }
            Deferred::Resolved(kind) => kind,
        };
        for query in kind.queries_mut() {
            *query = self.complete_query(query.clone()).await?;
        }
        for command in kind.commands_mut() {
            // Move the node without requiring command payloads to be cloneable.
            let node = std::mem::replace(command, CommandKind::Query(Self::empty(false)).into());
            *command = self.resolve_command(node).await?;
        }
        match &mut kind {
            CommandKind::Write(operation) => {
                let schema = operation.input.schema.resolved("write input")?;
                for sort in &mut operation.sort_order {
                    sort.expression = self
                        .resolve_expression(sort.expression.clone(), schema)
                        .await?;
                }
                if let crate::analyzer::writer::WriteDisposition::OverwriteIf {
                    condition,
                    schema,
                    ..
                } = &mut operation.mode
                {
                    **condition = self.resolve_expression(*condition.clone(), schema).await?;
                }
            }
            CommandKind::RowOperation(operation) => {
                let schema = operation.input.schema.resolved("row operation input")?;
                if let Some(condition) = &mut operation.condition {
                    condition.expression = self
                        .resolve_expression(condition.expression.clone(), schema)
                        .await?;
                }
                if let crate::analyzer::row_level::RowAction::Update {
                    assignments,
                    generated,
                    constraints,
                } = &mut operation.action
                {
                    for (_, expression) in assignments.iter_mut().chain(generated) {
                        *expression = self.resolve_expression(expression.clone(), schema).await?;
                    }
                    for constraint in constraints {
                        constraint.expression = self
                            .resolve_expression(constraint.expression.clone(), schema)
                            .await?;
                    }
                }
            }
            CommandKind::Merge(operation) => {
                let mut schema = operation.target.schema.resolved("merge target")?.clone();
                schema.attributes.extend(
                    operation
                        .source
                        .schema
                        .resolved("merge source")?
                        .attributes
                        .clone(),
                );
                // MERGE retains explicit attribute bindings across its action scopes.
                for expression in crate::analyzer::model::merge_expressions(operation) {
                    *expression = self.resolve_expression(expression.clone(), &schema).await?;
                }
            }
            CommandKind::Explain { input, .. } => {
                let plan =
                    std::mem::replace(input.as_mut(), AnalyzerPlan::Query(Self::empty(false)));
                **input = self.resolve_plan(plan).await?;
            }
            CommandKind::Query(_)
            | CommandKind::Show { .. }
            | CommandKind::Checkpoint { .. }
            | CommandKind::CreateTemporaryView { .. }
            | CommandKind::Barrier { .. }
            | CommandKind::Catalog(_)
            | CommandKind::Procedure { .. }
            | CommandKind::RegisterFunction(_)
            | CommandKind::SetVariable { .. }
            | CommandKind::ResetVariable { .. } => {}
        }
        Ok(kind.into())
    }
}
