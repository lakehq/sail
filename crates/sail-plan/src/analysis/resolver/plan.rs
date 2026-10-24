//! Resolve query and command requests through the same native model.
use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::model::*;
use crate::error::PlanResult;

impl Resolver<'_, '_> {
    #[async_recursion::async_recursion]
    pub(super) async fn native_plan(&mut self, plan: Plan) -> PlanResult<Plan> {
        Ok(match plan {
            Plan::Query(query) => Plan::Query(self.complete_query(query).await?),
            Plan::Command(command) => Plan::Command(self.native_command(command).await?),
        })
    }

    pub(super) async fn complete_query(&mut self, query: QueryNode) -> PlanResult<QueryNode> {
        let query = self.native_query(query).await?;
        let query = crate::analysis::row_identity::materialize(query, &mut self.context.ids)?;
        let query = crate::analysis::scalar_iterator::materialize(query, &mut self.context.ids)?;
        crate::analysis::ordering::preserve(query)
    }

    #[async_recursion::async_recursion]
    async fn native_command(&mut self, command: CommandNode) -> PlanResult<CommandNode> {
        let mut kind = match command.kind {
            Deferred::Unresolved(request) => return Ok(self.command(*request).await?.into()),
            Deferred::Resolved(kind) => kind,
        };
        for query in kind.queries_mut() {
            *query = self.complete_query(query.clone()).await?;
        }
        for command in kind.commands_mut() {
            // Move the node without requiring command payloads to be cloneable.
            let node = std::mem::replace(command, CommandKind::Query(Self::empty(false)).into());
            *command = self.native_command(node).await?;
        }
        match &mut kind {
            CommandKind::Write(operation) => {
                let schema = operation.input.schema.resolved("write input")?;
                for sort in &mut operation.sort_order {
                    sort.expression = self
                        .native_expression(sort.expression.clone(), schema)
                        .await?;
                }
                if let crate::analysis::writer::WriteDisposition::OverwriteIf {
                    condition,
                    schema,
                    ..
                } = &mut operation.mode
                {
                    **condition = self.native_expression(*condition.clone(), schema).await?;
                }
            }
            CommandKind::RowOperation(operation) => {
                let schema = operation.input.schema.resolved("row operation input")?;
                if let Some(condition) = &mut operation.condition {
                    condition.expression = self
                        .native_expression(condition.expression.clone(), schema)
                        .await?;
                }
                if let crate::analysis::row_level::RowAction::Update {
                    assignments,
                    generated,
                    constraints,
                } = &mut operation.action
                {
                    for (_, expression) in assignments.iter_mut().chain(generated) {
                        *expression = self.native_expression(expression.clone(), schema).await?;
                    }
                    for constraint in constraints {
                        constraint.expression = self
                            .native_expression(constraint.expression.clone(), schema)
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
                for expression in crate::analysis::model::merge_expressions(operation) {
                    *expression = self.native_expression(expression.clone(), &schema).await?;
                }
            }
            CommandKind::Explain { input, .. } => {
                let plan = std::mem::replace(input.as_mut(), Plan::Query(Self::empty(false)));
                **input = self.native_plan(plan).await?;
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
