//! Native query and command model for analysis and lowering.
use std::sync::Arc;

use datafusion_expr::ScalarUDF;
use sail_catalog::command::CatalogCommand;
use sail_common::spec;
use sail_logical_plan::show_string::ShowStringFormat;
pub use sail_query::model::*;

use super::deferred::{Deferred, Resolvable};

#[derive(Debug)]
pub enum Plan {
    Query(QueryNode),
    Command(CommandNode),
}

#[derive(Debug)]
pub enum CommandKind {
    Catalog(CatalogCommand),
    Barrier {
        preconditions: Vec<CommandNode>,
        command: Box<CommandNode>,
    },
    Write(Box<super::writer::WriteOperation>),
    RowOperation(Box<super::row_level::RowOperation>),
    Merge(Box<super::merge::MergeOperation>),
    Query(QueryNode),
    Procedure {
        call: Box<sail_common_datafusion::lakeprocedure::LakeProcedureCall>,
        target: sail_common_datafusion::lakeprocedure::LakeProcedurePlanningTarget,
    },
    RegisterFunction(ScalarUDF),
    CreateTemporaryView {
        view: String,
        is_global: bool,
        options: sail_catalog::provider::CreateTemporaryViewOptions,
    },
    Show {
        input: QueryNode,
        rows: usize,
        format: ShowStringFormat,
        name: String,
    },
    Checkpoint {
        input: QueryNode,
        relation_id: String,
    },
    SetVariable {
        variable: String,
        value: String,
    },
    ResetVariable {
        variable: String,
    },
    Explain {
        input: Box<Plan>,
        mode: spec::ExplainMode,
    },
}

#[derive(Debug)]
pub struct CommandNode {
    pub kind: Deferred<CommandKind>,
}

impl CommandNode {
    pub fn unresolved(command: spec::CommandPlan) -> Self {
        Self {
            kind: Deferred::unresolved(command),
        }
    }
}

impl From<CommandKind> for CommandNode {
    fn from(kind: CommandKind) -> Self {
        Self {
            kind: Deferred::Resolved(kind),
        }
    }
}

impl Resolvable for CommandKind {
    type Request = spec::CommandPlan;
}

impl From<spec::Plan> for Plan {
    fn from(plan: spec::Plan) -> Self {
        match plan {
            spec::Plan::Query(query) => Self::Query(QueryNode::unresolved(query)),
            spec::Plan::Command(command) => Self::Command(CommandNode::unresolved(command)),
        }
    }
}

impl From<spec::QueryPlan> for Plan {
    fn from(query: spec::QueryPlan) -> Self {
        Self::Query(QueryNode::unresolved(query))
    }
}

impl From<QueryNode> for Plan {
    fn from(query: QueryNode) -> Self {
        Self::Query(query)
    }
}

impl Plan {
    /// Reserve the entire identity domain before resolving any sibling input.
    pub(super) fn reserve_ids(&mut self, ids: &mut IdGenerator) -> crate::error::PlanResult<()> {
        match self {
            Self::Query(query) => ids.reserve_query(query)?,
            Self::Command(command) => command.reserve_ids(ids)?,
        }
        Ok(())
    }
}

impl CommandNode {
    fn reserve_ids(&mut self, ids: &mut IdGenerator) -> crate::error::PlanResult<()> {
        if let Deferred::Resolved(kind) = &mut self.kind {
            for query in kind.queries_mut() {
                ids.reserve_query(query)?;
            }
            if let CommandKind::Write(operation) = kind
                && let super::writer::WriteDisposition::OverwriteIf { schema, .. } = &operation.mode
            {
                ids.reserve_schema(schema)?;
            }
            for expression in kind.expressions_mut() {
                ids.reserve_expression(expression)?;
            }
            for command in kind.commands_mut() {
                command.reserve_ids(ids)?;
            }
            if let CommandKind::Explain { input, .. } = kind {
                input.reserve_ids(ids)?;
            }
        }
        Ok(())
    }
}

impl CommandKind {
    /// Immediate query inputs; nested commands and EXPLAIN are separate scopes.
    pub(super) fn queries_mut(&mut self) -> Vec<&mut QueryNode> {
        match self {
            Self::Query(input) | Self::Show { input, .. } | Self::Checkpoint { input, .. } => {
                vec![input]
            }
            Self::CreateTemporaryView { options, .. } => vec![Arc::make_mut(&mut options.input)],
            Self::Write(operation) => vec![&mut operation.input],
            Self::RowOperation(operation) => vec![&mut operation.input],
            Self::Merge(operation) => vec![&mut operation.target, &mut operation.source],
            Self::Barrier { .. }
            | Self::Explain { .. }
            | Self::Catalog(_)
            | Self::Procedure { .. }
            | Self::RegisterFunction(_)
            | Self::SetVariable { .. }
            | Self::ResetVariable { .. } => vec![],
        }
    }

    pub(super) fn commands_mut(&mut self) -> Vec<&mut CommandNode> {
        match self {
            Self::Barrier {
                preconditions,
                command,
            } => preconditions
                .iter_mut()
                .chain(std::iter::once(command.as_mut()))
                .collect(),
            Self::Write(operation) => operation.preconditions.iter_mut().collect(),
            Self::Query(_)
            | Self::Show { .. }
            | Self::Checkpoint { .. }
            | Self::CreateTemporaryView { .. }
            | Self::RowOperation(_)
            | Self::Merge(_)
            | Self::Explain { .. }
            | Self::Catalog(_)
            | Self::Procedure { .. }
            | Self::RegisterFunction(_)
            | Self::SetVariable { .. }
            | Self::ResetVariable { .. } => vec![],
        }
    }

    pub(super) fn expressions_mut(&mut self) -> Vec<&mut ExprNode> {
        use super::row_level::RowAction;
        use super::writer::WriteDisposition;
        let mut expressions = vec![];
        match self {
            Self::Write(operation) => {
                expressions.extend(
                    operation
                        .sort_order
                        .iter_mut()
                        .map(|sort| &mut sort.expression),
                );
                if let WriteDisposition::OverwriteIf { condition, .. } = &mut operation.mode {
                    expressions.push(condition.as_mut());
                }
            }
            Self::RowOperation(operation) => {
                if let Some(condition) = &mut operation.condition {
                    expressions.push(&mut condition.expression);
                }
                if let RowAction::Update {
                    assignments,
                    generated,
                    constraints,
                } = &mut operation.action
                {
                    expressions.extend(
                        assignments
                            .iter_mut()
                            .chain(generated)
                            .map(|(_, expression)| expression),
                    );
                    expressions.extend(
                        constraints
                            .iter_mut()
                            .map(|constraint| &mut constraint.expression),
                    );
                }
            }
            Self::Merge(operation) => expressions.extend(merge_expressions(operation)),
            Self::Query(_)
            | Self::Show { .. }
            | Self::Checkpoint { .. }
            | Self::CreateTemporaryView { .. }
            | Self::Barrier { .. }
            | Self::Explain { .. }
            | Self::Catalog(_)
            | Self::Procedure { .. }
            | Self::RegisterFunction(_)
            | Self::SetVariable { .. }
            | Self::ResetVariable { .. } => {}
        }
        expressions
    }
}

pub(super) fn merge_expressions(
    operation: &mut super::merge::MergeOperation,
) -> Vec<&mut ExprNode> {
    use super::merge::MergeAction;
    let mut expressions = vec![];
    expressions.push(&mut operation.condition.expression);
    for clause in operation
        .matched
        .iter_mut()
        .chain(&mut operation.source_missing)
        .chain(&mut operation.target_missing)
    {
        if let Some(condition) = &mut clause.condition {
            expressions.push(&mut condition.expression);
        }
        match &mut clause.action {
            MergeAction::Delete => {}
            MergeAction::Update(values) | MergeAction::Insert(values) => {
                expressions.extend(values.iter_mut().map(|(_, expression)| expression))
            }
        }
    }
    expressions.extend(
        operation
            .generated
            .iter_mut()
            .chain(&mut operation.defaults)
            .map(|(_, expression)| expression),
    );
    expressions.extend(
        operation
            .constraints
            .iter_mut()
            .map(|constraint| &mut constraint.expression),
    );
    for (left, right) in &mut operation.join_keys {
        expressions.extend([left, right]);
    }
    expressions.extend(&mut operation.residual);
    expressions.extend(&mut operation.target_predicates);
    expressions
}

impl From<spec::CommandPlan> for Plan {
    fn from(command: spec::CommandPlan) -> Self {
        Self::Command(CommandNode::unresolved(command))
    }
}

impl From<CommandNode> for Plan {
    fn from(command: CommandNode) -> Self {
        Self::Command(command)
    }
}
