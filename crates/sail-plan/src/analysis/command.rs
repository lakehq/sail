//! Analyzed commands retain native query inputs until the lowering boundary.
use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion::prelude::SessionContext;
use datafusion_expr::{Extension, LogicalPlan};
use sail_catalog::command::CatalogCommand;
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_logical_plan::show_string::ShowStringNode;

use super::lowering::lower_query;
use super::model::{CommandKind, CommandNode, Plan};
use crate::analysis::NamedPlan;
use crate::error::PlanResult;

impl Plan {
    #[async_recursion]
    pub async fn lower(self, session: &SessionContext) -> PlanResult<NamedPlan> {
        match self {
            Self::Query(query) => lower_query(query),
            Self::Command(command) => Ok(NamedPlan {
                plan: command.lower(session).await?,
                fields: None,
            }),
        }
    }
}

impl CommandNode {
    #[async_recursion]
    pub(super) async fn lower(self, session: &SessionContext) -> PlanResult<LogicalPlan> {
        Ok(match self.kind.into_resolved("command kind")? {
            CommandKind::Catalog(command) => catalog(session, command)?,
            CommandKind::Barrier {
                preconditions,
                command,
            } => {
                let mut inputs = vec![];
                for precondition in preconditions {
                    inputs.push(Arc::new(precondition.lower(session).await?));
                }
                LogicalPlan::Extension(Extension {
                    node: Arc::new(sail_logical_plan::barrier::BarrierNode::new(
                        inputs,
                        Arc::new(command.lower(session).await?),
                    )),
                })
            }
            CommandKind::Write(operation) => operation.lower(session).await?,
            CommandKind::RowOperation(operation) => operation.lower(session).await?,
            CommandKind::Merge(operation) => operation.lower(session).await?,
            CommandKind::Query(plan) => super::lowering::external_plan(plan)?,
            CommandKind::Procedure { call, target } => {
                let registry = session
                    .extension::<sail_common_datafusion::datasource::DataSourceRegistry>()?;
                let source = registry.get_lake_source(&call.lake_source)?;
                let provider = source.procedure_provider().ok_or_else(|| {
                    crate::error::PlanError::internal("missing procedure provider")
                })?;
                let plan = provider
                    .plan_procedure(&session.state(), target, &call)
                    .await?;
                LogicalPlan::Extension(Extension {
                    node: Arc::new(crate::procedure::LakeProcedureNode::try_new(
                        *call,
                        plan.implementation,
                        plan.root_placement,
                    )?),
                })
            }
            CommandKind::CreateTemporaryView {
                view,
                is_global,
                options,
            } => {
                let input = session
                    .extension::<CatalogManager>()?
                    .track_query(options.input)?;
                catalog(
                    session,
                    CatalogCommand::CreateTemporaryView {
                        view,
                        is_global,
                        options: sail_catalog::provider::CreateTemporaryViewOptions {
                            input,
                            columns: options.columns,
                            if_not_exists: options.if_not_exists,
                            replace: options.replace,
                            comment: options.comment,
                            properties: options.properties,
                            source: options.source,
                        },
                    },
                )?
            }
            CommandKind::RegisterFunction(function) => {
                let udf = session
                    .extension::<CatalogManager>()?
                    .track_function(function)?;
                catalog(session, CatalogCommand::RegisterFunction { udf })?
            }
            CommandKind::Show {
                input,
                rows,
                format,
                name,
            } => {
                let NamedPlan { plan, fields } = lower_query(input)?;
                LogicalPlan::Extension(Extension {
                    node: Arc::new(ShowStringNode::try_new(
                        Arc::new(plan),
                        fields.unwrap_or_default(),
                        rows,
                        format,
                        name,
                    )?),
                })
            }
            CommandKind::Checkpoint { input, relation_id } => {
                let fields = input
                    .schema
                    .resolved("checkpoint schema")?
                    .attributes
                    .iter()
                    .map(|attribute| {
                        Ok(attribute
                            .result_type
                            .resolved("checkpoint field")?
                            .to_field(attribute.name.clone()))
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                let plan = lower_query(input)?.plan;
                LogicalPlan::Extension(Extension {
                    node: Arc::new(
                        sail_logical_plan::remote_checkpoint::RemoteCheckpointCommandNode::new(
                            relation_id,
                            Arc::new(plan),
                            Arc::new(arrow_schema::Schema::new(fields)),
                        ),
                    ),
                })
            }
            CommandKind::SetVariable { variable, value } => {
                let statement =
                    datafusion_expr::Statement::SetVariable(datafusion_expr::SetVariable {
                        variable,
                        value,
                    });
                LogicalPlan::Statement(statement)
            }
            CommandKind::ResetVariable { variable } => {
                let statement =
                    datafusion_expr::Statement::ResetVariable(datafusion_expr::ResetVariable {
                        variable,
                    });
                LogicalPlan::Statement(statement)
            }
            CommandKind::Explain { input, mode } => {
                let NamedPlan { plan, fields } = input.lower(session).await?;
                let result = crate::explain::explain_string_from_logical_plan(
                    session,
                    plan,
                    fields,
                    crate::explain::ExplainOptions::from_mode(mode),
                )
                .await?;
                // The result is a literal-only command relation; it has no query expressions to analyze.
                datafusion_expr::LogicalPlanBuilder::values(vec![vec![datafusion_expr::lit(
                    result.output,
                )]])?
                .project(vec![datafusion_expr::col("column1").alias("plan")])?
                .build()?
            }
        })
    }
}

fn catalog(session: &SessionContext, command: CatalogCommand) -> PlanResult<LogicalPlan> {
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(crate::catalog::CatalogCommandNode::try_new(
            session, command,
        )?),
    }))
}
