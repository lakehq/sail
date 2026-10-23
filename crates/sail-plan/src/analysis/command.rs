//! Analyzed commands retain native query inputs until the lowering boundary.
use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion::prelude::SessionContext;
use datafusion_expr::{Extension, LogicalPlan as BackendPlan, ScalarUDF};
use sail_catalog::command::CatalogCommand;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_logical_plan::show_string::{ShowStringFormat, ShowStringNode};

use super::ir::{LogicalPlan, LogicalQuery};
use crate::analysis::NamedPlan;
use crate::error::PlanResult;

#[derive(Debug)]
pub enum AnalyzedPlan {
    Query(LogicalQuery),
    Command(LogicalCommand),
}

#[derive(Debug)]
pub enum LogicalCommand {
    Catalog(CatalogCommand),
    Barrier {
        preconditions: Vec<LogicalCommand>,
        command: Box<LogicalCommand>,
    },
    Write(Box<super::writer::WriteOperation>),
    RowOperation(Box<super::row_level::RowOperation>),
    Merge(Box<super::merge::MergeOperation>),
    Query(LogicalPlan),
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
        input: LogicalPlan,
        rows: usize,
        format: ShowStringFormat,
        name: String,
    },
    Checkpoint {
        input: LogicalPlan,
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
        input: Box<AnalyzedPlan>,
        mode: spec::ExplainMode,
    },
}

impl AnalyzedPlan {
    #[async_recursion]
    pub(super) async fn apply_layers(
        &mut self,
        pipeline: &super::AnalysisPipeline,
        context: &mut super::AnalysisContext<'_>,
    ) -> PlanResult<()> {
        match self {
            Self::Query(query) => *query = pipeline.apply_layers(query.clone(), context).await?,
            Self::Command(command) => command.apply_layers(pipeline, context).await?,
        }
        Ok(())
    }

    #[async_recursion]
    pub async fn lower(self, session: &SessionContext) -> PlanResult<NamedPlan> {
        match self {
            Self::Query(query) => query.into_named_plan(),
            Self::Command(command) => Ok(NamedPlan {
                plan: command.lower(session).await?,
                fields: None,
            }),
        }
    }
}

impl LogicalCommand {
    #[async_recursion]
    async fn apply_layers(
        &mut self,
        pipeline: &super::AnalysisPipeline,
        context: &mut super::AnalysisContext<'_>,
    ) -> PlanResult<()> {
        async fn query(
            plan: &mut LogicalPlan,
            pipeline: &super::AnalysisPipeline,
            context: &mut super::AnalysisContext<'_>,
        ) -> PlanResult<()> {
            *plan = pipeline
                .apply_layers(LogicalQuery { plan: plan.clone() }, context)
                .await?
                .plan;
            Ok(())
        }
        match self {
            Self::Query(input) | Self::Show { input, .. } | Self::Checkpoint { input, .. } => {
                query(input, pipeline, context).await?
            }
            Self::CreateTemporaryView { options, .. } => {
                query(Arc::make_mut(&mut options.input), pipeline, context).await?
            }
            Self::Write(operation) => {
                query(&mut operation.input, pipeline, context).await?;
                for command in &mut operation.preconditions {
                    command.apply_layers(pipeline, context).await?;
                }
            }
            Self::RowOperation(operation) => query(&mut operation.input, pipeline, context).await?,
            Self::Merge(operation) => {
                query(&mut operation.target, pipeline, context).await?;
                query(&mut operation.source, pipeline, context).await?;
            }
            Self::Barrier {
                preconditions,
                command,
            } => {
                for command in preconditions {
                    command.apply_layers(pipeline, context).await?;
                }
                command.apply_layers(pipeline, context).await?;
            }
            Self::Explain { input, .. } => input.apply_layers(pipeline, context).await?,
            Self::Catalog(_)
            | Self::Procedure { .. }
            | Self::RegisterFunction(_)
            | Self::SetVariable { .. }
            | Self::ResetVariable { .. } => {}
        }
        Ok(())
    }

    #[async_recursion]
    pub(super) async fn lower(self, session: &SessionContext) -> PlanResult<BackendPlan> {
        Ok(match self {
            Self::Catalog(command) => catalog(session, command)?,
            Self::Barrier {
                preconditions,
                command,
            } => {
                let mut inputs = vec![];
                for precondition in preconditions {
                    inputs.push(Arc::new(precondition.lower(session).await?));
                }
                BackendPlan::Extension(Extension {
                    node: Arc::new(sail_logical_plan::barrier::BarrierNode::new(
                        inputs,
                        Arc::new(command.lower(session).await?),
                    )),
                })
            }
            Self::Write(operation) => operation.lower(session).await?,
            Self::RowOperation(operation) => operation.lower(session).await?,
            Self::Merge(operation) => operation.lower(session).await?,
            Self::Query(plan) => super::lowering::external_plan(plan)?,
            Self::Procedure { call, target } => {
                let registry = session
                    .extension::<sail_common_datafusion::datasource::DataSourceRegistry>()?;
                let source = registry.get_lake_source(&call.lake_source)?;
                let provider = source.procedure_provider().ok_or_else(|| {
                    crate::error::PlanError::internal("missing procedure provider")
                })?;
                let plan = provider
                    .plan_procedure(&session.state(), target, &call)
                    .await?;
                BackendPlan::Extension(Extension {
                    node: Arc::new(crate::procedure::LakeProcedureNode::try_new(
                        *call,
                        plan.implementation,
                        plan.root_placement,
                    )?),
                })
            }
            Self::CreateTemporaryView {
                view,
                is_global,
                options,
            } => {
                let input = session
                    .extension::<CatalogManager>()?
                    .track_logical_plan(options.input)?;
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
            Self::RegisterFunction(function) => {
                let udf = session
                    .extension::<CatalogManager>()?
                    .track_function(function)?;
                catalog(session, CatalogCommand::RegisterFunction { udf })?
            }
            Self::Show {
                input,
                rows,
                format,
                name,
            } => {
                let NamedPlan { plan, fields } = LogicalQuery { plan: input }.into_named_plan()?;
                BackendPlan::Extension(Extension {
                    node: Arc::new(ShowStringNode::try_new(
                        Arc::new(plan),
                        fields.unwrap_or_default(),
                        rows,
                        format,
                        name,
                    )?),
                })
            }
            Self::Checkpoint { input, relation_id } => {
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
                let plan = LogicalQuery { plan: input }.into_named_plan()?.plan;
                BackendPlan::Extension(Extension {
                    node: Arc::new(
                        sail_logical_plan::remote_checkpoint::RemoteCheckpointCommandNode::new(
                            relation_id,
                            Arc::new(plan),
                            Arc::new(arrow_schema::Schema::new(fields)),
                        ),
                    ),
                })
            }
            Self::SetVariable { variable, value } => {
                let statement =
                    datafusion_expr::Statement::SetVariable(datafusion_expr::SetVariable {
                        variable,
                        value,
                    });
                BackendPlan::Statement(statement)
            }
            Self::ResetVariable { variable } => {
                let statement =
                    datafusion_expr::Statement::ResetVariable(datafusion_expr::ResetVariable {
                        variable,
                    });
                BackendPlan::Statement(statement)
            }
            Self::Explain { input, mode } => {
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

fn catalog(session: &SessionContext, command: CatalogCommand) -> PlanResult<BackendPlan> {
    Ok(BackendPlan::Extension(Extension {
        node: Arc::new(crate::catalog::CatalogCommandNode::try_new(
            session, command,
        )?),
    }))
}
