use std::future::Future;
use std::sync::Arc;

use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{collect, displayable, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
use datafusion_common::Result as DataFusionResult;
use datafusion_expr::LogicalPlan;
use sail_common::spec;
use sail_common_datafusion::rename::physical_plan::rename_physical_plan;

use crate::config::PlanConfig;
use crate::error::{PlanError, PlanResult};
use crate::execute_logical_plan;
use crate::resolver::plan::NamedPlan;
use crate::resolver::PlanResolver;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainKind {
    Simple,
    Extended,
    Codegen,
    Cost,
    Formatted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExplainOptions {
    pub kind: ExplainKind,
    pub verbose: bool,
    pub analyze: bool,
}

impl ExplainOptions {
    pub fn from_mode(mode: spec::ExplainMode) -> Self {
        match mode {
            spec::ExplainMode::Unspecified | spec::ExplainMode::Simple => Self {
                kind: ExplainKind::Simple,
                verbose: false,
                analyze: false,
            },
            spec::ExplainMode::Extended => Self {
                kind: ExplainKind::Extended,
                verbose: false,
                analyze: false,
            },
            spec::ExplainMode::Codegen => Self {
                kind: ExplainKind::Codegen,
                verbose: false,
                analyze: false,
            },
            spec::ExplainMode::Cost => Self {
                kind: ExplainKind::Cost,
                verbose: false,
                analyze: false,
            },
            spec::ExplainMode::Formatted => Self {
                kind: ExplainKind::Formatted,
                verbose: true,
                analyze: false,
            },
            spec::ExplainMode::Analyze => Self {
                kind: ExplainKind::Simple,
                verbose: true,
                analyze: true,
            },
            spec::ExplainMode::Verbose => Self {
                kind: ExplainKind::Simple,
                verbose: true,
                analyze: false,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExplainString {
    pub output: String,
    pub stringified_plans: Vec<StringifiedPlan>,
}

struct CollectedPlan {
    initial_logical: LogicalPlan,
    optimized_logical: LogicalPlan,
    physical_plan: Option<Arc<dyn ExecutionPlan>>,
    physical_error: Option<String>,
    stringified: Vec<StringifiedPlan>,
}

impl CollectedPlan {
    fn logical_string(&self, plan: &LogicalPlan, plan_type: PlanType) -> String {
        plan.to_stringified(plan_type).plan.to_string()
    }

    fn physical_string(
        &self,
        verbose: bool,
        with_stats: bool,
        with_schema: bool,
        with_metrics: bool,
    ) -> String {
        if let Some(plan) = &self.physical_plan {
            let displayable = if with_metrics {
                DisplayableExecutionPlan::with_metrics(plan.as_ref())
            } else {
                DisplayableExecutionPlan::new(plan.as_ref())
            };
            displayable
                .set_show_statistics(with_stats)
                .set_show_schema(with_schema)
                .indent(verbose)
                .to_string()
        } else if let Some(err) = &self.physical_error {
            format!("Physical plan error: {err}")
        } else {
            "Physical plan unavailable".to_string()
        }
    }
}

async fn collect_plan_with<F, Fut>(ctx: &SessionContext, plan_fn: F) -> PlanResult<CollectedPlan>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = PlanResult<(LogicalPlan, Option<Vec<String>>)>>,
{
    let (plan, fields) = plan_fn().await?;
    let initial_logical = plan.clone();
    let mut stringified = vec![initial_logical.to_stringified(PlanType::InitialLogicalPlan)];

    let df = execute_logical_plan(ctx, plan).await?;
    let (session_state, logical_plan) = df.into_parts();
    let optimized_logical = session_state.optimize(&logical_plan)?;
    stringified.push(optimized_logical.to_stringified(PlanType::FinalLogicalPlan));

    let mut physical_error = None;
    let mut physical_plan = match session_state
        .query_planner()
        .create_physical_plan(&optimized_logical, &session_state)
        .await
    {
        Ok(plan) => Some(plan),
        Err(err) => {
            let err = PlanError::from(err);
            let msg = err.to_string();
            stringified.push(StringifiedPlan::new(
                PlanType::PhysicalPlanError,
                msg.clone(),
            ));
            physical_error = Some(msg);
            None
        }
    };

    if let Some(plan) = physical_plan.take() {
        let plan = match fields {
            Some(fields) => match rename_physical_plan(plan, &fields) {
                Ok(plan) => Some(plan),
                Err(err) => {
                    let msg = err.to_string();
                    stringified.push(StringifiedPlan::new(
                        PlanType::PhysicalPlanError,
                        msg.clone(),
                    ));
                    physical_error = Some(msg);
                    None
                }
            },
            None => Some(plan),
        };

        if let Some(plan) = plan {
            stringified.push(StringifiedPlan::new(
                PlanType::FinalPhysicalPlan,
                displayable(plan.as_ref()).indent(true).to_string(),
            ));
            physical_plan = Some(plan);
        } else {
            physical_plan = None;
        }
    }

    Ok(CollectedPlan {
        initial_logical,
        optimized_logical,
        physical_plan,
        physical_error,
        stringified,
    })
}

fn render_section(title: &str, body: &str) -> String {
    format!("== {title} ==\n{body}")
}

async fn maybe_collect_metrics(
    options: &ExplainOptions,
    physical: &Option<Arc<dyn ExecutionPlan>>,
    ctx: &SessionContext,
) -> DataFusionResult<()> {
    if options.analyze {
        // Run the plan to populate metrics. Ignore the output batches.
        if let Some(plan) = physical {
            let _ = collect(Arc::clone(plan), ctx.task_ctx()).await?;
        }
    }
    Ok(())
}

pub async fn explain_string(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    plan: spec::Plan,
    options: ExplainOptions,
) -> PlanResult<ExplainString> {
    let collected = collect_plan_with(ctx, || {
        let config = Arc::clone(&config);
        async move {
            let resolver = PlanResolver::new(ctx, config);
            let NamedPlan { plan, fields } = resolver.resolve_named_plan(plan).await?;
            Ok((plan, fields))
        }
    })
    .await?;
    explain_from_collected(ctx, collected, options).await
}

pub async fn explain_string_from_logical_plan(
    ctx: &SessionContext,
    plan: LogicalPlan,
    fields: Option<Vec<String>>,
    options: ExplainOptions,
) -> PlanResult<ExplainString> {
    let collected = collect_plan_with(ctx, || async move { Ok((plan, fields)) }).await?;
    explain_from_collected(ctx, collected, options).await
}

async fn explain_from_collected(
    ctx: &SessionContext,
    collected: CollectedPlan,
    options: ExplainOptions,
) -> PlanResult<ExplainString> {
    maybe_collect_metrics(&options, &collected.physical_plan, ctx)
        .await
        .map_err(PlanError::from)?;

    let logical_simple =
        collected.logical_string(&collected.initial_logical, PlanType::InitialLogicalPlan);
    let logical_optimized =
        collected.logical_string(&collected.optimized_logical, PlanType::FinalLogicalPlan);

    let physical_plain = collected.physical_string(options.verbose, false, false, false);
    let physical_with_stats = collected.physical_string(true, true, false, false);
    let physical_with_schema = collected.physical_string(true, false, true, false);
    let physical_full = collected.physical_string(true, true, true, false);
    let physical_full_with_metrics = collected.physical_string(true, true, true, true);

    let physical_for_mode = if options.analyze {
        &physical_full_with_metrics
    } else {
        &physical_plain
    };

    let output = match options.kind {
        ExplainKind::Simple => {
            let mut sections = vec![render_section("Physical Plan", physical_for_mode)];
            if options.verbose && !options.analyze {
                sections.push(render_section(
                    "Physical Plan (with statistics)",
                    &physical_with_stats,
                ));
                sections.push(render_section(
                    "Physical Plan (with schema)",
                    &physical_with_schema,
                ));
            }
            sections.join("\n\n")
        }
        ExplainKind::Extended => [
            render_section("Parsed Logical Plan", &logical_simple),
            // TODO: Spark expects distinct analyzed vs optimized plans
            // Avoid duplicating the same plan until we can separate.
            render_section("Analyzed Logical Plan", &logical_optimized),
            render_section("Physical Plan", physical_for_mode),
        ]
        .join("\n\n"),
        ExplainKind::Codegen => [
            render_section(
                "Codegen",
                "Whole-stage codegen is not supported; showing physical plan instead.",
            ),
            render_section("Physical Plan", physical_for_mode),
        ]
        .join("\n\n"),
        ExplainKind::Cost => [
            render_section("Parsed Logical Plan", &logical_simple),
            render_section("Analyzed Logical Plan", &logical_optimized),
            // TODO: Spark COST mode shows logical plan + stats; we currently return physical +
            // stats
            render_section(
                "Physical Plan",
                if options.verbose || options.analyze {
                    &physical_full
                } else {
                    &physical_with_stats
                },
            ),
        ]
        .join("\n\n"),
        // TODO: Spark FORMATTED mode emits outline + node details
        ExplainKind::Formatted => render_section("Physical Plan", &physical_full),
    };

    Ok(ExplainString {
        output,
        stringified_plans: collected.stringified,
    })
}
