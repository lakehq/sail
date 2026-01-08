use std::future::Future;
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{collect, displayable, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{EmptyRelation, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use sail_common::spec;
use sail_common_datafusion::rename::physical_plan::rename_physical_plan;
use sail_logical_plan::precondition::WithPreconditionsNode;

use crate::catalog::CatalogCommandNode;
use crate::config::PlanConfig;
use crate::error::{PlanError, PlanResult};
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

#[derive(Default)]
struct PhysicalStrings {
    plain: Option<String>,
    with_stats: Option<String>,
    with_schema: Option<String>,
    full: Option<String>,
    full_with_metrics: Option<String>,
}

impl PhysicalStrings {
    fn plain<'a>(&'a mut self, collected: &CollectedPlan, verbose: bool) -> &'a str {
        self.plain
            .get_or_insert_with(|| collected.physical_string(verbose, false, false, false))
            .as_str()
    }

    fn with_stats<'a>(&'a mut self, collected: &CollectedPlan) -> &'a str {
        self.with_stats
            .get_or_insert_with(|| collected.physical_string(true, true, false, false))
            .as_str()
    }

    fn with_schema<'a>(&'a mut self, collected: &CollectedPlan) -> &'a str {
        self.with_schema
            .get_or_insert_with(|| collected.physical_string(true, false, true, false))
            .as_str()
    }

    fn full<'a>(&'a mut self, collected: &CollectedPlan) -> &'a str {
        self.full
            .get_or_insert_with(|| collected.physical_string(true, true, true, false))
            .as_str()
    }

    fn full_with_metrics<'a>(&'a mut self, collected: &CollectedPlan) -> &'a str {
        self.full_with_metrics
            .get_or_insert_with(|| collected.physical_string(true, true, true, true))
            .as_str()
    }
}

async fn collect_plan_with(
    ctx: &SessionContext,
    plan_future: impl Future<Output = PlanResult<(LogicalPlan, Option<Vec<String>>)>>,
) -> PlanResult<CollectedPlan> {
    let (plan, fields) = plan_future.await?;
    let initial_logical = plan.clone();
    let mut stringified = vec![initial_logical.to_stringified(PlanType::InitialLogicalPlan)];

    // NOTE: Do NOT call `execute_logical_plan` from EXPLAIN.
    // It would execute command nodes and trigger side effects (e.g. CREATE TABLE / INSERT).
    let session_state = ctx.state();
    let logical_plan = strip_explain_side_effect_nodes(plan)?;
    let config_options = session_state.config_options();
    let explain_config = &config_options.explain;

    let analyzed_logical = session_state.analyzer().execute_and_check(
        logical_plan,
        config_options.as_ref(),
        |analyzed_plan, analyzer| {
            let plan_type = PlanType::AnalyzedLogicalPlan {
                analyzer_name: analyzer.name().to_string(),
            };
            stringified.push(analyzed_plan.to_stringified(plan_type));
        },
    )?;
    stringified.push(analyzed_logical.to_stringified(PlanType::FinalAnalyzedLogicalPlan));

    let optimized_logical = session_state.optimizer().optimize(
        analyzed_logical,
        &session_state,
        |optimized_plan, optimizer| {
            let plan_type = PlanType::OptimizedLogicalPlan {
                optimizer_name: optimizer.name().to_string(),
            };
            stringified.push(optimized_plan.to_stringified(plan_type));
        },
    )?;
    stringified.push(optimized_logical.to_stringified(PlanType::FinalLogicalPlan));

    let session_state_no_phys_opt = SessionStateBuilder::new_from_existing(session_state.clone())
        .with_physical_optimizer_rules(vec![])
        .build();

    let mut physical_error = None;
    let mut physical_plan = match session_state_no_phys_opt
        .query_planner()
        .create_physical_plan(&optimized_logical, &session_state_no_phys_opt)
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
        let display_with = |plan: &dyn ExecutionPlan, show_statistics: bool, show_schema: bool| {
            displayable(plan)
                .set_show_statistics(show_statistics)
                .set_show_schema(show_schema)
                .indent(true)
                .to_string()
        };

        stringified.push(StringifiedPlan::new(
            PlanType::InitialPhysicalPlan,
            display_with(
                plan.as_ref(),
                explain_config.show_statistics,
                explain_config.show_schema,
            ),
        ));

        if !explain_config.show_statistics {
            stringified.push(StringifiedPlan::new(
                PlanType::InitialPhysicalPlanWithStats,
                display_with(plan.as_ref(), true, explain_config.show_schema),
            ));
        }
        if !explain_config.show_schema {
            stringified.push(StringifiedPlan::new(
                PlanType::InitialPhysicalPlanWithSchema,
                display_with(plan.as_ref(), explain_config.show_statistics, true),
            ));
        }

        let mut optimized_physical_plan = plan;
        for optimizer in session_state.physical_optimizers() {
            let optimizer_name = optimizer.name().to_string();
            match optimizer.optimize(Arc::clone(&optimized_physical_plan), config_options) {
                Ok(new_plan) => {
                    optimized_physical_plan = new_plan;
                    stringified.push(StringifiedPlan::new(
                        PlanType::OptimizedPhysicalPlan { optimizer_name },
                        display_with(
                            optimized_physical_plan.as_ref(),
                            explain_config.show_statistics,
                            explain_config.show_schema,
                        ),
                    ));
                }
                Err(DataFusionError::Context(_, err)) => {
                    stringified.push(StringifiedPlan::new(
                        PlanType::OptimizedPhysicalPlan { optimizer_name },
                        err.to_string(),
                    ));
                }
                Err(err) => return Err(PlanError::from(err)),
            }
        }

        let plan = match fields {
            Some(fields) => {
                match rename_physical_plan(Arc::clone(&optimized_physical_plan), &fields) {
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
                }
            }
            None => Some(optimized_physical_plan),
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

/// Remove/neutralize logical nodes that can trigger side effects during EXPLAIN.
///
/// - `WithPreconditionsNode` is stripped (preconditions are not executed).
/// - `CatalogCommandNode` is replaced with an empty relation with the same schema.
///
/// This is intentionally EXPLAIN-only: normal execution goes through `execute_logical_plan`,
/// which performs the required side effects.
pub(crate) fn strip_explain_side_effect_nodes(plan: LogicalPlan) -> PlanResult<LogicalPlan> {
    Ok(plan
        .transform_up(|plan| match &plan {
            LogicalPlan::Extension(Extension { node }) => {
                if let Some(n) = node.as_any().downcast_ref::<WithPreconditionsNode>() {
                    Ok(Transformed::yes(n.plan().clone()))
                } else if let Some(n) = node.as_any().downcast_ref::<CatalogCommandNode>() {
                    Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                        EmptyRelation {
                            produce_one_row: false,
                            schema: n.schema().clone(),
                        },
                    )))
                } else {
                    Ok(Transformed::no(plan))
                }
            }
            _ => Ok(Transformed::no(plan)),
        })
        .map_err(PlanError::from)?
        .data)
}

fn render_section(title: &str, body: &str) -> String {
    format!("== {title} ==\n{body}")
}

fn render_stringified_plans(plans: &[StringifiedPlan]) -> String {
    let mut rendered = Vec::with_capacity(plans.len());
    let mut prev: Option<&StringifiedPlan> = None;

    for plan in plans {
        let body = match prev {
            Some(previous) if !should_show(previous, plan) => "SAME TEXT AS ABOVE",
            _ => plan.plan.as_ref(),
        };
        rendered.push(format!("{}:\n{}", plan.plan_type, body));
        prev = Some(plan);
    }

    rendered.join("\n\n")
}

/// Decide whether we should render the full text for `this_plan`
/// given the previously rendered plan to avoid repeating identical
/// plan strings in explain output.
fn should_show(previous_plan: &StringifiedPlan, this_plan: &StringifiedPlan) -> bool {
    (previous_plan.plan != this_plan.plan) || this_plan.should_display(false)
}

async fn maybe_collect_metrics(
    options: &ExplainOptions,
    physical: &Option<Arc<dyn ExecutionPlan>>,
    ctx: &SessionContext,
) -> Result<()> {
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
    let collected = collect_plan_with(ctx, async {
        let resolver = PlanResolver::new(ctx, Arc::clone(&config));
        let NamedPlan { plan, fields } = resolver.resolve_named_plan(plan).await?;
        Ok((plan, fields))
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
    let collected = collect_plan_with(ctx, async move { Ok((plan, fields)) }).await?;
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

    let mut physical = PhysicalStrings::default();

    let output = match options.kind {
        ExplainKind::Simple => {
            let physical_for_mode = if options.analyze {
                physical.full_with_metrics(&collected)
            } else {
                physical.plain(&collected, options.verbose)
            };
            let mut sections = vec![render_section("Physical Plan", physical_for_mode)];
            if options.verbose && !options.analyze {
                sections.push(render_section(
                    "Physical Plan (with statistics)",
                    physical.with_stats(&collected),
                ));
                sections.push(render_section(
                    "Physical Plan (with schema)",
                    physical.with_schema(&collected),
                ));
            }
            sections.join("\n\n")
        }
        ExplainKind::Extended => [
            render_section("Parsed Logical Plan", &logical_simple),
            // TODO: Spark expects distinct analyzed vs optimized plans
            // Avoid duplicating the same plan until we can separate.
            render_section("Analyzed Logical Plan", &logical_optimized),
            render_section(
                "Physical Plan",
                if options.analyze {
                    physical.full_with_metrics(&collected)
                } else {
                    physical.plain(&collected, options.verbose)
                },
            ),
        ]
        .join("\n\n"),
        ExplainKind::Codegen => [
            render_section(
                "Codegen",
                "Whole-stage codegen is not supported; showing physical plan instead.",
            ),
            render_section(
                "Plan Steps",
                &render_stringified_plans(&collected.stringified),
            ),
            render_section(
                "Physical Plan",
                if options.analyze {
                    physical.full_with_metrics(&collected)
                } else {
                    physical.plain(&collected, options.verbose)
                },
            ),
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
                    physical.full(&collected)
                } else {
                    physical.with_stats(&collected)
                },
            ),
        ]
        .join("\n\n"),
        // TODO: Spark FORMATTED mode emits outline + node details
        ExplainKind::Formatted => render_section("Physical Plan", physical.full(&collected)),
    };

    Ok(ExplainString {
        output,
        stringified_plans: collected.stringified,
    })
}
