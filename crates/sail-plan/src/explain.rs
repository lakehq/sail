use std::future::Future;
use std::sync::{Arc, Mutex};

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::optimizer::{ConfigOnlyContext, PhysicalOptimizerContext};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::SessionContext;
use datafusion_common::config::ConfigOptions;
use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::LogicalPlan;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::physical_plan::rename_physical_plan;
use sail_common_datafusion::session::job::JobService;

use crate::config::PlanConfig;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::plan::NamedPlan;

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
    analyzed_logical: LogicalPlan,
    optimized_logical: LogicalPlan,
    physical_plan: Option<Arc<dyn ExecutionPlan>>,
    physical_error: Option<String>,
    stringified: Vec<StringifiedPlan>,
}

impl CollectedPlan {
    fn logical_string(&self, plan: &LogicalPlan, plan_type: PlanType) -> String {
        plan.to_stringified(plan_type).plan.to_string()
    }

    fn logical_string_with_schema(&self, plan: &LogicalPlan, plan_type: PlanType) -> String {
        let stringified_logical = self.logical_string(plan, plan_type);
        let stringified_schema = plan
            .schema()
            .inner()
            .fields()
            .iter()
            .map(|f| format!("{}: {}", f.name(), f.data_type()))
            .collect::<Vec<String>>()
            .join(", ");
        format!("{}\n{}", stringified_schema, stringified_logical)
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

#[derive(Debug, Default)]
struct PhysicalPlanRecords {
    plans: Vec<StringifiedPlan>,
}

#[derive(Debug)]
struct PhysicalPlanObserver {
    optimizer: Option<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    records: Arc<Mutex<PhysicalPlanRecords>>,
}

impl PhysicalOptimizerRule for PhysicalPlanObserver {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.optimize_with_context(plan, &ConfigOnlyContext::new(config))
    }

    fn optimize_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = &context.config_options().explain;
        let display = |plan: &dyn ExecutionPlan, show_statistics, show_schema| {
            displayable(plan)
                .set_show_statistics(show_statistics)
                .set_show_schema(show_schema)
                .indent(true)
                .to_string()
        };
        let Some(optimizer) = &self.optimizer else {
            let mut records = self.records.lock().map_err(|_| {
                DataFusionError::Internal("EXPLAIN physical plan recorder is poisoned".into())
            })?;
            records.plans.push(StringifiedPlan::new(
                PlanType::InitialPhysicalPlan,
                display(plan.as_ref(), config.show_statistics, config.show_schema),
            ));
            if !config.show_statistics {
                records.plans.push(StringifiedPlan::new(
                    PlanType::InitialPhysicalPlanWithStats,
                    display(plan.as_ref(), true, config.show_schema),
                ));
            }
            if !config.show_schema {
                records.plans.push(StringifiedPlan::new(
                    PlanType::InitialPhysicalPlanWithSchema,
                    display(plan.as_ref(), config.show_statistics, true),
                ));
            }
            return Ok(plan);
        };

        let result = optimizer.optimize_with_context(plan, context);
        let mut records = self.records.lock().map_err(|_| {
            DataFusionError::Internal("EXPLAIN physical plan recorder is poisoned".into())
        })?;
        let plan_type = PlanType::OptimizedPhysicalPlan {
            optimizer_name: optimizer.name().to_string(),
        };
        match result {
            Ok(plan) => {
                records.plans.push(StringifiedPlan::new(
                    plan_type,
                    display(plan.as_ref(), config.show_statistics, config.show_schema),
                ));
                Ok(plan)
            }
            Err(error) => {
                let diagnostic = match &error {
                    DataFusionError::Context(_, error) => error.to_string(),
                    error => error.to_string(),
                };
                records
                    .plans
                    .push(StringifiedPlan::new(plan_type, diagnostic));
                Err(error)
            }
        }
    }

    fn name(&self) -> &str {
        self.optimizer
            .as_ref()
            .map_or("ExplainInitialPhysicalPlan", |optimizer| optimizer.name())
    }

    fn schema_check(&self) -> bool {
        self.optimizer
            .as_ref()
            .is_none_or(|optimizer| optimizer.schema_check())
    }
}

async fn collect_plan_with(
    ctx: &SessionContext,
    plan_future: impl Future<Output = PlanResult<(LogicalPlan, Option<Vec<String>>)>>,
) -> PlanResult<CollectedPlan> {
    let (plan, fields) = plan_future.await?;
    let initial_logical = plan.clone();
    let mut stringified = vec![initial_logical.to_stringified(PlanType::InitialLogicalPlan)];

    let session_state = ctx.state();
    let config_options = session_state.config_options();

    let analyzed_logical = session_state.analyzer().execute_and_check(
        plan,
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
        analyzed_logical.clone(),
        &session_state,
        |optimized_plan, optimizer| {
            let plan_type = PlanType::OptimizedLogicalPlan {
                optimizer_name: optimizer.name().to_string(),
            };
            stringified.push(optimized_plan.to_stringified(plan_type));
        },
    )?;
    stringified.push(optimized_logical.to_stringified(PlanType::FinalLogicalPlan));

    let records = Arc::new(Mutex::new(PhysicalPlanRecords::default()));
    let optimizers = std::iter::once(None)
        .chain(
            session_state
                .physical_optimizers()
                .iter()
                .cloned()
                .map(Some),
        )
        .map(|optimizer| {
            Arc::new(PhysicalPlanObserver {
                optimizer,
                records: Arc::clone(&records),
            }) as Arc<dyn PhysicalOptimizerRule + Send + Sync>
        })
        .collect();
    // Capture within the real optimizer lifecycle: initial plans need not yet
    // satisfy the executable invariants enforced after the last rule.
    let explain_state = SessionStateBuilder::new_from_existing(session_state.clone())
        .with_physical_optimizer_rules(optimizers)
        .build();

    let result = explain_state
        .query_planner()
        .create_physical_plan(&optimized_logical, &explain_state)
        .await;
    let records = std::mem::take(&mut *records.lock().map_err(|_| {
        DataFusionError::Internal("EXPLAIN physical plan recorder is poisoned".into())
    })?);
    stringified.extend(records.plans);
    let mut physical_error = None;
    let mut physical_plan = match result {
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

    if let Some(optimized_physical_plan) = physical_plan.take() {
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
        analyzed_logical,
        optimized_logical,
        physical_plan,
        physical_error,
        stringified,
    })
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

fn distributed_plan_string(
    ctx: &SessionContext,
    physical: &Option<Arc<dyn ExecutionPlan>>,
) -> Option<String> {
    let plan = physical.as_ref()?;
    let service = ctx.extension::<JobService>().ok()?;
    match service.runner().explain(Arc::clone(plan)) {
        Ok(plan) => Some(plan),
        Err(err) => Some(format!("Distributed plan error: {err}")),
    }
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
    let logical_analyzed_schema = collected.logical_string_with_schema(
        &collected.analyzed_logical,
        PlanType::FinalAnalyzedLogicalPlan,
    );
    let logical_optimized =
        collected.logical_string(&collected.optimized_logical, PlanType::FinalLogicalPlan);

    let mut physical = PhysicalStrings::default();

    let sections = match options.kind {
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
            sections
        }
        ExplainKind::Extended => {
            vec![
                render_section("Parsed Logical Plan", &logical_simple),
                // prepend schema to make analyzed plan distinct from optimized plan
                render_section("Analyzed Logical Plan", &logical_analyzed_schema),
                render_section("Optimized Logical Plan", &logical_optimized),
                render_section(
                    "Physical Plan",
                    if options.analyze {
                        physical.full_with_metrics(&collected)
                    } else {
                        physical.plain(&collected, options.verbose)
                    },
                ),
            ]
        }
        ExplainKind::Codegen => {
            let mut sections = vec![
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
            ];
            if let Some(plan) = distributed_plan_string(ctx, &collected.physical_plan) {
                sections.push(render_section("Distributed Plan", &plan));
            }
            sections
        }
        ExplainKind::Cost => {
            vec![
                render_section("Optimized Logical Plan", &logical_optimized),
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
        }
        // TODO: Spark FORMATTED mode emits outline + node details
        ExplainKind::Formatted => {
            vec![render_section("Physical Plan", physical.full(&collected))]
        }
    };
    let output = sections.join("\n\n");

    Ok(ExplainString {
        output,
        stringified_plans: collected.stringified,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::{MemTable, provider_as_source};
    use datafusion::physical_plan::execution_plan::InvariantLevel;
    use datafusion::physical_plan::joins::SortMergeJoinExec;
    use datafusion::physical_plan::operator_statistics::StatisticsRegistry;
    use datafusion::prelude::SessionConfig;
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion_expr::{JoinType, LogicalPlanBuilder, col};

    use super::*;

    #[derive(Debug)]
    struct ContextRule {
        name: &'static str,
        calls: Arc<AtomicUsize>,
        fail: bool,
        skip_failed_rules: bool,
    }

    impl PhysicalOptimizerRule for ContextRule {
        fn optimize(
            &self,
            _plan: Arc<dyn ExecutionPlan>,
            _config: &ConfigOptions,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Err(DataFusionError::Internal(
                "EXPLAIN lost the physical optimizer context".into(),
            ))
        }

        fn optimize_with_context(
            &self,
            plan: Arc<dyn ExecutionPlan>,
            context: &dyn PhysicalOptimizerContext,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            assert!(context.statistics_registry().is_some());
            assert_eq!(
                context.config_options().optimizer.skip_failed_rules,
                self.skip_failed_rules
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(DataFusionError::Context(
                    "delegate context".into(),
                    Box::new(DataFusionError::Plan("physical rule failure".into())),
                ))
            } else {
                Ok(plan)
            }
        }

        fn name(&self) -> &str {
            self.name
        }

        fn schema_check(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn explain_repairs_join_distribution_before_executable_validation() -> PlanResult<()> {
        let config = SessionConfig::new()
            .with_target_partitions(4)
            .set_bool("datafusion.optimizer.prefer_hash_join", false);
        let context = SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(config)
                .build(),
        );
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let partitions = [vec![1, 3], vec![2, 4]]
            .into_iter()
            .map(|values| {
                Ok(vec![RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(values))],
                )?])
            })
            .collect::<Result<Vec<_>>>()?;
        let source = provider_as_source(Arc::new(MemTable::try_new(schema, partitions)?));
        let logical = LogicalPlanBuilder::scan("l", Arc::clone(&source), None)?
            .join(
                LogicalPlanBuilder::scan("r", source, None)?.build()?,
                JoinType::Left,
                (vec!["k"], vec!["k"]),
                None,
            )?
            .project(vec![col("l.k").alias("lk"), col("r.k").alias("rk")])?
            .build()?;
        let collected = collect_plan_with(&context, async {
            Ok((logical, Some(vec!["left_key".into(), "right_key".into()])))
        })
        .await?;
        assert!(collected.physical_error.is_none());
        let plan = collected
            .physical_plan
            .as_ref()
            .ok_or_else(|| PlanError::internal("EXPLAIN did not produce a physical plan"))?;
        assert_eq!(plan.schema().field(0).name(), "left_key");
        assert_eq!(plan.schema().field(1).name(), "right_key");
        let mut joins = 0;
        plan.apply(|node| {
            node.check_invariants(InvariantLevel::Executable)?;
            joins += usize::from(node.is::<SortMergeJoinExec>());
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(joins, 1);
        let observed = collected
            .stringified
            .iter()
            .filter_map(|plan| match &plan.plan_type {
                PlanType::OptimizedPhysicalPlan { optimizer_name } => Some(optimizer_name.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let state = context.state();
        assert_eq!(
            observed,
            state
                .physical_optimizers()
                .iter()
                .map(|rule| rule.name())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            collected
                .stringified
                .iter()
                .filter(|plan| matches!(plan.plan_type, PlanType::FinalPhysicalPlan))
                .count(),
            1,
        );
        Ok(())
    }

    #[tokio::test]
    async fn explain_preserves_initial_variants_and_runs_context_rules_once() -> PlanResult<()> {
        for show_statistics in [false, true] {
            for show_schema in [false, true] {
                for enable_rule in [false, true] {
                    let calls = Arc::new(AtomicUsize::new(0));
                    let rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> = if enable_rule {
                        vec![Arc::new(ContextRule {
                            name: "context_rule",
                            calls: Arc::clone(&calls),
                            fail: false,
                            skip_failed_rules: false,
                        })]
                    } else {
                        vec![]
                    };
                    let config = SessionConfig::new()
                        .set_bool("datafusion.explain.show_statistics", show_statistics)
                        .set_bool("datafusion.explain.show_schema", show_schema)
                        .set_bool("datafusion.optimizer.skip_failed_rules", false);
                    let context = SessionContext::new_with_state(
                        SessionStateBuilder::new()
                            .with_default_features()
                            .with_config(config)
                            .with_statistics_registry(StatisticsRegistry::new())
                            .with_physical_optimizer_rules(rules)
                            .build(),
                    );
                    let logical = LogicalPlanBuilder::empty(false).build()?;
                    let collected =
                        collect_plan_with(&context, async { Ok((logical, None)) }).await?;
                    assert!(collected.physical_error.is_none());
                    assert!(collected.physical_plan.is_some());
                    assert_eq!(calls.load(Ordering::SeqCst), usize::from(enable_rule));
                    let physical = collected
                        .stringified
                        .iter()
                        .filter_map(|plan| match plan.plan_type {
                            PlanType::InitialPhysicalPlan => Some("initial"),
                            PlanType::InitialPhysicalPlanWithStats => Some("stats"),
                            PlanType::InitialPhysicalPlanWithSchema => Some("schema"),
                            PlanType::OptimizedPhysicalPlan { .. } => Some("rule"),
                            PlanType::FinalPhysicalPlan => Some("final"),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    let mut expected = vec!["initial"];
                    if !show_statistics {
                        expected.push("stats");
                    }
                    if !show_schema {
                        expected.push("schema");
                    }
                    if enable_rule {
                        expected.push("rule");
                    }
                    expected.push("final");
                    assert_eq!(physical, expected);
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn explain_stops_after_contextual_physical_errors_without_replay() -> PlanResult<()> {
        for skip_failed_rules in [false, true] {
            let failure_calls = Arc::new(AtomicUsize::new(0));
            let later_calls = Arc::new(AtomicUsize::new(0));
            let context = SessionContext::new_with_state(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .set_bool("datafusion.optimizer.skip_failed_rules", skip_failed_rules),
                    )
                    .with_statistics_registry(StatisticsRegistry::new())
                    .with_physical_optimizer_rules(vec![
                        Arc::new(ContextRule {
                            name: "failing_rule",
                            calls: Arc::clone(&failure_calls),
                            fail: true,
                            skip_failed_rules,
                        }),
                        Arc::new(ContextRule {
                            name: "later_rule",
                            calls: Arc::clone(&later_calls),
                            fail: false,
                            skip_failed_rules,
                        }),
                    ])
                    .build(),
            );
            let logical = LogicalPlanBuilder::empty(false).build()?;
            let collected = collect_plan_with(&context, async { Ok((logical, None)) }).await?;
            assert_eq!(failure_calls.load(Ordering::SeqCst), 1);
            assert_eq!(later_calls.load(Ordering::SeqCst), 0);
            assert!(collected.physical_plan.is_none());
            let expected = PlanError::from(DataFusionError::Context(
                "failing_rule".into(),
                Box::new(DataFusionError::Context(
                    "delegate context".into(),
                    Box::new(DataFusionError::Plan("physical rule failure".into())),
                )),
            ))
            .to_string();
            assert_eq!(collected.physical_error.as_deref(), Some(expected.as_str()));
            let stages = collected
                .stringified
                .iter()
                .filter_map(|plan| match &plan.plan_type {
                    PlanType::OptimizedPhysicalPlan { optimizer_name } => {
                        Some((optimizer_name.as_str(), plan.plan.as_str()))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(
                stages,
                vec![(
                    "failing_rule",
                    "Error during planning: physical rule failure"
                )]
            );
            assert!(
                !collected
                    .stringified
                    .iter()
                    .any(|plan| matches!(plan.plan_type, PlanType::FinalPhysicalPlan))
            );
        }
        Ok(())
    }
}
