use async_recursion::async_recursion;
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

mod aggregate;
mod alias;
mod column_op;
mod cte;
mod dedup;
mod filter;
mod join;
mod lateral;
mod limit;
mod misc;
mod na;
mod pivoting;
mod project;
mod read;
mod recursion;
mod repartition;
mod sample;
mod set_op;
mod sort;
mod stat;
mod udf;
mod udtf;
mod values;

impl PlanResolver<'_> {
    /// Resolve query plan.
    /// No hidden fields are kept in the resolved plan.
    #[async_recursion]
    pub(super) async fn resolve_query_plan(
        &self,
        plan: spec::QueryPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let plan = self
            .resolve_query_plan_with_hidden_fields(plan, state)
            .await?;
        self.remove_hidden_fields(plan, state)
    }

    /// Resolve query plan.
    /// The resolved plan may contain hidden fields.
    /// If the hidden fields cannot be handled,
    /// [`Self::resolve_query_plan`] should be used instead,
    #[async_recursion]
    async fn resolve_query_plan_with_hidden_fields(
        &self,
        plan: spec::QueryPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::QueryNode;

        let plan_id = plan.plan_id;
        let plan = match plan.node {
            QueryNode::Read {
                read_type,
                is_streaming: _,
            } => match read_type {
                spec::ReadType::NamedTable(table) => {
                    self.resolve_query_read_named_table(table, state).await?
                }
                spec::ReadType::Udtf(udtf) => self.resolve_query_read_udtf(udtf, state).await?,
                spec::ReadType::DataSource(source) => {
                    self.resolve_query_read_data_source(source, state).await?
                }
            },
            QueryNode::Project { input, expressions } => {
                self.resolve_query_project(input.map(|x| *x), expressions, state)
                    .await?
            }
            QueryNode::Filter { input, condition } => {
                self.resolve_query_filter(*input, condition, state).await?
            }
            QueryNode::Join(join) => self.resolve_query_join(join, state).await?,
            QueryNode::SetOperation(op) => self.resolve_query_set_operation(op, state).await?,
            QueryNode::Sort {
                input,
                order,
                is_global,
            } => {
                self.resolve_query_sort(*input, order, is_global, state)
                    .await?
            }
            QueryNode::Limit { input, skip, limit } => {
                self.resolve_query_limit(*input, skip, limit, state).await?
            }
            QueryNode::Aggregate(aggregate) => {
                self.resolve_query_aggregate(aggregate, state).await?
            }
            QueryNode::WithParameters {
                input,
                positional_arguments,
                named_arguments,
            } => {
                self.resolve_query_with_parameters(
                    *input,
                    positional_arguments,
                    named_arguments,
                    state,
                )
                .await?
            }
            QueryNode::LocalRelation { data, schema } => {
                self.resolve_query_local_relation(data, schema, state)
                    .await?
            }
            QueryNode::Sample(sample) => self.resolve_query_sample(sample, state).await?,
            QueryNode::Deduplicate(deduplicate) => {
                self.resolve_query_deduplicate(deduplicate, state).await?
            }
            QueryNode::Range(range) => self.resolve_query_range(range, state).await?,
            QueryNode::SubqueryAlias {
                input,
                alias,
                qualifier,
            } => {
                self.resolve_query_subquery_alias(*input, alias, qualifier, state)
                    .await?
            }
            QueryNode::Repartition {
                input,
                num_partitions,
                // TODO: avoid unnecessary repartition if `shuffle` is false
                shuffle: _,
            } => {
                self.resolve_query_repartition(*input, num_partitions, state)
                    .await?
            }
            QueryNode::ToDf {
                input,
                column_names,
            } => {
                self.resolve_query_to_df(*input, column_names, state)
                    .await?
            }
            QueryNode::WithColumnsRenamed {
                input,
                rename_columns_map,
            } => {
                self.resolve_query_with_columns_renamed(*input, rename_columns_map, state)
                    .await?
            }
            QueryNode::Drop {
                input,
                columns,
                column_names,
            } => {
                self.resolve_query_drop(*input, columns, column_names, state)
                    .await?
            }
            QueryNode::Tail { input, limit } => {
                self.resolve_query_tail(*input, limit, state).await?
            }
            QueryNode::WithColumns { input, aliases } => {
                self.resolve_query_with_columns(*input, aliases, state)
                    .await?
            }
            QueryNode::Hint {
                input,
                name,
                parameters,
            } => {
                self.resolve_query_hint(*input, name, parameters, state)
                    .await?
            }
            QueryNode::Pivot(pivot) => self.resolve_query_pivot(pivot, state).await?,
            QueryNode::Unpivot(unpivot) => self.resolve_query_unpivot(unpivot, state).await?,
            QueryNode::ToSchema { input, schema } => {
                self.resolve_query_to_schema(*input, schema, state).await?
            }
            QueryNode::RepartitionByExpression {
                input,
                partition_expressions,
                num_partitions,
            } => {
                self.resolve_query_repartition_by_expression(
                    *input,
                    partition_expressions,
                    num_partitions,
                    state,
                )
                .await?
            }
            QueryNode::MapPartitions {
                input,
                function,
                is_barrier,
            } => {
                self.resolve_query_map_partitions(*input, function, is_barrier, state)
                    .await?
            }
            QueryNode::CollectMetrics {
                input,
                name,
                metrics,
            } => {
                self.resolve_query_collect_metrics(*input, name, metrics, state)
                    .await?
            }
            QueryNode::Parse(parse) => self.resolve_query_parse(parse, state).await?,
            QueryNode::GroupMap(map) => self.resolve_query_group_map(map, state).await?,
            QueryNode::CoGroupMap(map) => self.resolve_query_co_group_map(map, state).await?,
            QueryNode::WithWatermark(watermark) => {
                self.resolve_query_with_watermark(watermark, state).await?
            }
            QueryNode::ApplyInPandasWithState(apply) => {
                self.resolve_query_apply_in_pandas_with_state(apply, state)
                    .await?
            }
            QueryNode::CachedLocalRelation { .. } => {
                return Err(PlanError::todo("cached local relation"));
            }
            QueryNode::CachedRemoteRelation { .. } => {
                return Err(PlanError::todo("cached remote relation"));
            }
            QueryNode::CommonInlineUserDefinedTableFunction(udtf) => {
                self.resolve_query_common_inline_udtf(udtf, state).await?
            }
            QueryNode::FillNa {
                input,
                columns,
                values,
            } => {
                self.resolve_query_fill_na(*input, columns, values, state)
                    .await?
            }
            QueryNode::DropNa {
                input,
                columns,
                min_non_nulls,
            } => {
                self.resolve_query_drop_na(*input, columns, min_non_nulls, state)
                    .await?
            }
            QueryNode::Replace {
                input,
                columns,
                replacements,
            } => {
                self.resolve_query_replace(*input, columns, replacements, state)
                    .await?
            }
            QueryNode::StatSummary { input, statistics } => {
                self.resolve_query_stat_summary(*input, vec![], statistics, state)
                    .await?
            }
            QueryNode::StatCrosstab {
                input,
                left_column,
                right_column,
            } => {
                self.resolve_query_stat_cross_tab(*input, left_column, right_column, state)
                    .await?
            }
            QueryNode::StatDescribe { input, columns } => {
                self.resolve_query_stat_describe(*input, columns, state)
                    .await?
            }
            QueryNode::StatCov {
                input,
                left_column,
                right_column,
            } => {
                self.resolve_query_stat_cov(*input, left_column, right_column, state)
                    .await?
            }
            QueryNode::StatCorr {
                input,
                left_column,
                right_column,
                method,
            } => {
                self.resolve_query_stat_corr(*input, left_column, right_column, method, state)
                    .await?
            }
            QueryNode::StatApproxQuantile { .. } => {
                return Err(PlanError::todo("approx quantile"));
            }
            QueryNode::StatFreqItems { .. } => {
                return Err(PlanError::todo("freq items"));
            }
            QueryNode::StatSampleBy {
                input,
                column,
                fractions,
                seed,
            } => {
                self.resolve_query_stat_sample_by(*input, column, fractions, seed, state)
                    .await?
            }
            QueryNode::Empty { produce_one_row } => self.resolve_query_empty(produce_one_row)?,
            QueryNode::Values(values) => self.resolve_query_values(values, state).await?,
            QueryNode::TableAlias {
                input,
                name,
                columns,
            } => {
                self.resolve_query_table_alias(*input, name, columns, state)
                    .await?
            }
            QueryNode::WithCtes {
                input,
                recursive,
                ctes,
            } => {
                self.resolve_query_with_ctes(*input, recursive, ctes, state)
                    .await?
            }
            QueryNode::LateralView {
                input,
                function,
                arguments,
                named_arguments,
                table_alias,
                column_aliases,
                outer,
            } => {
                self.resolve_query_lateral_view(
                    input.map(|x| *x),
                    function,
                    arguments,
                    named_arguments,
                    table_alias,
                    column_aliases,
                    outer,
                    state,
                )
                .await?
            }
        };
        self.verify_query_plan(&plan, state)?;
        self.register_schema_with_plan_id(&plan, plan_id, state)?;
        Ok(plan)
    }

    fn remove_hidden_fields(
        &self,
        plan: LogicalPlan,
        state: &PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut columns = vec![];
        let mut has_hidden_columns = false;
        for column in plan.schema().columns() {
            let info = state.get_field_info(column.name())?;
            if info.is_hidden() {
                has_hidden_columns = true;
            } else {
                columns.push(column);
            }
        }
        if has_hidden_columns {
            let plan = LogicalPlanBuilder::new(plan)
                .project(columns.into_iter().map(Expr::Column))?
                .build()?;
            Ok(plan)
        } else {
            Ok(plan)
        }
    }

    /// All resolved plans must have "resolved columns".
    /// If you define new fields in the plan, register the field in the state and use the "resolved field name" to alias the newly created field.
    /// If you fetch an existing field in the plan, you likely have the "unresolved" field name from the spec.
    /// Convert the unresolved field name to the "resolved field name" using the state.
    fn verify_query_plan(&self, plan: &LogicalPlan, state: &PlanResolverState) -> PlanResult<()> {
        let invalid = plan
            .schema()
            .fields()
            .iter()
            .filter_map(|f| {
                if state.get_field_info(f.name()).is_ok() {
                    None
                } else {
                    Some(f.name().to_string())
                }
            })
            .collect::<Vec<_>>();
        if invalid.is_empty() {
            Ok(())
        } else {
            Err(PlanError::internal(format!(
                "a plan resolver bug has produced invalid fields: {invalid:?}",
            )))
        }
    }

    fn register_schema_with_plan_id(
        &self,
        plan: &LogicalPlan,
        plan_id: Option<i64>,
        state: &mut PlanResolverState,
    ) -> PlanResult<()> {
        if let Some(plan_id) = plan_id {
            for field in plan.schema().fields() {
                state.register_plan_id_for_field(field.name(), plan_id)?;
            }
        }
        Ok(())
    }
}
