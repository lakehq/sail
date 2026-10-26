use async_recursion::async_recursion;
use sail_common::spec;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    #[async_recursion]
    pub(in crate::resolver) async fn resolve_query_plan(
        &mut self,
        request: spec::QueryPlan,
    ) -> PlanResult<Query> {
        let plan_id = request.plan_id;
        let mut plan = match request.node {
            node @ (spec::QueryNode::Empty { .. }
            | spec::QueryNode::Values(_)
            | spec::QueryNode::Range(_)
            | spec::QueryNode::LocalRelation { .. }
            | spec::QueryNode::Read { .. }
            | spec::QueryNode::CachedRemoteRelation { .. }) => self.query_source(node).await?,
            spec::QueryNode::Project { input, expressions } => {
                self.resolve_query_project(input.map(|input| *input), expressions)
                    .await?
            }
            spec::QueryNode::Aggregate(aggregate) => {
                self.resolve_query_aggregate(aggregate).await?
            }
            spec::QueryNode::Filter { input, condition } => {
                self.resolve_query_filter(*input, condition).await?
            }
            spec::QueryNode::Sort {
                input,
                order,
                is_global,
            } => self.resolve_query_sort(*input, order, is_global).await?,
            spec::QueryNode::Limit { input, skip, limit } => {
                self.resolve_query_limit(*input, skip, limit).await?
            }
            spec::QueryNode::SubqueryAlias {
                input,
                alias,
                qualifier,
            } => {
                self.resolve_query_subquery_alias(*input, alias, qualifier)
                    .await?
            }
            spec::QueryNode::TableAlias {
                input,
                name,
                columns,
            } => {
                self.resolve_query_table_alias(*input, name, columns)
                    .await?
            }
            spec::QueryNode::NamedWindows { input, windows } => {
                self.resolve_query_named_windows(*input, windows).await?
            }
            spec::QueryNode::WithCtes {
                input,
                recursive,
                ctes,
            } => {
                self.resolve_query_with_ctes(*input, recursive, ctes)
                    .await?
            }
            spec::QueryNode::WithRelations { root, references } => {
                self.with_relations(*root, references).await?
            }
            spec::QueryNode::WithParameters {
                input,
                positional_arguments,
                named_arguments,
            } => {
                self.resolve_query_with_parameters(*input, positional_arguments, named_arguments)
                    .await?
            }
            node @ (spec::QueryNode::ToDf { .. }
            | spec::QueryNode::WithColumns { .. }
            | spec::QueryNode::Drop { .. }
            | spec::QueryNode::ToSchema { .. }
            | spec::QueryNode::WithColumnsRenamed { .. }
            | spec::QueryNode::FillNa { .. }
            | spec::QueryNode::DropNa { .. }
            | spec::QueryNode::Replace { .. }) => self.query_columns(node).await?,
            node @ (spec::QueryNode::LateralView { .. }
            | spec::QueryNode::Repartition { .. }
            | spec::QueryNode::RepartitionByExpression { .. }
            | spec::QueryNode::Deduplicate(_)
            | spec::QueryNode::Hint { .. }
            | spec::QueryNode::Sample(_)
            | spec::QueryNode::TableSample { .. }
            | spec::QueryNode::SetOperation(_)
            | spec::QueryNode::LateralJoin { .. }
            | spec::QueryNode::Join(_)) => self.query_relations(node).await?,
            node @ (spec::QueryNode::CommonInlineUserDefinedTableFunction(_)
            | spec::QueryNode::GroupMap(_)
            | spec::QueryNode::CoGroupMap(_)
            | spec::QueryNode::MapPartitions { .. }
            | spec::QueryNode::Pivot(_)
            | spec::QueryNode::Unpivot(_)
            | spec::QueryNode::Tail { .. }
            | spec::QueryNode::StatCrosstab { .. }
            | spec::QueryNode::StatCov { .. }
            | spec::QueryNode::StatCorr { .. }
            | spec::QueryNode::StatSummary { .. }
            | spec::QueryNode::StatDescribe { .. }
            | spec::QueryNode::StatSampleBy { .. }) => self.query_other(node).await?,
            spec::QueryNode::CachedLocalRelation { .. } => {
                return Err(PlanError::todo("cached local relation"));
            }
            spec::QueryNode::CollectMetrics { .. } => {
                return Err(PlanError::todo("collect metrics"));
            }
            spec::QueryNode::Parse(_) => return Err(PlanError::todo("parse")),
            spec::QueryNode::WithWatermark(_) => return Err(PlanError::todo("with watermark")),
            spec::QueryNode::ApplyInPandasWithState(_) => {
                return Err(PlanError::todo("apply in pandas with state"));
            }
            spec::QueryNode::StatApproxQuantile { .. } => {
                return Err(PlanError::todo("approx quantile"));
            }
            spec::QueryNode::StatFreqItems { .. } => return Err(PlanError::todo("freq items")),
        };
        if let Some(id) = plan_id {
            let mut schema = plan.schema.into_resolved("query schema")?;
            for attribute in &mut schema.attributes {
                if !attribute.plan_ids.contains(&id) {
                    attribute.plan_ids.push(id);
                }
            }
            self.state
                .bindings
                .entry(id)
                .or_default()
                .push(schema.clone());
            plan.schema = Deferred::Resolved(schema);
        }
        Ok(plan)
    }
}

const IMPLICIT_CARTESIAN_PRODUCT_MSG: &str = "Detected implicit cartesian product for INNER join between logical plans. \
    Join condition is missing or trivial. \
    Either: use the CROSS JOIN syntax to allow cartesian products between \
    these relations, or: enable implicit cartesian products by setting the \
    configuration variable spark.sql.crossJoin.enabled=true;";

pub(in crate::resolver) mod aggregate;
pub(in crate::resolver) mod aggregate_request;
pub(in crate::resolver) mod alias;
pub(in crate::resolver) mod column_op;
pub(in crate::resolver) mod cte;
pub(in crate::resolver) mod join;
pub(in crate::resolver) mod lateral_join;
pub(in crate::resolver) mod limit;
pub(in crate::resolver) mod misc;
pub(in crate::resolver) mod missing_input;
pub(in crate::resolver) mod na;
pub(in crate::resolver) mod pivoting;
pub(in crate::resolver) mod project;
pub(in crate::resolver) mod read;
pub(in crate::resolver) mod sample;
pub(in crate::resolver) mod set_op;
pub(in crate::resolver) mod stat;
pub(in crate::resolver) mod time_travel;
pub(in crate::resolver) mod udf;
pub(in crate::resolver) mod udtf;
pub(in crate::resolver) mod values;

pub(in crate::resolver) mod filter;

pub(in crate::resolver) mod sort;

pub(in crate::resolver) mod window;
