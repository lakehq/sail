use async_recursion::async_recursion;
use sail_common::spec;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    #[async_recursion]
    pub(in crate::resolver) async fn query_other(
        &mut self,
        node: spec::QueryNode,
    ) -> PlanResult<Query> {
        Ok(match node {
            spec::QueryNode::CommonInlineUserDefinedTableFunction(function) => {
                self.inline_python_table(function).await?
            }
            spec::QueryNode::GroupMap(map) => self.group_map(map).await?,
            spec::QueryNode::CoGroupMap(map) => self.co_group_map(map).await?,
            spec::QueryNode::MapPartitions {
                input,
                function,
                is_barrier: _,
            } => self.map_partitions(*input, function).await?,
            spec::QueryNode::Pivot(pivot) => self.resolve_query_pivot(pivot).await?,
            spec::QueryNode::Unpivot(unpivot) => self.resolve_query_unpivot(unpivot).await?,
            spec::QueryNode::Tail { input, limit } => self.tail(*input, limit).await?,
            spec::QueryNode::StatCrosstab {
                input,
                left_column,
                right_column,
            } => self.crosstab(*input, left_column, right_column).await?,
            spec::QueryNode::StatCov {
                input,
                left_column,
                right_column,
            } => {
                self.pair_statistic(*input, left_column, right_column, "covar_samp", "cov")
                    .await?
            }
            spec::QueryNode::StatCorr {
                input,
                left_column,
                right_column,
                method,
            } => {
                if !method.eq_ignore_ascii_case("pearson") {
                    return Err(PlanError::unsupported(format!(
                        "unsupported correlation method: {method}"
                    )));
                }
                self.pair_statistic(*input, left_column, right_column, "corr", "corr")
                    .await?
            }
            spec::QueryNode::StatSummary { input, statistics } => {
                self.summary(*input, vec![], statistics).await?
            }
            spec::QueryNode::StatDescribe { input, columns } => {
                self.summary(
                    *input,
                    columns,
                    ["count", "mean", "stddev", "min", "max"]
                        .map(String::from)
                        .to_vec(),
                )
                .await?
            }
            spec::QueryNode::StatSampleBy {
                input,
                column,
                fractions,
                seed,
            } => self.stat_sample_by(*input, column, fractions, seed).await?,
            node => {
                return Err(PlanError::unsupported(format!(
                    "query resolution: {node:?}"
                )));
            }
        })
    }
}

impl PlanResolver<'_> {
    pub(in crate::resolver) fn empty(produce_one_row: bool) -> Query {
        Query {
            kind: Deferred::Resolved(QueryKind::Empty { produce_one_row }),
            schema: Deferred::Resolved(QuerySchema::default()),
        }
    }
}

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_query_with_parameters(
        &mut self,
        input: spec::QueryPlan,
        positional_arguments: Vec<spec::Expr>,
        named_arguments: Vec<(String, spec::Expr)>,
    ) -> PlanResult<Query> {
        Ok({
            // Bind against the enclosing parameter scope and an empty row scope.
            // A parameter is a value, never a delayed column reference.
            let mut bindings = Vec::new();
            for (name, argument) in positional_arguments
                .into_iter()
                .enumerate()
                .map(|(i, argument)| (format!("${}", i + 1), argument))
                .chain(named_arguments)
            {
                let expression = self
                    .resolve_named_expression(argument, &QuerySchema::default())
                    .await?;
                let value = crate::analyzer::lowering::evaluate_constant(&expression.expression)?;
                bindings.push((name, value));
            }
            let previous = self.state.scope.parameters.clone();
            self.state.scope.parameters.extend(bindings);
            let result = self
                .resolve_query_plan(input)
                .await
                .map(|input| self.scoped_query(input));
            self.state.scope.parameters = previous;
            result?
        })
    }
}
