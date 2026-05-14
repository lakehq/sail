use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::functions_aggregate::approx_median::approx_median_udaf;
use datafusion::functions_aggregate::approx_percentile_cont::approx_percentile_cont_udaf;
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::functions_aggregate::stddev::stddev_udaf;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion_common::{Column, ExprSchema, ScalarValue};
use datafusion_expr::expr::{AggregateFunctionParams, ScalarFunction};
use datafusion_expr::{
    and, col, expr, lit, or, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, ScalarUDF,
};
use sail_common::spec;
use sail_common_datafusion::logical_expr::alias_preserving_metadata;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::math::random::Random;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_stat_summary(
        &self,
        input: spec::QueryPlan,
        columns: Vec<spec::Identifier>,
        statistics: Vec<String>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let columns: Vec<Column> = if columns.is_empty() {
            input.schema().columns()
        } else {
            self.resolve_columns(input.schema(), &columns, state)?
        };
        let statistics: HashSet<String> = if statistics.is_empty() {
            HashSet::from([
                "count".to_string(),
                "mean".to_string(),
                "stddev".to_string(),
                "min".to_string(),
                "25%".to_string(),
                "50%".to_string(),
                "75%".to_string(),
                "max".to_string(),
            ])
        } else {
            statistics
                .into_iter()
                .map(|x| x.trim().to_lowercase())
                .collect()
        };

        let mut all_aggregates = Vec::new();
        for column in &columns {
            let column_metadata = input
                .schema()
                .field_from_column(column)
                .ok()
                .map(|f| f.metadata().clone())
                .unwrap_or_default();
            if statistics.contains("count") {
                let count = alias_preserving_metadata(
                    Expr::AggregateFunction(expr::AggregateFunction {
                        func: count_udaf(),
                        params: AggregateFunctionParams {
                            args: vec![Expr::Column(column.clone())],
                            distinct: false,
                            filter: None,
                            order_by: vec![],
                            null_treatment: None,
                        },
                    }),
                    state.register_field_name(format!("count_{}", column.name())),
                    column_metadata.clone(),
                );
                all_aggregates.push(count);
            }

            if let Ok(field) = input.schema().field_from_column(column) {
                if field.data_type().is_numeric() {
                    if statistics.contains("mean") {
                        let mean = alias_preserving_metadata(
                            Expr::AggregateFunction(expr::AggregateFunction {
                                func: avg_udaf(),
                                params: AggregateFunctionParams {
                                    args: vec![Expr::Column(column.clone())],
                                    distinct: false,
                                    filter: None,
                                    order_by: vec![],
                                    null_treatment: None,
                                },
                            }),
                            state.register_field_name(format!("mean_{}", column.name())),
                            column_metadata.clone(),
                        );
                        all_aggregates.push(mean);
                    }
                    if statistics.contains("stddev") {
                        let stddev = alias_preserving_metadata(
                            Expr::AggregateFunction(expr::AggregateFunction {
                                func: stddev_udaf(),
                                params: AggregateFunctionParams {
                                    args: vec![Expr::Column(column.clone())],
                                    distinct: false,
                                    filter: None,
                                    order_by: vec![],
                                    null_treatment: None,
                                },
                            }),
                            state.register_field_name(format!("stddev_{}", column.name())),
                            column_metadata.clone(),
                        );
                        all_aggregates.push(stddev);
                    }
                    if statistics.contains("25%") {
                        let percentile_25 = alias_preserving_metadata(
                            Expr::AggregateFunction(expr::AggregateFunction {
                                func: approx_percentile_cont_udaf(),
                                params: AggregateFunctionParams {
                                    args: vec![
                                        Expr::Column(column.clone()),
                                        Expr::Literal(ScalarValue::Float64(Some(0.25_f64)), None),
                                    ],
                                    distinct: false,
                                    filter: None,
                                    order_by: vec![],
                                    null_treatment: None,
                                },
                            }),
                            state.register_field_name(format!("25%_{}", column.name())),
                            column_metadata.clone(),
                        );
                        all_aggregates.push(percentile_25);
                    }
                    if statistics.contains("50%") {
                        let percentile_50 = alias_preserving_metadata(
                            Expr::AggregateFunction(expr::AggregateFunction {
                                func: approx_median_udaf(),
                                params: AggregateFunctionParams {
                                    args: vec![Expr::Column(column.clone())],
                                    distinct: false,
                                    filter: None,
                                    order_by: vec![],
                                    null_treatment: None,
                                },
                            }),
                            state.register_field_name(format!("50%_{}", column.name())),
                            column_metadata.clone(),
                        );
                        all_aggregates.push(percentile_50);
                    }
                    if statistics.contains("75%") {
                        let percentile_75 = alias_preserving_metadata(
                            Expr::AggregateFunction(expr::AggregateFunction {
                                func: approx_percentile_cont_udaf(),
                                params: AggregateFunctionParams {
                                    args: vec![
                                        Expr::Column(column.clone()),
                                        Expr::Literal(ScalarValue::Float64(Some(0.75_f64)), None),
                                    ],
                                    distinct: false,
                                    filter: None,
                                    order_by: vec![],
                                    null_treatment: None,
                                },
                            }),
                            state.register_field_name(format!("75%_{}", column.name())),
                            column_metadata.clone(),
                        );
                        all_aggregates.push(percentile_75);
                    }
                }
            }

            if statistics.contains("min") {
                let min = alias_preserving_metadata(
                    Expr::AggregateFunction(expr::AggregateFunction {
                        func: min_udaf(),
                        params: AggregateFunctionParams {
                            args: vec![Expr::Column(column.clone())],
                            distinct: false,
                            filter: None,
                            order_by: vec![],
                            null_treatment: None,
                        },
                    }),
                    state.register_field_name(format!("min_{}", column.name())),
                    column_metadata.clone(),
                );
                all_aggregates.push(min);
            }

            if statistics.contains("max") {
                let max = alias_preserving_metadata(
                    Expr::AggregateFunction(expr::AggregateFunction {
                        func: max_udaf(),
                        params: AggregateFunctionParams {
                            args: vec![Expr::Column(column.clone())],
                            distinct: false,
                            filter: None,
                            order_by: vec![],
                            null_treatment: None,
                        },
                    }),
                    state.register_field_name(format!("max_{}", column.name())),
                    column_metadata.clone(),
                );
                all_aggregates.push(max);
            }
        }

        let stats_plan = LogicalPlanBuilder::from(input)
            .aggregate(Vec::<Expr>::new(), all_aggregates)?
            .build()?;

        let summary_alias = state.register_field_name("summary");
        let create_stat_row = |stat_name: &str,
                               stats_by_column: Vec<(String, Expr)>|
         -> PlanResult<LogicalPlan> {
            let stats_plan_clone = stats_plan.clone();
            let summary_inner = Expr::Literal(ScalarValue::Utf8(Some(stat_name.to_string())), None);
            let summary_metadata = summary_inner
                .to_field(stats_plan_clone.schema().as_ref())?
                .1
                .metadata()
                .clone();
            let mut projections = vec![alias_preserving_metadata(
                summary_inner,
                summary_alias.as_str(),
                summary_metadata,
            )];
            for (col_name, expr) in stats_by_column {
                let metadata = if let Expr::Column(col) = &expr {
                    stats_plan_clone
                        .schema()
                        .field_from_column(col)
                        .ok()
                        .map(|f| f.metadata().clone())
                        .unwrap_or_default()
                } else {
                    HashMap::new()
                };
                let expr = expr.cast_to(&DataType::Utf8, stats_plan_clone.schema())?;
                projections.push(alias_preserving_metadata(expr, &col_name, metadata));
            }
            let plan = LogicalPlanBuilder::from(stats_plan_clone)
                .project(projections)?
                .build()?;
            Ok(plan)
        };

        let mut union_plan = None;
        for stat_type in statistics {
            let stat_type = stat_type.as_str();
            let mut stats_by_column = Vec::new();
            for column in &columns {
                let column_name = column.name().to_string();
                let stat_expr = match stat_type {
                    "count" => Some(Expr::Column(self.resolve_one_column(
                        stats_plan.schema(),
                        &format!("count_{column_name}"),
                        state,
                    )?)),
                    "mean" => self
                        .resolve_optional_column(
                            stats_plan.schema(),
                            &format!("mean_{column_name}"),
                            None,
                            state,
                        )?
                        .map(Expr::Column),
                    "stddev" => self
                        .resolve_optional_column(
                            stats_plan.schema(),
                            &format!("stddev_{column_name}"),
                            None,
                            state,
                        )?
                        .map(Expr::Column),
                    "min" => Some(Expr::Column(self.resolve_one_column(
                        stats_plan.schema(),
                        &format!("min_{column_name}"),
                        state,
                    )?)),
                    "25%" => Some(Expr::Column(self.resolve_one_column(
                        stats_plan.schema(),
                        &format!("25%_{column_name}"),
                        state,
                    )?)),
                    "50%" => Some(Expr::Column(self.resolve_one_column(
                        stats_plan.schema(),
                        &format!("50%_{column_name}"),
                        state,
                    )?)),
                    "75%" => Some(Expr::Column(self.resolve_one_column(
                        stats_plan.schema(),
                        &format!("75%_{column_name}"),
                        state,
                    )?)),
                    "max" => Some(Expr::Column(self.resolve_one_column(
                        stats_plan.schema(),
                        &format!("max_{column_name}"),
                        state,
                    )?)),
                    _ => None,
                };

                if let Some(expr) = stat_expr {
                    stats_by_column.push((column_name, expr));
                }
            }

            if !stats_by_column.is_empty() {
                let stat_row = create_stat_row(stat_type, stats_by_column)?;
                union_plan = Some(match union_plan {
                    Some(plan) => LogicalPlanBuilder::from(plan).union(stat_row)?.build()?,
                    None => stat_row,
                });
            }
        }
        union_plan.ok_or_else(|| PlanError::internal("No describe statistics generated"))
    }

    pub(super) async fn resolve_query_stat_describe(
        &self,
        input: spec::QueryPlan,
        columns: Vec<spec::Identifier>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let statistics = vec![
            "count".to_string(),
            "mean".to_string(),
            "stddev".to_string(),
            "min".to_string(),
            "max".to_string(),
        ];
        self.resolve_query_stat_summary(input, columns, statistics, state)
            .await
    }

    pub(super) async fn resolve_query_stat_cross_tab(
        &self,
        input: spec::QueryPlan,
        left_column: spec::Identifier,
        right_column: spec::Identifier,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let cross_tab_alias = state.register_field_name(format!(
            "{}_{}",
            left_column.as_ref(),
            right_column.as_ref()
        ));
        let left_column = self.resolve_one_column(input.schema(), left_column.as_ref(), state)?;
        let right_column = self.resolve_one_column(input.schema(), right_column.as_ref(), state)?;
        let left_column_metadata = input
            .schema()
            .field_from_column(&left_column)
            .ok()
            .map(|f| f.metadata().clone())
            .unwrap_or_default();

        let projected_plan = LogicalPlanBuilder::from(input.clone())
            .project(vec![Expr::Cast(expr::Cast {
                expr: Box::new(col(right_column.clone())),
                data_type: DataType::Utf8,
            })
            .alias_qualified(
                right_column.relation.clone(),
                right_column.name.clone(),
            )])?
            .build()?;
        let distinct_values = LogicalPlanBuilder::from(projected_plan)
            .project(vec![Expr::Column(right_column.clone())])?
            .distinct()?
            .build()?;
        // TODO: This can be expensive for large input datasets
        let distinct_values_batches = self
            .ctx
            .execute_logical_plan(distinct_values)
            .await?
            .collect()
            .await?;

        let mut unique_values: Vec<(String, String)> = vec![];
        for batch in distinct_values_batches {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| PlanError::internal("Expected string array"))?;

            for i in 0..array.len() {
                if array.is_valid(i) {
                    let value = array.value(i).to_string();
                    let alias = state.register_field_name(&value);
                    unique_values.push((value, alias));
                }
            }
        }

        let input_schema = input.schema().clone();
        let mut projection_exprs = vec![col(left_column.clone())];
        for (value, alias) in &unique_values {
            let case_inner = Expr::Case(expr::Case {
                expr: None,
                when_then_expr: vec![(
                    Box::new(lit(value).eq(col(right_column.clone()))),
                    Box::new(lit(1)),
                )],
                else_expr: Some(Box::new(lit(0))),
            });
            let case_metadata = case_inner
                .to_field(input_schema.as_ref())?
                .1
                .metadata()
                .clone();
            projection_exprs.push(alias_preserving_metadata(
                case_inner,
                alias.as_str(),
                case_metadata,
            ));
        }

        let projected_counts_plan = LogicalPlanBuilder::from(input)
            .project(projection_exprs)?
            .build()?;
        let projected_schema = projected_counts_plan.schema().clone();

        let aggregate_exprs = unique_values
            .iter()
            .map(|(value, alias)| {
                let agg_inner = Expr::AggregateFunction(expr::AggregateFunction {
                    func: sum_udaf(),
                    params: AggregateFunctionParams {
                        args: vec![col(alias)],
                        distinct: false,
                        filter: None,
                        order_by: vec![],
                        null_treatment: None,
                    },
                });
                let agg_metadata = agg_inner
                    .to_field(projected_schema.as_ref())?
                    .1
                    .metadata()
                    .clone();
                Ok(alias_preserving_metadata(
                    agg_inner,
                    state.register_field_name(value),
                    agg_metadata,
                ))
            })
            .collect::<PlanResult<Vec<Expr>>>()?;

        let plan = LogicalPlanBuilder::from(projected_counts_plan)
            .aggregate(
                vec![alias_preserving_metadata(
                    col(left_column),
                    cross_tab_alias.as_str(),
                    left_column_metadata,
                )],
                aggregate_exprs,
            )?
            .build()?;
        let expr: Vec<Expr> = plan
            .schema()
            .columns()
            .into_iter()
            .map(|column| {
                if column.name() == cross_tab_alias {
                    Expr::Cast(expr::Cast {
                        expr: Box::new(Expr::Column(column.clone())),
                        data_type: DataType::Utf8,
                    })
                } else {
                    Expr::Column(column)
                }
            })
            .collect();
        LogicalPlanBuilder::from(plan)
            .project(expr)?
            .build()
            .map_err(Into::into)
    }

    pub(super) async fn resolve_query_stat_cov(
        &self,
        input: spec::QueryPlan,
        left_column: spec::Identifier,
        right_column: spec::Identifier,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let covar_inner = Expr::AggregateFunction(expr::AggregateFunction {
            func: datafusion::functions_aggregate::covariance::covar_samp_udaf(),
            params: AggregateFunctionParams {
                args: vec![
                    Expr::Column(self.resolve_one_column(
                        input.schema(),
                        left_column.as_ref(),
                        state,
                    )?),
                    Expr::Column(self.resolve_one_column(
                        input.schema(),
                        right_column.as_ref(),
                        state,
                    )?),
                ],
                distinct: false,
                filter: None,
                order_by: vec![],
                null_treatment: None,
            },
        });
        let covar_metadata = covar_inner
            .to_field(input.schema().as_ref())?
            .1
            .metadata()
            .clone();
        let covar_samp = alias_preserving_metadata(
            covar_inner,
            state.register_field_name("cov"),
            covar_metadata,
        );
        Ok(LogicalPlanBuilder::from(input)
            .aggregate(Vec::<Expr>::new(), vec![covar_samp])?
            .build()?)
    }

    pub(super) async fn resolve_query_stat_corr(
        &self,
        input: spec::QueryPlan,
        left_column: spec::Identifier,
        right_column: spec::Identifier,
        method: String,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        if !method.eq_ignore_ascii_case("pearson") {
            return Err(PlanError::unsupported(format!(
                "Unsupported correlation method: {method}. Currently only Pearson is supported.",
            )));
        }
        let input = self.resolve_query_plan(input, state).await?;
        let corr_inner = Expr::AggregateFunction(expr::AggregateFunction {
            func: datafusion::functions_aggregate::correlation::corr_udaf(),
            params: AggregateFunctionParams {
                args: vec![
                    Expr::Column(self.resolve_one_column(
                        input.schema(),
                        left_column.as_ref(),
                        state,
                    )?),
                    Expr::Column(self.resolve_one_column(
                        input.schema(),
                        right_column.as_ref(),
                        state,
                    )?),
                ],
                distinct: false,
                filter: None,
                order_by: vec![],
                null_treatment: None,
            },
        });
        let corr_metadata = corr_inner
            .to_field(input.schema().as_ref())?
            .1
            .metadata()
            .clone();
        let corr =
            alias_preserving_metadata(corr_inner, state.register_field_name("corr"), corr_metadata);
        Ok(LogicalPlanBuilder::from(input)
            .aggregate(Vec::<Expr>::new(), vec![corr])?
            .build()?)
    }

    pub(super) async fn resolve_query_stat_sample_by(
        &self,
        input: spec::QueryPlan,
        column: spec::Expr,
        fractions: Vec<spec::Fraction>,
        seed: Option<i64>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        if fractions
            .iter()
            .any(|f| f.fraction < 0.0 || f.fraction > 1.0)
        {
            return Err(PlanError::invalid(
                "All fraction values must be >= 0.0 and <= 1.0",
            ));
        }

        let input: LogicalPlan = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let schema = input.schema();
        let column_expr: Column = match &column {
            spec::Expr::UnresolvedAttribute {
                name,
                plan_id,
                is_metadata_column: false,
            } => {
                let name: Vec<String> = name.clone().into();
                let Ok(name) = name.one() else {
                    return Err(PlanError::invalid("Expected simple column name"));
                };
                match self.resolve_optional_column(schema, &name, *plan_id, state)? {
                    Some(col) => col,
                    None => {
                        return Err(PlanError::invalid(format!(
                            "Could not resolve column: {name}"
                        )));
                    }
                }
            }
            _ => {
                return Err(PlanError::invalid("Expected UnresolvedAttribute"));
            }
        };

        let init_exprs: Vec<Expr> = input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect();
        let rand_column_name: String = state.register_hidden_field_name("rand_value");

        let rand_inner = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::from(Random::new())),
            args: vec![Expr::Literal(ScalarValue::Int64(seed), None)],
        });
        let rand_metadata = rand_inner
            .to_field(input.schema().as_ref())?
            .1
            .metadata()
            .clone();
        let rand_expr =
            alias_preserving_metadata(rand_inner, rand_column_name.as_str(), rand_metadata);
        let mut all_exprs: Vec<Expr> = init_exprs.clone();
        all_exprs.push(rand_expr);
        let plan_with_rand: LogicalPlan = LogicalPlanBuilder::from(input)
            .project(all_exprs)?
            .build()?;

        let mut acc_exprs: Vec<Expr> = vec![];
        for frac in &fractions {
            let key_val = self.resolve_literal(frac.stratum.clone(), state)?;
            let f = and(
                Expr::Column(column_expr.clone()).eq(lit(key_val)),
                col(&rand_column_name).lt_eq(lit(frac.fraction)),
            );
            acc_exprs.push(f);
        }

        let final_expr: Expr = acc_exprs.into_iter().reduce(or).unwrap_or(lit(false));
        Ok(LogicalPlanBuilder::from(plan_with_rand)
            .filter(final_expr)?
            .build()?)
    }
}
