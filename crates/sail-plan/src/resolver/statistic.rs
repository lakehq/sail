use std::collections::HashSet;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes as adt;
use datafusion::functions_aggregate::approx_median::approx_median_udaf;
use datafusion::functions_aggregate::approx_percentile_cont::approx_percentile_cont_udaf;
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::functions_aggregate::stddev::stddev_udaf;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::{col, expr, lit, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_summary(
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
            if statistics.contains("count") {
                let count = Expr::AggregateFunction(expr::AggregateFunction {
                    func: count_udaf(),
                    args: vec![Expr::Column(column.clone())],
                    distinct: false,
                    filter: None,
                    order_by: None,
                    null_treatment: None,
                })
                .alias(state.register_field_name(format!("count_{}", column.name())));
                all_aggregates.push(count);
            }

            if let Ok(field) = input.schema().field_from_column(column) {
                if field.data_type().is_numeric() {
                    if statistics.contains("mean") {
                        let mean = Expr::AggregateFunction(expr::AggregateFunction {
                            func: avg_udaf(),
                            args: vec![Expr::Column(column.clone())],
                            distinct: false,
                            filter: None,
                            order_by: None,
                            null_treatment: None,
                        })
                        .alias(state.register_field_name(format!("mean_{}", column.name())));
                        all_aggregates.push(mean);
                    }
                    if statistics.contains("stddev") {
                        let stddev = Expr::AggregateFunction(expr::AggregateFunction {
                            func: stddev_udaf(),
                            args: vec![Expr::Column(column.clone())],
                            distinct: false,
                            filter: None,
                            order_by: None,
                            null_treatment: None,
                        })
                        .alias(state.register_field_name(format!("stddev_{}", column.name())));
                        all_aggregates.push(stddev);
                    }
                    if statistics.contains("25%") {
                        let percentile_25 = Expr::AggregateFunction(expr::AggregateFunction {
                            func: approx_percentile_cont_udaf(),
                            args: vec![
                                Expr::Column(column.clone()),
                                Expr::Literal(ScalarValue::Float64(Some(0.25_f64))),
                            ],
                            distinct: false,
                            filter: None,
                            order_by: None,
                            null_treatment: None,
                        })
                        .alias(state.register_field_name(format!("25%_{}", column.name())));
                        all_aggregates.push(percentile_25);
                    }
                    if statistics.contains("50%") {
                        let percentile_50 = Expr::AggregateFunction(expr::AggregateFunction {
                            func: approx_median_udaf(),
                            args: vec![Expr::Column(column.clone())],
                            distinct: false,
                            filter: None,
                            order_by: None,
                            null_treatment: None,
                        })
                        .alias(state.register_field_name(format!("50%_{}", column.name())));
                        all_aggregates.push(percentile_50);
                    }
                    if statistics.contains("75%") {
                        let percentile_75 = Expr::AggregateFunction(expr::AggregateFunction {
                            func: approx_percentile_cont_udaf(),
                            args: vec![
                                Expr::Column(column.clone()),
                                Expr::Literal(ScalarValue::Float64(Some(0.75_f64))),
                            ],
                            distinct: false,
                            filter: None,
                            order_by: None,
                            null_treatment: None,
                        })
                        .alias(state.register_field_name(format!("75%_{}", column.name())));
                        all_aggregates.push(percentile_75);
                    }
                }
            }

            if statistics.contains("min") {
                let min = Expr::AggregateFunction(expr::AggregateFunction {
                    func: min_udaf(),
                    args: vec![Expr::Column(column.clone())],
                    distinct: false,
                    filter: None,
                    order_by: None,
                    null_treatment: None,
                })
                .alias(state.register_field_name(format!("min_{}", column.name())));
                all_aggregates.push(min);
            }

            if statistics.contains("max") {
                let max = Expr::AggregateFunction(expr::AggregateFunction {
                    func: max_udaf(),
                    args: vec![Expr::Column(column.clone())],
                    distinct: false,
                    filter: None,
                    order_by: None,
                    null_treatment: None,
                })
                .alias(state.register_field_name(format!("max_{}", column.name())));
                all_aggregates.push(max);
            }
        }

        let stats_plan = LogicalPlanBuilder::from(input)
            .aggregate(Vec::<Expr>::new(), all_aggregates)?
            .build()?;

        let summary_alias = state.register_field_name("summary");
        let create_stat_row =
            |stat_name: &str, stats_by_column: Vec<(String, Expr)>| -> PlanResult<LogicalPlan> {
                let stats_plan_clone = stats_plan.clone();
                let mut projections =
                    vec![
                        Expr::Literal(ScalarValue::Utf8(Some(stat_name.to_string())))
                            .alias(&summary_alias),
                    ];
                for (col_name, expr) in stats_by_column {
                    let expr = expr.cast_to(&adt::DataType::Utf8, stats_plan_clone.schema())?;
                    projections.push(expr.alias(&col_name));
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
                        &format!("count_{}", column_name),
                        state,
                    )?)),
                    "mean" => self
                        .resolve_optional_column(
                            stats_plan.schema(),
                            &format!("mean_{}", column_name),
                            None,
                            state,
                        )?
                        .map(Expr::Column),
                    "stddev" => self
                        .resolve_optional_column(
                            stats_plan.schema(),
                            &format!("stddev_{}", column_name),
                            None,
                            state,
                        )?
                        .map(Expr::Column),
                    "min" => Some(Expr::Column(self.resolve_one_column(
                        stats_plan.schema(),
                        &format!("min_{}", column_name),
                        state,
                    )?)),
                    "25%" => Some(Expr::Column(self.resolve_one_column(
                        stats_plan.schema(),
                        &format!("25%_{}", column_name),
                        state,
                    )?)),
                    "50%" => Some(Expr::Column(self.resolve_one_column(
                        stats_plan.schema(),
                        &format!("50%_{}", column_name),
                        state,
                    )?)),
                    "75%" => Some(Expr::Column(self.resolve_one_column(
                        stats_plan.schema(),
                        &format!("75%_{}", column_name),
                        state,
                    )?)),
                    "max" => Some(Expr::Column(self.resolve_one_column(
                        stats_plan.schema(),
                        &format!("max_{}", column_name),
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

    pub(super) async fn resolve_query_cross_tab(
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

        let projected_plan = LogicalPlanBuilder::from(input.clone())
            .project(vec![Expr::Cast(expr::Cast {
                expr: Box::new(col(right_column.clone())),
                data_type: adt::DataType::Utf8,
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

        let mut projection_exprs = vec![col(left_column.clone())];
        for (value, alias) in &unique_values {
            let column_expr = Expr::Case(expr::Case {
                expr: None,
                when_then_expr: vec![(
                    Box::new(lit(value).eq(col(right_column.clone()))),
                    Box::new(lit(1)),
                )],
                else_expr: Some(Box::new(lit(0))),
            })
            .alias(alias);
            projection_exprs.push(column_expr);
        }

        let projected_counts_plan = LogicalPlanBuilder::from(input)
            .project(projection_exprs)?
            .build()?;

        let aggregate_exprs = unique_values
            .iter()
            .map(|(value, alias)| {
                Ok(Expr::AggregateFunction(expr::AggregateFunction {
                    func: sum_udaf(),
                    args: vec![col(alias)],
                    distinct: false,
                    filter: None,
                    order_by: None,
                    null_treatment: None,
                })
                .alias(state.register_field_name(value)))
            })
            .collect::<PlanResult<Vec<Expr>>>()?;

        let plan = LogicalPlanBuilder::from(projected_counts_plan)
            .aggregate(
                vec![col(left_column).alias(cross_tab_alias.as_str())],
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
                        data_type: adt::DataType::Utf8,
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
}
