use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::select_expr::SelectExpr;
use datafusion_expr::{col, lit, Expr, Extension, LogicalPlan, LogicalPlanBuilder, ScalarUDF};
use rand::{rng, Rng};
use sail_common::spec;
use sail_common::spec::{NullOrdering, SortDirection, SortOrder};
use sail_function::scalar::array::spark_sequence::SparkSequence;
use sail_function::scalar::math::rand_poisson::RandPoisson;
use sail_function::scalar::math::random::Random;
use sail_logical_plan::sort::SortWithinPartitionsNode;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

/// Copied from `arrow_ord::rank::can_rank` (private in arrow-ord).
fn can_rank(data_type: &DataType) -> bool {
    data_type.is_primitive()
        || matches!(
            data_type,
            DataType::Boolean
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
        )
}

/// Copied from `arrow_ord::sort::can_sort_to_indices` (private in arrow-ord).
fn can_sort_to_indices(data_type: &DataType) -> bool {
    data_type.is_primitive()
        || matches!(
            data_type,
            DataType::Boolean
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::FixedSizeBinary(_)
        )
        || match data_type {
            DataType::List(f) if can_rank(f.data_type()) => true,
            DataType::LargeList(f) if can_rank(f.data_type()) => true,
            DataType::FixedSizeList(f, _) if can_rank(f.data_type()) => true,
            DataType::Dictionary(_, values) if can_rank(values.as_ref()) => true,
            DataType::RunEndEncoded(_, f) if can_sort_to_indices(f.data_type()) => true,
            _ => false,
        }
}

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_sample(
        &self,
        sample: spec::Sample,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::Sample {
            input,
            lower_bound,
            upper_bound,
            with_replacement,
            seed,
            deterministic_order,
        } = sample;
        if lower_bound >= upper_bound {
            return Err(PlanError::invalid(format!(
                "invalid sample bounds: [{lower_bound}, {upper_bound})"
            )));
        }
        // if defined seed use these values otherwise use random seed
        // to generate the random values in with_replacement mode, in lambda value
        let seed: i64 = seed.unwrap_or_else(|| {
            let mut rng = rng();
            rng.random::<i64>()
        });

        let mut input: LogicalPlan = self
            .resolve_query_plan_with_hidden_fields(*input, state)
            .await?;

        if deterministic_order {
            let schema = input.schema();
            let columns = schema.columns();

            // Build spec::SortOrder using user-facing column names
            let sort_orders: Vec<SortOrder> = schema
                .fields()
                .iter()
                .enumerate()
                .filter_map(|(idx, field)| {
                    if !can_sort_to_indices(field.data_type()) {
                        return None;
                    }
                    let col = &columns[idx];
                    let info = state.get_field_info(&col.name).ok()?;
                    if info.is_hidden() {
                        return None;
                    }
                    // Use user-facing name for proper resolution
                    Some(SortOrder {
                        child: Box::new(spec::Expr::UnresolvedAttribute {
                            name: spec::ObjectName::bare(info.name().to_string()),
                            plan_id: None,
                            is_metadata_column: false,
                        }),
                        direction: SortDirection::Ascending,
                        null_ordering: NullOrdering::NullsFirst,
                    })
                })
                .collect();

            if !sort_orders.is_empty() {
                let sort_exprs = self
                    .resolve_sort_orders(sort_orders, true, input.schema(), state)
                    .await?;

                input = LogicalPlan::Extension(Extension {
                    node: Arc::new(SortWithinPartitionsNode::new(
                        Arc::new(input),
                        sort_exprs,
                        None,
                    )),
                });
            }
        }

        let rand_column_name: String = state.register_field_name("rand_value");
        let rand_expr: Expr = if with_replacement {
            Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(ScalarUDF::from(RandPoisson::new())),
                args: vec![
                    Expr::Literal(ScalarValue::Float64(Some(upper_bound)), None),
                    Expr::Literal(ScalarValue::Int64(Some(seed)), None),
                ],
            })
            .alias(&rand_column_name)
        } else {
            Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(ScalarUDF::from(Random::new())),
                args: vec![Expr::Literal(ScalarValue::Int64(Some(seed)), None)],
            })
            .alias(&rand_column_name)
        };
        let init_exprs: Vec<Expr> = input
            .schema()
            .columns()
            .iter()
            .map(|col| Expr::Column(col.clone()))
            .collect();
        let mut all_exprs: Vec<Expr> = init_exprs.clone();
        all_exprs.push(rand_expr);
        let plan_with_rand: LogicalPlan = LogicalPlanBuilder::from(input)
            .project(all_exprs)?
            .build()?;

        if with_replacement {
            Self::resolve_sample_with_replacement(
                plan_with_rand,
                &rand_column_name,
                init_exprs,
                state,
            )
        } else {
            Self::resolve_sample_without_replacement(
                plan_with_rand,
                &rand_column_name,
                lower_bound,
                upper_bound,
                init_exprs,
            )
        }
    }

    /// Bernoulli sampling - filter rows where random value falls in [lower, upper)
    fn resolve_sample_without_replacement(
        plan_with_rand: LogicalPlan,
        rand_column_name: &str,
        lower_bound: f64,
        upper_bound: f64,
        init_exprs: Vec<Expr>,
    ) -> PlanResult<LogicalPlan> {
        let plan = LogicalPlanBuilder::from(plan_with_rand)
            .filter(col(rand_column_name).lt(lit(upper_bound)))?
            .filter(col(rand_column_name).gt_eq(lit(lower_bound)))?
            .build()?;
        let plan = LogicalPlanBuilder::from(plan)
            .project(init_exprs)?
            .build()?;
        Ok(plan)
    }

    /// Poisson sampling - replicate rows based on Poisson distribution
    fn resolve_sample_with_replacement(
        plan_with_rand: LogicalPlan,
        rand_column_name: &str,
        init_exprs: Vec<Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let init_exprs_aux: Vec<Expr> = plan_with_rand
            .schema()
            .columns()
            .iter()
            .map(|col| Expr::Column(col.clone()))
            .collect();
        let array_column_name: String = state.register_field_name("array_value");
        let arr_expr: Expr = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::from(SparkSequence::new())),
            args: vec![
                Expr::Literal(ScalarValue::Int64(Some(1)), None),
                col(rand_column_name),
            ],
        })
        .alias(&array_column_name);
        let plan = LogicalPlanBuilder::from(plan_with_rand)
            .project(
                init_exprs_aux
                    .into_iter()
                    .chain(vec![arr_expr])
                    .map(Into::into)
                    .collect::<Vec<SelectExpr>>(),
            )?
            .build()?;
        let plan = LogicalPlanBuilder::from(plan)
            .unnest_column(array_column_name)?
            .build()?;
        let plan = LogicalPlanBuilder::from(plan)
            .project(
                init_exprs
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<SelectExpr>>(),
            )?
            .build()?;
        Ok(plan)
    }
}
