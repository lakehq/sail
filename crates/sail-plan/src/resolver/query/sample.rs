use std::sync::Arc;

use datafusion_common::ScalarValue;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::select_expr::SelectExpr;
use datafusion_expr::{col, lit, Expr, LogicalPlan, LogicalPlanBuilder, ScalarUDF};
use rand::{rng, Rng};
use sail_common::spec;
use sail_function::scalar::array::spark_sequence::SparkSequence;
use sail_function::scalar::math::rand_poisson::RandPoisson;
use sail_function::scalar::math::random::Random;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

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
            ..
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

        let input: LogicalPlan = self
            .resolve_query_plan_with_hidden_fields(*input, state)
            .await?;
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

        if !with_replacement {
            let plan: LogicalPlan = LogicalPlanBuilder::from(plan_with_rand)
                .filter(col(&rand_column_name).lt(lit(upper_bound)))?
                .filter(col(&rand_column_name).gt_eq(lit(lower_bound)))?
                .build()?;
            let plan: LogicalPlan = LogicalPlanBuilder::from(plan)
                .project(init_exprs)?
                .build()?;
            Ok(plan)
        } else {
            let plan: LogicalPlan = plan_with_rand.clone();
            let init_exprs_aux: Vec<Expr> = plan
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
                    col(&rand_column_name),
                ],
            })
            .alias(&array_column_name);
            let plan: LogicalPlan = LogicalPlanBuilder::from(plan)
                .project(
                    init_exprs_aux
                        .clone()
                        .into_iter()
                        .chain(vec![arr_expr])
                        .map(Into::into)
                        .collect::<Vec<SelectExpr>>(),
                )?
                .build()?;
            let plan: LogicalPlan = LogicalPlanBuilder::from(plan)
                .unnest_column(array_column_name.clone())?
                .build()?;

            Ok(LogicalPlanBuilder::from(plan)
                .project(
                    init_exprs
                        .into_iter()
                        .map(Into::into)
                        .collect::<Vec<SelectExpr>>(),
                )?
                .build()?)
        }
    }
}
