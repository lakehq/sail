use std::sync::Arc;

use datafusion_expr::ScalarUDF;
use sail_function::scalar::csv::SparkSchemaOfCsv;
use sail_function::scalar::csv::spark_from_csv::SparkFromCSV;
use sail_function::scalar::csv::spark_to_csv::SparkToCsv;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::{Expr, ExprKind};
use crate::error::PlanResult;
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn from_csv(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    if let Some(schema) = values.get_mut(1)
        && !matches!(schema.kind, Deferred::Resolved(ExprKind::Literal(_)))
        && let Ok(value) = crate::analyzer::lowering::evaluate_constant(schema)
    {
        *schema = context.literal(value);
    }
    let function = ScalarUDF::from(SparkFromCSV::new(timezone));
    context.scalar_udf(Arc::new(function), values)
}

fn to_csv(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    context.scalar_udf(Arc::new(ScalarUDF::from(SparkToCsv::new(timezone))), values)
}

pub(super) fn list_built_in_csv_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("from_csv", F::custom(from_csv)),
        ("schema_of_csv", F::udf(SparkSchemaOfCsv::new())),
        ("to_csv", F::custom(to_csv)),
    ]
}
