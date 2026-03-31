use datafusion_expr::expr;
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_function::scalar::xml::xpath_udf;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn xpath(
    ScalarFunctionInput {
        arguments,
        function_context: _,
    }: ScalarFunctionInput,
) -> PlanResult<expr::Expr> {
    let (xml, path) = arguments
        .into_iter()
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|arguments: Vec<_>| PlanError::invalid(format!("xpath expects 2 arguments, got {}", arguments.len())))?;
    validate_xpath_path(&path)?;
    Ok(xpath_udf().call(vec![xml, path]))
}

fn validate_xpath_path(path: &expr::Expr) -> PlanResult<()> {
    LiteralEvaluator::new().evaluate(path).map(|_| ()).map_err(|_| {
        PlanError::invalid(format!(
            "Cannot resolve \"xpath(xml, path)\" due to data type mismatch: the input path should be a foldable \"STRING\" expression; however, got \"{path}\"."
        ))
    })
}

pub(super) fn list_built_in_xml_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("from_xml", F::unknown("from_xml")),
        ("schema_of_xml", F::unknown("schema_of_xml")),
        ("to_xml", F::unknown("to_xml")),
        ("xpath", F::custom(xpath)),
        ("xpath_boolean", F::unknown("xpath_boolean")),
        ("xpath_double", F::unknown("xpath_double")),
        ("xpath_float", F::unknown("xpath_float")),
        ("xpath_int", F::unknown("xpath_int")),
        ("xpath_long", F::unknown("xpath_long")),
        ("xpath_number", F::unknown("xpath_number")),
        ("xpath_short", F::unknown("xpath_short")),
        ("xpath_string", F::unknown("xpath_string")),
    ]
}
