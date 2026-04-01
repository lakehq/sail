use datafusion_expr::{expr, Expr, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::xml::xpath::Xpath;

use crate::error::PlanResult;
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn xpath(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let (xml, path) = input.arguments.two()?;
    let func = Xpath::new();
    Ok(Expr::ScalarFunction(
        datafusion_expr::expr::ScalarFunction {
            func: std::sync::Arc::new(ScalarUDF::from(func)),
            args: vec![xml, path],
        },
    ))
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
