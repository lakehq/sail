use datafusion_expr::{expr, Expr, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::xml::xpath::Xpath;
use sail_function::scalar::xml::xpath_typed::{XpathTyped, XpathTypedKind};

use crate::error::PlanResult;
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn xpath(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (xml, path) = input.arguments.two()?;
    let func = Xpath::new();
    Ok(Expr::ScalarFunction(expr::ScalarFunction {
        func: std::sync::Arc::new(ScalarUDF::from(func)),
        args: vec![xml, path],
    }))
}

fn xpath_typed(kind: XpathTypedKind) -> impl Fn(ScalarFunctionInput) -> PlanResult<Expr> {
    move |input: ScalarFunctionInput| {
        let (xml, path) = input.arguments.two()?;
        let func = XpathTyped::new(kind);
        Ok(Expr::ScalarFunction(expr::ScalarFunction {
            func: std::sync::Arc::new(ScalarUDF::from(func)),
            args: vec![xml, path],
        }))
    }
}

pub(super) fn list_built_in_xml_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("from_xml", F::unknown("from_xml")),
        ("schema_of_xml", F::unknown("schema_of_xml")),
        ("to_xml", F::unknown("to_xml")),
        ("xpath", F::custom(xpath)),
        (
            "xpath_boolean",
            F::custom(xpath_typed(XpathTypedKind::Boolean)),
        ),
        (
            "xpath_double",
            F::custom(xpath_typed(XpathTypedKind::Double)),
        ),
        ("xpath_float", F::custom(xpath_typed(XpathTypedKind::Float))),
        ("xpath_int", F::custom(xpath_typed(XpathTypedKind::Int))),
        ("xpath_long", F::custom(xpath_typed(XpathTypedKind::Long))),
        (
            "xpath_number",
            F::custom(xpath_typed(XpathTypedKind::Number)),
        ),
        ("xpath_short", F::custom(xpath_typed(XpathTypedKind::Short))),
        (
            "xpath_string",
            F::custom(xpath_typed(XpathTypedKind::String)),
        ),
    ]
}
