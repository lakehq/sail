use datafusion_expr::{Expr, ScalarUDF, expr};
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::xml::from_xml::SparkFromXml;
use sail_function::scalar::xml::to_xml::SparkToXml;
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

fn to_xml(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let tz = input.function_context.plan_config.session_timezone.clone();
    let udf = ScalarUDF::from(SparkToXml::new(tz));
    Ok(udf.call(input.arguments))
}

fn from_xml(
    ScalarFunctionInput {
        mut arguments,
        function_context,
    }: ScalarFunctionInput,
) -> PlanResult<Expr> {
    let tz = function_context.plan_config.session_timezone.clone();
    if arguments.len() >= 2 && !matches!(&arguments[1], expr::Expr::Literal(_, _)) {
        let evaluator = LiteralEvaluator::new();
        if let Ok(scalar) = evaluator.evaluate(&arguments[1]) {
            let scalar = match scalar {
                datafusion_common::ScalarValue::Utf8View(v)
                | datafusion_common::ScalarValue::LargeUtf8(v) => {
                    datafusion_common::ScalarValue::Utf8(v)
                }
                other => other,
            };
            arguments[1] = expr::Expr::Literal(scalar, None);
        }
    }
    let udf = ScalarUDF::from(SparkFromXml::new(tz));
    Ok(udf.call(arguments))
}

pub(super) fn list_built_in_xml_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("from_xml", F::custom(from_xml)),
        ("schema_of_xml", F::unknown("schema_of_xml")),
        ("to_xml", F::custom(to_xml)),
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
