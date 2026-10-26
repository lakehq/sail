use std::sync::Arc;

use datafusion_common::ScalarValue;
use datafusion_expr::ScalarUDF;
use sail_function::scalar::xml::from_xml::SparkFromXml;
use sail_function::scalar::xml::to_xml::SparkToXml;
use sail_function::scalar::xml::xpath::Xpath;
use sail_function::scalar::xml::xpath_typed::{XpathTyped, XpathTypedKind};

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::{Expr, ExprKind};
use crate::error::PlanResult;
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn to_xml(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    context.scalar_udf(Arc::new(ScalarUDF::from(SparkToXml::new(timezone))), values)
}

fn from_xml(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let mut values = input.arguments;
    let timezone = context.config.session_timezone.clone();
    if let Some(schema) = values.get_mut(1)
        && !matches!(schema.kind, Deferred::Resolved(ExprKind::Literal(_)))
        && let Ok(value) = crate::analyzer::lowering::evaluate_constant(schema)
    {
        let value = match value {
            ScalarValue::Utf8View(v) | ScalarValue::LargeUtf8(v) => ScalarValue::Utf8(v),
            value => value,
        };
        *schema = context.literal(value);
    }
    let function = ScalarUDF::from(SparkFromXml::new(timezone));
    context.scalar_udf(Arc::new(function), values)
}

pub(super) fn list_built_in_xml_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("from_xml", F::custom(from_xml)),
        ("schema_of_xml", F::unknown("schema_of_xml")),
        ("to_xml", F::custom(to_xml)),
        ("xpath", F::udf(Xpath::new())),
        (
            "xpath_boolean",
            F::udf(XpathTyped::new(XpathTypedKind::Boolean)),
        ),
        (
            "xpath_double",
            F::udf(XpathTyped::new(XpathTypedKind::Double)),
        ),
        (
            "xpath_float",
            F::udf(XpathTyped::new(XpathTypedKind::Float)),
        ),
        ("xpath_int", F::udf(XpathTyped::new(XpathTypedKind::Int))),
        ("xpath_long", F::udf(XpathTyped::new(XpathTypedKind::Long))),
        (
            "xpath_number",
            F::udf(XpathTyped::new(XpathTypedKind::Number)),
        ),
        (
            "xpath_short",
            F::udf(XpathTyped::new(XpathTypedKind::Short)),
        ),
        (
            "xpath_string",
            F::udf(XpathTyped::new(XpathTypedKind::String)),
        ),
    ]
}
