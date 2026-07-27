use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, ScalarUDF, expr};
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::xml::from_xml::SparkFromXml;
use sail_function::scalar::xml::to_xml::SparkToXml;
use sail_function::scalar::xml::xpath::Xpath;
use sail_function::scalar::xml::xpath_typed::{XpathTyped, XpathTypedKind};

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

/// Spark compiles the XPath once, when it analyzes the query, so it requires the `path` argument
/// to be **foldable** and rejects a per-row path with `DATATYPE_MISMATCH.NON_FOLDABLE_INPUT`. Only
/// the path is gated: the XML itself is an ordinary column.
///
/// Foldable is not the same as literal. Spark's rule is that the expression references no column
/// and is deterministic, so `concat('a', '/b')` and `substring('zza/b', 3)` are both accepted. Do
/// not decide this by trying to evaluate the expression: evaluation needs the argument types to be
/// coerced first, so a perfectly foldable `substring('zza/b', 3)` (whose position arrives as
/// `Int32` against an `Int64` signature) would fail and be rejected.
///
/// So gate on the rule, and fold only as a bonus: folding here keeps the plan readable and hands
/// the function a literal, and when it does not work the optimizer's constant folding gets there
/// later anyway.
fn foldable_path(name: &str, path: Expr) -> PlanResult<Expr> {
    if path.any_column_refs() || path.is_volatile() {
        return Err(PlanError::AnalysisError(format!(
            "[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT] Cannot resolve `{name}` due to data type mismatch: the input `path` should be a foldable \"STRING\" expression; however, got a non-foldable expression."
        )));
    }
    if matches!(path, Expr::Literal(_, _)) {
        return Ok(path);
    }
    match LiteralEvaluator::new().evaluate(&path) {
        Ok(ScalarValue::Utf8View(v) | ScalarValue::LargeUtf8(v)) => {
            Ok(Expr::Literal(ScalarValue::Utf8(v), None))
        }
        Ok(scalar) => Ok(Expr::Literal(scalar, None)),
        Err(_) => Ok(path),
    }
}

fn xpath(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (xml, path) = input.arguments.two()?;
    let path = foldable_path("xpath", path)?;
    let func = Xpath::new();
    Ok(Expr::ScalarFunction(expr::ScalarFunction {
        func: std::sync::Arc::new(ScalarUDF::from(func)),
        args: vec![xml, path],
    }))
}

fn xpath_typed(
    name: &'static str,
    kind: XpathTypedKind,
) -> impl Fn(ScalarFunctionInput) -> PlanResult<Expr> {
    move |input: ScalarFunctionInput| {
        let (xml, path) = input.arguments.two()?;
        let path = foldable_path(name, path)?;
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
            F::custom(xpath_typed("xpath_boolean", XpathTypedKind::Boolean)),
        ),
        (
            "xpath_double",
            F::custom(xpath_typed("xpath_double", XpathTypedKind::Double)),
        ),
        (
            "xpath_float",
            F::custom(xpath_typed("xpath_float", XpathTypedKind::Float)),
        ),
        (
            "xpath_int",
            F::custom(xpath_typed("xpath_int", XpathTypedKind::Int)),
        ),
        (
            "xpath_long",
            F::custom(xpath_typed("xpath_long", XpathTypedKind::Long)),
        ),
        (
            "xpath_number",
            F::custom(xpath_typed("xpath_number", XpathTypedKind::Number)),
        ),
        (
            "xpath_short",
            F::custom(xpath_typed("xpath_short", XpathTypedKind::Short)),
        ),
        (
            "xpath_string",
            F::custom(xpath_typed("xpath_string", XpathTypedKind::String)),
        ),
    ]
}
