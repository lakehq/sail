use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::functions::math;
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::predicate::rewrite_like_pattern::RewriteLikePatternFunc;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::{Expr, ExprKind, ExprType};
use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};

fn resolve_pattern(
    mut input: ScalarFunctionInput<'_>,
    name: &str,
    regex: bool,
) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (value, mut pattern, escape) = match values.len() {
        2 => {
            let (a, b) = values.two()?;
            (a, b, None)
        }
        3 if !regex => {
            let (a, b, c) = values.three()?;
            (a, b, Some(c))
        }
        n => {
            return Err(PlanError::invalid(format!(
                "{name} has an invalid argument count: {n}"
            )));
        }
    };
    let mut escape_char = None;
    if let Some(escape) = escape {
        let escape = crate::analyzer::lowering::evaluate_constant(&escape)?;
        let value = match escape {
            ScalarValue::Utf8(Some(value))
            | ScalarValue::Utf8View(Some(value))
            | ScalarValue::LargeUtf8(Some(value)) => value,
            _ => {
                return Err(PlanError::invalid(
                    "escape character must be a string literal",
                ));
            }
        };
        let mut chars = value.chars();
        let (Some(character), None) = (chars.next(), chars.next()) else {
            return Err(PlanError::invalid(
                "escape character must be a single character",
            ));
        };
        escape_char = Some(character);
        if character != '\\' {
            let character = context.literal(ScalarValue::Utf8(Some(character.to_string())));
            pattern = context.scalar_udf(
                Arc::new(ScalarUDF::from(RewriteLikePatternFunc::new())),
                vec![pattern, character],
            )?;
            escape_char = Some('\\');
        }
    }
    crate::function::scalar::predicate::pattern(
        context,
        value,
        pattern,
        regex,
        name == "ilike",
        escape_char,
    )
}

pub(crate) fn pattern(
    context: &mut FunctionContextInput<'_>,
    value: Expr,
    pattern: Expr,
    regex: bool,
    case_insensitive: bool,
    escape_char: Option<char>,
) -> PlanResult<Expr> {
    let value = context.cast(value, DataType::Utf8)?;
    let pattern = context.cast(pattern, DataType::Utf8)?;
    let nullable = value.result_type.resolved("pattern value")?.nullable
        || pattern.result_type.resolved("pattern")?.nullable;
    let node = if regex {
        ExprKind::SimilarTo {
            expression: Box::new(value),
            pattern: Box::new(pattern),
            negated: false,
            escape_char,
            case_insensitive,
        }
    } else {
        ExprKind::Like {
            expression: Box::new(value),
            pattern: Box::new(pattern),
            negated: false,
            escape_char,
            case_insensitive,
        }
    };
    Ok(Expr {
        id: context.ids.expression(),
        kind: Deferred::Resolved(node),
        result_type: Deferred::Resolved(ExprType::new(DataType::Boolean, nullable)),
    })
}

fn resolve_not(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let value = input.arguments.one()?;
    let nullable = value.result_type.resolved("predicate input")?.nullable;
    let expression = input.context.cast(value, DataType::Boolean)?;
    Ok(Expr {
        id: input.context.ids.expression(),
        kind: Deferred::Resolved(ExprKind::Not(Box::new(expression))),
        result_type: Deferred::Resolved(ExprType::new(DataType::Boolean, nullable)),
    })
}

pub(super) fn list_built_in_predicate_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("!", F::custom(resolve_not)),
        ("!=", F::binary_op(Operator::NotEq)),
        ("<", F::binary_op(Operator::Lt)),
        ("<=", F::binary_op(Operator::LtEq)),
        ("<=>", F::binary_op(Operator::IsNotDistinctFrom)),
        ("=", F::binary_op(Operator::Eq)),
        ("==", F::binary_op(Operator::Eq)),
        (">", F::binary_op(Operator::Gt)),
        (">=", F::binary_op(Operator::GtEq)),
        ("and", F::binary_op(Operator::And)),
        (
            "ilike",
            F::custom(|input| resolve_pattern(input, "ilike", false)),
        ),
        (
            "in",
            F::custom(|mut input| {
                let (value, list) = input.arguments.at_least_one()?;
                crate::resolver::in_list(&mut input.context, value, list, false)
            }),
        ),
        ("isnan", F::scalar_udf(math::isnan)),
        (
            "isnotnull",
            F::custom(|mut input| Ok(input.context.is_not_null(input.arguments.one()?))),
        ),
        (
            "isnull",
            F::custom(|mut input| Ok(input.context.is_null(input.arguments.one()?))),
        ),
        (
            "like",
            F::custom(|input| resolve_pattern(input, "like", false)),
        ),
        ("not", F::custom(resolve_not)),
        ("or", F::binary_op(Operator::Or)),
        (
            "regexp",
            F::custom(|input| resolve_pattern(input, "regexp", true)),
        ),
        (
            "regexp_like",
            F::custom(|input| resolve_pattern(input, "regexp_like", true)),
        ),
        (
            "rlike",
            F::custom(|input| resolve_pattern(input, "rlike", true)),
        ),
        ("<>", F::binary_op(Operator::NotEq)),
        ("between", F::unknown("between")),
    ]
}
