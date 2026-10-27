//! Spark's cast of TIMESTAMP_NTZ to TIMESTAMP in the session time zone.

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::expr::{Cast, InList};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{
    BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator, ScalarUDF, Volatility, cast, lit,
};
use sail_common::utils::datetime::is_fixed_zero_offset_timezone;
use sail_function::scalar::datetime::convert_tz::ConvertTz;

/// Converts the casts of TIMESTAMP_NTZ to TIMESTAMP in the session time zone, which type coercion
/// adds, like Spark. Arrow's cast fails for wall-clock times in DST gaps and overlaps, which Spark
/// moves forward and resolves to the earlier offset. A time zone with a fixed offset has neither.
///
/// A comparison with a constant keeps its cast, which the optimizer can unwrap.
pub(crate) fn convert_session_timestamp_casts(
    plan: LogicalPlan,
    config: &ConfigOptions,
) -> Result<LogicalPlan> {
    let Some(timezone) = config.execution.time_zone.as_deref() else {
        return Ok(plan);
    };
    if is_fixed_zero_offset_timezone(timezone) || timezone.starts_with(['+', '-']) {
        return Ok(plan);
    }
    plan.transform_up_with_subqueries(|plan| {
        if !has_session_cast(&plan, timezone)? {
            return Ok(Transformed::no(plan));
        }
        let schema = merge_schema(&plan.inputs());
        let name_preserver = NamePreserver::new(&plan);
        plan.map_expressions(|expr| {
            let name = name_preserver.save(&expr);
            expr.transform_down(|expr| rewrite(expr, &schema, timezone))
                .map(|expr| expr.update_data(|expr| name.restore(expr)))
        })
    })
    .data()
}

fn rewrite(expr: Expr, schema: &DFSchema, timezone: &str) -> Result<Transformed<Expr>> {
    match &expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if is_comparison(*op)
                && ((is_unwrappable_cast(left, schema, timezone)?
                    && is_foldable_constant(right))
                    || (is_unwrappable_cast(right, schema, timezone)?
                        && is_foldable_constant(left))) =>
        {
            Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump))
        }
        Expr::InList(InList {
            expr: value, list, ..
        }) if is_unwrappable_cast(value, schema, timezone)?
            && list.iter().all(is_foldable_constant) =>
        {
            Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump))
        }
        Expr::Cast(Cast { expr: value, field }) if is_session_cast(&expr, timezone) => {
            if !matches!(value.get_type(schema)?, DataType::Timestamp(_, None)) {
                return Ok(Transformed::no(expr));
            }
            let DataType::Timestamp(unit, label) = field.data_type() else {
                return Ok(Transformed::no(expr));
            };
            let timestamp_ntz = cast(
                value.as_ref().clone(),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            );
            let instant = ScalarUDF::from(ConvertTz::new(false)).call(vec![
                lit(timezone),
                lit("UTC"),
                timestamp_ntz,
            ]);
            let converted = cast(
                cast(instant, DataType::Int64),
                DataType::Timestamp(TimeUnit::Microsecond, label.clone()),
            );
            Ok(Transformed::yes(if *unit == TimeUnit::Microsecond {
                converted
            } else {
                cast(converted, field.data_type().clone())
            }))
        }
        _ => Ok(Transformed::no(expr)),
    }
}

/// Whether the expression is a cast of a non-constant TIMESTAMP_NTZ value to TIMESTAMP in the
/// session time zone, which the optimizer can unwrap in a comparison with a constant.
fn is_unwrappable_cast(expr: &Expr, schema: &DFSchema, timezone: &str) -> Result<bool> {
    let Expr::Cast(Cast { expr: value, .. }) = expr else {
        return Ok(false);
    };
    Ok(is_session_cast(expr, timezone)
        && !is_foldable_constant(value)
        && matches!(value.get_type(schema)?, DataType::Timestamp(_, None)))
}

/// Whether the expression is a cast to TIMESTAMP in the session time zone.
fn is_session_cast(expr: &Expr, timezone: &str) -> bool {
    matches!(
        expr,
        Expr::Cast(Cast { field, .. })
            if matches!(field.data_type(), DataType::Timestamp(_, Some(tz)) if tz.as_ref() == timezone)
    )
}

fn has_session_cast(plan: &LogicalPlan, timezone: &str) -> Result<bool> {
    let mut found = false;
    plan.apply_expressions(|expr| {
        expr.apply(|expr| {
            found = is_session_cast(expr, timezone);
            Ok(if found {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })
    })?;
    Ok(found)
}

fn is_comparison(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    )
}

/// Whether the expression is a constant that the optimizer evaluates before comparing it.
fn is_foldable_constant(expr: &Expr) -> bool {
    !expr
        .exists(|expr| {
            Ok(match expr {
                Expr::Literal(..)
                | Expr::Cast(_)
                | Expr::TryCast(_)
                | Expr::BinaryExpr(_)
                | Expr::Negative(_) => false,
                // Functions without arguments, such as `current_timestamp()`, depend on the query
                // execution time.
                Expr::ScalarFunction(function) => match function.func.signature().volatility {
                    Volatility::Immutable => false,
                    Volatility::Stable => function.args.is_empty(),
                    Volatility::Volatile => true,
                },
                _ => true,
            })
        })
        .unwrap_or(true)
}
