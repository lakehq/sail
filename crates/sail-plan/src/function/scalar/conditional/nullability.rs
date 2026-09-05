use std::sync::Arc;

use arrow::datatypes::{DataType, IntervalUnit};
use datafusion::functions::core::named_struct::NamedStructFunc;
use datafusion::functions_nested::make_array::MakeArray;
use datafusion_common::Result;
use datafusion_expr::type_coercion::functions::fields_with_udf;
use datafusion_expr::{Expr, ExprSchemable, Operator, ReturnFieldArgs};

use super::{is_date_type, is_string_type};
use crate::function::common::FunctionContextInput;

/// Infer CASE result nullability without treating every nullable descendant as
/// nullable output: operators and functions can consume NULLs without returning them.
pub(super) fn expr_nullable(value: &Expr, context: &FunctionContextInput<'_>) -> Result<bool> {
    match value {
        Expr::Alias(alias) => expr_nullable(&alias.expr, context),
        Expr::Not(value) | Expr::Negative(value) => expr_nullable(value, context),
        Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNotFalse(_)
        | Expr::IsNotUnknown(_)
        | Expr::Exists(_) => Ok(false),
        Expr::Cast(cast) => Ok(expr_nullable(&cast.expr, context)?
            || cast_force_nullable(&cast.expr.get_type(context.schema)?, cast.field.data_type())),
        Expr::TryCast(cast) => {
            let from = cast.expr.get_type(context.schema)?;
            let to = cast.field.data_type();
            if is_string_type(&from) && is_binary_type(to) {
                return expr_nullable(&cast.expr, context);
            }
            // Spark retains nullable decimal-to-integral TRY_CAST even when
            // the decimal's declared range fits in the target integer.
            Ok((from.is_decimal() && to.is_integer())
                || expr_nullable(&cast.expr, context)?
                || !can_up_cast(&from, to))
        }
        Expr::BinaryExpr(binary) => {
            if matches!(
                binary.op,
                Operator::IsDistinctFrom | Operator::IsNotDistinctFrom
            ) {
                return Ok(false);
            }
            // Spark's DivModLike is nullable even in ANSI mode. Other decimal
            // arithmetic can return NULL on overflow outside ANSI mode.
            Ok(matches!(
                binary.op,
                Operator::Divide | Operator::IntegerDivide | Operator::Modulo
            ) || expr_nullable(&binary.left, context)?
                || expr_nullable(&binary.right, context)?
                || (!context.plan_config.ansi_mode
                    && binary.op.is_numerical_operators()
                    && value.get_type(context.schema)?.is_decimal()))
        }
        Expr::Between(between) => Ok(expr_nullable(&between.expr, context)?
            || expr_nullable(&between.low, context)?
            || expr_nullable(&between.high, context)?),
        Expr::Like(like) | Expr::SimilarTo(like) => {
            Ok(expr_nullable(&like.expr, context)? || expr_nullable(&like.pattern, context)?)
        }
        Expr::InList(list) => {
            if expr_nullable(&list.expr, context)? {
                return Ok(true);
            }
            for value in &list.list {
                if expr_nullable(value, context)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Expr::Case(case) => {
            // Raw DataFusion CASE also represents IF and internal expressions.
            // Spark's literal-TRUE prefix rule belongs to the CASE resolver;
            // IF always retains both result branches in its nullability.
            for (_, value) in &case.when_then_expr {
                if expr_nullable(value, context)? {
                    return Ok(true);
                }
            }
            case.else_expr
                .as_deref()
                .map_or(Ok(true), |value| expr_nullable(value, context))
        }
        Expr::ScalarFunction(function) => {
            if function
                .func
                .inner()
                .downcast_ref::<NamedStructFunc>()
                .is_some()
                || function.func.inner().downcast_ref::<MakeArray>().is_some()
            {
                // Spark CreateNamedStruct/CreateArray never return a null container.
                // TODO: named_struct child fields still inherit DataFusion's
                // nullable metadata; correct that constructor contract separately.
                return Ok(false);
            }
            let fields = function
                .args
                .iter()
                .map(|arg| {
                    let field = arg.to_field(context.schema)?.1;
                    Ok(Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_nullable(expr_nullable(arg, context)?),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let coerced_fields = fields_with_udf(&fields, function.func.as_ref())?
                .into_iter()
                .zip(&fields)
                .map(|(coerced, original)| {
                    let nullable = original.is_nullable()
                        || cast_force_nullable(original.data_type(), coerced.data_type());
                    Arc::new(coerced.as_ref().clone().with_nullable(nullable))
                })
                .collect::<Vec<_>>();
            let scalar_arguments = function
                .args
                .iter()
                .map(|arg| match arg {
                    Expr::Literal(value, _) => Some(value),
                    _ => None,
                })
                .collect::<Vec<_>>();
            Ok(function
                .func
                .return_field_from_args(ReturnFieldArgs {
                    arg_fields: &coerced_fields,
                    scalar_arguments: &scalar_arguments,
                })?
                .is_nullable())
        }
        Expr::ScalarSubquery(_) => Ok(true),
        Expr::InSubquery(subquery) => {
            Ok(expr_nullable(&subquery.expr, context)? || value.nullable(context.schema)?)
        }
        _ => value.nullable(context.schema),
    }
}

/// Spark Cast.forceNullable for Sail's represented SQL types. This describes
/// the schema of an already valid cast; it does not expand which casts are valid.
pub(super) fn cast_force_nullable(from: &DataType, to: &DataType) -> bool {
    if from.is_null() || from == to || (is_date_type(from) && is_date_type(to)) {
        return false;
    }
    if is_string_type(from) {
        return !(is_string_type(to) || is_binary_type(to));
    }
    if is_string_type(to) {
        return false;
    }
    match (from, to) {
        (DataType::Timestamp(_, Some(_)), DataType::Int8 | DataType::Int16 | DataType::Int32)
        | (DataType::Time32(_) | DataType::Time64(_), DataType::Int8 | DataType::Int16)
        | (DataType::Float32 | DataType::Float64, DataType::Timestamp(_, Some(_))) => true,
        (DataType::Timestamp(_, Some(_)), to) if is_date_type(to) => false,
        (_, to) if is_date_type(to) => true,
        (from, DataType::Timestamp(_, Some(_))) if is_date_type(from) => false,
        (from, _) if is_date_type(from) => true,
        (_, DataType::Interval(IntervalUnit::MonthDayNano)) => true,
        (_, DataType::Decimal128(precision, scale)) => {
            !cast_to_decimal_is_safe(from, *precision, *scale)
        }
        (from, to) if (from.is_floating() || from.is_decimal()) && to.is_integer() => true,
        _ => false,
    }
}

fn cast_to_decimal_is_safe(from: &DataType, precision: u8, scale: i8) -> bool {
    let source = if *from == DataType::Boolean {
        Some((1, 0))
    } else if from.is_integer() || from.is_decimal() {
        decimal_for_numeric(from)
    } else {
        None
    };
    let Some((from_precision, from_scale)) = source else {
        return false;
    };
    let from_digits = i16::from(from_precision) - i16::from(from_scale);
    let to_digits = i16::from(precision) - i16::from(scale);
    // Cast.canNullSafeCastToDecimal permits reducing a decimal's scale when
    // the target has an additional integral digit for rounding.
    (scale >= from_scale && to_digits >= from_digits)
        || (from.is_decimal() && to_digits > from_digits)
}

// Spark UpCastRule, used only to infer an already supported TRY_CAST's schema.
fn can_up_cast(from: &DataType, to: &DataType) -> bool {
    if from.is_null() || from == to || (is_date_type(from) && is_date_type(to)) {
        return true;
    }
    if let (Some((from_precision, from_scale)), DataType::Decimal128(precision, scale)) =
        (decimal_for_numeric(from), to)
        && (from.is_integer() || from.is_decimal())
        && decimal_is_wider(*precision, *scale, from_precision, from_scale)
    {
        return true;
    }
    if let (DataType::Decimal128(precision, scale), Some((to_precision, _))) =
        (from, decimal_for_numeric(to))
        && to.is_integer()
        && *scale == 0
        && *precision < to_precision
    {
        return true;
    }
    if let (Some(from), Some(to)) = (numeric_precedence(from), numeric_precedence(to))
        && from < to
    {
        return true;
    }
    if is_string_type(to) {
        return is_string_type(from)
            || is_binary_type(from)
            || from.is_numeric()
            || is_date_type(from)
            || matches!(
                from,
                DataType::Boolean
                    | DataType::Timestamp(_, _)
                    | DataType::Time32(_)
                    | DataType::Time64(_)
                    | DataType::Duration(_)
                    | DataType::Interval(_)
            );
    }
    match (from, to) {
        (from, DataType::Timestamp(_, _)) if is_date_type(from) => true,
        (DataType::Timestamp(_, _), DataType::Timestamp(_, _))
        | (DataType::Timestamp(_, Some(_)), DataType::Int64)
        | (DataType::Int64, DataType::Timestamp(_, Some(_)))
        | (DataType::Duration(_), DataType::Duration(_))
        | (
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::YearMonth),
        ) => true,
        (DataType::Struct(from), DataType::Struct(to)) => {
            from.len() == to.len()
                && from.iter().zip(to).all(|(from, to)| {
                    (!from.is_nullable() || to.is_nullable())
                        && can_up_cast(from.data_type(), to.data_type())
                })
        }
        (DataType::Map(from, _), DataType::Map(to, _)) => {
            can_up_cast(from.data_type(), to.data_type())
        }
        (
            DataType::List(from)
            | DataType::LargeList(from)
            | DataType::FixedSizeList(from, _)
            | DataType::ListView(from)
            | DataType::LargeListView(from),
            DataType::List(to)
            | DataType::LargeList(to)
            | DataType::FixedSizeList(to, _)
            | DataType::ListView(to)
            | DataType::LargeListView(to),
        ) => {
            (!from.is_nullable() || to.is_nullable())
                && can_up_cast(from.data_type(), to.data_type())
        }
        _ => false,
    }
}

fn decimal_for_numeric(data_type: &DataType) -> Option<(u8, i8)> {
    match data_type {
        DataType::Int8 => Some((3, 0)),
        DataType::Int16 => Some((5, 0)),
        DataType::Int32 => Some((10, 0)),
        DataType::Int64 => Some((20, 0)),
        DataType::Decimal128(precision, scale) => Some((*precision, *scale)),
        _ => None,
    }
}

fn decimal_is_wider(precision: u8, scale: i8, from_precision: u8, from_scale: i8) -> bool {
    scale >= from_scale
        && i16::from(precision) - i16::from(scale)
            >= i16::from(from_precision) - i16::from(from_scale)
}

fn numeric_precedence(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Int8 => Some(0),
        DataType::Int16 => Some(1),
        DataType::Int32 => Some(2),
        DataType::Int64 => Some(3),
        DataType::Float32 => Some(4),
        DataType::Float64 => Some(5),
        _ => None,
    }
}

fn is_binary_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}
