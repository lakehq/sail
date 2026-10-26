//! Native expression typing and coercion.

pub(crate) mod aggregate;
pub(crate) mod cast;
pub(crate) mod coercion;
pub(crate) mod higher_order;
pub(crate) mod relational;
pub(crate) mod scalar;
pub(crate) mod window;

use datafusion_common::arrow::compute::can_cast_types;

use crate::error::PlanResult;

/// Resolve a native scalar call before constructing any DataFusion expression.
/// Each inserted cast receives a fresh occurrence ID; the call retains its ID.
pub fn resolve_scalar_call(
    id: super::model::ExprId,
    function: std::sync::Arc<datafusion_expr::ScalarUDF>,
    arguments: Vec<super::model::Expr>,
    ids: &mut super::model::IdGenerator,
) -> PlanResult<super::model::Expr> {
    use super::deferred::Deferred;
    use super::model::{Expr, ExprKind, ExprType};

    let fields = arguments
        .iter()
        .map(|argument| {
            Ok(argument
                .result_type
                .resolved("function argument type")?
                .to_field(""))
        })
        .collect::<PlanResult<Vec<_>>>()?;
    let literals = arguments
        .iter()
        .map(
            |argument| match argument.kind.resolved("function argument")? {
                ExprKind::Literal(value) => Ok(Some(value)),
                _ => Ok(None),
            },
        )
        .collect::<PlanResult<Vec<_>>>()?;
    let typing = scalar::resolve(function.as_ref(), &fields, &literals)?;
    let arguments = arguments
        .into_iter()
        .zip(typing.arguments)
        .map(|(argument, field)| {
            if argument
                .result_type
                .resolved("function argument type")?
                .data_type
                == *field.data_type()
            {
                return Ok(argument);
            }
            if function.name() == "spark_concat"
                && field.data_type().is_string()
                && matches!(
                    argument.result_type.resolved("concat input")?.data_type,
                    arrow_schema::DataType::Timestamp(_, _)
                        | arrow_schema::DataType::Date32
                        | arrow_schema::DataType::Date64
                )
            {
                use sail_function::scalar::spark_to_string::{
                    SparkToLargeUtf8, SparkToUtf8, SparkToUtf8View,
                };
                let cast = match field.data_type() {
                    arrow_schema::DataType::LargeUtf8 => {
                        datafusion_expr::ScalarUDF::from(SparkToLargeUtf8::new())
                    }
                    arrow_schema::DataType::Utf8View => {
                        datafusion_expr::ScalarUDF::from(SparkToUtf8View::new())
                    }
                    _ => datafusion_expr::ScalarUDF::from(SparkToUtf8::new()),
                };
                return resolve_scalar_call(
                    ids.expression(),
                    std::sync::Arc::new(cast),
                    vec![argument],
                    ids,
                );
            }
            coerce(argument, field.data_type(), ids)
        })
        .collect::<PlanResult<Vec<_>>>()?;
    Ok(Expr {
        id,
        kind: Deferred::Resolved(ExprKind::ScalarFunction {
            function,
            arguments,
        }),
        result_type: Deferred::Resolved(ExprType::from_field(&typing.result)),
    })
}

/// Insert an implicit cast, materializing literal conversions when possible. Shared by
/// operators, functions, VALUES, and set operations.
pub fn coerce(
    expression: super::model::Expr,
    target: &arrow_schema::DataType,
    ids: &mut super::model::IdGenerator,
) -> PlanResult<super::model::Expr> {
    use super::deferred::Deferred;
    use super::model::{Expr, ExprKind, ExprType};
    let source = expression.result_type.resolved("cast input type")?;
    if &source.data_type == target {
        return Ok(expression);
    }
    if cast::same_layout(&source.data_type, target)
        && matches!(
            target,
            arrow_schema::DataType::Struct(_)
                | arrow_schema::DataType::List(_)
                | arrow_schema::DataType::LargeList(_)
                | arrow_schema::DataType::FixedSizeList(_, _)
                | arrow_schema::DataType::Map(_, _)
        )
    {
        return resolve_scalar_call(
            ids.expression(),
            std::sync::Arc::new(datafusion_expr::ScalarUDF::from(
                sail_function::scalar::spark_struct_rename::SparkStructRename::new(target.clone()),
            )),
            vec![expression],
            ids,
        );
    }
    if !can_cast_types(&source.data_type, target) {
        return Err(crate::error::PlanError::analysis(format!(
            "cannot cast {} to {target}",
            source.data_type
        )));
    }
    let target = ExprType::from_field(&coercion::cast_field(&source.to_field(""), target));
    // Keep typed constants visible to source pruning and literal-sensitive
    // functions before any backend optimizer runs. Failed conversions retain
    // their cast so evaluation preserves the normal runtime error behavior.
    if let Deferred::Resolved(ExprKind::Literal(value)) = &expression.kind
        && let Ok(value) = value.cast_to(&target.data_type)
    {
        return Ok(Expr {
            id: ids.expression(),
            kind: Deferred::Resolved(ExprKind::Literal(value)),
            result_type: Deferred::Resolved(target),
        });
    }
    Ok(Expr {
        id: ids.expression(),
        kind: Deferred::Resolved(ExprKind::Cast {
            expression: Box::new(expression),
            target: target.clone(),
            safe: false,
        }),
        result_type: Deferred::Resolved(target),
    })
}

pub fn resolve_binary(
    id: super::model::ExprId,
    left: super::model::Expr,
    operator: datafusion_expr::Operator,
    right: super::model::Expr,
    ids: &mut super::model::IdGenerator,
) -> PlanResult<super::model::Expr> {
    use super::deferred::Deferred;
    use super::model::{Expr, ExprKind, ExprType};
    let typing = coercion::binary(
        &left.result_type.resolved("left operand type")?.to_field(""),
        operator,
        &right
            .result_type
            .resolved("right operand type")?
            .to_field(""),
    )?;
    Ok(Expr {
        id,
        kind: Deferred::Resolved(ExprKind::Binary {
            left: Box::new(coerce(left, &typing.left, ids)?),
            operator,
            right: Box::new(coerce(right, &typing.right, ids)?),
        }),
        result_type: Deferred::Resolved(ExprType::new(typing.result, typing.nullable)),
    })
}

pub fn resolve_in_list(
    id: super::model::ExprId,
    expression: super::model::Expr,
    values: Vec<super::model::Expr>,
    negated: bool,
    ids: &mut super::model::IdGenerator,
) -> PlanResult<super::model::Expr> {
    use super::deferred::Deferred;
    use super::model::{Expr, ExprKind, ExprType};
    let fields = std::iter::once(&expression)
        .chain(&values)
        .map(|value| Ok(value.result_type.resolved("IN argument type")?))
        .collect::<PlanResult<Vec<_>>>()?;
    let nullable = fields.iter().any(|field| field.nullable);
    let common = coercion::comparison_type(
        &fields
            .iter()
            .map(|field| field.data_type.clone())
            .collect::<Vec<_>>(),
    )?;
    Ok(Expr {
        id,
        kind: Deferred::Resolved(ExprKind::InList {
            expression: Box::new(coerce(expression, &common, ids)?),
            values: values
                .into_iter()
                .map(|value| coerce(value, &common, ids))
                .collect::<PlanResult<_>>()?,
            negated,
        }),
        result_type: Deferred::Resolved(ExprType::new(arrow_schema::DataType::Boolean, nullable)),
    })
}

pub fn resolve_case(
    id: super::model::ExprId,
    operand: Option<super::model::Expr>,
    branches: Vec<(super::model::Expr, super::model::Expr)>,
    otherwise: Option<super::model::Expr>,
    ids: &mut super::model::IdGenerator,
) -> PlanResult<super::model::Expr> {
    use super::deferred::Deferred;
    use super::model::{Expr, ExprKind, ExprType};
    if branches.is_empty() {
        return Err(crate::error::PlanError::analysis(
            "CASE requires at least one branch",
        ));
    }
    let results = branches
        .iter()
        .map(|(_, value)| value)
        .chain(otherwise.as_ref())
        .map(|value| Ok(value.result_type.resolved("CASE result type")?))
        .collect::<PlanResult<Vec<_>>>()?;
    let mut nullable = otherwise
        .as_ref()
        .map(|value| {
            value
                .result_type
                .resolved("CASE else type")
                .map(|field| field.nullable)
        })
        .transpose()?
        .unwrap_or(true);
    for (condition, value) in &branches {
        if !value.result_type.resolved("CASE branch type")?.nullable {
            continue;
        }
        let guarded = if operand.is_none() {
            if !condition_can_be_true(condition)? {
                continue;
            }
            let mut inner = value;
            while let ExprKind::Cast {
                expression,
                safe: false,
                ..
            } = inner.kind.resolved("CASE branch")?
            {
                inner = expression;
            }
            matches!(condition.kind.resolved("CASE condition")?, ExprKind::IsNotNull(test) if test.equivalent(inner)?)
        } else {
            false
        };
        nullable |= !guarded;
    }
    let result_type = coercion::common_type(
        &results
            .iter()
            .map(|field| field.data_type.clone())
            .collect::<Vec<_>>(),
    )?;
    let condition_type = if let Some(operand) = &operand {
        let types = std::iter::once(operand)
            .chain(branches.iter().map(|(condition, _)| condition))
            .map(|value| {
                Ok(value
                    .result_type
                    .resolved("CASE operand type")?
                    .data_type
                    .clone())
            })
            .collect::<PlanResult<Vec<_>>>()?;
        coercion::common_type(&types)?
    } else {
        arrow_schema::DataType::Boolean
    };
    let operand = operand
        .map(|operand| coerce(operand, &condition_type, ids).map(Box::new))
        .transpose()?;
    let branches = branches
        .into_iter()
        .map(|(condition, value)| {
            Ok((
                coerce(condition, &condition_type, ids)?,
                coerce(value, &result_type, ids)?,
            ))
        })
        .collect::<PlanResult<_>>()?;
    let otherwise = otherwise
        .map(|value| coerce(value, &result_type, ids).map(Box::new))
        .transpose()?;
    Ok(Expr {
        id,
        kind: Deferred::Resolved(ExprKind::Case {
            operand,
            branches,
            otherwise,
        }),
        result_type: Deferred::Resolved(ExprType::new(result_type, nullable)),
    })
}

pub fn argument_fields(
    arguments: &[super::model::Expr],
) -> PlanResult<Vec<arrow_schema::FieldRef>> {
    arguments
        .iter()
        .map(|argument| {
            Ok(argument
                .result_type
                .resolved("function argument type")?
                .to_field(""))
        })
        .collect()
}

pub fn coerce_arguments(
    arguments: Vec<super::model::Expr>,
    fields: &[arrow_schema::FieldRef],
    ids: &mut super::model::IdGenerator,
) -> PlanResult<Vec<super::model::Expr>> {
    coercion::validate_arity("function coercion", fields.len(), arguments.len())?;
    arguments
        .into_iter()
        .zip(fields)
        .map(|(argument, field)| coerce(argument, field.data_type(), ids))
        .collect()
}

/// Conservative reachability for searched CASE branches. In particular a NULL
/// result behind `IS NULL(nonnullable)` cannot make the whole CASE nullable.
fn condition_can_be_true(expression: &super::model::Expr) -> PlanResult<bool> {
    use super::model::ExprKind;
    Ok(match expression.kind.resolved("CASE condition")? {
        ExprKind::Literal(datafusion_common::ScalarValue::Boolean(value)) => *value == Some(true),
        ExprKind::IsNull(value) => value.result_type.resolved("NULL predicate input")?.nullable,
        ExprKind::WithMetadata(value) => condition_can_be_true(value)?,
        ExprKind::Binary {
            left,
            operator: datafusion_expr::Operator::And,
            right,
        } => condition_can_be_true(left)? && condition_can_be_true(right)?,
        ExprKind::Binary {
            left,
            operator: datafusion_expr::Operator::Or,
            right,
        } => condition_can_be_true(left)? || condition_can_be_true(right)?,
        _ => true,
    })
}
