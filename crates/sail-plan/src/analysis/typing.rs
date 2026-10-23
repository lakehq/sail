//! Native expression typing and coercion.

use datafusion_common::arrow::compute::can_cast_types;

use crate::error::PlanResult;

/// Resolve a native scalar call before constructing any DataFusion expression.
/// Each inserted cast receives a fresh occurrence ID; the call retains its ID.
pub fn resolve_scalar_call(
    id: super::ir::ExpressionId,
    function: std::sync::Arc<datafusion_expr::ScalarUDF>,
    arguments: Vec<super::ir::LogicalExpression>,
    ids: &mut super::ir::IdGenerator,
) -> PlanResult<super::ir::LogicalExpression> {
    use super::deferred::Deferred;
    use super::ir::{ExpressionNode, ExpressionType, LogicalExpression};

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
            |argument| match argument.node.resolved("function argument")? {
                ExpressionNode::Literal(value) => Ok(Some(value)),
                _ => Ok(None),
            },
        )
        .collect::<PlanResult<Vec<_>>>()?;
    let typing = sail_type_system::scalar::resolve(function.as_ref(), &fields, &literals)?;
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
    Ok(LogicalExpression {
        id,
        node: Deferred::Resolved(ExpressionNode::ScalarFunction {
            function,
            arguments,
        }),
        result_type: Deferred::Resolved(ExpressionType::from_field(&typing.result)),
    })
}

/// Insert an implicit cast, materializing literal conversions when possible. Shared by
/// operators, functions, VALUES, and set operations.
pub fn coerce(
    expression: super::ir::LogicalExpression,
    target: &arrow_schema::DataType,
    ids: &mut super::ir::IdGenerator,
) -> PlanResult<super::ir::LogicalExpression> {
    use super::deferred::Deferred;
    use super::ir::{ExpressionNode, ExpressionType, LogicalExpression};
    let source = expression.result_type.resolved("cast input type")?;
    if &source.data_type == target {
        return Ok(expression);
    }
    if sail_type_system::cast::same_layout(&source.data_type, target)
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
    let target = ExpressionType::from_field(&sail_type_system::coercion::cast_field(
        &source.to_field(""),
        target,
    ));
    // Keep typed constants visible to source pruning and literal-sensitive
    // functions before any backend optimizer runs. Failed conversions retain
    // their cast so evaluation preserves the normal runtime error behavior.
    if let Deferred::Resolved(ExpressionNode::Literal(value)) = &expression.node
        && let Ok(value) = value.cast_to(&target.data_type)
    {
        return Ok(LogicalExpression {
            id: ids.expression(),
            node: Deferred::Resolved(ExpressionNode::Literal(value)),
            result_type: Deferred::Resolved(target),
        });
    }
    Ok(LogicalExpression {
        id: ids.expression(),
        node: Deferred::Resolved(ExpressionNode::Cast {
            expression: Box::new(expression),
            target: target.clone(),
            safe: false,
        }),
        result_type: Deferred::Resolved(target),
    })
}

pub fn resolve_binary(
    id: super::ir::ExpressionId,
    left: super::ir::LogicalExpression,
    operator: datafusion_expr::Operator,
    right: super::ir::LogicalExpression,
    ids: &mut super::ir::IdGenerator,
) -> PlanResult<super::ir::LogicalExpression> {
    use super::deferred::Deferred;
    use super::ir::{ExpressionNode, ExpressionType, LogicalExpression};
    let typing = sail_type_system::coercion::binary(
        &left.result_type.resolved("left operand type")?.to_field(""),
        operator,
        &right
            .result_type
            .resolved("right operand type")?
            .to_field(""),
    )?;
    Ok(LogicalExpression {
        id,
        node: Deferred::Resolved(ExpressionNode::Binary {
            left: Box::new(coerce(left, &typing.left, ids)?),
            operator,
            right: Box::new(coerce(right, &typing.right, ids)?),
        }),
        result_type: Deferred::Resolved(ExpressionType::new(typing.result, typing.nullable)),
    })
}

pub fn resolve_in_list(
    id: super::ir::ExpressionId,
    expression: super::ir::LogicalExpression,
    values: Vec<super::ir::LogicalExpression>,
    negated: bool,
    ids: &mut super::ir::IdGenerator,
) -> PlanResult<super::ir::LogicalExpression> {
    use super::deferred::Deferred;
    use super::ir::{ExpressionNode, ExpressionType, LogicalExpression};
    let fields = std::iter::once(&expression)
        .chain(&values)
        .map(|value| Ok(value.result_type.resolved("IN argument type")?))
        .collect::<PlanResult<Vec<_>>>()?;
    let nullable = fields.iter().any(|field| field.nullable);
    let common = sail_type_system::coercion::comparison_type(
        &fields
            .iter()
            .map(|field| field.data_type.clone())
            .collect::<Vec<_>>(),
    )?;
    Ok(LogicalExpression {
        id,
        node: Deferred::Resolved(ExpressionNode::InList {
            expression: Box::new(coerce(expression, &common, ids)?),
            values: values
                .into_iter()
                .map(|value| coerce(value, &common, ids))
                .collect::<PlanResult<_>>()?,
            negated,
        }),
        result_type: Deferred::Resolved(ExpressionType::new(
            arrow_schema::DataType::Boolean,
            nullable,
        )),
    })
}

pub fn resolve_case(
    id: super::ir::ExpressionId,
    operand: Option<super::ir::LogicalExpression>,
    branches: Vec<(super::ir::LogicalExpression, super::ir::LogicalExpression)>,
    otherwise: Option<super::ir::LogicalExpression>,
    ids: &mut super::ir::IdGenerator,
) -> PlanResult<super::ir::LogicalExpression> {
    use super::deferred::Deferred;
    use super::ir::{ExpressionNode, ExpressionType, LogicalExpression};
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
            while let ExpressionNode::Cast {
                expression,
                safe: false,
                ..
            } = inner.node.resolved("CASE branch")?
            {
                inner = expression;
            }
            matches!(condition.node.resolved("CASE condition")?, ExpressionNode::IsNotNull(test) if test.equivalent(inner)?)
        } else {
            false
        };
        nullable |= !guarded;
    }
    let result_type = sail_type_system::coercion::common_type(
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
        sail_type_system::coercion::common_type(&types)?
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
    Ok(LogicalExpression {
        id,
        node: Deferred::Resolved(ExpressionNode::Case {
            operand,
            branches,
            otherwise,
        }),
        result_type: Deferred::Resolved(ExpressionType::new(result_type, nullable)),
    })
}

pub fn argument_fields(
    arguments: &[super::ir::LogicalExpression],
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
    arguments: Vec<super::ir::LogicalExpression>,
    fields: &[arrow_schema::FieldRef],
    ids: &mut super::ir::IdGenerator,
) -> PlanResult<Vec<super::ir::LogicalExpression>> {
    sail_type_system::coercion::validate_arity("function coercion", fields.len(), arguments.len())?;
    arguments
        .into_iter()
        .zip(fields)
        .map(|(argument, field)| coerce(argument, field.data_type(), ids))
        .collect()
}

/// Conservative reachability for searched CASE branches. In particular a NULL
/// result behind `IS NULL(nonnullable)` cannot make the whole CASE nullable.
fn condition_can_be_true(expression: &super::ir::LogicalExpression) -> PlanResult<bool> {
    use super::ir::ExpressionNode;
    Ok(match expression.node.resolved("CASE condition")? {
        ExpressionNode::Literal(datafusion_common::ScalarValue::Boolean(value)) => {
            *value == Some(true)
        }
        ExpressionNode::IsNull(value) => {
            value.result_type.resolved("NULL predicate input")?.nullable
        }
        ExpressionNode::WithMetadata(value) => condition_can_be_true(value)?,
        ExpressionNode::Binary {
            left,
            operator: datafusion_expr::Operator::And,
            right,
        } => condition_can_be_true(left)? && condition_can_be_true(right)?,
        ExpressionNode::Binary {
            left,
            operator: datafusion_expr::Operator::Or,
            right,
        } => condition_can_be_true(left)? || condition_can_be_true(right)?,
        _ => true,
    })
}
