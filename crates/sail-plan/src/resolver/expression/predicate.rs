use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, Field, Fields, IntervalUnit, TimeUnit};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, DFSchemaRef, Result as DataFusionResult, ScalarValue};
use datafusion_expr::binary::try_type_union_resolution;
use datafusion_expr::expr::{BinaryExpr, HigherOrderFunction, InList, Lambda, LambdaVariable};
use datafusion_expr::{ExprSchemable, HigherOrderUDF, ScalarUDF, cast, expr, lit};
use datafusion_expr_common::operator::Operator;
use datafusion_functions::core::get_field;
use sail_common::spec;
use sail_function::scalar::array::spark_array_transform::SparkArrayTransform;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_struct_rename::SparkStructRename;
use sail_function::scalar::spark_to_string::SparkToUtf8;
use sail_function::scalar::update_struct_field::UpdateStructField;
use sail_logical_optimizer::type_coercion::{
    case_result_type, cast_case_result_value, cast_nested_value,
};

use crate::config::PlanConfig;
use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;

static SPARK_IN_ARRAY_TRANSFORM_UDF: LazyLock<Arc<HigherOrderUDF>> =
    LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkArrayTransform::new())));

enum TimestampStringInCoercion {
    ToString,
    ToTimestamp(Option<Arc<str>>),
    List {
        null_input_type: DataType,
        element: Box<TimestampStringInCoercion>,
    },
    Struct {
        null_input_type: DataType,
        field_names: Vec<String>,
        fields: Vec<Option<TimestampStringInCoercion>>,
    },
}

fn find_timestamp_string_in_coercion(
    data_types: &[DataType],
    session_timezone: &Arc<str>,
    ansi_mode: bool,
    case_sensitive: bool,
) -> Option<TimestampStringInCoercion> {
    let non_null_types = data_types
        .iter()
        .filter(|data_type| !data_type.is_null())
        .cloned()
        .collect::<Vec<_>>();
    let first = non_null_types.first()?;

    match first {
        DataType::List(_) | DataType::LargeList(_) => {
            let fields = non_null_types
                .iter()
                .map(|data_type| match data_type {
                    DataType::List(field) | DataType::LargeList(field) => Some(field.as_ref()),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            let element_types = fields
                .iter()
                .map(|field| field.data_type().clone())
                .collect::<Vec<_>>();
            let element = find_timestamp_string_in_coercion(
                &element_types,
                session_timezone,
                ansi_mode,
                case_sensitive,
            )?;
            return Some(TimestampStringInCoercion::List {
                null_input_type: first.clone(),
                element: Box::new(element),
            });
        }
        DataType::Struct(first_fields) => {
            let structs = non_null_types
                .iter()
                .map(|data_type| match data_type {
                    DataType::Struct(fields) => Some(fields),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            if structs.iter().any(|fields| {
                fields.len() != first_fields.len()
                    || fields
                        .iter()
                        .zip(first_fields.iter())
                        .any(|(field, first)| {
                            !struct_field_names_equal(field.name(), first.name(), case_sensitive)
                        })
            }) {
                return None;
            }

            let fields = first_fields
                .iter()
                .enumerate()
                .map(|(index, _)| {
                    let field_types = structs
                        .iter()
                        .map(|fields| fields[index].data_type().clone())
                        .collect::<Vec<_>>();
                    find_timestamp_string_in_coercion(
                        &field_types,
                        session_timezone,
                        ansi_mode,
                        case_sensitive,
                    )
                })
                .collect::<Vec<_>>();
            if fields.iter().all(Option::is_none) {
                return None;
            }
            return Some(TimestampStringInCoercion::Struct {
                null_input_type: first.clone(),
                field_names: first_fields
                    .iter()
                    .map(|field| field.name().clone())
                    .collect(),
                fields,
            });
        }
        _ => {}
    }

    let has_timestamp = non_null_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, _)));
    let has_string = non_null_types.iter().any(is_string_type);
    if !has_timestamp || !has_string {
        return None;
    }

    if !ansi_mode {
        return data_types
            .iter()
            .all(non_ansi_in_string_promotable)
            .then_some(TimestampStringInCoercion::ToString);
    }

    if !data_types.iter().all(|data_type| {
        data_type.is_null()
            || data_type.is_string()
            || matches!(
                data_type,
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
            )
    }) {
        return None;
    }

    let timezone = non_null_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, Some(_))))
        .then(|| Arc::clone(session_timezone));
    Some(TimestampStringInCoercion::ToTimestamp(timezone))
}

fn apply_timestamp_string_in_coercion(
    expression: expr::Expr,
    data_type: &DataType,
    coercion: &TimestampStringInCoercion,
    schema: &DFSchemaRef,
    session_timezone: &Arc<str>,
    ansi_mode: bool,
    list_depth: usize,
) -> DataFusionResult<expr::Expr> {
    if !timestamp_string_coercion_requires_rewrite(data_type, coercion) {
        return Ok(expression);
    }

    match coercion {
        TimestampStringInCoercion::ToString => {
            if data_type.is_null() {
                Ok(cast(expression, DataType::Utf8))
            } else {
                stringify_non_ansi_expression(expression, data_type, schema, session_timezone)
            }
        }
        TimestampStringInCoercion::ToTimestamp(timezone) => {
            let target_type = DataType::Timestamp(TimeUnit::Microsecond, timezone.clone());
            Ok(match data_type {
                data_type if data_type.is_string() => {
                    ScalarUDF::from(SparkTimestamp::try_new(timezone.clone(), ansi_mode, false)?)
                        .call(vec![expression])
                }
                DataType::Null => cast(expression, target_type),
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None)
                    if timezone.is_some() =>
                {
                    let string = ScalarUDF::from(SparkToUtf8::new()).call(vec![expression]);
                    ScalarUDF::from(SparkTimestamp::try_new(timezone.clone(), ansi_mode, false)?)
                        .call(vec![string])
                }
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {
                    cast(expression, target_type)
                }
                _ => expression,
            })
        }
        TimestampStringInCoercion::List {
            null_input_type,
            element,
        } => {
            let mut expression = expression;
            let data_type = if data_type.is_null() {
                expression = cast(expression, null_input_type.clone());
                null_input_type
            } else {
                data_type
            };
            let field = match data_type {
                DataType::List(field) | DataType::LargeList(field) => Arc::clone(field),
                _ => return Ok(expression),
            };
            let parameter = format!("__sail_in_element_{list_depth}");
            let variable = expr::Expr::LambdaVariable(LambdaVariable::new(
                parameter.clone(),
                Some(Arc::clone(&field)),
            ));
            let body = apply_timestamp_string_in_coercion(
                variable,
                field.data_type(),
                element,
                schema,
                session_timezone,
                ansi_mode,
                list_depth + 1,
            )?;
            Ok(expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
                Arc::clone(&SPARK_IN_ARRAY_TRANSFORM_UDF),
                vec![
                    expression,
                    expr::Expr::Lambda(Lambda::new(vec![parameter], body)),
                ],
            )))
        }
        TimestampStringInCoercion::Struct {
            null_input_type,
            field_names,
            fields: field_coercions,
        } => {
            let mut expression = expression;
            let data_type = if data_type.is_null() {
                expression = cast(expression, null_input_type.clone());
                null_input_type
            } else {
                data_type
            };
            let source_fields = match data_type {
                DataType::Struct(fields) => fields,
                _ => return Ok(expression),
            };

            let unique_names = (0..source_fields.len())
                .map(|index| format!("__sail_in_field_{index}"))
                .collect::<Vec<_>>();
            let unique_fields = source_fields
                .iter()
                .zip(&unique_names)
                .map(|(field, name)| Arc::new(field.as_ref().clone().with_name(name)))
                .collect::<Vec<Arc<Field>>>();
            expression = ScalarUDF::from(SparkStructRename::new(DataType::Struct(Fields::from(
                unique_fields,
            ))))
            .call(vec![expression]);

            for ((field, field_coercion), unique_name) in
                source_fields.iter().zip(field_coercions).zip(&unique_names)
            {
                let Some(field_coercion) = field_coercion else {
                    continue;
                };
                let value = get_field().call(vec![expression.clone(), lit(unique_name.clone())]);
                let value = apply_timestamp_string_in_coercion(
                    value,
                    field.data_type(),
                    field_coercion,
                    schema,
                    session_timezone,
                    ansi_mode,
                    list_depth,
                )?;
                expression = ScalarUDF::from(UpdateStructField::new(vec![unique_name.clone()]))
                    .call(vec![expression, value]);
            }

            let DataType::Struct(updated_fields) = expression.get_type(schema.as_ref())? else {
                return Ok(expression);
            };
            let output_fields = updated_fields
                .iter()
                .zip(field_names)
                .map(|(field, name)| Arc::new(field.as_ref().clone().with_name(name)))
                .collect::<Vec<Arc<Field>>>();
            Ok(
                ScalarUDF::from(SparkStructRename::new(DataType::Struct(Fields::from(
                    output_fields,
                ))))
                .call(vec![expression]),
            )
        }
    }
}

fn timestamp_string_coercion_requires_rewrite(
    data_type: &DataType,
    coercion: &TimestampStringInCoercion,
) -> bool {
    match coercion {
        TimestampStringInCoercion::ToString => !is_string_type(data_type),
        TimestampStringInCoercion::ToTimestamp(timezone) => {
            data_type != &DataType::Timestamp(TimeUnit::Microsecond, timezone.clone())
        }
        TimestampStringInCoercion::List { element, .. } => {
            data_type.is_null()
                || match data_type {
                    DataType::List(field) | DataType::LargeList(field) => {
                        timestamp_string_coercion_requires_rewrite(field.data_type(), element)
                    }
                    _ => false,
                }
        }
        TimestampStringInCoercion::Struct {
            field_names,
            fields: field_coercions,
            ..
        } => {
            data_type.is_null()
                || match data_type {
                    DataType::Struct(fields) => fields.iter().enumerate().any(|(index, field)| {
                        field.name() != &field_names[index]
                            || field_coercions[index].as_ref().is_some_and(|coercion| {
                                timestamp_string_coercion_requires_rewrite(
                                    field.data_type(),
                                    coercion,
                                )
                            })
                    }),
                    _ => false,
                }
        }
    }
}

/// Apply Spark's numeric precision and timestamp parsing rules before native coercion.
pub(super) fn coerce_comparison_predicate(
    expression: expr::Expr,
    schema: &DFSchemaRef,
    config: &PlanConfig,
) -> PlanResult<expr::Expr> {
    Ok(match expression {
        expr::Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_comparison(op) => {
            let (left, right) = coerce_comparison_pair(*left, *right, op, schema, config)?;
            expr::Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
        }
        expr::Expr::InList(InList {
            mut expr,
            mut list,
            negated,
        }) => {
            let mut expr_type = expr.get_type(schema.as_ref())?;
            let mut list_types = list
                .iter()
                .map(|value| value.get_type(schema.as_ref()))
                .collect::<DataFusionResult<Vec<_>>>()?;
            let mut data_types = Vec::with_capacity(list_types.len() + 1);
            data_types.push(expr_type.clone());
            data_types.extend(list_types.iter().cloned());

            let mut widened = false;
            let column = matches!(expr.as_ref(), expr::Expr::Column(_));
            if config.ansi_mode
                && widens_float_comparison(
                    &std::iter::once((expr.as_ref(), &expr_type))
                        .chain(list.iter().zip(&list_types))
                        .collect::<Vec<_>>(),
                )
                && let Some(targets) =
                    ansi_float_comparison_types(&data_types, config.case_sensitive)
            {
                widened = true;
                expr = Box::new(cast_comparison_operand(*expr, &expr_type, &targets[0]));
                list = list
                    .into_iter()
                    .zip(&list_types)
                    .zip(&targets[1..])
                    .map(|((value, from), to)| cast_comparison_operand(value, from, to))
                    .collect();
                expr_type = targets[0].clone();
                list_types = targets[1..].to_vec();
                data_types = targets;
            }

            let Some(coercion) = find_timestamp_string_in_coercion(
                &data_types,
                &config.session_timezone,
                config.ansi_mode,
                config.case_sensitive,
            ) else {
                // DataFusion rewrites a short IN list on a column into equalities, which merge
                // -0.0 and 0.0; its IN set does not. Keep that rewrite for the cast column.
                if widened
                    && column
                    && (1..=3).contains(&list.len())
                    && !list.iter().any(is_null_literal_expression)
                {
                    return Ok(recursive_list_in_as_comparisons(*expr, list, negated));
                }
                return Ok(expr::Expr::InList(InList::new(expr, list, negated)));
            };
            let has_null_list_value = list.iter().any(is_null_literal_expression);
            let recursive_list_coercion =
                matches!(&coercion, TimestampStringInCoercion::List { .. });

            let expr = apply_timestamp_string_in_coercion(
                *expr,
                &expr_type,
                &coercion,
                schema,
                &config.session_timezone,
                config.ansi_mode,
                0,
            )?;
            let list = list
                .into_iter()
                .zip(list_types)
                .map(|(value, data_type)| {
                    apply_timestamp_string_in_coercion(
                        value,
                        &data_type,
                        &coercion,
                        schema,
                        &config.session_timezone,
                        config.ansi_mode,
                        0,
                    )
                })
                .collect::<DataFusionResult<Vec<_>>>()?;
            if recursive_list_coercion && has_null_list_value && !expr.is_volatile() {
                // A static nested IN set can miss an equal value when the set also contains NULL.
                // Spark defines IN as a Kleene equality disjunction for non-volatile operands.
                recursive_list_in_as_comparisons(expr, list, negated)
            } else {
                expr::Expr::InList(InList::new(Box::new(expr), list, negated))
            }
        }
        expression => expression,
    })
}

fn is_null_literal_expression(expression: &expr::Expr) -> bool {
    match expression {
        expr::Expr::Literal(value, _) => value.is_null(),
        expr::Expr::Alias(alias) => is_null_literal_expression(&alias.expr),
        expr::Expr::Cast(datafusion_expr::expr::Cast { expr, .. })
        | expr::Expr::TryCast(datafusion_expr::expr::TryCast { expr, .. }) => {
            is_null_literal_expression(expr)
        }
        _ => false,
    }
}

fn recursive_list_in_as_comparisons(
    expression: expr::Expr,
    list: Vec<expr::Expr>,
    negated: bool,
) -> expr::Expr {
    let mut comparisons = list.into_iter().map(|value| {
        expr::Expr::BinaryExpr(BinaryExpr::new(
            Box::new(expression.clone()),
            Operator::Eq,
            Box::new(value),
        ))
    });
    let Some(first) = comparisons.next() else {
        return expr::Expr::InList(InList::new(Box::new(expression), vec![], negated));
    };
    let combined = comparisons.fold(first, |left, right| {
        expr::Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            Operator::Or,
            Box::new(right),
        ))
    });
    if negated {
        expr::Expr::Not(Box::new(combined))
    } else {
        combined
    }
}

/// Spark ANSI computes arithmetic and conditionals over integral and FLOAT values as DOUBLE,
/// where native coercion keeps FLOAT.
// TODO: Spark also widens map(), map_concat(), concat() of arrays, set operations, nested
//  array and struct leaves, and FLOAT with DECIMAL to DOUBLE.
pub(super) fn coerce_ansi_float_widening(
    expression: expr::Expr,
    schema: &DFSchemaRef,
    config: &PlanConfig,
) -> DataFusionResult<expr::Expr> {
    if !config.ansi_mode {
        return Ok(expression);
    }
    let widen = |values: Vec<expr::Expr>| -> DataFusionResult<Vec<expr::Expr>> {
        // Leave operands that are not typed yet, such as `grouping_id()`, to native coercion.
        let Ok(types) = values
            .iter()
            .map(|value| value.get_type(schema.as_ref()))
            .collect::<DataFusionResult<Vec<_>>>()
        else {
            return Ok(values);
        };
        let integral = |t: &DataType| {
            matches!(
                t,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            )
        };
        let widened = types.iter().any(|t| matches!(t, DataType::Float32))
            && types.iter().any(integral)
            && types.iter().all(|t| {
                t.is_null() || integral(t) || matches!(t, DataType::Float32 | DataType::Float64)
            });
        Ok(if widened {
            values
                .into_iter()
                .zip(&types)
                .map(|(value, from)| cast_comparison_operand(value, from, &DataType::Float64))
                .collect()
        } else {
            values
        })
    };
    Ok(match expression {
        expr::Expr::BinaryExpr(BinaryExpr { left, op, right })
            if matches!(
                op,
                Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Modulo
            ) =>
        {
            let mut operands = widen(vec![*left, *right])?.into_iter();
            match (operands.next(), operands.next()) {
                (Some(left), Some(right)) => {
                    expr::Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
                }
                _ => unreachable!("two operands"),
            }
        }
        expr::Expr::ScalarFunction(mut function)
            if matches!(
                function.name(),
                "coalesce" | "greatest" | "least" | "pmod" | "spark_array"
            ) =>
        {
            function.args = widen(function.args)?;
            expr::Expr::ScalarFunction(function)
        }
        expr::Expr::Case(mut case) => {
            let mut values = case
                .when_then_expr
                .iter()
                .map(|(_, then)| then.as_ref().clone())
                .chain(case.else_expr.as_deref().cloned())
                .collect::<Vec<_>>();
            values = widen(values)?;
            let mut values = values.into_iter();
            for (_, then) in &mut case.when_then_expr {
                if let Some(value) = values.next() {
                    **then = value;
                }
            }
            if let (Some(else_expr), Some(value)) = (case.else_expr.as_mut(), values.next()) {
                **else_expr = value;
            }
            expr::Expr::Case(case)
        }
        expression => expression,
    })
}

/// Casts the values of a conditional expression to their common type.
///
/// DataFusion types a CASE expression by its first non-null result value, and the analyzer
/// casts the values to their common type only after the expression is resolved. Casting the
/// values to the same common type here gives the resolved expression its final type, which
/// enclosing expressions (e.g. `typeof` or the arguments of a function) depend on.
///
/// A value of a nested type is cast even if it already has the common type. Constant folding
/// can make nested fields of the value non-nullable later (e.g. `array(named_struct('a', 1))`),
/// but CASE evaluation requires every value to have exactly the type of the expression.
/// This also applies to the arguments of `coalesce`, which is evaluated as a CASE expression.
pub(super) fn coerce_conditional_values(
    expression: expr::Expr,
    schema: &DFSchemaRef,
) -> DataFusionResult<expr::Expr> {
    match expression {
        expr::Expr::Case(case) => coerce_case_result_values(case, schema.as_ref()),
        // DataFusion simplifies its `coalesce` function to a CASE expression.
        expr::Expr::ScalarFunction(function) if function.name() == "coalesce" => {
            Ok(cast_nested_coalesce_arguments(function, schema.as_ref()))
        }
        expression => Ok(expression),
    }
}

fn coerce_case_result_values(
    mut case: expr::Case,
    schema: &DFSchema,
) -> DataFusionResult<expr::Expr> {
    // Leave values that are not typed yet, such as `grouping_id()`, to the analyzer.
    let Ok(then_types) = case
        .when_then_expr
        .iter()
        .map(|(_, then)| then.get_type(schema))
        .collect::<DataFusionResult<Vec<_>>>()
    else {
        return Ok(expr::Expr::Case(case));
    };
    let Ok(else_type) = case
        .else_expr
        .as_ref()
        .map(|value| value.get_type(schema))
        .transpose()
    else {
        return Ok(expr::Expr::Case(case));
    };
    // The analyzer reports the error if there is no common type.
    let Some(common_type) = case_result_type(&then_types, else_type.as_ref()) else {
        return Ok(expr::Expr::Case(case));
    };
    let coerce =
        |value: Box<expr::Expr>, data_type: &DataType| -> DataFusionResult<Box<expr::Expr>> {
            if data_type.is_null() {
                // The analyzer casts NULL values, including parameters that are not bound yet.
                Ok(value)
            } else {
                Ok(Box::new(cast_case_result_value(
                    *value,
                    data_type,
                    &common_type,
                    schema,
                )?))
            }
        };
    case.when_then_expr = case
        .when_then_expr
        .into_iter()
        .zip(&then_types)
        .map(|((when, then), data_type)| Ok((when, coerce(then, data_type)?)))
        .collect::<DataFusionResult<_>>()?;
    case.else_expr = case
        .else_expr
        .zip(else_type.as_ref())
        .map(|(value, data_type)| coerce(value, data_type))
        .transpose()?;
    Ok(expr::Expr::Case(case))
}

/// Casts the nested arguments of `coalesce` that already have the type that the analyzer
/// coerces them to. The analyzer casts the other arguments.
fn cast_nested_coalesce_arguments(
    mut function: expr::ScalarFunction,
    schema: &DFSchema,
) -> expr::Expr {
    let Ok(data_types) = function
        .args
        .iter()
        .map(|arg| arg.get_type(schema))
        .collect::<DataFusionResult<Vec<_>>>()
    else {
        return expr::Expr::ScalarFunction(function);
    };
    let Ok(coerced_types) = try_type_union_resolution(&data_types) else {
        return expr::Expr::ScalarFunction(function);
    };
    function.args = function
        .args
        .into_iter()
        .zip(data_types.iter().zip(&coerced_types))
        .map(|(arg, (data_type, coerced_type))| {
            if data_type == coerced_type {
                cast_nested_value(arg, data_type)
            } else {
                arg
            }
        })
        .collect();
    expr::Expr::ScalarFunction(function)
}

/// Whether to compare FLOAT and integral operands as DOUBLE, like Spark ANSI. Integral
/// literals that FLOAT represents exactly compare the same either way, without casting
/// every FLOAT value.
fn widens_float_comparison(operands: &[(&expr::Expr, &DataType)]) -> bool {
    const FLOAT_EXACT: i128 = 1 << 24;
    let exact_literal = |expression: &expr::Expr| match expression {
        expr::Expr::Literal(value, _) => match value {
            ScalarValue::Int8(Some(v)) => i128::from(*v).abs() <= FLOAT_EXACT,
            ScalarValue::Int16(Some(v)) => i128::from(*v).abs() <= FLOAT_EXACT,
            ScalarValue::Int32(Some(v)) => i128::from(*v).abs() <= FLOAT_EXACT,
            ScalarValue::Int64(Some(v)) => i128::from(*v).abs() <= FLOAT_EXACT,
            _ => false,
        },
        _ => false,
    };
    !operands
        .iter()
        .all(|(expression, data_type)| match data_type {
            DataType::Float32 | DataType::Null => true,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                exact_literal(expression)
            }
            _ => false,
        })
}

/// Spark ANSI compares FLOAT with integral values as DOUBLE, including nested leaves.
/// Keep each operand's other fields intact for the existing coercion rules.
pub(in crate::resolver) fn ansi_float_comparison_types(
    data_types: &[DataType],
    case_sensitive: bool,
) -> Option<Vec<DataType>> {
    use DataType::*;
    if data_types.iter().any(|t| matches!(t, Float32))
        && data_types
            .iter()
            .any(|t| matches!(t, Int8 | Int16 | Int32 | Int64))
        && data_types.iter().all(|t| {
            matches!(t, Null | Int8 | Int16 | Int32 | Int64 | Float32 | Float64)
                || t.is_string()
                || t.is_decimal()
        })
    {
        return Some(vec![Float64; data_types.len()]);
    }
    let first = data_types.iter().find(|t| !t.is_null())?;
    match first {
        List(_) | LargeList(_) | FixedSizeList(_, _) => {
            let elements = data_types
                .iter()
                .map(|t| match t {
                    List(field) | LargeList(field) | FixedSizeList(field, _) => {
                        Some(field.data_type().clone())
                    }
                    Null => Some(Null),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            let targets = ansi_float_comparison_types(&elements, case_sensitive)?;
            Some(
                data_types
                    .iter()
                    .zip(targets)
                    .map(|(t, target)| match t {
                        List(field) => {
                            List(Arc::new(field.as_ref().clone().with_data_type(target)))
                        }
                        LargeList(field) => {
                            LargeList(Arc::new(field.as_ref().clone().with_data_type(target)))
                        }
                        FixedSizeList(field, size) => FixedSizeList(
                            Arc::new(field.as_ref().clone().with_data_type(target)),
                            *size,
                        ),
                        _ => t.clone(),
                    })
                    .collect(),
            )
        }
        Struct(first_fields) => {
            if !data_types.iter().all(|t| match t {
                Struct(fields) => {
                    fields.len() == first_fields.len()
                        && fields.iter().zip(first_fields).all(|(field, first)| {
                            struct_field_names_equal(field.name(), first.name(), case_sensitive)
                        })
                }
                Null => true,
                _ => false,
            }) {
                return None;
            }
            let mut result = data_types
                .iter()
                .map(|t| match t {
                    Struct(fields) => Some(fields.to_vec()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let mut changed = false;
            for index in 0..first_fields.len() {
                let fields = data_types
                    .iter()
                    .map(|t| match t {
                        Struct(fields) => fields[index].data_type().clone(),
                        _ => Null,
                    })
                    .collect::<Vec<_>>();
                if let Some(targets) = ansi_float_comparison_types(&fields, case_sensitive) {
                    changed = true;
                    for (fields, target) in result.iter_mut().zip(targets) {
                        if let Some(fields) = fields {
                            fields[index] =
                                Arc::new(fields[index].as_ref().clone().with_data_type(target));
                        }
                    }
                }
            }
            changed.then(|| {
                result
                    .into_iter()
                    .zip(data_types)
                    .map(|(fields, original)| {
                        fields.map_or_else(|| original.clone(), |fields| Struct(fields.into()))
                    })
                    .collect()
            })
        }
        _ => None,
    }
}

fn cast_comparison_operand(expression: expr::Expr, from: &DataType, to: &DataType) -> expr::Expr {
    if from == to {
        expression
    } else {
        // Cast the complete operand once, including volatile arrays and structs.
        cast(expression, to.clone())
    }
}

fn coerce_comparison_pair(
    left: expr::Expr,
    right: expr::Expr,
    operator: Operator,
    schema: &DFSchemaRef,
    config: &PlanConfig,
) -> DataFusionResult<(expr::Expr, expr::Expr)> {
    let left_type = left.get_type(schema.as_ref())?;
    let right_type = right.get_type(schema.as_ref())?;

    if config.ansi_mode
        && widens_float_comparison(&[(&left, &left_type), (&right, &right_type)])
        && let Some(targets) = ansi_float_comparison_types(
            &[left_type.clone(), right_type.clone()],
            config.case_sensitive,
        )
    {
        return Ok((
            cast_comparison_operand(left, &left_type, &targets[0]),
            cast_comparison_operand(right, &right_type, &targets[1]),
        ));
    }

    if !config.ansi_mode
        && config.legacy_type_coercion_datetime_to_string
        && is_ordering_comparison(operator)
    {
        if is_date_or_timestamp_ltz(&left_type) && is_string_type(&right_type) {
            let left =
                stringify_non_ansi_expression(left, &left_type, schema, &config.session_timezone)?;
            return Ok((left, right));
        }
        if is_string_type(&left_type) && is_date_or_timestamp_ltz(&right_type) {
            let right = stringify_non_ansi_expression(
                right,
                &right_type,
                schema,
                &config.session_timezone,
            )?;
            return Ok((left, right));
        }
    }

    if let Some(timezone) = timestamp_parse_timezone_for_type(&left_type, &config.session_timezone)
        && is_string_type(&right_type)
    {
        let right = coerce_string_to_timestamp(right, timezone, config.ansi_mode)?;
        return Ok((left, right));
    }
    if let Some(timezone) = timestamp_parse_timezone_for_type(&right_type, &config.session_timezone)
        && is_string_type(&left_type)
    {
        let left = coerce_string_to_timestamp(left, timezone, config.ansi_mode)?;
        return Ok((left, right));
    }
    Ok((left, right))
}

fn coerce_string_to_timestamp(
    expression: expr::Expr,
    timezone: Option<Arc<str>>,
    ansi_mode: bool,
) -> DataFusionResult<expr::Expr> {
    let function = ScalarUDF::from(SparkTimestamp::try_new(timezone, ansi_mode, false)?);
    Ok(function.call(vec![expression]))
}

fn stringify_non_ansi_expression(
    expression: expr::Expr,
    data_type: &DataType,
    schema: &DFSchemaRef,
    session_timezone: &Arc<str>,
) -> DataFusionResult<expr::Expr> {
    if is_string_type(data_type) || data_type == &DataType::Null {
        return Ok(expression);
    }
    if let Some(interval) = spark_interval_metadata_for_expression(&expression, schema)? {
        let metadata = interval
            .to_json()
            .map_err(|error| datafusion_common::DataFusionError::Plan(error.to_string()))?;
        return Ok(ScalarUDF::from(SparkToUtf8::new()).call(vec![expression, lit(metadata)]));
    }
    let expression = localize_timestamp_for_string(expression, data_type, session_timezone);
    Ok(ScalarUDF::from(SparkToUtf8::new()).call(vec![expression]))
}

pub(in crate::resolver) fn spark_interval_metadata_for_expression(
    expression: &expr::Expr,
    schema: &DFSchemaRef,
) -> DataFusionResult<Option<spec::SparkIntervalMetadata>> {
    let field = expression.to_field(schema.as_ref())?.1;
    if !matches!(
        field.data_type(),
        DataType::Duration(TimeUnit::Microsecond) | DataType::Interval(IntervalUnit::YearMonth)
    ) {
        return Ok(None);
    }

    let interval_type = field.data_type().clone();
    let mut combined = None::<spec::SparkIntervalMetadata>;
    expression.apply(|candidate| {
        let field = candidate.to_field(schema.as_ref())?.1;
        if field.data_type() != &interval_type {
            return Ok(TreeNodeRecursion::Jump);
        }
        let Some(value) = field.metadata().get(spec::SAIL_SPARK_INTERVAL_METADATA_KEY) else {
            return Ok(TreeNodeRecursion::Continue);
        };
        let candidate = spec::SparkIntervalMetadata::from_json(value)
            .map_err(|error| datafusion_common::DataFusionError::Plan(error.to_string()))?;
        combined = Some(match combined {
            None => candidate,
            Some(current) => current.wider(candidate).ok_or_else(|| {
                datafusion_common::DataFusionError::Plan(
                    "incompatible Spark interval metadata in expression".to_string(),
                )
            })?,
        });
        Ok(TreeNodeRecursion::Jump)
    })?;
    Ok(combined)
}

fn localize_timestamp_for_string(
    expression: expr::Expr,
    data_type: &DataType,
    session_timezone: &Arc<str>,
) -> expr::Expr {
    match data_type {
        DataType::Timestamp(unit, Some(_)) => cast(
            expression,
            DataType::Timestamp(*unit, Some(Arc::clone(session_timezone))),
        ),
        _ => expression,
    }
}

fn non_ansi_in_string_promotable(data_type: &DataType) -> bool {
    data_type.is_null()
        || data_type.is_numeric()
        || data_type.is_string()
        || (data_type.is_temporal()
            && !matches!(data_type, DataType::Interval(IntervalUnit::MonthDayNano)))
}

fn is_string_type(data_type: &DataType) -> bool {
    data_type.is_string()
}

/// Coerces an IN-subquery pair whose right-hand expression belongs to a separate logical plan.
pub(super) fn coerce_in_subquery_pair(
    left: expr::Expr,
    left_type: &DataType,
    left_schema: &DFSchemaRef,
    right: expr::Expr,
    right_type: &DataType,
    right_schema: &DFSchemaRef,
    config: &PlanConfig,
) -> DataFusionResult<(expr::Expr, expr::Expr, bool, bool)> {
    let targets = (config.ansi_mode
        && widens_float_comparison(&[(&left, left_type), (&right, right_type)]))
    .then(|| {
        ansi_float_comparison_types(
            &[left_type.clone(), right_type.clone()],
            config.case_sensitive,
        )
    })
    .flatten();
    let (left, right, left_type, right_type, left_numeric, right_numeric) =
        if let Some(targets) = targets {
            (
                cast_comparison_operand(left, left_type, &targets[0]),
                cast_comparison_operand(right, right_type, &targets[1]),
                targets[0].clone(),
                targets[1].clone(),
                left_type != &targets[0],
                right_type != &targets[1],
            )
        } else {
            (
                left,
                right,
                left_type.clone(),
                right_type.clone(),
                false,
                false,
            )
        };
    let Some(coercion) = find_timestamp_string_in_coercion(
        &[left_type.clone(), right_type.clone()],
        &config.session_timezone,
        config.ansi_mode,
        config.case_sensitive,
    ) else {
        return Ok((left, right, left_numeric, right_numeric));
    };

    let left_changed =
        left_numeric || timestamp_string_coercion_requires_rewrite(&left_type, &coercion);
    let right_changed =
        right_numeric || timestamp_string_coercion_requires_rewrite(&right_type, &coercion);
    let left = apply_timestamp_string_in_coercion(
        left,
        &left_type,
        &coercion,
        left_schema,
        &config.session_timezone,
        config.ansi_mode,
        0,
    )?;
    let right = apply_timestamp_string_in_coercion(
        right,
        &right_type,
        &coercion,
        right_schema,
        &config.session_timezone,
        config.ansi_mode,
        0,
    )?;
    Ok((left, right, left_changed, right_changed))
}

fn timestamp_parse_timezone_for_type(
    data_type: &DataType,
    session_timezone: &Arc<str>,
) -> Option<Option<Arc<str>>> {
    match data_type {
        DataType::Timestamp(_, Some(_)) => Some(Some(Arc::clone(session_timezone))),
        DataType::Timestamp(_, None) => Some(None),
        _ => None,
    }
}

fn struct_field_names_equal(left: &str, right: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        left == right
    } else {
        left.eq_ignore_ascii_case(right)
    }
}

fn is_date_or_timestamp_ltz(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, Some(_))
    )
}

fn is_ordering_comparison(operator: Operator) -> bool {
    matches!(
        operator,
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    )
}

fn is_comparison(operator: Operator) -> bool {
    matches!(
        operator,
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

impl PlanResolver<'_> {
    // TODO: Construct better names for the expression (e.g. a IN (b, c)) for all functions below.

    pub(super) async fn resolve_expression_in_list(
        &self,
        expr: spec::Expr,
        list: Vec<spec::Expr>,
        negated: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = Box::new(self.resolve_expression(expr, schema, state).await?);
        let list = self.resolve_expressions(list, schema, state).await?;
        let expression = coerce_comparison_predicate(
            expr::Expr::InList(expr::InList::new(expr, list, negated)),
            schema,
            &self.config,
        )?;
        Ok(NamedExpr::new(vec!["in_list".to_string()], expression))
    }

    pub(super) async fn resolve_expression_is_false(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_false".to_string()],
            expr::Expr::IsFalse(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_not_false(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_not_false".to_string()],
            expr::Expr::IsNotFalse(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_true(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_true".to_string()],
            expr::Expr::IsTrue(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_not_true(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_not_true".to_string()],
            expr::Expr::IsNotTrue(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_null(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_null".to_string()],
            expr::Expr::IsNull(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_not_null(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_not_null".to_string()],
            expr::Expr::IsNotNull(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_unknown(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_unknown".to_string()],
            expr::Expr::IsUnknown(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_not_unknown(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_not_unknown".to_string()],
            expr::Expr::IsNotUnknown(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_between(
        &self,
        expr: spec::Expr,
        negated: bool,
        low: spec::Expr,
        high: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        let low = self.resolve_expression(low, schema, state).await?;
        let high = self.resolve_expression(high, schema, state).await?;

        // DataFusion's BETWEEN operator has a bug, so we construct the expression manually.
        let greater_eq = coerce_comparison_predicate(
            expr::Expr::BinaryExpr(BinaryExpr::new(
                Box::new(expr.clone()),
                Operator::GtEq,
                Box::new(low),
            )),
            schema,
            &self.config,
        )?;
        let less_eq = coerce_comparison_predicate(
            expr::Expr::BinaryExpr(BinaryExpr::new(
                Box::new(expr),
                Operator::LtEq,
                Box::new(high),
            )),
            schema,
            &self.config,
        )?;
        let between_expr = expr::Expr::BinaryExpr(BinaryExpr::new(
            Box::new(greater_eq),
            Operator::And,
            Box::new(less_eq),
        ));
        let between_expr = if negated {
            expr::Expr::Not(Box::new(between_expr))
        } else {
            between_expr
        };
        Ok(NamedExpr::new(vec!["between".to_string()], between_expr))
    }

    pub(super) async fn resolve_expression_is_distinct_from(
        &self,
        left: spec::Expr,
        right: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let left = self.resolve_expression(left, schema, state).await?;
        let right = self.resolve_expression(right, schema, state).await?;
        let expression = coerce_comparison_predicate(
            expr::Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op: Operator::IsDistinctFrom,
                right: Box::new(right),
            }),
            schema,
            &self.config,
        )?;
        let expression = expr::Expr::IsTrue(Box::new(expression));
        Ok(NamedExpr::new(
            vec!["is_distinct_from".to_string()],
            expression,
        ))
    }

    pub(super) async fn resolve_expression_is_not_distinct_from(
        &self,
        left: spec::Expr,
        right: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let left = self.resolve_expression(left, schema, state).await?;
        let right = self.resolve_expression(right, schema, state).await?;
        let expression = coerce_comparison_predicate(
            expr::Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op: Operator::IsNotDistinctFrom,
                right: Box::new(right),
            }),
            schema,
            &self.config,
        )?;
        let expression = expr::Expr::IsTrue(Box::new(expression));
        Ok(NamedExpr::new(
            vec!["is_not_distinct_from".to_string()],
            expression,
        ))
    }

    pub(super) async fn resolve_expression_similar_to(
        &self,
        expr: spec::Expr,
        pattern: spec::Expr,
        negated: bool,
        escape_char: Option<char>,
        case_insensitive: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        let pattern = self.resolve_expression(pattern, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["similar_to".to_string()],
            expr::Expr::SimilarTo(expr::Like::new(
                negated,
                Box::new(expr),
                Box::new(pattern),
                escape_char,
                case_insensitive,
            )),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::compute::can_cast_types;
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
    use datafusion_common::{DFSchema, DFSchemaRef, Result, ScalarValue, internal_err};
    use datafusion_expr::{Expr, ExprSchemable, col, lit, when};
    use datafusion_functions::core::expr_fn::coalesce;
    use sail_logical_optimizer::type_coercion::case_result_type;

    use super::coerce_conditional_values;

    fn schema(fields: Vec<Field>) -> Result<DFSchemaRef> {
        let fields = std::iter::once(Field::new("c", DataType::Boolean, true)).chain(fields);
        Ok(Arc::new(DFSchema::try_from(Schema::new(
            fields.collect::<Vec<_>>(),
        ))?))
    }

    fn values(case: &Expr) -> Result<Vec<&Expr>> {
        let Expr::Case(case) = case else {
            return internal_err!("not a CASE expression: {case}");
        };
        Ok(case
            .when_then_expr
            .iter()
            .map(|(_, then)| then.as_ref())
            .chain(case.else_expr.as_deref())
            .collect())
    }

    fn list(field: Field) -> DataType {
        DataType::List(Arc::new(field))
    }

    #[test]
    fn case_result_values_have_the_common_type() -> Result<()> {
        let item = |data_type: DataType, nullable: bool| Field::new_list_field(data_type, nullable);
        let types = [
            DataType::Int8,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int64,
            DataType::Float16,
            DataType::Float32,
            DataType::Decimal32(9, 2),
            DataType::Decimal64(18, 4),
            DataType::Decimal128(38, 0),
            DataType::Decimal256(50, 2),
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Date32,
            list(item(DataType::Int32, false)),
            list(item(DataType::Int32, true)),
            list(item(DataType::Int64, true)),
            DataType::LargeList(Arc::new(item(DataType::Int32, false))),
            DataType::LargeList(Arc::new(item(DataType::Int32, true))),
            DataType::FixedSizeList(Arc::new(item(DataType::Int32, false)), 2),
            DataType::FixedSizeList(Arc::new(item(DataType::Int32, true)), 2),
            DataType::Struct(Fields::from(vec![Field::new("n", DataType::Int32, false)])),
            DataType::Struct(Fields::from(vec![Field::new("n", DataType::Int64, true)])),
        ];
        let schema = schema(
            types
                .iter()
                .enumerate()
                .map(|(i, data_type)| Field::new(format!("v{i}"), data_type.clone(), true))
                .collect(),
        )?;
        for (i, first) in types.iter().enumerate() {
            for (j, second) in types.iter().enumerate() {
                let case = when(col("c"), col(format!("v{i}"))).otherwise(col(format!("v{j}")))?;
                let Some(common_type) = case_result_type(std::slice::from_ref(first), Some(second))
                else {
                    // The analyzer reports the error for values without a common type.
                    assert_eq!(coerce_conditional_values(case.clone(), &schema)?, case);
                    continue;
                };
                if !can_cast_types(first, &common_type) || !can_cast_types(second, &common_type) {
                    // The analyzer reports the same error for values that cannot be cast.
                    assert!(coerce_conditional_values(case, &schema).is_err());
                    continue;
                }
                let case = coerce_conditional_values(case, &schema)?;
                assert_eq!(
                    case.get_type(schema.as_ref())?,
                    common_type,
                    "{first} and {second}"
                );
                for (value, data_type) in values(&case)?.into_iter().zip([first, second]) {
                    assert_eq!(value.get_type(schema.as_ref())?, common_type);
                    let cast = *data_type != common_type || common_type.is_nested();
                    assert_eq!(matches!(value, Expr::Cast(_)), cast, "{value}");
                }
                assert_eq!(coerce_conditional_values(case.clone(), &schema)?, case);
            }
        }
        Ok(())
    }

    #[test]
    fn case_result_values_keep_nested_field_metadata() -> Result<()> {
        let metadata = HashMap::from([("key".to_string(), "value".to_string())]);
        let fields = |n: DataType| {
            Fields::from(vec![
                Field::new("v", DataType::Int32, true).with_metadata(metadata.clone()),
                Field::new("n", n, true),
            ])
        };
        let schema = schema(vec![
            Field::new(
                "a",
                list(Field::new_list_field(
                    DataType::Struct(fields(DataType::Int32)),
                    false,
                )),
                true,
            ),
            Field::new(
                "b",
                list(Field::new_list_field(
                    DataType::Struct(fields(DataType::Int64)),
                    true,
                )),
                true,
            ),
        ])?;
        let case =
            coerce_conditional_values(when(col("c"), col("a")).otherwise(col("b"))?, &schema)?;
        let DataType::List(item) = case.get_type(schema.as_ref())? else {
            return internal_err!("not a list");
        };
        assert!(item.is_nullable());
        let DataType::Struct(fields) = item.data_type() else {
            return internal_err!("not a struct");
        };
        assert_eq!(fields[0].metadata(), &metadata);
        assert_eq!(fields[1].data_type(), &DataType::Int64);
        // NULL values are cast to the common type by the analyzer.
        let case = when(col("c"), lit(ScalarValue::Null)).otherwise(col("b"))?;
        let case = coerce_conditional_values(case, &schema)?;
        assert_eq!(values(&case)?[0], &lit(ScalarValue::Null));
        Ok(())
    }

    #[test]
    fn coalesce_nested_arguments_of_the_coerced_type_are_cast() -> Result<()> {
        let item = |nullable: bool| Field::new_list_field(DataType::Int32, nullable);
        let schema = schema(vec![
            Field::new("a", list(item(true)), true),
            Field::new("b", list(item(true)), true),
            Field::new("c2", list(item(false)), true),
            Field::new("d", DataType::Int32, true),
        ])?;
        let arguments = |expr: &Expr| match expr {
            Expr::ScalarFunction(function) => Ok(function.args.clone()),
            _ => internal_err!("not a function: {expr}"),
        };
        let expr = coalesce(vec![col("a"), col("b"), col("c2")]);
        let expr = coerce_conditional_values(expr, &schema)?;
        let args = arguments(&expr)?;
        // The analyzer casts the argument whose type differs from the coerced type.
        assert!(matches!(args[0], Expr::Cast(_)));
        assert!(matches!(args[1], Expr::Cast(_)));
        assert_eq!(args[2], col("c2"));
        assert_eq!(coerce_conditional_values(expr.clone(), &schema)?, expr);
        let expr = coalesce(vec![col("d"), lit(1)]);
        assert_eq!(coerce_conditional_values(expr.clone(), &schema)?, expr);
        Ok(())
    }
}
