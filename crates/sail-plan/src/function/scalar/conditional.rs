use std::sync::Arc;

use arrow::datatypes::{DECIMAL128_MAX_PRECISION, DataType, FieldRef, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::type_coercion::other::get_coerce_type_for_case_expression;
use datafusion_expr::{ExprSchemable, ScalarUDF, ScalarUDFImpl, cast, expr, lit};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::conditional::{
    SparkConditionalCast, SparkNvl2, preserve_nested_metadata,
};
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_to_string::SparkToUtf8;

use crate::config::PlanConfig;
use crate::error::PlanResult;
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};

fn case(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let mut conditions = Vec::new();
    let mut branch_values = Vec::new();
    let mut iter = arguments.into_iter();
    while let Some(condition) = iter.next() {
        match iter.next() {
            Some(result) => {
                conditions.push(condition);
                branch_values.push(result);
            }
            _ => {
                conditions.push(lit(true));
                branch_values.push(condition);
                break;
            }
        }
    }
    let branch_values = coerce_branch_values(branch_values, &function_context)?;
    let when_then_expr = conditions
        .into_iter()
        .zip(branch_values)
        .map(|(condition, value)| (Box::new(condition), Box::new(value)))
        .collect();
    Ok(expr::Expr::Case(expr::Case {
        expr: None, // Expr::Case in from_ast_expression incorporates into when_then_expr
        when_then_expr,
        else_expr: None,
    }))
}

fn if_expr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (when_expr, then_expr, else_expr) = arguments.three()?;
    let (then_expr, else_expr) =
        coerce_branch_values(vec![then_expr, else_expr], &function_context)?.two()?;
    Ok(expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(Box::new(when_expr), Box::new(then_expr))],
        else_expr: Some(Box::new(else_expr)),
    }))
}

fn nvl2(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (tested, if_non_null, if_null) = arguments.three()?;
    // NVL2 temporal branches use the same creation-time ANSI mode as numeric branches.
    let mut config = Arc::clone(function_context.plan_config);
    if let Some(ansi_mode) = config.view_conditional_ansi_mode {
        Arc::make_mut(&mut config).ansi_mode = ansi_mode;
    }
    let function_context = FunctionContextInput {
        plan_config: &config,
        ..function_context
    };
    let branches = coerce_branch_values(vec![if_non_null, if_null], &function_context)?;
    let (if_non_null, if_null) = branches.two()?;
    let function = SparkNvl2::new(Arc::clone(&function_context.plan_config.session_timezone));
    // Lower branches that already have their common type, avoiding repeated
    // field derivation through nested logical NVL2 functions.
    // Unresolved bindings and branches needing coercion retain the logical UDF.
    let non_null_type = if_non_null.get_type(function_context.schema)?;
    let null_type = if_null.get_type(function_context.schema)?;
    let resolved = !null_type.is_null()
        && non_null_type == null_type
        && function.return_type(&[DataType::Null, non_null_type, null_type.clone()])? == null_type;
    if resolved {
        return Ok(SparkNvl2::lower(tested, if_non_null, if_null));
    }
    Ok(ScalarUDF::from(function).call(vec![tested, if_non_null, if_null]))
}

fn coalesce(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let data_types = argument_types(&arguments, &function_context)?;
    let arguments = coerce_string_temporal_values(arguments, data_types, &function_context)?;
    Ok(expr_fn::coalesce(arguments))
}

/// Coerces the CASE/IF result values to a common type before building the expression,
/// since DataFusion types a CASE expression by its first non-null result value.
fn coerce_branch_values(
    arguments: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let data_types = argument_types(&arguments, function_context)?;
    if data_types.iter().any(is_temporal_type) {
        // A temporal branch cannot have a numeric common type.
        coerce_string_temporal_values(arguments, data_types, function_context)
    } else {
        coerce_numeric_values(arguments, data_types, function_context)
    }
}

fn argument_types(
    arguments: &[expr::Expr],
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<DataType>> {
    Ok(arguments
        .iter()
        .map(|arg| arg.get_type(function_context.schema))
        .collect::<Result<Vec<_>, _>>()?)
}

fn conditional_common_type(data_types: &[DataType]) -> Option<DataType> {
    let common_type = get_coerce_type_for_case_expression(data_types, None)?;
    let Some(source_type) = data_types.iter().find(|data_type| !data_type.is_null()) else {
        return Some(common_type);
    };
    // DataFusion rebuilds nested fields while coercing their types. Keep source metadata
    // such as interval qualifiers, which enclosing expressions need before analysis.
    // TODO: Widen nested interval qualifiers across every branch instead of keeping
    // only the first qualifier (for example, YEAR and MONTH require YEAR TO MONTH).
    Some(preserve_nested_metadata(source_type, &common_type))
}

/// Casts numeric values to Spark's wider common type (`findWiderCommonType`).
/// Preserves DataFusion's existing nested coercion except ANSI STRING/numeric leaves.
fn coerce_numeric_values(
    arguments: Vec<expr::Expr>,
    data_types: Vec<DataType>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let ansi_mode = function_context
        .plan_config
        .view_conditional_ansi_mode
        .unwrap_or(
            function_context.plan_config.ansi_mode
                && !function_context
                    .plan_config
                    .preserve_view_conditional_float_type,
        );
    let retain_fraction_digits = function_context
        .plan_config
        .legacy_decimal_retain_fraction_digits;
    let common_type = data_types.iter().try_fold(DataType::Null, |left, right| {
        wider_numeric_type(&left, right, ansi_mode, retain_fraction_digits)
    });
    let common_type = common_type.or_else(|| {
        if (data_types.iter().any(is_string_type)
            && data_types.iter().any(is_numeric_type)
            && data_types
                .iter()
                .all(|t| t.is_null() || is_numeric_type(t) || is_string_type(t)))
            || data_types.iter().all(|t| t.is_null() || t.is_nested())
        {
            // Preserve DataFusion's existing coercion for nested types and
            // string/numeric branches before an enclosing numeric CASE/IF uses their type.
            conditional_common_type(&data_types)
        } else {
            None
        }
    });
    let Some(common_type) = common_type else {
        return Ok(arguments);
    };
    let repaired_type = if ansi_mode {
        ansi_string_numeric_type(&data_types, &common_type, function_context.plan_config)
    } else {
        common_type.clone()
    };
    let ansi_string_coercion = repaired_type != common_type;
    let common_type = repaired_type;
    arguments
        .into_iter()
        .zip(data_types)
        .map(|(arg, data_type)| {
            if data_type.is_null() || data_type == common_type {
                // Defer NULL coercion and avoid re-deriving an already common
                // branch type in Expr::cast_to (important for nested conditionals).
                Ok(arg)
            } else {
                // TODO: Return NULL for overflowing implicit DECIMAL casts in non-ANSI mode.
                // Retaining fractional digits can narrow the integral range.
                // TODO: Match Spark's DECIMAL-to-DOUBLE rounding at high scales once shared
                // casts support it; Arrow can currently return an adjacent floating value.
                // Like DataFusion's type coercion, this keeps values of the common type unchanged
                // and casts a scalar subquery inside the subquery.
                if ansi_string_coercion {
                    // Keep invalid literals lazy: DataFusion eagerly rejects literal CASTs
                    // even in an unselected CASE branch, but defers errors from this UDF.
                    Ok(
                        ScalarUDF::from(SparkConditionalCast::new(common_type.clone()))
                            .call(vec![arg]),
                    )
                } else {
                    Ok(arg.cast_to(&common_type, function_context.schema)?)
                }
            }
        })
        .collect()
}

/// Repairs only ANSI STRING/numeric leaves in the existing common type. In particular,
/// source projections must expose the numeric result before store-assignment validation.
pub(crate) fn ansi_string_numeric_type(
    data_types: &[DataType],
    common_type: &DataType,
    config: &PlanConfig,
) -> DataType {
    if is_string_type(common_type)
        && data_types.iter().any(is_string_type)
        && data_types.iter().any(is_numeric_type)
        && data_types
            .iter()
            .all(|t| t.is_null() || is_string_type(t) || is_numeric_type(t))
    {
        // ANSI findWiderCommonType folds in branch order; STRING/INT/DECIMAL
        // does not necessarily have the same result as DECIMAL/INT/STRING.
        return data_types
            .iter()
            .try_fold(DataType::Null, |left, right| {
                if left.is_null() {
                    Some(right.clone())
                } else if right.is_null() || left == *right {
                    Some(left)
                } else if is_string_type(&left) || is_string_type(right) {
                    let other = if is_string_type(&left) { right } else { &left };
                    if integral_decimal_precision(other).is_some() {
                        Some(DataType::Int64)
                    } else if is_numeric_type(other) {
                        Some(DataType::Float64)
                    } else {
                        Some(common_type.clone())
                    }
                } else {
                    wider_numeric_type(
                        &left,
                        right,
                        true,
                        config.legacy_decimal_retain_fraction_digits,
                    )
                }
            })
            .unwrap_or_else(|| common_type.clone());
    }
    match common_type {
        DataType::List(field) | DataType::LargeList(field) => {
            let element_types = data_types
                .iter()
                .map(|t| match t {
                    DataType::Null => Some(DataType::Null),
                    DataType::List(field)
                    | DataType::LargeList(field)
                    | DataType::FixedSizeList(field, _) => Some(field.data_type().clone()),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>();
            let Some(element_types) = element_types else {
                return common_type.clone();
            };
            let field = ansi_string_numeric_field(field, &element_types, config);
            if matches!(common_type, DataType::List(_)) {
                DataType::List(field)
            } else {
                DataType::LargeList(field)
            }
        }
        DataType::Map(field, sorted) => {
            let DataType::Struct(entries) = field.data_type() else {
                return common_type.clone();
            };
            if entries.len() != 2 {
                return common_type.clone();
            }
            let value_types = data_types
                .iter()
                .map(|t| match t {
                    DataType::Null => Some(DataType::Null),
                    DataType::Map(field, _) => match field.data_type() {
                        DataType::Struct(entries) if entries.len() == 2 => {
                            Some(entries[1].data_type().clone())
                        }
                        _ => None,
                    },
                    _ => None,
                })
                .collect::<Option<Vec<_>>>();
            let Some(value_types) = value_types else {
                return common_type.clone();
            };
            // Spark does not allow map-key coercions that can introduce NULL.
            // Repair only values and keep the existing key type unchanged.
            // TODO: Reject incompatible numeric/STRING map keys in ANSI conditionals.
            let value = ansi_string_numeric_field(&entries[1], &value_types, config);
            DataType::Map(
                Arc::new(field.as_ref().clone().with_data_type(DataType::Struct(
                    vec![Arc::clone(&entries[0]), value].into(),
                ))),
                *sorted,
            )
        }
        DataType::Struct(fields) => {
            let compatible = data_types.iter().all(|t| match t {
                DataType::Null => true,
                DataType::Struct(source) if source.len() == fields.len() => {
                    source.iter().zip(fields).all(|(source, target)| {
                        if config.case_sensitive {
                            source.name() == target.name()
                        } else {
                            source.name().eq_ignore_ascii_case(target.name())
                        }
                    })
                }
                _ => false,
            });
            if !compatible {
                return common_type.clone();
            }
            DataType::Struct(
                fields
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        let field_types = data_types
                            .iter()
                            .map(|t| match t {
                                DataType::Struct(source) => source[index].data_type().clone(),
                                _ => DataType::Null,
                            })
                            .collect::<Vec<_>>();
                        ansi_string_numeric_field(field, &field_types, config)
                    })
                    .collect(),
            )
        }
        _ => common_type.clone(),
    }
}

fn ansi_string_numeric_field(
    field: &FieldRef,
    data_types: &[DataType],
    config: &PlanConfig,
) -> FieldRef {
    let data_type = ansi_string_numeric_type(data_types, field.data_type(), config);
    let nullable = field.is_nullable()
        || (is_numeric_type(&data_type) && data_types.iter().any(is_string_type));
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(data_type)
            .with_nullable(nullable),
    )
}

/// Returns Spark's wider type of two numeric (or NULL) types,
/// following `findWiderTypeForTwo` in `TypeCoercion` and `AnsiTypeCoercion`.
pub(crate) fn wider_numeric_type(
    left: &DataType,
    right: &DataType,
    ansi_mode: bool,
    retain_fraction_digits: bool,
) -> Option<DataType> {
    match (left, right) {
        (DataType::Null, other) | (other, DataType::Null) => {
            (other.is_null() || is_numeric_type(other)).then(|| other.clone())
        }
        (
            DataType::Decimal128(p1, s1) | DataType::Decimal256(p1, s1),
            DataType::Decimal128(p2, s2) | DataType::Decimal256(p2, s2),
        ) => Some(wider_decimal_type(
            (*p1, *s1),
            (*p2, *s2),
            retain_fraction_digits,
        )),
        (
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale),
            other,
        )
        | (
            other,
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale),
        ) => match other {
            DataType::Float32 | DataType::Float64 => Some(DataType::Float64),
            _ => Some(wider_decimal_type(
                (integral_decimal_precision(other)?, 0),
                (*precision, *scale),
                retain_fraction_digits,
            )),
        },
        _ => {
            let wider = if numeric_precedence(left)? >= numeric_precedence(right)? {
                left
            } else {
                right
            };
            // In ANSI mode, Spark widens integral and FLOAT values to DOUBLE
            // to avoid losing precision.
            if ansi_mode && left != right && wider == &DataType::Float32 {
                Some(DataType::Float64)
            } else {
                Some(wider.clone())
            }
        }
    }
}

/// Follows `DecimalPrecisionTypeCoercion.widerDecimalType` in Spark.
fn wider_decimal_type(
    (p1, s1): (u8, i8),
    (p2, s2): (u8, i8),
    retain_fraction_digits: bool,
) -> DataType {
    let scale = i16::from(s1.max(s2));
    let range = (i16::from(p1) - i16::from(s1)).max(i16::from(p2) - i16::from(s2));
    let precision = scale + range;
    let max_precision = i16::from(DECIMAL128_MAX_PRECISION);
    if retain_fraction_digits {
        DataType::Decimal128(
            precision.min(max_precision) as u8,
            scale.min(max_precision) as i8,
        )
    } else if precision <= max_precision {
        DataType::Decimal128(precision as u8, scale as i8)
    } else {
        let scale = (scale - (precision - max_precision)).max(0);
        DataType::Decimal128(DECIMAL128_MAX_PRECISION, scale as i8)
    }
}

fn is_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
    ) || numeric_precedence(data_type).is_some()
}

/// Follows `UpCastRule.numericPrecedence` in Spark.
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

/// Follows `DecimalType.forType` in Spark for integral types.
fn integral_decimal_precision(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Int8 => Some(3),
        DataType::Int16 => Some(5),
        DataType::Int32 => Some(10),
        DataType::Int64 => Some(20),
        _ => None,
    }
}

// TODO: Coerce DATE and TIMESTAMP branches to Spark's microsecond TIMESTAMP.
//  DataFusion's common type is Timestamp(Nanosecond, None), which cannot be returned.
fn coerce_string_temporal_values(
    arguments: Vec<expr::Expr>,
    data_types: Vec<DataType>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let has_string = data_types.iter().any(is_string_type);
    let temporal_type =
        common_temporal_type(&data_types, &function_context.plan_config.session_timezone);
    let arguments = if has_string {
        if let Some(temporal_type) = temporal_type {
            if function_context.plan_config.ansi_mode {
                arguments
                    .into_iter()
                    .zip(data_types.iter())
                    .map(|(arg, data_type)| coerce_to_temporal(arg, data_type, &temporal_type))
                    .collect::<PlanResult<Vec<_>>>()?
            } else {
                arguments
                    .into_iter()
                    .zip(data_types)
                    .map(|(arg, data_type)| {
                        if is_temporal_type(&data_type) {
                            ScalarUDF::from(SparkToUtf8::new()).call(vec![arg])
                        } else {
                            arg
                        }
                    })
                    .collect()
            }
        } else {
            arguments
        }
    } else {
        arguments
    };
    Ok(arguments)
}

fn coerce_to_temporal(
    arg: expr::Expr,
    data_type: &DataType,
    target_type: &DataType,
) -> PlanResult<expr::Expr> {
    if data_type == target_type {
        return Ok(arg);
    }
    if is_string_type(data_type) {
        match target_type {
            DataType::Date32 => Ok(ScalarUDF::from(SparkDate::new(false)).call(vec![arg])),
            // This is only reached when ANSI mode requires a temporal common type.
            DataType::Timestamp(_, timezone) => {
                Ok(
                    ScalarUDF::from(SparkTimestamp::try_new(timezone.clone(), true, false)?)
                        .call(vec![arg]),
                )
            }
            _ => Ok(cast(arg, target_type.clone())),
        }
    } else if is_temporal_type(data_type) {
        Ok(cast(arg, target_type.clone()))
    } else {
        Ok(arg)
    }
}

fn common_temporal_type(data_types: &[DataType], session_timezone: &Arc<str>) -> Option<DataType> {
    if data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, Some(_))))
    {
        Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::clone(session_timezone)),
        ))
    } else if data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, None)))
    {
        Some(DataType::Timestamp(TimeUnit::Microsecond, None))
    } else {
        data_types
            .iter()
            .any(is_date_type)
            .then_some(DataType::Date32)
    }
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_temporal_type(data_type: &DataType) -> bool {
    is_date_type(data_type) || matches!(data_type, DataType::Timestamp(_, _))
}

fn is_date_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Date64)
}

pub(super) fn list_built_in_conditional_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("coalesce", F::custom(coalesce)),
        ("if", F::custom(if_expr)),
        ("ifnull", F::binary(expr_fn::nvl)),
        ("nanvl", F::binary(expr_fn::nanvl)),
        ("nullif", F::binary(expr_fn::nullif)),
        ("nullifzero", F::custom(nullifzero)),
        ("nvl", F::binary(expr_fn::nvl)),
        ("nvl2", F::custom(nvl2)),
        ("zeroifnull", F::custom(zeroifnull)),
        ("when", F::custom(case)),
        ("case", F::custom(case)),
    ]
}

/// Create a zero literal with the same type as the input expression
fn create_zero_literal(data_type: &DataType) -> ScalarValue {
    match data_type {
        DataType::Int8 => ScalarValue::Int8(Some(0)),
        DataType::Int16 => ScalarValue::Int16(Some(0)),
        DataType::Int32 => ScalarValue::Int32(Some(0)),
        DataType::Int64 => ScalarValue::Int64(Some(0)),
        DataType::UInt8 => ScalarValue::UInt8(Some(0)),
        DataType::UInt16 => ScalarValue::UInt16(Some(0)),
        DataType::UInt32 => ScalarValue::UInt32(Some(0)),
        DataType::UInt64 => ScalarValue::UInt64(Some(0)),
        DataType::Float32 => ScalarValue::Float32(Some(0.0)),
        DataType::Float64 => ScalarValue::Float64(Some(0.0)),
        DataType::Decimal128(precision, scale) => {
            ScalarValue::Decimal128(Some(0), *precision, *scale)
        }
        DataType::Decimal256(precision, scale) => {
            ScalarValue::Decimal256(Some(0.into()), *precision, *scale)
        }
        // For non-numeric types, default to Int32
        _ => ScalarValue::Int32(Some(0)),
    }
}

/// Implementation of nullifzero function with type-aware casting
fn nullifzero(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;

    // Get the data type of the input argument
    let data_type = arg.to_field(function_context.schema)?.1.data_type().clone();

    // Create a zero literal with the same type as the input
    let zero_literal = lit(create_zero_literal(&data_type));

    // Return nullif(arg, zero_literal)
    Ok(expr_fn::nullif(arg, zero_literal))
}

/// Implementation of zeroifnull function with type-aware casting
fn zeroifnull(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;

    // Get the data type of the input argument
    let data_type = arg.to_field(function_context.schema)?.1.data_type().clone();

    // Create a zero literal with the same type as the input
    let zero_literal = lit(create_zero_literal(&data_type));

    // Return nvl(arg, zero_literal)
    Ok(expr_fn::nvl(arg, zero_literal))
}
