use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, Fields, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::type_coercion::binary::type_union_coercion;
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit};
use sail_common::spec::ARROW_DECIMAL128_MAX_PRECISION;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_to_string::SparkToUtf8;

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
    let branch_values = coerce_conditional_branches(branch_values, &function_context)?;
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
        coerce_conditional_branches(vec![then_expr, else_expr], &function_context)?.two()?;
    Ok(expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(Box::new(when_expr), Box::new(then_expr))],
        else_expr: Some(Box::new(else_expr)),
    }))
}

fn coalesce(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arguments = coerce_string_temporal_values(arguments, &function_context)?;
    Ok(expr_fn::coalesce(arguments))
}

/// Unifies the result branches of `CASE`/`IF`. The order matters: `type_union_coercion` answers
/// `Date32` for `(Utf8, Date32)`, the opposite of Spark's non-ANSI `stringPromotion`, so the
/// string/temporal pass has to run first.
fn coerce_conditional_branches(
    values: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let values = coerce_string_temporal_values(values, function_context)?;
    coerce_branch_values(values, function_context)
}

/// Casts the result branches to their common type, the way `CaseWhenCoercion` / `IfCoercion` do
/// (`TypeCoercionHelper.scala:498-538`). Without this the plan takes `Expr::Case::get_type`, which
/// reports the first non-null `THEN` branch and ignores the `ELSE`.
fn coerce_branch_values(
    values: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let data_types = values
        .iter()
        .map(|value| value.get_type(function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
    let Some(common_type) = common_branch_type(&data_types) else {
        return Ok(values);
    };
    Ok(values
        .into_iter()
        .map(|value| value.cast_to(&common_type, function_context.schema))
        .collect::<Result<Vec<_>, _>>()?)
}

/// The type Spark gives a conditional, folding the branches left to right as `findWiderCommonType`
/// does. `None` means no common type, and the caller then leaves the branches alone the way Spark's
/// `.getOrElse(c)` does. Spark's non-ANSI string partition is not reproduced.
///
/// A `Decimal256` anywhere in any branch switches the rules below off for the WHOLE conditional.
/// Spark has no such type, so they would only narrow it, and deciding per pair instead would let a
/// fold alternate between two rule sets and depend on branch order.
fn common_branch_type(data_types: &[DataType]) -> Option<DataType> {
    let (first, rest) = data_types.split_first()?;
    if data_types.iter().any(contains_decimal256_type) {
        return rest.iter().try_fold(first.clone(), |left, right| {
            type_union_coercion(&left, right)
        });
    }
    rest.iter().try_fold(first.clone(), |left, right| {
        branch_type_coercion(&left, right)
    })
}

/// The wider of two branch types, following the two arms of Spark's `findWiderTypeForDecimal`
/// (`TypeCoercionHelper.scala:186-198`) that DataFusion answers differently. Both also reach a
/// container's element type, since `findTypeForComplex` recurses into it. Everything else is left
/// to `type_union_coercion`.
fn branch_type_coercion(left: &DataType, right: &DataType) -> Option<DataType> {
    if (left.is_floating() && right.is_decimal()) || (left.is_decimal() && right.is_floating()) {
        return Some(DataType::Float64);
    }
    if let Some(data_type) = wider_decimal_type(left, right) {
        return Some(data_type);
    }
    let coerced = type_union_coercion(left, right)?;
    Some(coerce_nested_types(left, right, coerced))
}

/// Substitutes the element type of a container, keeping the shape `type_union_coercion` decided.
/// Matching on the COERCED type is deliberate: DataFusion can answer a different container variant
/// than either branch, and requiring all three to agree would skip the substitution exactly there.
fn coerce_nested_types(left: &DataType, right: &DataType, coerced: DataType) -> DataType {
    if let (Some(left_field), Some(right_field), Some(coerced_field)) = (
        container_element_field(left),
        container_element_field(right),
        container_element_field(&coerced),
    ) {
        let field = coerce_element_field(left_field, right_field, coerced_field);
        return with_element_field(&coerced, field);
    }
    if let (DataType::Struct(left), DataType::Struct(right), DataType::Struct(coerced_fields)) =
        (left, right, &coerced)
    {
        if let Some(fields) = coerce_struct_fields(left, right, coerced_fields) {
            return DataType::Struct(fields);
        }
    }
    coerced
}

/// The field a container holds its elements in: the element for a list, the entries struct for a map.
fn container_element_field(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => Some(field),
        _ => None,
    }
}

fn with_element_field(data_type: &DataType, field: Field) -> DataType {
    let field = Arc::new(field);
    match data_type {
        DataType::List(_) => DataType::List(field),
        DataType::LargeList(_) => DataType::LargeList(field),
        DataType::FixedSizeList(_, size) => DataType::FixedSizeList(field, *size),
        DataType::Map(_, sorted) => DataType::Map(field, *sorted),
        other => other.clone(),
    }
}

fn coerce_element_field(left: &Field, right: &Field, coerced: &Field) -> Field {
    match branch_type_coercion(left.data_type(), right.data_type()) {
        Some(data_type) => coerced.clone().with_data_type(data_type),
        None => coerced.clone(),
    }
}

/// `None` when the fields are not named the same way at the same positions. DataFusion pairs struct
/// fields by NAME, so pairing them positionally here would coerce fields that are not the same
/// column; Spark rejects those branches outright, so standing aside is the closer answer. The
/// comparison ignores case because Spark's resolver does under the default `caseSensitive = false`.
fn coerce_struct_fields(left: &Fields, right: &Fields, coerced: &Fields) -> Option<Fields> {
    if left.len() != right.len() || left.len() != coerced.len() {
        return None;
    }
    if left
        .iter()
        .zip(right.iter())
        .zip(coerced.iter())
        .any(|((left, right), coerced)| {
            !left.name().eq_ignore_ascii_case(right.name())
                || !left.name().eq_ignore_ascii_case(coerced.name())
        })
    {
        return None;
    }
    Some(Fields::from(
        left.iter()
            .zip(right.iter())
            .zip(coerced.iter())
            .map(|((left, right), coerced)| coerce_element_field(left, right, coerced))
            .collect::<Vec<_>>(),
    ))
}

/// Spark's `widerDecimalType` (`DecimalPrecisionTypeCoercion.scala:195-199`), applied only past 38
/// digits — below that DataFusion already agrees. Assumes the non-legacy `boundedPreferIntegralDigits`,
/// which is the default (`spark.sql.legacy.decimal.retainFractionDigitsOnTruncate = false`).
fn wider_decimal_type(left: &DataType, right: &DataType) -> Option<DataType> {
    let (left_precision, left_scale) = decimal_type_for(left)?;
    let (right_precision, right_scale) = decimal_type_for(right)?;
    let scale = left_scale.max(right_scale);
    let range = (left_precision - left_scale).max(right_precision - right_scale);
    let max_precision = i32::from(ARROW_DECIMAL128_MAX_PRECISION);
    if range + scale <= max_precision {
        return None;
    }
    // Spark keeps the integral digits and gives what is left to the scale, so the result is
    // `38 - range`. `range` is at least 1 here, so this is in `[0, 37]`.
    let scale = i8::try_from((max_precision - range).max(0)).unwrap_or(0);
    Some(DataType::Decimal128(ARROW_DECIMAL128_MAX_PRECISION, scale))
}

/// The decimal Spark uses for a type: decimals as they are, integrals via `DecimalType.forType`
/// (`DecimalType.scala:133-140`). `Decimal256` answers `None`, holding `common_branch_type`'s line.
fn decimal_type_for(data_type: &DataType) -> Option<(i32, i32)> {
    match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale) => {
            Some((i32::from(*precision), i32::from(*scale)))
        }
        DataType::Int8 | DataType::UInt8 => Some((3, 0)),
        DataType::Int16 | DataType::UInt16 => Some((5, 0)),
        DataType::Int32 | DataType::UInt32 => Some((10, 0)),
        DataType::Int64 | DataType::UInt64 => Some((20, 0)),
        _ => None,
    }
}

fn contains_decimal256_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Decimal256(_, _) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => contains_decimal256_type(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_decimal256_type(field.data_type())),
        _ => false,
    }
}

fn coerce_string_temporal_values(
    arguments: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let data_types = arguments
        .iter()
        .map(|arg| arg.get_type(function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
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
        ("nvl2", F::ternary(expr_fn::nvl2)),
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
