use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::type_coercion::binary::type_union_coercion;
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit};
use sail_common::spec::ARROW_DECIMAL128_MAX_PRECISION;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_struct_rename::SparkStructRename;
use sail_function::scalar::spark_to_string::SparkToUtf8;

use crate::config::PlanConfig;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};
use crate::resolver::expression::cast::{build_rename_target_type, needs_struct_field_rename};

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
    let common_type =
        common_branch_type(&data_types, function_context.plan_config).ok_or_else(|| {
            PlanError::analysis(format!(
                "[DATATYPE_MISMATCH.DATA_DIFF_TYPES] input to the conditional should all be the \
                 same type, but got {data_types:?}"
            ))
        })?;
    values
        .into_iter()
        .zip(data_types)
        .map(|(value, data_type)| {
            // DataFusion casts struct fields by name and case-sensitively, so a branch whose field
            // names only differ in case (or in order) needs Sail's positional rename adapter first.
            let value = if needs_struct_field_rename(&data_type, &common_type) {
                let target = build_rename_target_type(&data_type, &common_type);
                ScalarUDF::from(SparkStructRename::new(target)).call(vec![value])
            } else {
                value
            };
            Ok(value.cast_to(&common_type, function_context.schema)?)
        })
        .collect()
}

/// The type Spark gives a conditional, folding the branches as `findWiderCommonType` does.
/// `None` means the branches have no common type, which the caller turns into the analysis error
/// Spark raises from `checkInputDataTypes` (`conditionalExpressions.scala:220-227`).
///
/// A `Decimal256` anywhere in any branch switches the rules below off for the WHOLE conditional.
/// Spark has no such type, so they would only narrow it, and deciding per pair instead would let a
/// fold alternate between two rule sets and depend on branch order.
fn common_branch_type(data_types: &[DataType], config: &PlanConfig) -> Option<DataType> {
    if data_types.iter().any(contains_decimal256_type) {
        let (first, rest) = data_types.split_first()?;
        return rest.iter().try_fold(first.clone(), |left, right| {
            type_union_coercion(&left, right)
        });
    }
    // Outside ANSI, `findWiderTypeForTwo` is not associative for strings, so Spark folds the
    // string-typed branches first (`TypeCoercion.scala:175-186`).
    let ordered: Vec<&DataType> = if config.ansi_mode {
        data_types.iter().collect()
    } else {
        let (strings, others): (Vec<_>, Vec<_>) =
            data_types.iter().partition(|t| contains_string_type(t));
        strings.into_iter().chain(others).collect()
    };
    let (first, rest) = ordered.split_first()?;
    rest.iter().try_fold((*first).clone(), |left, right| {
        branch_type_coercion(&left, right, config)
    })
}

/// Spark's `findWiderTypeForTwo` (`TypeCoercionHelper.scala:168-198`), which differs from
/// `type_union_coercion` in both directions: it widens pairs DataFusion narrows, and it rejects
/// pairs DataFusion happily unifies.
fn branch_type_coercion(
    left: &DataType,
    right: &DataType,
    config: &PlanConfig,
) -> Option<DataType> {
    if left == right {
        return Some(left.clone());
    }
    match (left, right) {
        (DataType::Null, other) | (other, DataType::Null) => return Some(other.clone()),
        (DataType::List(left), DataType::List(right)) => {
            return Some(DataType::List(Arc::new(coerce_element_field(
                left, right, config,
            )?)));
        }
        (DataType::LargeList(left), DataType::LargeList(right)) => {
            return Some(DataType::LargeList(Arc::new(coerce_element_field(
                left, right, config,
            )?)));
        }
        (DataType::FixedSizeList(left, size), DataType::FixedSizeList(right, other_size))
            if size == other_size =>
        {
            return Some(DataType::FixedSizeList(
                Arc::new(coerce_element_field(left, right, config)?),
                *size,
            ));
        }
        (DataType::Map(left, sorted), DataType::Map(right, _)) => {
            return coerce_map_type(left, right, *sorted, config);
        }
        (DataType::Struct(left), DataType::Struct(right)) => {
            return coerce_struct_fields(left, right, config).map(DataType::Struct);
        }
        // Spark has no rule pairing a complex type with anything else.
        _ if is_complex_type(left) || is_complex_type(right) => return None,
        _ => {}
    }
    if is_string_type(left) || is_string_type(right) {
        return string_branch_type(left, right, config.ansi_mode);
    }
    // `findWiderDateTimeType` rejects every pairing that involves TIME, and Spark has no arm
    // pairing a datetime with a non-datetime.
    if is_time_type(left) || is_time_type(right) {
        return None;
    }
    if is_temporal_type(left) || is_temporal_type(right) {
        return (is_temporal_type(left) && is_temporal_type(right))
            .then(|| wider_temporal_type(left, right, &config.session_timezone))
            .flatten();
    }
    if left.is_numeric() && right.is_numeric() {
        // ANSI returns DOUBLE rather than FLOAT for integral-meets-float, to avoid the precision
        // loss of routing an integral through float32 (`AnsiTypeCoercion.scala:113-124`).
        if config.ansi_mode
            && ((is_integral_type(left) && matches!(right, DataType::Float32))
                || (matches!(left, DataType::Float32) && is_integral_type(right)))
        {
            return Some(DataType::Float64);
        }
        if (left.is_floating() && right.is_decimal()) || (left.is_decimal() && right.is_floating())
        {
            return Some(DataType::Float64);
        }
        return wider_decimal_type(left, right).or_else(|| type_union_coercion(left, right));
    }
    (is_binary_type(left) && is_binary_type(right))
        .then(|| type_union_coercion(left, right))
        .flatten()
}

/// `stringPromotion` outside ANSI promotes the other side to the string; ANSI promotes the string
/// to the other side instead (`AnsiStringPromotionTypeCoercion.scala:92-108`).
fn string_branch_type(left: &DataType, right: &DataType, ansi_mode: bool) -> Option<DataType> {
    if is_string_type(left) && is_string_type(right) {
        return type_union_coercion(left, right);
    }
    let (string_type, other) = if is_string_type(left) {
        (left, right)
    } else {
        (right, left)
    };
    if !ansi_mode {
        return (is_spark_atomic_type(other)
            && !matches!(other, DataType::Boolean)
            && !is_binary_type(other))
        .then(|| string_type.clone());
    }
    if is_integral_type(other) {
        return Some(DataType::Int64);
    }
    if other.is_floating() || other.is_decimal() {
        return Some(DataType::Float64);
    }
    is_spark_atomic_type(other).then(|| other.clone())
}

/// `findWiderDateTimeType` (`TypeCoercionHelper.scala:243-255`): a zoned timestamp wins over an
/// unzoned one, and both win over a date.
fn wider_temporal_type(
    left: &DataType,
    right: &DataType,
    session_timezone: &Arc<str>,
) -> Option<DataType> {
    if matches!(left, DataType::Timestamp(_, Some(_)))
        || matches!(right, DataType::Timestamp(_, Some(_)))
    {
        return Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::clone(session_timezone)),
        ));
    }
    if matches!(left, DataType::Timestamp(_, None)) || matches!(right, DataType::Timestamp(_, None))
    {
        return Some(DataType::Timestamp(TimeUnit::Microsecond, None));
    }
    (is_date_type(left) && is_date_type(right)).then_some(DataType::Date32)
}

/// Spark rejects a map merge whose KEY cast could produce nulls, because a null map key is illegal
/// (`TypeCoercionHelper.scala:149-153`).
fn coerce_map_type(
    left: &Field,
    right: &Field,
    sorted: bool,
    config: &PlanConfig,
) -> Option<DataType> {
    let (DataType::Struct(left_fields), DataType::Struct(right_fields)) =
        (left.data_type(), right.data_type())
    else {
        return None;
    };
    if left_fields.len() != 2 || right_fields.len() != 2 {
        return None;
    }
    let (left_key, right_key) = (left_fields[0].as_ref(), right_fields[0].as_ref());
    let key_type = branch_type_coercion(left_key.data_type(), right_key.data_type(), config)?;
    if cast_forces_null(left_key.data_type(), &key_type)
        || cast_forces_null(right_key.data_type(), &key_type)
    {
        return None;
    }
    let key = left_key.clone().with_data_type(key_type);
    let value = coerce_element_field(left_fields[1].as_ref(), right_fields[1].as_ref(), config)?;
    let entries = left
        .clone()
        .with_data_type(DataType::Struct(Fields::from(vec![key, value])));
    Some(DataType::Map(Arc::new(entries), sorted))
}

/// Spark ORs the two source nullabilities with whether either inserted cast can produce a null
/// (`findTypeForComplex`, `TypeCoercionHelper.scala:141-176`).
fn coerce_element_field(left: &Field, right: &Field, config: &PlanConfig) -> Option<Field> {
    let data_type = branch_type_coercion(left.data_type(), right.data_type(), config)?;
    let nullable = left.is_nullable()
        || right.is_nullable()
        || cast_forces_null(left.data_type(), &data_type)
        || cast_forces_null(right.data_type(), &data_type);
    Some(
        left.clone()
            .with_data_type(data_type)
            .with_nullable(nullable),
    )
}

/// `None` when the fields do not line up by name at every position, which is what Spark requires
/// (`resolver(field1.name, field2.name)`); the resolver is case-insensitive unless the session says
/// otherwise.
fn coerce_struct_fields(left: &Fields, right: &Fields, config: &PlanConfig) -> Option<Fields> {
    if left.len() != right.len() {
        return None;
    }
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| {
            field_names_match(left.name(), right.name(), config.case_sensitive)
                .then(|| coerce_element_field(left, right, config))
                .flatten()
        })
        .collect::<Option<Vec<_>>>()
        .map(Fields::from)
}

fn field_names_match(left: &str, right: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        left == right
    } else {
        left.eq_ignore_ascii_case(right)
    }
}

/// `Cast.forceNullable` (`Cast.scala:427-448`), restricted to the conversions a conditional can
/// produce: parsing a string, narrowing to a decimal that cannot hold the source range, and
/// truncating a fractional value to an integral one.
fn cast_forces_null(from: &DataType, to: &DataType) -> bool {
    if from == to || matches!(from, DataType::Null) {
        return false;
    }
    if is_string_type(from) {
        return !is_string_type(to) && !is_binary_type(to);
    }
    if to.is_decimal() {
        return !decimal_cast_is_null_safe(from, to);
    }
    (from.is_floating() || from.is_decimal()) && is_integral_type(to)
}

/// `canNullSafeCastToDecimal` (`Cast.scala:413-420`): the target must have strictly more integral
/// digits, or at least as many along with at least as much scale.
fn decimal_cast_is_null_safe(from: &DataType, to: &DataType) -> bool {
    let (Some((to_precision, to_scale)), Some((from_precision, from_scale))) =
        (decimal_type_for(to), decimal_type_for(from))
    else {
        return false;
    };
    let (to_range, from_range) = (to_precision - to_scale, from_precision - from_scale);
    to_range > from_range || (to_range >= from_range && to_scale >= from_scale)
}

fn contains_string_type(data_type: &DataType) -> bool {
    match data_type {
        _ if is_string_type(data_type) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => contains_string_type(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_string_type(field.data_type())),
        _ => false,
    }
}

fn is_complex_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
            | DataType::Struct(_)
    )
}

fn is_integral_type(data_type: &DataType) -> bool {
    data_type.is_integer()
}

fn is_binary_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}

fn is_time_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Time32(_) | DataType::Time64(_))
}

fn is_spark_atomic_type(data_type: &DataType) -> bool {
    data_type.is_numeric()
        || is_string_type(data_type)
        || is_binary_type(data_type)
        || is_temporal_type(data_type)
        || is_time_type(data_type)
        || matches!(
            data_type,
            DataType::Boolean | DataType::Duration(_) | DataType::Interval(_)
        )
}

/// Spark's `widerDecimalType` (`DecimalPrecisionTypeCoercion.scala:195-199`), applied only past 38
/// digits — below that DataFusion already agrees. Assumes the non-legacy
/// `boundedPreferIntegralDigits`, which is the default.
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
