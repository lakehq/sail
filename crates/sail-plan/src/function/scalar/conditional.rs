use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion::functions::regex::regexpcount::RegexpCountFunc;
use datafusion::functions::regex::regexpinstr::RegexpInstrFunc;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::type_coercion::binary::type_union_coercion;
use datafusion_expr::{ExprSchemable, ScalarUDF, ValueOrLambda, cast, expr, lit};
use sail_common_datafusion::conditional_type_hint::SparkConditionalTypeHint;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_to_string::SparkToUtf8;

use super::lambda::conditional_lambda_parameter_observations;
use crate::error::PlanResult;
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};
use crate::resolver::ConditionalTypeContext;
use crate::resolver::conditional::{ConditionalTypeCache, conditional_column_type};

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
    let branch_values = coerce_string_temporal_values(branch_values, &function_context)?;
    let when_then_expr = conditions
        .into_iter()
        .zip(branch_values)
        .map(|(condition, value)| (Box::new(condition), Box::new(value)))
        .collect();
    resolve_numeric_conditional(
        expr::Case {
            expr: None, // Expr::Case in from_ast_expression incorporates into when_then_expr
            when_then_expr,
            else_expr: None,
        },
        &function_context,
    )
}

fn if_expr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (when_expr, then_expr, else_expr) = arguments.three()?;
    let (then_expr, else_expr) =
        coerce_string_temporal_values(vec![then_expr, else_expr], &function_context)?.two()?;
    resolve_numeric_conditional(
        expr::Case {
            expr: None,
            when_then_expr: vec![(Box::new(when_expr), Box::new(then_expr))],
            else_expr: Some(Box::new(else_expr)),
        },
        &function_context,
    )
}

fn coalesce(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arguments = coerce_string_temporal_values(arguments, &function_context)?;
    Ok(expr_fn::coalesce(arguments))
}

fn resolve_numeric_conditional(
    case: expr::Case,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<expr::Expr> {
    let data_types = case
        .when_then_expr
        .iter()
        .map(|(_, value)| value)
        .chain(case.else_expr.iter())
        .map(|value| value.get_type(function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
    if !data_types
        .iter()
        .all(|data_type| data_type.is_numeric() || data_type == &DataType::Null)
    {
        // TODO: Match Spark's ANSI numeric/string and recursive complex coercion.
        // Preserve the existing analyzer handling of these branches until then.
        return Ok(expr::Expr::Case(case));
    }
    let common_type = data_types.iter().try_fold(DataType::Null, |left, right| {
        conditional_common_type(&left, right, function_context.plan_config.ansi_mode)
    });
    let Some(common_type) = common_type else {
        return Ok(expr::Expr::Case(case));
    };
    let first_type = data_types.iter().find(|data_type| !data_type.is_null());
    if first_type.is_none() {
        return Ok(expr::Expr::Case(case));
    }
    if matches!(
        common_type,
        DataType::Decimal128(..) | DataType::Decimal256(..)
    ) && !matches!(
        first_type,
        Some(DataType::Decimal128(..) | DataType::Decimal256(..))
    ) {
        // TODO: Resolve deferred branch types before exposing a decimal type.
        // Division selects native decimal arithmetic for Decimal128/256. A
        // nested or projected decimal/floating branch can still resolve later,
        // so changing the first non-null branch's numeric family here can turn
        // previously correct DOUBLE division into decimal rounding or overflow.
        return Ok(expr::Expr::Case(case));
    }
    let analyzer_type = data_types.iter().try_fold(DataType::Null, |left, right| {
        type_union_coercion(&left, right)
    });
    if analyzer_type.as_ref() != Some(&common_type) {
        // TODO: Match Spark's ANSI integral/FLOAT and decimal/floating promotion,
        // and precision-38 scale reduction with HALF_UP rounding. These require
        // coercion after all branch types resolve: eager casts change numeric
        // siblings when a projected branch later resolves to STRING or DECIMAL.
        return Ok(expr::Expr::Case(case));
    }

    // Keep the original CASE type throughout value planning. The identity marks
    // only user conditionals for observation; generated CASE expressions in other
    // functions must not acquire conditional metadata coercion.
    Ok(ScalarUDF::from(SparkConditionalTypeHint::new()).call(vec![
        expr::Expr::Case(case),
        lit(ScalarValue::Null),
        // Retain the initial type for conservative observation fallbacks after
        // child publication schemas change. Execution ignores this argument.
        lit(ScalarValue::try_new_null(
            first_type.unwrap_or(&DataType::Null),
        )?),
    ]))
}

/// Type information used only while observing a conditional, never for execution.
/// Arrow and Spark views are retained because a native parent must not inherit Spark-only
/// narrowing from a signed child (for example UINT8 + a regex conditional).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ConditionalTypeObservation {
    pub arrow: DataType,
    pub spark: DataType,
    /// Hypothetical type used only to decide whether to retain the initial INT type.
    pub spark_without_opaque_casts: DataType,
    pub contains_native: bool,
}

pub(crate) fn conditional_type_observation(
    expression: &expr::Expr,
    schema: &DFSchemaRef,
    ansi: bool,
    context: &ConditionalTypeContext,
) -> PlanResult<ConditionalTypeObservation> {
    conditional_type_observation_with_cache(
        expression,
        schema,
        ansi,
        context,
        &mut ConditionalTypeCache::new(),
    )
}

pub(crate) fn conditional_type_observation_with_cache(
    expression: &expr::Expr,
    schema: &DFSchemaRef,
    ansi: bool,
    context: &ConditionalTypeContext,
    cache: &mut ConditionalTypeCache,
) -> PlanResult<ConditionalTypeObservation> {
    let views = conditional_views(expression.clone(), schema, ansi, context, true, cache)?;
    Ok(ConditionalTypeObservation {
        arrow: views.arrow.get_type(schema)?,
        spark: views.spark.get_type(schema)?,
        spark_without_opaque_casts: views.spark_without_opaque_casts.get_type(schema)?,
        contains_native: views.contains_native,
    })
}

/// Produces a temporary view for type observations, never an execution expression.
/// Standalone producers keep their existing types. Their Spark types matter only
/// when they contribute to a marked CASE/IF (including through a column or lambda).
pub(crate) fn conditional_type_view(
    expression: expr::Expr,
    schema: &DFSchemaRef,
    ansi: bool,
    context: &ConditionalTypeContext,
) -> PlanResult<expr::Expr> {
    Ok(conditional_views(
        expression,
        schema,
        ansi,
        context,
        false,
        &mut ConditionalTypeCache::new(),
    )?
    .spark)
}

struct ConditionalViews {
    arrow: expr::Expr,
    spark: expr::Expr,
    spark_without_opaque_casts: expr::Expr,
    contains_native: bool,
}

fn observation_cast(expression: expr::Expr, original: &DataType, target: DataType) -> expr::Expr {
    if original == &target {
        expression
    } else {
        cast(expression, target)
    }
}

fn conditional_views(
    expression: expr::Expr,
    schema: &DFSchemaRef,
    ansi: bool,
    context: &ConditionalTypeContext,
    contributing: bool,
    cache: &mut ConditionalTypeCache,
) -> datafusion_common::Result<ConditionalViews> {
    use expr::Expr;
    let original_type = expression.get_type(schema)?;
    let unchanged = |expression: Expr| ConditionalViews {
        arrow: expression.clone(),
        spark: expression.clone(),
        spark_without_opaque_casts: expression,
        contains_native: contains_native_type(&original_type),
    };
    // A declared target is a semantic boundary, including casts of arrays/maps.
    if matches!(expression, Expr::Cast(_) | Expr::TryCast(_)) {
        let opaque = contributing
            && original_type == DataType::Int64
            && expression.exists(|node| {
                Ok(matches!(node, Expr::BinaryExpr(binary)
                if binary.op == datafusion_expr::Operator::BitwiseShiftRight))
            })?
            && expression.exists(|node| match node {
                Expr::ScalarFunction(function) => Ok(function.func.inner().is::<RegexpCountFunc>()
                    || function.func.inner().is::<RegexpInstrFunc>()),
                Expr::Column(_)
                | Expr::OuterReferenceColumn(..)
                | Expr::LambdaVariable(_)
                | Expr::ScalarSubquery(_)
                    if node.get_type(schema)? == DataType::Int64 =>
                {
                    let observation =
                        conditional_type_observation_with_cache(node, schema, ansi, context, cache)
                            .map_err(|error| {
                                datafusion_common::DataFusionError::External(Box::new(error))
                            })?;
                    Ok(observation.spark == DataType::Int32)
                }
                _ => Ok(false),
            })?;
        let mut views = unchanged(expression);
        if opaque {
            // TODO: Preserve the distinction between generated shift casts and
            // explicit SQL casts before observing regex-dependent shifted casts.
            // This hypothetical type is used only to retain a previously exposed
            // INT conditional; it never replaces the cast's declared target.
            views.spark_without_opaque_casts = cast(views.spark.clone(), DataType::Int32);
        }
        return Ok(views);
    }
    if contributing {
        let observed = match &expression {
            Expr::Column(column) => conditional_column_type(column, context, ansi, cache),
            Expr::OuterReferenceColumn(_, column) => match &context.outer {
                Some(outer) => conditional_column_type(column, outer, ansi, cache),
                None => Ok(None),
            },
            Expr::ScalarSubquery(subquery) => {
                let schema = subquery.subquery.schema();
                if schema.fields().len() == 1 {
                    let (qualifier, field) = schema.qualified_field(0);
                    let column = datafusion_common::Column::new(qualifier.cloned(), field.name());
                    let context = ConditionalTypeContext {
                        inputs: vec![Arc::clone(&subquery.subquery)],
                        outer: Some(Arc::new(context.clone())),
                        ..Default::default()
                    };
                    conditional_column_type(&column, &context, ansi, cache)
                } else {
                    Ok(None)
                }
            }
            Expr::LambdaVariable(variable) => Ok(context
                .lambda_parameters
                .iter()
                .rev()
                .flat_map(|frame| frame.iter())
                .find(|(name, _)| name.eq_ignore_ascii_case(&variable.name))
                .and_then(|(_, observation)| observation.clone())),
            _ => Ok(None),
        }
        .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?;
        if let Some(observed) = observed {
            return Ok(ConditionalViews {
                // Retain the declared Arrow type at a field boundary. Producer
                // lookup may correct the signed observation, but must not change
                // the previously visible width of a native parent.
                arrow: expression.clone(),
                spark: observation_cast(expression.clone(), &original_type, observed.spark),
                spark_without_opaque_casts: observation_cast(
                    expression,
                    &original_type,
                    observed.spark_without_opaque_casts,
                ),
                contains_native: observed.contains_native,
            });
        }
        if let Expr::ScalarFunction(function) = &expression
            && (function.func.inner().is::<RegexpCountFunc>()
                || function.func.inner().is::<RegexpInstrFunc>())
        {
            // Spark RegExpCount (Size) and RegExpInStr return IntegerType.
            // Retain their Arrow execution/standalone types, including UInt parents.
            return Ok(ConditionalViews {
                arrow: expression.clone(),
                spark: observation_cast(expression.clone(), &original_type, DataType::Int32),
                spark_without_opaque_casts: observation_cast(
                    expression,
                    &original_type,
                    DataType::Int32,
                ),
                contains_native: false,
            });
        }
    }
    if let Expr::ScalarFunction(function) = &expression
        && function.func.inner().is::<SparkConditionalTypeHint>()
        && let Some(Expr::Case(case)) = function.args.first()
    {
        let branches = case
            .when_then_expr
            .iter()
            .map(|(_, value)| value)
            .chain(case.else_expr.iter())
            .map(|value| {
                conditional_type_observation_with_cache(value, schema, ansi, context, cache)
            })
            .collect::<PlanResult<Vec<_>>>()
            .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?;
        // Predicates do not contribute to the result type.
        let contains_native = branches.iter().any(|branch| branch.contains_native);
        let arrow_types: Vec<_> = branches.iter().map(|branch| &branch.arrow).collect();
        let spark_types: Vec<_> = branches.iter().map(|branch| &branch.spark).collect();
        let arrow =
            observed_common_type(&arrow_types, ansi).unwrap_or_else(|| original_type.clone());
        let without_opaque_casts: Vec<_> = branches
            .iter()
            .map(|branch| &branch.spark_without_opaque_casts)
            .collect();
        let mut spark = if contains_native {
            arrow.clone()
        } else {
            observed_common_type(&spark_types, ansi).unwrap_or_else(|| original_type.clone())
        };
        if !contains_native
            && function
                .args
                .get(2)
                .is_some_and(|hint| matches!(hint, Expr::Literal(ScalarValue::Int32(None), _)))
            && spark == DataType::Int64
            && observed_common_type(&without_opaque_casts, ansi) == Some(DataType::Int32)
        {
            // Preserve baseline when an opaque shifted cast is the only
            // reason to widen. Independent BIGINT branches still require
            // BIGINT outside the opaque cast; native parents continue
            // to use the Arrow view. Cast contents remain deferred.
            spark = DataType::Int32;
        }
        let spark = observation_cast(expression.clone(), &original_type, spark);
        return Ok(ConditionalViews {
            arrow: observation_cast(expression, &original_type, arrow),
            spark_without_opaque_casts: spark.clone(),
            spark,
            contains_native,
        });
    }
    // Revisited lambda bodies need the same observation bindings as their
    // original resolution. Keep the actual UDF's reordered parameter contract
    // and derive these frames separately from its unchanged execution fields.
    let mut argument_views = Vec::new();
    let lambda_observations = if let Expr::HigherOrderFunction(function) = &expression {
        (|| -> PlanResult<_> {
            let fields = function
                .args
                .iter()
                .map(|argument| {
                    Ok(match argument {
                        Expr::Lambda(_) => ValueOrLambda::Lambda(None),
                        _ => ValueOrLambda::Value(argument.to_field(schema)?.1),
                    })
                })
                .collect::<PlanResult<Vec<_>>>()?;
            let observations = function
                .args
                .iter()
                .map(|argument| match argument {
                    Expr::Lambda(_) => {
                        argument_views.push(None);
                        Ok(None)
                    }
                    _ => {
                        let views = conditional_views(
                            argument.clone(),
                            schema,
                            ansi,
                            context,
                            true,
                            cache,
                        )?;
                        let observation = ConditionalTypeObservation {
                            arrow: views.arrow.get_type(schema)?,
                            spark: views.spark.get_type(schema)?,
                            spark_without_opaque_casts: views
                                .spark_without_opaque_casts
                                .get_type(schema)?,
                            contains_native: views.contains_native,
                        };
                        argument_views.push(Some(views));
                        Ok(Some(observation))
                    }
                })
                .collect::<PlanResult<Vec<_>>>()?;
            conditional_lambda_parameter_observations(
                function.func.as_ref(),
                &fields,
                &observations,
            )
        })()
        .ok()
    } else {
        None
    };
    let mut lambda_observations = lambda_observations.into_iter().flatten();
    let mut argument_views = argument_views.into_iter();
    let mut spark_children = Vec::new();
    let mut without_opaque_casts_children = Vec::new();
    let mut contains_native = contains_native_type(&original_type);
    let arrow = expression
        .clone()
        .map_children(|child| {
            let argument_view = argument_views.next().flatten();
            let mut lambda_context;
            let child_context = if let Expr::Lambda(lambda) = &child {
                let observations = lambda_observations.next();
                lambda_context = context.clone();
                lambda_context.lambda_parameters.push(
                    lambda
                        .params
                        .iter()
                        .enumerate()
                        .map(|(index, name)| {
                            (
                                name.clone(),
                                observations
                                    .as_ref()
                                    .and_then(|params| params.get(index))
                                    // Native ancestry recovered on this revisit
                                    // must not reinterpret the lambda's declared
                                    // field. None preserves that field boundary
                                    // while still shadowing an outer binding.
                                    .filter(|observation| !observation.contains_native)
                                    .cloned(),
                            )
                        })
                        .collect(),
                );
                &lambda_context
            } else {
                context
            };
            // A bare lambda result retains its declared type. Only a result
            // contributing to a marked conditional may use its Spark view.
            let views = if contributing && let Some(views) = argument_view {
                // The parameter frame already observed this value argument.
                // Reuse that traversal so directly nested HOFs do not duplicate
                // all descendant observation work at every level.
                views
            } else {
                conditional_views(child, schema, ansi, child_context, contributing, cache)?
            };
            contains_native |= views.contains_native;
            spark_children.push(views.spark);
            without_opaque_casts_children.push(views.spark_without_opaque_casts);
            Ok(Transformed::yes(views.arrow))
        })?
        .data;
    let mut spark_children = spark_children.into_iter();
    let spark = expression
        .clone()
        .map_children(|child| Ok(Transformed::yes(spark_children.next().unwrap_or(child))))?
        .data;
    let mut without_opaque_casts_children = without_opaque_casts_children.into_iter();
    let spark_without_opaque_casts = expression
        .map_children(|child| {
            Ok(Transformed::yes(
                without_opaque_casts_children.next().unwrap_or(child),
            ))
        })?
        .data;
    Ok(ConditionalViews {
        arrow,
        spark,
        spark_without_opaque_casts,
        contains_native,
    })
}

fn observed_common_type(types: &[&DataType], ansi: bool) -> Option<DataType> {
    if !types.iter().all(|t| t.is_numeric() || t.is_null()) {
        // TODO: Resolve ANSI string and recursive complex conditional types.
        return None;
    }
    let common = types.iter().try_fold(DataType::Null, |left, right| {
        conditional_common_type(&left, right, ansi)
    })?;
    let analyzer = types.iter().try_fold(DataType::Null, |left, right| {
        type_union_coercion(&left, right)
    });
    if analyzer.as_ref() != Some(&common) {
        // TODO: Match deferred decimal/floating and ANSI integral/FLOAT promotion,
        // and precision-38 scale reduction, before changing observations.
        return None;
    }
    if matches!(common, DataType::Decimal128(..) | DataType::Decimal256(..))
        && !matches!(
            types.iter().find(|t| !t.is_null()),
            Some(DataType::Decimal128(..) | DataType::Decimal256(..))
        )
    {
        // TODO: Resolve integral-first decimal cases consistently with value consumers.
        return None;
    }
    Some(common)
}

pub(crate) fn contains_native_type(data_type: &DataType) -> bool {
    use DataType::*;
    match data_type {
        UInt8 | UInt16 | UInt32 | UInt64 | Float16 | Decimal32(..) | Decimal64(..)
        | Decimal256(..) => true,
        List(field)
        | LargeList(field)
        | FixedSizeList(field, _)
        | ListView(field)
        | LargeListView(field)
        | Map(field, _) => contains_native_type(field.data_type()),
        Struct(fields) => fields
            .iter()
            .any(|field| contains_native_type(field.data_type())),
        Dictionary(key, value) => contains_native_type(key) || contains_native_type(value),
        _ => false,
    }
}

fn conditional_common_type(left: &DataType, right: &DataType, ansi: bool) -> Option<DataType> {
    use DataType::*;

    match (left, right) {
        (left, right) if left == right => Some(left.clone()),
        (Null, other) | (other, Null) => Some(other.clone()),
        (Decimal128(..), Float32 | Float64) | (Float32 | Float64, Decimal128(..)) => Some(Float64),
        (Float32, Int8 | Int16 | Int32 | Int64) | (Int8 | Int16 | Int32 | Int64, Float32)
            if ansi =>
        {
            Some(Float64)
        }
        (Decimal128(..), _) | (_, Decimal128(..)) if left.is_numeric() && right.is_numeric() => {
            let decimal = |data_type: &DataType| match data_type {
                Int8 => Some((3, 0)),
                Int16 => Some((5, 0)),
                Int32 => Some((10, 0)),
                Int64 => Some((20, 0)),
                Decimal128(p, s) => Some((i16::from(*p), i16::from(*s))),
                _ => None,
            };
            match (decimal(left), decimal(right)) {
                (Some((p1, s1)), Some((p2, s2))) => {
                    // Spark's widerDecimalType and boundedPreferIntegralDigits.
                    let scale = s1.max(s2);
                    let precision = (p1 - s1).max(p2 - s2) + scale;
                    let scale = if precision > 38 {
                        (scale - (precision - 38)).max(0)
                    } else {
                        scale
                    };
                    Some(Decimal128(precision.min(38) as u8, scale as i8))
                }
                _ => type_union_coercion(left, right),
            }
        }
        // The analyzer's numeric union also handles Sail's unsigned integers,
        // Float16, and Decimal32/64/256 without changing their native widths.
        _ => type_union_coercion(left, right),
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
