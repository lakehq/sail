use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DataType, Field};
use datafusion::functions::expr_fn::{coalesce, isnan};
use datafusion::functions_aggregate::{
    approx_distinct, array_agg, average, bit_and_or_xor, bool_and_or, correlation, count,
    covariance, first_last, grouping, min_max, percentile_cont, stddev, sum, variance,
};
use datafusion::functions_nested::string::array_to_string;
use datafusion_common::utils::expr::COUNT_STAR_EXPANSION;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::expr::{
    AggregateFunction, AggregateFunctionParams, HigherOrderFunction, Lambda, LambdaVariable,
};
use datafusion_expr::{
    AggregateUDF, BinaryExpr, ExprSchemable, HigherOrderUDF, Operator, ScalarUDF, cast, expr, lit,
    try_cast, when,
};
use datafusion_spark::function::aggregate::try_sum::SparkTrySum;
use lazy_static::lazy_static;
use sail_common::spec::{SAIL_LIST_FIELD_NAME, SAIL_SPARK_UDT_METADATA_KEY};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::aggregate::bitmap_and_agg::BitmapAndAggFunction;
use sail_function::aggregate::bitmap_construct_agg::BitmapConstructAggFunction;
use sail_function::aggregate::bitmap_or_agg::BitmapOrAggFunction;
use sail_function::aggregate::count_min_sketch::CountMinSketchFunction;
use sail_function::aggregate::grouping_id::GroupingIdFunction;
use sail_function::aggregate::histogram_numeric::HistogramNumericFunction;
use sail_function::aggregate::hll_sketch::{HllSketchAggFunction, HllUnionAggFunction};
use sail_function::aggregate::kurtosis::KurtosisFunction;
use sail_function::aggregate::max_min_by::{MaxByFunction, MinByFunction};
use sail_function::aggregate::mode::ModeFunction;
use sail_function::aggregate::percentile::PercentileFunction;
use sail_function::aggregate::percentile_disc::percentile_disc_udaf;
use sail_function::aggregate::product::ProductFunction;
use sail_function::aggregate::regr::{Regr, RegrType};
use sail_function::aggregate::schema_of_variant_agg::SchemaOfVariantAggFunction;
use sail_function::aggregate::skewness::SkewnessFunc;
use sail_function::aggregate::theta_sketch::{
    ThetaIntersectionAggFunction, ThetaSketchAggFunction, ThetaUnionAggFunction,
};
use sail_function::aggregate::try_avg::TryAvgFunction;
use sail_function::aggregate::vector::avg::VectorAvg;
use sail_function::aggregate::vector_sum::VectorSum;
use sail_function::scalar::array::spark_array_transform::SparkArrayTransform;
use sail_function::scalar::struct_function::StructFunction;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    AggFunction, AggFunctionInput, count_min_sketch_args, get_arguments_and_null_treatment,
    get_null_treatment, hll_args_with_default_lg, hll_union_args_with_default_allow_different_lg,
    theta_args_with_default_lg,
};
use crate::function::transform_count_star_wildcard_expr;

lazy_static! {
    static ref BUILT_IN_AGGREGATE_FUNCTIONS: HashMap<&'static str, AggFunction> =
        HashMap::from_iter(list_built_in_aggregate_functions());
}

fn avg(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let (args, null_treatment) =
        get_arguments_and_null_treatment(input.arguments, input.ignore_nulls)?;
    let arg_type = args
        .first()
        .map(|arg| arg.get_type(input.function_context.schema))
        .transpose()?;
    if arg_type == Some(DataType::Null) {
        return Ok(lit(ScalarValue::Null));
    }
    // The decimal average of DataFusion truncates the result, while Spark rounds it half up.
    // The average is computed with one more digit and then cast to the result type, which
    // rounds half up. This is only done when the result type has room for the extra digit.
    let rounding = match arg_type {
        Some(DataType::Decimal128(precision, scale))
            if precision <= DECIMAL128_MAX_PRECISION - 5 && scale <= DECIMAL128_MAX_SCALE - 5 =>
        {
            Some((precision, scale))
        }
        _ => None,
    };
    let args = match rounding {
        Some((precision, scale)) => args
            .into_iter()
            .map(|arg| cast(arg, DataType::Decimal128(precision + 1, scale + 1)))
            .collect(),
        None => args,
    };
    let avg = expr::Expr::AggregateFunction(AggregateFunction {
        func: average::avg_udaf(),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment,
        },
    });
    Ok(match rounding {
        Some((precision, scale)) => cast(avg, DataType::Decimal128(precision + 4, scale + 4)),
        None => avg,
    })
}

fn vector_aggregate(input: AggFunctionInput, func: AggregateUDF) -> PlanResult<expr::Expr> {
    let mut args = input.arguments;
    if input.distinct && args.len() == 1 {
        args = args
            .into_iter()
            .map(|arg| normalize_vector_aggregate_argument(arg, input.function_context.schema))
            .collect::<PlanResult<_>>()?;
    }
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(func),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn normalize_vector_aggregate_argument(
    arg: expr::Expr,
    schema: &DFSchema,
) -> PlanResult<expr::Expr> {
    let field = match arg.get_type(schema)? {
        DataType::List(field) | DataType::LargeList(field)
            if field.data_type() == &DataType::Float32 =>
        {
            field
        }
        _ => return Ok(arg),
    };
    // DISTINCT can become GROUP BY before reaching the normalization-aware accumulator.
    let parameter = "__sail_vector_aggregate_element".to_string();
    let variable = expr::Expr::LambdaVariable(LambdaVariable::new(parameter.clone(), Some(field)));
    let body = when(isnan(variable.clone()), lit(f32::NAN))
        .when(variable.clone().eq(lit(0.0f32)), lit(0.0f32))
        .otherwise(variable)?;
    Ok(expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
        Arc::new(HigherOrderUDF::new_from_impl(SparkArrayTransform::new())),
        vec![arg, expr::Expr::Lambda(Lambda::new(vec![parameter], body))],
    )))
}

fn spark_sum(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let AggFunctionInput {
        arguments,
        distinct,
        ignore_nulls,
        filter,
        order_by,
        preserve_count_argument_columns: _,
        function_context,
    } = input;
    let supports_linear_rewrite =
        !distinct && ignore_nulls.is_none() && filter.is_none() && order_by.is_empty();
    let arguments = coerce_string_sum_arguments(
        arguments,
        filter.as_deref(),
        function_context.schema.as_ref(),
        function_context.plan_config.ansi_mode,
    )?;
    let arguments = if supports_linear_rewrite {
        arguments
            .into_iter()
            .map(|argument| {
                widen_safe_linear_sum_argument(argument, function_context.schema.as_ref())
            })
            .collect()
    } else {
        arguments
    };

    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: sum::sum_udaf(),
        params: AggregateFunctionParams {
            args: arguments,
            distinct,
            filter,
            order_by,
            null_treatment: get_null_treatment(ignore_nulls),
        },
    }))
}

pub(super) fn coerce_string_sum_arguments(
    arguments: Vec<expr::Expr>,
    filter: Option<&expr::Expr>,
    schema: &DFSchema,
    ansi_mode: bool,
) -> PlanResult<Vec<expr::Expr>> {
    arguments
        .into_iter()
        .map(|argument| {
            let argument_type = argument.get_type(schema)?;
            match argument_type {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                    if ansi_mode {
                        let argument = if let Some(filter) = filter {
                            // Grouped aggregates evaluate arguments before FILTER, so mask
                            // excluded rows before applying the fallible ANSI cast.
                            let null_value = ScalarValue::try_from(&argument_type)?;
                            when(filter.clone(), argument).otherwise(lit(null_value))?
                        } else {
                            argument
                        };
                        Ok(cast(argument, DataType::Float64))
                    } else {
                        Ok(try_cast(argument, DataType::Float64))
                    }
                }
                _ => Ok(argument),
            }
        })
        .collect()
}

fn widen_safe_linear_sum_argument(
    argument: expr::Expr,
    schema: &datafusion_common::DFSchema,
) -> expr::Expr {
    if argument.get_type(schema).ok() != Some(DataType::Int32) {
        return argument;
    }
    let expr::Expr::BinaryExpr(BinaryExpr { left, op, right }) = &argument else {
        return argument;
    };
    if *op != Operator::Plus {
        return argument;
    }

    let left_literal = widen_int32_literal(left);
    let right_literal = widen_int32_literal(right);
    let (value, widened_literal, value_expression, literal_is_left) =
        match (left_literal, right_literal) {
            (Some((value, literal)), None) => (value, literal, right.as_ref(), true),
            (None, Some((value, literal))) => (value, literal, left.as_ref(), false),
            _ => return argument,
        };
    let Ok(value_type) = value_expression.get_type(schema) else {
        return argument;
    };
    let Some((minimum, maximum)) = signed_integer_bounds(&value_type) else {
        return argument;
    };
    if minimum + value < i64::from(i32::MIN) || maximum + value > i64::from(i32::MAX) {
        return argument;
    }

    let widened_value = cast(value_expression.clone(), DataType::Int64);
    if literal_is_left {
        widened_literal + widened_value
    } else {
        widened_value + widened_literal
    }
}

fn widen_int32_literal(argument: &expr::Expr) -> Option<(i64, expr::Expr)> {
    let expr::Expr::Literal(ScalarValue::Int32(Some(value)), metadata) = argument else {
        return None;
    };
    let value = i64::from(*value);
    Some((
        value,
        expr::Expr::Literal(ScalarValue::Int64(Some(value)), metadata.clone()),
    ))
}

fn signed_integer_bounds(data_type: &DataType) -> Option<(i64, i64)> {
    match data_type {
        DataType::Int8 => Some((i64::from(i8::MIN), i64::from(i8::MAX))),
        DataType::Int16 => Some((i64::from(i16::MIN), i64::from(i16::MAX))),
        DataType::Int32 => Some((i64::from(i32::MIN), i64::from(i32::MAX))),
        _ => None,
    }
}

fn grouping_id(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let AggFunctionInput {
        arguments,
        distinct,
        ignore_nulls,
        filter,
        order_by,
        preserve_count_argument_columns: _,
        function_context: _,
    } = input;
    if distinct || ignore_nulls.is_some() || filter.is_some() || !order_by.is_empty() {
        return Err(PlanError::invalid("invalid grouping_id function clause"));
    }
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(GroupingIdFunction::new())),
        params: AggregateFunctionParams {
            args: arguments,
            distinct: false,
            filter: None,
            order_by: vec![],
            null_treatment: None,
        },
    }))
}

fn first_value(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let (args, null_treatment) =
        get_arguments_and_null_treatment(input.arguments, input.ignore_nulls)?;
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: first_last::first_value_udaf(),
        params: AggregateFunctionParams {
            args,
            // Spark's `EliminateDistinct` strips DISTINCT from `First` as duplicate-agnostic.
            distinct: false,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment,
        },
    }))
}

fn last_value(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let (args, null_treatment) =
        get_arguments_and_null_treatment(input.arguments, input.ignore_nulls)?;
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: first_last::last_value_udaf(),
        params: AggregateFunctionParams {
            args,
            // Spark's `EliminateDistinct` strips DISTINCT from `Last` as duplicate-agnostic.
            distinct: false,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment,
        },
    }))
}

fn kurtosis(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let args = input
        .arguments
        .into_iter()
        .map(|arg| expr::Expr::Cast(expr::Cast::new(Box::new(arg), DataType::Float64)))
        .collect();
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(KurtosisFunction::new())),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn product(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let args = input
        .arguments
        .into_iter()
        .map(|arg| expr::Expr::Cast(expr::Cast::new(Box::new(arg), DataType::Float64)))
        .collect();
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(ProductFunction::new())),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn max_by(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(MaxByFunction::new())),
        params: AggregateFunctionParams {
            args: input.arguments,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn min_by(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(MinByFunction::new())),
        params: AggregateFunctionParams {
            args: input.arguments,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn mode(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(ModeFunction::new())),
        params: AggregateFunctionParams {
            args: input.arguments,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn schema_of_variant_agg(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(SchemaOfVariantAggFunction::new())),
        params: AggregateFunctionParams {
            args: input.arguments,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

/// Builds a percentile_cont aggregate expression from WITHIN GROUP syntax.
///
/// DataFusion's percentile_cont expects args = [column, percentile], but Spark's
/// SQL syntax `percentile_cont(0.5) WITHIN GROUP (ORDER BY col)` puts the column
/// in order_by and the percentile in arguments. This function combines them.
fn percentile_cont(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    // Extract the single column expression from ORDER BY (error if multiple)
    let sort = input.order_by.clone().one()?;
    let column = sort.expr;

    // Get the percentile value from arguments
    let percentile = input.arguments.one()?;

    // Combine: [column, percentile] as DataFusion expects
    let args = vec![column, percentile];

    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: percentile_cont::percentile_cont_udaf(),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

/// Builds a percentile_disc aggregate expression from WITHIN GROUP syntax.
fn percentile_disc(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let sort = input.order_by.clone().one()?;
    let column = sort.expr;
    let percentile = input.arguments.one()?;
    let args = vec![column, percentile];
    let ansi_mode = input.function_context.plan_config.ansi_mode;

    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: percentile_disc_udaf(ansi_mode),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

/// Spark's `percentile_approx` returns values of the column, and the exact percentile is
/// within any accuracy, so it is computed with `percentile_disc`. The percentage can be
/// an array, which returns an array of percentiles of the column type.
fn percentile_approx(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    let (value, percentage) = match input.arguments.as_slice() {
        [value, percentage] | [value, percentage, _] => (value.clone(), percentage.clone()),
        _ => {
            return Err(PlanError::invalid(format!(
                "percentile_approx requires 2 or 3 arguments, got {}",
                input.arguments.len()
            )));
        }
    };
    let value_type = value.get_type(schema)?;
    let return_type = match percentage.get_type(schema)? {
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            DataType::List(Arc::new(Field::new(
                SAIL_LIST_FIELD_NAME,
                value_type,
                false,
            )))
        }
        _ => value_type,
    };
    let percentile = expr::Expr::AggregateFunction(AggregateFunction {
        func: percentile_disc_udaf(input.function_context.plan_config.ansi_mode),
        params: AggregateFunctionParams {
            args: vec![value.clone(), percentage],
            distinct: input.distinct,
            filter: input.filter,
            order_by: vec![value.sort(true, false)],
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    });
    Ok(cast(percentile, return_type))
}

fn skewness(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let args = input
        .arguments
        .into_iter()
        .map(|arg| expr::Expr::Cast(expr::Cast::new(Box::new(arg), DataType::Float64)))
        .collect();
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(SkewnessFunc::new())),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn try_sum(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let args = input.arguments;

    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(SparkTrySum::new())),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn try_avg(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let args = input.arguments;

    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(TryAvgFunction::new())),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn count(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let AggFunctionInput {
        arguments,
        mut distinct,
        ignore_nulls,
        filter,
        order_by,
        preserve_count_argument_columns,
        function_context,
    } = input;
    let null_treatment = get_null_treatment(ignore_nulls);
    // For COUNT(DISTINCT *), the resolver already expanded the wildcard to column references
    // (with hidden-column filtering). For COUNT(*), convert to COUNT(1).
    let mut args = transform_count_star_wildcard_expr(arguments);
    if args.is_empty() {
        if function_context
            .plan_config
            .legacy_allow_parameterless_count
        {
            args.push(lit(ScalarValue::Null));
        } else {
            return Err(PlanError::invalid(
                "The `count` function requires at least one parameter; set \
                 `spark.sql.legacy.allowParameterlessCount` to true to restore the legacy \
                 COUNT() result of zero",
            ));
        }
    }

    // Implicit SQL PIVOT grouping collects columns from the resolved aggregate, so its
    // preservation scope must retain the original arguments through constant-filter folding.
    let filter = match filter {
        Some(filter)
            if matches!(
                filter.as_ref(),
                expr::Expr::Literal(ScalarValue::Boolean(Some(true)), _)
            ) =>
        {
            None
        }
        Some(filter)
            if !preserve_count_argument_columns
                && matches!(
                    filter.as_ref(),
                    expr::Expr::Literal(ScalarValue::Boolean(Some(false) | None), _)
                        | expr::Expr::Literal(ScalarValue::Null, _)
                ) =>
        {
            args = vec![lit(ScalarValue::Null)];
            None
        }
        filter => filter,
    };

    if args.iter().all(is_typed_null_literal) {
        // Keep an aggregate in the plan so global COUNT still emits one row on empty input.
        args = vec![lit(ScalarValue::Null)];
        distinct = false;
    }

    if distinct && order_by.is_empty() && args.iter().all(is_non_null_literal) {
        // A distinct tuple of constants contributes exactly one value when the filtered input is
        // non-empty. Express that through row COUNT so exact table cardinality can answer it.
        let rows = expr::Expr::AggregateFunction(AggregateFunction {
            func: count::count_udaf(),
            params: AggregateFunctionParams {
                args: vec![expr::Expr::Literal(COUNT_STAR_EXPANSION, None)],
                distinct: false,
                filter,
                order_by,
                null_treatment,
            },
        });
        return Ok(when(rows.gt(lit(0_i64)), lit(1_i64)).otherwise(lit(0_i64))?);
    }

    if !distinct && !preserve_count_argument_columns {
        let mut nullable_args = Vec::with_capacity(args.len());
        for argument in args {
            if argument.nullable(function_context.schema)?
                && !matches!(&argument, expr::Expr::Column(column) if nullable_args.iter().any(
                    |existing| matches!(existing, expr::Expr::Column(other) if other == column)
                ))
            {
                nullable_args.push(argument);
            }
        }
        // COUNT only tests whether every argument is non-null. Arguments proven non-null can be
        // removed; an empty remainder is DataFusion's canonical COUNT(*) expansion.
        args = if nullable_args.is_empty() {
            vec![expr::Expr::Literal(COUNT_STAR_EXPANSION, None)]
        } else {
            nullable_args
        };
    }
    // TODO: remove StructFunction call when count distinct from multiple arguments is implemented
    // https://github.com/apache/datafusion/blob/58ddf0d4390c770bc571f3ac2727c7de77aa25ab/datafusion/functions-aggregate/src/count.rs#L333
    let args = if distinct && (args.len() > 1) {
        // In Spark, COUNT(DISTINCT col1, col2, ...) skips rows where ANY column is NULL.
        // Since we wrap multiple columns into a struct for DataFusion, a struct with NULL
        // fields is still a non-NULL value and would be counted. To match Spark semantics,
        // return NULL (instead of a struct with NULL fields) when any argument is NULL.
        // Compute any_null first (borrowing args), then move args into .call() to avoid cloning.
        let any_null = args
            .iter()
            .map(|arg| arg.clone().is_null())
            .reduce(|a, b| a.or(b));
        let struct_expr = ScalarUDF::from(StructFunction::new(
            (0..args.len()).map(|i| format!("col{i}")).collect(),
        ))
        .call(args);
        // `any_null` is always `Some` here since `args.len() > 1` guarantees `reduce` succeeds.
        match any_null {
            Some(any_null) => vec![expr::Expr::Case(expr::Case {
                expr: None,
                when_then_expr: vec![(Box::new(any_null), Box::new(lit(ScalarValue::Null)))],
                else_expr: Some(Box::new(struct_expr)),
            })],
            None => vec![struct_expr],
        }
    } else {
        args
    };
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: count::count_udaf(),
        params: AggregateFunctionParams {
            args,
            distinct,
            filter,
            order_by,
            null_treatment,
        },
    }))
}

fn is_typed_null_literal(expression: &expr::Expr) -> bool {
    match expression {
        expr::Expr::Alias(alias) => is_typed_null_literal(alias.expr.as_ref()),
        expr::Expr::Literal(value, _) => value.is_null(),
        expr::Expr::Cast(cast) => is_typed_null_literal(cast.expr.as_ref()),
        expr::Expr::TryCast(cast) => is_typed_null_literal(cast.expr.as_ref()),
        _ => false,
    }
}

fn is_non_null_literal(expression: &expr::Expr) -> bool {
    match expression {
        expr::Expr::Alias(alias) => is_non_null_literal(alias.expr.as_ref()),
        expr::Expr::Literal(value, _) => !value.is_null(),
        _ => false,
    }
}

fn min_value(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    duplicate_agnostic_extreme(input, min_max::min_udaf)
}

fn max_value(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    duplicate_agnostic_extreme(input, min_max::max_udaf)
}

fn duplicate_agnostic_extreme(
    input: AggFunctionInput,
    function: fn() -> Arc<AggregateUDF>,
) -> PlanResult<expr::Expr> {
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: function(),
        params: AggregateFunctionParams {
            args: input.arguments,
            // Duplicate elimination cannot change an extremum, and retaining DISTINCT creates
            // an avoidable grouped aggregate that prevents exact-statistics substitution.
            distinct: false,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn count_if(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    match input.arguments.len() {
        1 => {
            let filter = input
                .arguments
                .first()
                .ok_or_else(|| PlanError::invalid("`count_if` requires 1 argument"))?
                .clone();
            Ok(expr::Expr::AggregateFunction(AggregateFunction {
                func: count::count_udaf(),
                params: AggregateFunctionParams {
                    args: vec![lit(0)],
                    distinct: input.distinct,
                    order_by: input.order_by,
                    filter: Some(Box::new(filter)),
                    null_treatment: get_null_treatment(input.ignore_nulls),
                },
            }))
        }
        _ => Err(PlanError::invalid("`count_if` requires 1 argument")),
    }
}

fn collect_set(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    // Spark's collect_set ignores NULLs by default
    let ignore_nulls = input.ignore_nulls.or(Some(true));
    let arg = input.arguments.one()?;
    let element_type = arg.get_type(schema)?;
    let element_field = arg.to_field(schema)?.1;

    // WORKAROUND: DataFusion's array_agg doesn't properly handle null_treatment when distinct=true
    // So we need to add an explicit filter for NULLs
    let (filter, null_treatment) = if ignore_nulls == Some(true) {
        let null_filter = arg.clone().is_not_null();
        let combined_filter = match input.filter {
            Some(existing) => existing.as_ref().clone().and(null_filter),
            None => null_filter,
        };
        // Don't use null_treatment when we have explicit filter
        (Some(Box::new(combined_filter)), None)
    } else {
        (input.filter, get_null_treatment(ignore_nulls))
    };

    let agg = expr::Expr::AggregateFunction(AggregateFunction {
        func: array_agg::array_agg_udaf(),
        params: AggregateFunctionParams {
            args: vec![arg],
            distinct: true,
            order_by: input.order_by,
            filter,
            null_treatment,
        },
    });
    Ok(with_element_udt(
        coalesce_to_empty_array(agg, &element_type),
        &element_field,
    ))
}

fn array_agg_compacted(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    // Spark's collect_list ignores NULLs by default
    let ignore_nulls = input.ignore_nulls.or(Some(true));
    let arg = input.arguments.one()?;
    let element_type = arg.get_type(schema)?;
    let element_field = arg.to_field(schema)?.1;

    // WORKAROUND: DataFusion's array_agg doesn't properly handle null_treatment when distinct=true
    // So we need to add an explicit filter for NULLs when both distinct and ignore_nulls are true
    let (filter, null_treatment) = if input.distinct && ignore_nulls == Some(true) {
        let null_filter = arg.clone().is_not_null();
        let combined_filter = match input.filter {
            Some(existing) => existing.as_ref().clone().and(null_filter),
            None => null_filter,
        };
        // Don't use null_treatment when we have explicit filter
        (Some(Box::new(combined_filter)), None)
    } else {
        (input.filter, get_null_treatment(ignore_nulls))
    };

    let agg = expr::Expr::AggregateFunction(AggregateFunction {
        func: array_agg::array_agg_udaf(),
        params: AggregateFunctionParams {
            args: vec![arg],
            distinct: input.distinct,
            order_by: input.order_by,
            filter,
            null_treatment,
        },
    });
    Ok(with_element_udt(
        coalesce_to_empty_array(agg, &element_type),
        &element_field,
    ))
}

/// Spark's `collect_list`/`collect_set` return an empty array for a group with no (non-NULL)
/// values to collect, but DataFusion's `array_agg` returns NULL over zero rows. Coalesce the
/// aggregate to a typed empty array so the empty case matches Spark instead of yielding NULL.
fn coalesce_to_empty_array(agg: expr::Expr, element_type: &DataType) -> expr::Expr {
    let empty = ScalarValue::List(ScalarValue::new_list_nullable(&[], element_type));
    coalesce(vec![agg, lit(empty)])
}

/// Keeps the user-defined type of the collected values, which is only in the metadata of the
/// field and is lost in the array type that `array_agg` returns.
fn with_element_udt(agg: expr::Expr, element_field: &Field) -> expr::Expr {
    match element_field.metadata().get(SAIL_SPARK_UDT_METADATA_KEY) {
        Some(udt) => cast(
            agg,
            DataType::List(Arc::new(
                Field::new_list_field(element_field.data_type().clone(), true).with_metadata(
                    HashMap::from([(SAIL_SPARK_UDT_METADATA_KEY.to_string(), udt.clone())]),
                ),
            )),
        ),
        None => agg,
    }
}

fn listagg(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    let (agg_col, other_args) = input.arguments.at_least_one()?;
    if agg_col.get_type(schema)? == DataType::Null {
        return Ok(lit(ScalarValue::Null));
    }
    let delim = other_args.first().cloned().unwrap_or_else(|| lit(""));

    let agg = expr::Expr::AggregateFunction(AggregateFunction {
        func: array_agg::array_agg_udaf(),
        params: AggregateFunctionParams {
            args: vec![agg_col.clone()],
            distinct: input.distinct,
            order_by: if input.distinct {
                vec![agg_col.clone().sort(true, true)]
            } else {
                input.order_by
            },
            filter: input.filter,
            null_treatment: get_null_treatment(Some(true)),
        },
    });

    let string_agg = array_to_string(
        agg.cast_to(
            &DataType::List(Arc::new(Field::new(
                SAIL_LIST_FIELD_NAME,
                DataType::Utf8,
                true,
            ))),
            schema,
        )?,
        delim.cast_to(&DataType::Utf8, schema)?,
    );

    let casted_agg = match agg_col.get_type(schema)? {
        DataType::Binary | DataType::BinaryView => string_agg.cast_to(&DataType::Binary, schema)?,
        DataType::LargeBinary => string_agg.cast_to(&DataType::LargeBinary, schema)?,
        _ => string_agg,
    };

    Ok(when(casted_agg.clone().is_not_null(), casted_agg)
        .when(lit(true), lit(ScalarValue::Null))
        .end()?)
}

fn histogram_numeric(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(HistogramNumericFunction::new())),
        params: AggregateFunctionParams {
            args: input.arguments,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn median(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let mut args = input.arguments.clone();
    args.push(lit(0.5_f64));
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(PercentileFunction::new())),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn percentile_exact(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(PercentileFunction::new())),
        params: AggregateFunctionParams {
            args: input.arguments,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn approx_count_distinct(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    // The optional relative standard deviation only tunes the precision of the estimate.
    let (value, _) = input.arguments.clone().at_least_one()?;
    // DataFusion cannot count floating-point values, so they are counted by their string
    // representation, which is unique for each value once `-0.0` is normalized to `0.0`
    // as Spark does.
    let value = match value.get_type(input.function_context.schema)? {
        data_type @ (DataType::Float16 | DataType::Float32 | DataType::Float64) => {
            let zero = cast(lit(0.0), data_type);
            cast(
                when(value.clone().eq(zero.clone()), zero).otherwise(value)?,
                DataType::Utf8,
            )
        }
        _ => value,
    };
    Ok(cast(
        expr::Expr::AggregateFunction(AggregateFunction {
            func: approx_distinct::approx_distinct_udaf(),
            params: AggregateFunctionParams {
                args: vec![value],
                distinct: input.distinct,
                order_by: input.order_by,
                filter: input.filter,
                null_treatment: get_null_treatment(input.ignore_nulls),
            },
        }),
        DataType::Int64,
    ))
}

fn hll_sketch_agg(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let args = hll_args_with_default_lg(input.arguments, "hll_sketch_agg")?;
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(HllSketchAggFunction::new())),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn hll_union_agg(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let args = hll_union_args_with_default_allow_different_lg(input.arguments)?;
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(HllUnionAggFunction::new())),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn count_min_sketch(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let args = count_min_sketch_args(input.arguments)?;
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(CountMinSketchFunction::new())),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn theta_sketch_agg(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let args = theta_args_with_default_lg(input.arguments, "theta_sketch_agg")?;
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(ThetaSketchAggFunction::new())),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn theta_union_agg(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let args = theta_args_with_default_lg(input.arguments, "theta_union_agg")?;
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(ThetaUnionAggFunction::new())),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

/// Creates a list of built-in aggregate functions.
/// This is used to create a hashmap that the resolver uses to look up
/// aggregate functions by name.
fn list_built_in_aggregate_functions() -> Vec<(&'static str, AggFunction)> {
    use crate::function::common::AggFunctionBuilder as F;

    vec![
        ("any", F::default(bool_and_or::bool_or_udaf)),
        ("any_value", F::custom(first_value)),
        ("approx_count_distinct", F::custom(approx_count_distinct)),
        ("approx_percentile", F::custom(percentile_approx)),
        ("array_agg", F::custom(array_agg_compacted)),
        ("avg", F::custom(avg)),
        ("bit_and", F::default(bit_and_or_xor::bit_and_udaf)),
        ("bit_or", F::default(bit_and_or_xor::bit_or_udaf)),
        ("bit_xor", F::default(bit_and_or_xor::bit_xor_udaf)),
        (
            "bitmap_and_agg",
            F::default(|| Arc::new(AggregateUDF::from(BitmapAndAggFunction::new()))),
        ),
        (
            "bitmap_construct_agg",
            F::default(|| Arc::new(AggregateUDF::from(BitmapConstructAggFunction::new()))),
        ),
        (
            "bitmap_or_agg",
            F::default(|| Arc::new(AggregateUDF::from(BitmapOrAggFunction::new()))),
        ),
        ("bool_and", F::default(bool_and_or::bool_and_udaf)),
        ("bool_or", F::default(bool_and_or::bool_or_udaf)),
        ("collect_list", F::custom(array_agg_compacted)),
        ("collect_set", F::custom(collect_set)),
        ("corr", F::default(correlation::corr_udaf)),
        ("count", F::custom(count)),
        ("count_if", F::custom(count_if)),
        ("count_min_sketch", F::custom(count_min_sketch)),
        ("covar_pop", F::default(covariance::covar_pop_udaf)),
        ("covar_samp", F::default(covariance::covar_samp_udaf)),
        ("every", F::default(bool_and_or::bool_and_udaf)),
        ("first", F::custom(first_value)),
        ("first_value", F::custom(first_value)),
        ("grouping", F::default(grouping::grouping_udaf)),
        ("grouping_id", F::custom(grouping_id)),
        ("histogram_numeric", F::custom(histogram_numeric)),
        ("hll_sketch_agg", F::custom(hll_sketch_agg)),
        ("hll_union_agg", F::custom(hll_union_agg)),
        ("kll_merge_agg_bigint", F::unknown("kll_merge_agg_bigint")),
        ("kll_merge_agg_double", F::unknown("kll_merge_agg_double")),
        ("kll_merge_agg_float", F::unknown("kll_merge_agg_float")),
        (
            "theta_intersection_agg",
            F::default(|| Arc::new(AggregateUDF::from(ThetaIntersectionAggFunction::new()))),
        ),
        ("theta_sketch_agg", F::custom(theta_sketch_agg)),
        ("theta_union_agg", F::custom(theta_union_agg)),
        ("kurtosis", F::custom(kurtosis)),
        ("last", F::custom(last_value)),
        ("last_value", F::custom(last_value)),
        ("listagg", F::custom(listagg)),
        ("max", F::custom(max_value)),
        ("max_by", F::custom(max_by)),
        ("mean", F::custom(avg)),
        ("measure", F::unknown("measure")),
        ("median", F::custom(median)),
        ("min", F::custom(min_value)),
        ("min_by", F::custom(min_by)),
        ("mode", F::custom(mode)),
        ("percentile", F::custom(percentile_exact)),
        ("percentile_approx", F::custom(percentile_approx)),
        ("percentile_cont", F::custom(percentile_cont)),
        ("percentile_disc", F::custom(percentile_disc)),
        ("product", F::custom(product)),
        (
            "regr_avgx",
            F::default(|| Arc::new(AggregateUDF::from(Regr::new(RegrType::AvgX, "regr_avgx")))),
        ),
        (
            "regr_avgy",
            F::default(|| Arc::new(AggregateUDF::from(Regr::new(RegrType::AvgY, "regr_avgy")))),
        ),
        (
            "regr_count",
            F::default(|| Arc::new(AggregateUDF::from(Regr::new(RegrType::Count, "regr_count")))),
        ),
        (
            "regr_intercept",
            F::default(|| {
                Arc::new(AggregateUDF::from(Regr::new(
                    RegrType::Intercept,
                    "regr_intercept",
                )))
            }),
        ),
        (
            "regr_r2",
            F::default(|| Arc::new(AggregateUDF::from(Regr::new(RegrType::R2, "regr_r2")))),
        ),
        (
            "regr_slope",
            F::default(|| Arc::new(AggregateUDF::from(Regr::new(RegrType::Slope, "regr_slope")))),
        ),
        (
            "regr_sxx",
            F::default(|| Arc::new(AggregateUDF::from(Regr::new(RegrType::Sxx, "regr_sxx")))),
        ),
        (
            "regr_sxy",
            F::default(|| Arc::new(AggregateUDF::from(Regr::new(RegrType::Sxy, "regr_sxy")))),
        ),
        (
            "regr_syy",
            F::default(|| Arc::new(AggregateUDF::from(Regr::new(RegrType::Syy, "regr_syy")))),
        ),
        ("schema_of_variant_agg", F::custom(schema_of_variant_agg)),
        ("skewness", F::custom(skewness)),
        ("some", F::default(bool_and_or::bool_or_udaf)),
        ("std", F::default(stddev::stddev_udaf)),
        ("stddev", F::default(stddev::stddev_udaf)),
        ("stddev_pop", F::default(stddev::stddev_pop_udaf)),
        ("stddev_samp", F::default(stddev::stddev_udaf)),
        ("string_agg", F::custom(listagg)),
        ("sum", F::custom(spark_sum)),
        (
            "tuple_intersection_agg_double",
            F::unknown("tuple_intersection_agg_double"),
        ),
        (
            "tuple_intersection_agg_integer",
            F::unknown("tuple_intersection_agg_integer"),
        ),
        (
            "tuple_sketch_agg_double",
            F::unknown("tuple_sketch_agg_double"),
        ),
        (
            "tuple_sketch_agg_integer",
            F::unknown("tuple_sketch_agg_integer"),
        ),
        (
            "tuple_union_agg_double",
            F::unknown("tuple_union_agg_double"),
        ),
        (
            "tuple_union_agg_integer",
            F::unknown("tuple_union_agg_integer"),
        ),
        ("try_avg", F::custom(try_avg)),
        ("try_sum", F::custom(try_sum)),
        ("var_pop", F::default(variance::var_pop_udaf)),
        ("var_samp", F::default(variance::var_samp_udaf)),
        ("variance", F::default(variance::var_samp_udaf)),
        (
            "vector_avg",
            F::custom(|input| vector_aggregate(input, VectorAvg::new().into())),
        ),
        (
            "vector_sum",
            F::custom(|input| vector_aggregate(input, VectorSum::new().into())),
        ),
    ]
}

pub(crate) fn get_built_in_aggregate_function(name: &str) -> PlanResult<AggFunction> {
    Ok(BUILT_IN_AGGREGATE_FUNCTIONS
        .get(name)
        .ok_or_else(|| PlanError::unsupported(format!("unknown aggregate function: {name}")))?
        .clone())
}

pub(crate) fn list_built_in_aggregate_function_names() -> impl Iterator<Item = &'static str> {
    BUILT_IN_AGGREGATE_FUNCTIONS.keys().copied()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::Schema;
    use datafusion::optimizer::single_distinct_to_groupby::SingleDistinctToGroupBy;
    use datafusion::optimizer::{OptimizerContext, OptimizerRule};
    use datafusion::prelude::SessionContext;
    use datafusion_expr::{EmptyRelation, ExprSchemable, LogicalPlan, LogicalPlanBuilder, col};

    use super::*;
    use crate::config::PlanConfig;
    use crate::function::common::FunctionContextInput;

    fn sum_test_schema(data_type: DataType) -> PlanResult<Arc<DFSchema>> {
        Ok(Arc::new(DFSchema::try_from(Schema::new(vec![
            Field::new("value", data_type, true),
            Field::new("included", DataType::Boolean, true),
        ]))?))
    }

    fn vector_aggregate_test_expression(
        name: &str,
        schema: &Arc<DFSchema>,
        arguments: Vec<expr::Expr>,
        distinct: bool,
        ignore_nulls: Option<bool>,
        clauses: bool,
    ) -> PlanResult<expr::Expr> {
        let plan_config = Arc::new(PlanConfig::default());
        let session_context = SessionContext::new();
        let function = get_built_in_aggregate_function(name)?;
        function(AggFunctionInput {
            arguments,
            distinct,
            ignore_nulls,
            filter: clauses.then(|| Box::new(col("included"))),
            order_by: if clauses {
                vec![col("value").sort(true, false)]
            } else {
                vec![]
            },
            preserve_count_argument_columns: false,
            function_context: FunctionContextInput {
                argument_display_names: &[],
                plan_config: &plan_config,
                session_context: &session_context,
                schema,
            },
        })
    }

    #[test]
    fn vector_aggregates_normalize_only_distinct_arguments_and_preserve_clauses() -> PlanResult<()>
    {
        for nullable in [false, true] {
            let field = Arc::new(Field::new_list_field(DataType::Float32, nullable));
            for data_type in [
                DataType::List(Arc::clone(&field)),
                DataType::LargeList(Arc::clone(&field)),
            ] {
                let schema = sum_test_schema(data_type.clone())?;
                let normalized = normalize_vector_aggregate_argument(col("value"), &schema)?;
                let variable = expr::Expr::LambdaVariable(LambdaVariable::new(
                    "__sail_vector_aggregate_element".to_string(),
                    Some(Arc::clone(&field)),
                ));
                let expected_body = when(isnan(variable.clone()), lit(f32::NAN))
                    .when(variable.clone().eq(lit(0.0f32)), lit(0.0f32))
                    .otherwise(variable)?;
                let expr::Expr::HigherOrderFunction(transform) = &normalized else {
                    return Err(PlanError::internal("expected array transform"));
                };
                assert_eq!(
                    transform.func.as_ref(),
                    &HigherOrderUDF::new_from_impl(SparkArrayTransform::new())
                );
                assert_eq!(
                    transform.args,
                    vec![
                        col("value"),
                        expr::Expr::Lambda(Lambda::new(
                            vec!["__sail_vector_aggregate_element".to_string()],
                            expected_body,
                        ))
                    ]
                );
                assert_eq!(normalized.get_type(&schema)?, data_type);
                assert!(normalized.nullable(&schema)?);
                for name in ["vector_avg", "vector_sum"] {
                    for distinct in [false, true] {
                        for ignore_nulls in [None, Some(false), Some(true)] {
                            let expr::Expr::AggregateFunction(aggregate) =
                                vector_aggregate_test_expression(
                                    name,
                                    &schema,
                                    vec![col("value")],
                                    distinct,
                                    ignore_nulls,
                                    true,
                                )?
                            else {
                                return Err(PlanError::internal("expected vector aggregate"));
                            };
                            assert_eq!(aggregate.func.name(), name);
                            assert_eq!(
                                aggregate.params.args,
                                vec![if distinct {
                                    normalized.clone()
                                } else {
                                    col("value")
                                }]
                            );
                            assert_eq!(aggregate.params.distinct, distinct);
                            assert_eq!(aggregate.params.filter, Some(Box::new(col("included"))));
                            assert_eq!(
                                aggregate.params.order_by,
                                vec![col("value").sort(true, false)]
                            );
                            assert_eq!(
                                aggregate.params.null_treatment,
                                get_null_treatment(ignore_nulls)
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn vector_aggregates_preserve_invalid_arguments_for_type_checking() -> PlanResult<()> {
        let float_field = Arc::new(Field::new_list_field(DataType::Float32, true));
        let double_field = Arc::new(Field::new_list_field(DataType::Float64, true));
        for (data_type, arguments) in [
            (DataType::List(Arc::clone(&float_field)), vec![]),
            (
                DataType::List(Arc::clone(&float_field)),
                vec![col("value"), col("value")],
            ),
            (DataType::Null, vec![col("value")]),
            (DataType::Float32, vec![col("value")]),
            (
                DataType::List(Arc::clone(&double_field)),
                vec![col("value")],
            ),
            (DataType::LargeList(double_field), vec![col("value")]),
            (DataType::FixedSizeList(float_field, 2), vec![col("value")]),
        ] {
            let schema = sum_test_schema(data_type)?;
            for name in ["vector_avg", "vector_sum"] {
                for distinct in [false, true] {
                    let expression = vector_aggregate_test_expression(
                        name,
                        &schema,
                        arguments.clone(),
                        distinct,
                        None,
                        false,
                    )?;
                    assert!(expression.get_type(&schema).is_err());
                    let expr::Expr::AggregateFunction(aggregate) = expression else {
                        return Err(PlanError::internal("expected vector aggregate"));
                    };
                    assert_eq!(aggregate.func.name(), name);
                    assert_eq!(aggregate.params.args, arguments);
                    assert_eq!(aggregate.params.distinct, distinct);
                    assert_eq!(aggregate.params.filter, None);
                    assert!(aggregate.params.order_by.is_empty());
                    assert_eq!(aggregate.params.null_treatment, None);
                }
            }
        }
        Ok(())
    }

    #[test]
    fn vector_aggregates_distinct_rewrite_groups_normalized_values() -> PlanResult<()> {
        for data_type in [
            DataType::List(Arc::new(Field::new_list_field(DataType::Float32, true))),
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Float32, true))),
        ] {
            let schema = sum_test_schema(data_type)?;
            for name in ["vector_avg", "vector_sum"] {
                let aggregate = vector_aggregate_test_expression(
                    name,
                    &schema,
                    vec![col("value")],
                    true,
                    None,
                    false,
                )?;
                let input = LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::clone(&schema),
                });
                let plan = LogicalPlanBuilder::from(input)
                    .aggregate(Vec::<expr::Expr>::new(), vec![aggregate])?
                    .build()?;
                let rewritten = SingleDistinctToGroupBy::new()
                    .rewrite(plan, &OptimizerContext::new())?
                    .data;
                let LogicalPlan::Projection(projection) = rewritten else {
                    return Err(PlanError::internal("expected distinct rewrite projection"));
                };
                let LogicalPlan::Aggregate(outer) = projection.input.as_ref() else {
                    return Err(PlanError::internal("expected outer aggregate"));
                };
                let expr::Expr::AggregateFunction(aggregate) = &outer.aggr_expr[0] else {
                    return Err(PlanError::internal("expected rewritten vector aggregate"));
                };
                assert_eq!(aggregate.func.name(), name);
                assert!(!aggregate.params.distinct);
                assert_eq!(aggregate.params.args, vec![col("alias1")]);
                let LogicalPlan::Aggregate(inner) = outer.input.as_ref() else {
                    return Err(PlanError::internal("expected distinct grouping"));
                };
                assert!(inner.aggr_expr.is_empty());
                assert_eq!(
                    inner.group_expr,
                    vec![
                        normalize_vector_aggregate_argument(col("value"), &schema)?.alias("alias1")
                    ]
                );
            }
        }
        Ok(())
    }

    #[test]
    fn string_sum_uses_mode_specific_cast_for_all_string_types() -> PlanResult<()> {
        for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let schema = sum_test_schema(data_type)?;
            let ansi_arguments =
                coerce_string_sum_arguments(vec![col("value")], None, schema.as_ref(), true)?;
            assert_eq!(ansi_arguments, vec![cast(col("value"), DataType::Float64)]);

            let legacy_arguments =
                coerce_string_sum_arguments(vec![col("value")], None, schema.as_ref(), false)?;
            assert_eq!(
                legacy_arguments,
                vec![try_cast(col("value"), DataType::Float64)]
            );
        }
        Ok(())
    }

    #[test]
    fn ansi_string_sum_masks_filter_before_cast_and_preserves_clauses() -> PlanResult<()> {
        let schema = sum_test_schema(DataType::Utf8)?;
        let argument_display_names = ["value".to_string()];
        let plan_config = Arc::new(PlanConfig::default());
        let session_context = SessionContext::new();
        let filter = col("included");
        let expression = spark_sum(AggFunctionInput {
            arguments: vec![col("value")],
            distinct: true,
            ignore_nulls: None,
            filter: Some(Box::new(filter.clone())),
            order_by: vec![],
            preserve_count_argument_columns: false,
            function_context: FunctionContextInput {
                argument_display_names: &argument_display_names,
                plan_config: &plan_config,
                session_context: &session_context,
                schema: &schema,
            },
        })?;

        assert_eq!(expression.get_type(schema.as_ref())?, DataType::Float64);
        assert!(expression.nullable(schema.as_ref())?);
        let expr::Expr::AggregateFunction(sum) = expression else {
            return Err(PlanError::internal("expected aggregate SUM expression"));
        };
        let masked_argument =
            when(filter.clone(), col("value")).otherwise(lit(ScalarValue::Utf8(None)))?;
        assert_eq!(
            sum.params.args,
            vec![cast(masked_argument, DataType::Float64)]
        );
        assert_eq!(sum.params.filter, Some(Box::new(filter)));
        assert!(sum.params.distinct);
        Ok(())
    }
}
