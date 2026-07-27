use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion::functions::expr_fn::coalesce;
use datafusion::functions_aggregate::{
    approx_distinct, approx_percentile_cont, array_agg, average, bit_and_or_xor, bool_and_or,
    correlation, count, covariance, first_last, grouping, min_max, percentile_cont, stddev, sum,
    variance,
};
use datafusion::functions_nested::string::array_to_string;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams};
use datafusion_expr::{
    AggregateUDF, BinaryExpr, ExprSchemable, Operator, ScalarUDF, cast, expr, lit, when,
};
use datafusion_spark::function::aggregate::try_sum::SparkTrySum;
use lazy_static::lazy_static;
use sail_common::spec::SAIL_LIST_FIELD_NAME;
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
    if args
        .first()
        .map(|arg| arg.get_type(input.function_context.schema))
        .transpose()?
        == Some(DataType::Null)
    {
        return Ok(lit(ScalarValue::Null));
    }
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: average::avg_udaf(),
        params: AggregateFunctionParams {
            args,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment,
        },
    }))
}

fn spark_sum(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let AggFunctionInput {
        arguments,
        distinct,
        ignore_nulls,
        filter,
        order_by,
        function_context,
    } = input;
    let supports_linear_rewrite =
        !distinct && ignore_nulls.is_none() && filter.is_none() && order_by.is_empty();
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
    // Do not pre-cast the argument here: `KurtosisFunction::coerce_types` governs
    // the accepted input types (numeric, decimal and numeric strings are coerced
    // to DOUBLE, while e.g. BOOLEAN is rejected like Spark).
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(KurtosisFunction::new())),
        params: AggregateFunctionParams {
            args: input.arguments,
            distinct: input.distinct,
            filter: input.filter,
            order_by: input.order_by,
            null_treatment: get_null_treatment(input.ignore_nulls),
        },
    }))
}

fn product(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    // Do not pre-cast the argument here: `ProductFunction::coerce_types` governs
    // the accepted input types (numeric, decimal and numeric strings are coerced
    // to DOUBLE, while e.g. BOOLEAN is rejected like Spark).
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(ProductFunction::new())),
        params: AggregateFunctionParams {
            args: input.arguments,
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

fn skewness(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    // Do not pre-cast the argument here: `SkewnessFunc::coerce_types` governs the
    // accepted input types (numeric, decimal and numeric strings are coerced to
    // DOUBLE, while e.g. BOOLEAN is rejected like Spark).
    Ok(expr::Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(SkewnessFunc::new())),
        params: AggregateFunctionParams {
            args: input.arguments,
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
        distinct,
        ignore_nulls,
        filter,
        order_by,
        function_context: _,
    } = input;
    let null_treatment = get_null_treatment(ignore_nulls);
    // For COUNT(DISTINCT *), the resolver already expanded the wildcard to column references
    // (with hidden-column filtering). For COUNT(*), convert to COUNT(1).
    let args = transform_count_star_wildcard_expr(arguments);
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
    Ok(coalesce_to_empty_array(agg, &element_type))
}

fn array_agg_compacted(input: AggFunctionInput) -> PlanResult<expr::Expr> {
    let schema = input.function_context.schema;
    // Spark's collect_list ignores NULLs by default
    let ignore_nulls = input.ignore_nulls.or(Some(true));
    let arg = input.arguments.one()?;
    let element_type = arg.get_type(schema)?;

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
    Ok(coalesce_to_empty_array(agg, &element_type))
}

/// Spark's `collect_list`/`collect_set` return an empty array for a group with no (non-NULL)
/// values to collect, but DataFusion's `array_agg` returns NULL over zero rows. Coalesce the
/// aggregate to a typed empty array so the empty case matches Spark instead of yielding NULL.
fn coalesce_to_empty_array(agg: expr::Expr, element_type: &DataType) -> expr::Expr {
    let empty = ScalarValue::List(ScalarValue::new_list_nullable(&[], element_type));
    coalesce(vec![agg, lit(empty)])
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
    Ok(cast(
        expr::Expr::AggregateFunction(AggregateFunction {
            func: approx_distinct::approx_distinct_udaf(),
            params: AggregateFunctionParams {
                args: input.arguments.clone(),
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
        (
            "approx_percentile",
            F::default(approx_percentile_cont::approx_percentile_cont_udaf),
        ),
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
        ("max", F::default(min_max::max_udaf)),
        ("max_by", F::custom(max_by)),
        ("mean", F::default(average::avg_udaf)),
        ("median", F::custom(median)),
        ("min", F::default(min_max::min_udaf)),
        ("min_by", F::custom(min_by)),
        ("mode", F::custom(mode)),
        ("percentile", F::custom(percentile_exact)),
        (
            "percentile_approx",
            F::default(approx_percentile_cont::approx_percentile_cont_udaf),
        ),
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
        ("try_avg", F::custom(try_avg)),
        ("try_sum", F::custom(try_sum)),
        ("var_pop", F::default(variance::var_pop_udaf)),
        ("var_samp", F::default(variance::var_samp_udaf)),
        ("variance", F::default(variance::var_samp_udaf)),
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
