use std::sync::Arc;

use arrow_schema::{DataType, Field};
use datafusion::functions_aggregate::{
    approx_distinct, approx_median, approx_percentile_cont, array_agg, average, bit_and_or_xor,
    bool_and_or, correlation, count, covariance, first_last, grouping, min_max, percentile_cont,
    stddev, sum, variance,
};
use datafusion_common::ScalarValue;
use datafusion_expr::expr::NullTreatment;
use datafusion_expr::{AggregateUDF, Operator, ScalarUDF};
use datafusion_spark::function::aggregate::try_sum::SparkTrySum;
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
use sail_function::sketch::{DEFAULT_HLL_LG_CONFIG_K, DEFAULT_THETA_LG_NOM_ENTRIES};

use self::AggregateFunctionBuilder as F;
use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::{Expr, ExprKind, ExprType, SortKey};
use crate::analyzer::typing;
use crate::error::{PlanError, PlanResult};
use crate::function::common::FunctionContextInput;

#[derive(Default)]
pub(crate) struct AggregateOptions {
    pub distinct: bool,
    pub window: bool,
    pub ignore_nulls: Option<bool>,
    pub filter: Option<Box<Expr>>,
    pub order_by: Vec<SortKey>,
}

pub(crate) fn aggregate_call_with_options(
    context: &mut FunctionContextInput<'_>,
    function: Arc<AggregateUDF>,
    arguments: Vec<Expr>,
    options: AggregateOptions,
) -> PlanResult<Expr> {
    let fields = typing::argument_fields(&arguments)?;
    let resolved = crate::analyzer::typing::aggregate::resolve(&function, &fields)?;
    let arguments = typing::coerce_arguments(arguments, &resolved.arguments, context.ids)?;
    let filter = options
        .filter
        .map(|value| context.cast(*value, DataType::Boolean).map(Box::new))
        .transpose()?;
    Ok(Expr {
        id: context.ids.expression(),
        kind: Deferred::Resolved(ExprKind::AggregateFunction {
            function,
            arguments,
            distinct: options.distinct,
            filter,
            order_by: options.order_by,
            null_treatment: options.ignore_nulls.map(|ignore| {
                if ignore {
                    NullTreatment::IgnoreNulls
                } else {
                    NullTreatment::RespectNulls
                }
            }),
        }),
        result_type: Deferred::Resolved(ExprType::from_field(&resolved.result)),
    })
}

pub(crate) fn spark_aggregate(
    context: &mut FunctionContextInput<'_>,
    name: &str,
    function: Arc<AggregateUDF>,
    args: Vec<Expr>,
    options: AggregateOptions,
) -> PlanResult<Expr> {
    let resolver = crate::function::get_built_in_aggregate_function(name)
        .map(|function| function.resolver)
        .unwrap_or(resolve_aggregate);
    resolver(context, name, function, args, options)
}

fn resolve_aggregate(
    context: &mut FunctionContextInput<'_>,
    _name: &str,
    function: Arc<AggregateUDF>,
    args: Vec<Expr>,
    options: AggregateOptions,
) -> PlanResult<Expr> {
    aggregate_call_with_options(context, function, args, options)
}
fn spark_sum(
    context: &mut FunctionContextInput<'_>,
    _name: &str,
    function: Arc<AggregateUDF>,
    mut args: Vec<Expr>,
    options: AggregateOptions,
) -> PlanResult<Expr> {
    args = coerce_string_sum_arguments(context, args, options.filter.as_deref())?;

    aggregate_call_with_options(context, function, args, options)
}
fn kurtosis(
    context: &mut FunctionContextInput<'_>,
    _name: &str,
    function: Arc<AggregateUDF>,
    mut args: Vec<Expr>,
    options: AggregateOptions,
) -> PlanResult<Expr> {
    args = args
        .into_iter()
        .map(|a| context.cast(a, DataType::Float64))
        .collect::<PlanResult<_>>()?;

    aggregate_call_with_options(context, function, args, options)
}
fn median(
    context: &mut FunctionContextInput<'_>,
    _name: &str,
    function: Arc<AggregateUDF>,
    mut args: Vec<Expr>,
    options: AggregateOptions,
) -> PlanResult<Expr> {
    if !options.window {
        args.push(context.literal(ScalarValue::Float64(Some(0.5))));
    }
    let window = options.window;
    let result = aggregate_call_with_options(context, function, args, options)?;
    if window {
        context.cast(result, DataType::Float64)
    } else {
        Ok(result)
    }
}
fn percentile_cont(
    context: &mut FunctionContextInput<'_>,
    _name: &str,
    function: Arc<AggregateUDF>,
    mut args: Vec<Expr>,
    options: AggregateOptions,
) -> PlanResult<Expr> {
    let percentile = args.one()?;
    let sort = options
        .order_by
        .first()
        .filter(|_| options.order_by.len() == 1)
        .ok_or_else(|| {
            PlanError::invalid("percentile requires one WITHIN_GROUP ordering expression")
        })?;
    let value = context.copy_expression(&sort.expression)?;
    args = vec![value, percentile];

    aggregate_call_with_options(context, function, args, options)
}
fn hll_sketch_agg(
    context: &mut FunctionContextInput<'_>,
    name: &str,
    function: Arc<AggregateUDF>,
    mut args: Vec<Expr>,
    options: AggregateOptions,
) -> PlanResult<Expr> {
    if args.len() == 1 {
        args.push(context.literal(match name {
            "hll_sketch_agg" => ScalarValue::Int32(Some(DEFAULT_HLL_LG_CONFIG_K)),
            "hll_union_agg" => ScalarValue::Boolean(Some(false)),
            _ => ScalarValue::Int32(Some(DEFAULT_THETA_LG_NOM_ENTRIES)),
        }));
    }
    let (value, config) = args.two()?;
    let config = context.cast(
        config,
        if name == "hll_union_agg" {
            DataType::Boolean
        } else {
            DataType::Int32
        },
    )?;
    args = vec![value, config];

    aggregate_call_with_options(context, function, args, options)
}
fn count_min_sketch(
    context: &mut FunctionContextInput<'_>,
    name: &str,
    function: Arc<AggregateUDF>,
    args: Vec<Expr>,
    options: AggregateOptions,
) -> PlanResult<Expr> {
    crate::analyzer::typing::coercion::validate_arity(name, args.len(), 4)?;

    aggregate_call_with_options(context, function, args, options)
}
fn count_if(
    context: &mut FunctionContextInput<'_>,
    _name: &str,
    function: Arc<AggregateUDF>,
    mut args: Vec<Expr>,
    mut options: AggregateOptions,
) -> PlanResult<Expr> {
    let predicate = args.one()?;
    let zero = context.literal(ScalarValue::Int32(Some(0)));
    if options.window {
        let null = context.literal(ScalarValue::Int32(None));
        args = vec![context.case(vec![(predicate, zero)], Some(null))?];
    } else {
        options.filter = Some(Box::new(predicate));
        args = vec![zero];
    }

    aggregate_call_with_options(context, function, args, options)
}
fn count(
    context: &mut FunctionContextInput<'_>,
    _name: &str,
    function: Arc<AggregateUDF>,
    mut args: Vec<Expr>,
    mut options: AggregateOptions,
) -> PlanResult<Expr> {
    if args.is_empty() {
        if !context.config.legacy_allow_parameterless_count {
            return Err(PlanError::invalid(
                "The count function requires at least one parameter; set spark.sql.legacy.allowParameterlessCount to true to restore the legacy COUNT() result of zero",
            ));
        }
        args.push(context.literal(ScalarValue::Null));
    }
    if args.iter().all(is_typed_null_literal) {
        args = vec![context.literal(ScalarValue::Null)];
        options.distinct = false;
    } else if args.len() > 1 {
        let mut any_null = None;
        for value in &args {
            let value = context.copy_expression(value)?;
            let test = context.is_null(value);
            any_null = Some(match any_null {
                Some(previous) => context.binary(previous, Operator::Or, test)?,
                None => test,
            });
        }
        let structure = context.scalar_udf(
            Arc::new(ScalarUDF::from(StructFunction::new(
                (0..args.len()).map(|i| format!("col{i}")).collect(),
            ))),
            args,
        )?;
        let null = context.literal(ScalarValue::Null);
        args = vec![context.case(
            vec![(
                any_null.ok_or_else(|| PlanError::internal("missing count arguments"))?,
                null,
            )],
            Some(structure),
        )?];
    }

    aggregate_call_with_options(context, function, args, options)
}
fn array_agg_compacted(
    context: &mut FunctionContextInput<'_>,
    name: &str,
    function: Arc<AggregateUDF>,
    args: Vec<Expr>,
    mut options: AggregateOptions,
) -> PlanResult<Expr> {
    let arg = args.one()?;
    let element_type = arg
        .result_type
        .resolved("collect argument")?
        .data_type
        .clone();
    if options.window && matches!(name, "collect_list") {
        return aggregate_call_with_options(context, function, vec![arg], options);
    }
    options.ignore_nulls = if options.window {
        Some(true)
    } else {
        options.ignore_nulls.or(Some(true))
    };
    options.distinct |= name == "collect_set";
    if options.distinct && options.ignore_nulls == Some(true) {
        let copy = context.copy_expression(&arg)?;
        let test = context.is_not_null(copy);
        options.filter = Some(Box::new(match options.filter {
            Some(previous) => context.binary(*previous, Operator::And, test)?,
            None => test,
        }));
        options.ignore_nulls = if options.window { Some(true) } else { None };
    }
    let window = options.window;
    let agg = aggregate_call_with_options(context, function, vec![arg], options)?;
    if window {
        return Ok(agg);
    }
    let empty = context.literal(ScalarValue::List(ScalarValue::new_list_nullable(
        &[],
        &element_type,
    )));
    context.scalar("coalesce", vec![agg, empty])
}
fn listagg(
    context: &mut FunctionContextInput<'_>,
    _name: &str,
    function: Arc<AggregateUDF>,
    args: Vec<Expr>,
    mut options: AggregateOptions,
) -> PlanResult<Expr> {
    let (value, rest) = args.at_least_one()?;
    let kind = value
        .result_type
        .resolved("listagg argument")?
        .data_type
        .clone();
    if kind == DataType::Null {
        return Ok(context.literal(ScalarValue::Null));
    }
    let delimiter = rest
        .into_iter()
        .next()
        .unwrap_or_else(|| context.literal(ScalarValue::Utf8(Some(String::new()))));
    if options.distinct && !options.window {
        options.order_by = vec![SortKey {
            expression: context.copy_expression(&value)?,
            ascending: true,
            nulls_first: true,
        }];
    }
    options.ignore_nulls = Some(true);
    let result = aggregate_call_with_options(context, function, vec![value], options)?;
    let result = context.cast(
        result,
        DataType::List(Arc::new(Field::new(
            SAIL_LIST_FIELD_NAME,
            DataType::Utf8,
            true,
        ))),
    )?;
    let delimiter = context.cast(delimiter, DataType::Utf8)?;
    let result = context.scalar("array_to_string", vec![result, delimiter])?;
    let result = match kind {
        DataType::Binary | DataType::BinaryView => context.cast(result, DataType::Binary)?,
        DataType::LargeBinary => context.cast(result, DataType::LargeBinary)?,
        _ => result,
    };
    let copy = context.copy_expression(&result)?;
    let test = context.is_not_null(copy);
    let yes = context.literal(ScalarValue::Boolean(Some(true)));
    let null = context.literal(ScalarValue::Null);
    context.case(vec![(test, result), (yes, null)], None)
}
fn mode(
    context: &mut FunctionContextInput<'_>,
    _name: &str,
    function: Arc<AggregateUDF>,
    mut args: Vec<Expr>,
    options: AggregateOptions,
) -> PlanResult<Expr> {
    if args.is_empty() {
        let [sort] = options.order_by.as_slice() else {
            return Err(PlanError::analysis(
                "mode requires one WITHIN_GROUP ordering expression",
            ));
        };
        args.push(context.copy_expression(&sort.expression)?);
    }
    aggregate_call_with_options(context, function, args, options)
}
fn avg(
    context: &mut FunctionContextInput<'_>,
    name: &str,
    function: Arc<AggregateUDF>,
    mut args: Vec<Expr>,
    mut options: AggregateOptions,
) -> PlanResult<Expr> {
    if args.len() == 2 {
        if options.ignore_nulls.is_some() {
            return Err(PlanError::invalid(
                "arguments conflict with IGNORE NULLS clause",
            ));
        }
        let flag = args
            .pop()
            .ok_or_else(|| PlanError::internal("missing null treatment"))?;
        options.ignore_nulls = Some(match flag.kind.into_resolved("null treatment")? {
            ExprKind::Literal(ScalarValue::Boolean(Some(flag))) => flag,
            _ => {
                return Err(PlanError::invalid(
                    "requires a boolean literal as the second argument",
                ));
            }
        });
    }
    crate::analyzer::typing::coercion::validate_arity(name, args.len(), 1)?;
    if args[0].result_type.resolved("average argument")?.data_type == DataType::Null {
        return Ok(context.literal(ScalarValue::Null));
    }
    aggregate_call_with_options(context, function, args, options)
}
fn first_value(
    context: &mut FunctionContextInput<'_>,
    name: &str,
    function: Arc<AggregateUDF>,
    mut args: Vec<Expr>,
    mut options: AggregateOptions,
) -> PlanResult<Expr> {
    if args.len() == 2 {
        if options.ignore_nulls.is_some() {
            return Err(PlanError::invalid(
                "arguments conflict with IGNORE NULLS clause",
            ));
        }
        let flag = args
            .pop()
            .ok_or_else(|| PlanError::internal("missing null treatment"))?;
        options.ignore_nulls = Some(match flag.kind.into_resolved("null treatment")? {
            ExprKind::Literal(ScalarValue::Boolean(Some(flag))) => flag,
            _ => {
                return Err(PlanError::invalid(
                    "requires a boolean literal as the second argument",
                ));
            }
        });
    }
    crate::analyzer::typing::coercion::validate_arity(name, args.len(), 1)?;
    if !options.window {
        options.distinct = false;
    }
    aggregate_call_with_options(context, function, args, options)
}
fn duplicate_agnostic_extreme(
    context: &mut FunctionContextInput<'_>,
    _name: &str,
    function: Arc<AggregateUDF>,
    args: Vec<Expr>,
    mut options: AggregateOptions,
) -> PlanResult<Expr> {
    if !options.window {
        options.distinct = false;
    }
    aggregate_call_with_options(context, function, args, options)
}
fn grouping_id(
    context: &mut FunctionContextInput<'_>,
    _name: &str,
    function: Arc<AggregateUDF>,
    args: Vec<Expr>,
    options: AggregateOptions,
) -> PlanResult<Expr> {
    if options.distinct
        || options.ignore_nulls.is_some()
        || options.filter.is_some()
        || !options.order_by.is_empty()
    {
        return Err(PlanError::invalid("invalid grouping_id function clause"));
    }
    aggregate_call_with_options(context, function, args, options)
}
fn approx_count_distinct(
    context: &mut FunctionContextInput<'_>,
    _name: &str,
    function: Arc<AggregateUDF>,
    args: Vec<Expr>,
    options: AggregateOptions,
) -> PlanResult<Expr> {
    let result = aggregate_call_with_options(context, function, args, options)?;
    context.cast(result, DataType::Int64)
}

pub(crate) fn coerce_string_sum_arguments(
    context: &mut FunctionContextInput<'_>,
    args: Vec<Expr>,
    filter: Option<&Expr>,
) -> PlanResult<Vec<Expr>> {
    args.into_iter()
        .map(|mut arg| {
            let kind = arg.result_type.resolved("sum argument")?.data_type.clone();
            if !kind.is_string() {
                return Ok(arg);
            }
            if context.config.ansi_mode {
                if let Some(filter) = filter {
                    let filter = context.copy_expression(filter)?;
                    let null = context.literal(ScalarValue::try_from(&kind)?);
                    arg = context.case(vec![(filter, arg)], Some(null))?;
                }
                context.cast(arg, DataType::Float64)
            } else {
                context.try_cast(arg, DataType::Float64)
            }
        })
        .collect()
}

fn is_typed_null_literal(value: &Expr) -> bool {
    match &value.kind {
        Deferred::Resolved(ExprKind::Literal(value)) => value.is_null(),
        Deferred::Resolved(
            ExprKind::Cast { expression, .. } | ExprKind::WithMetadata(expression),
        ) => is_typed_null_literal(expression),
        _ => false,
    }
}

pub(crate) struct AggregateFunctionInput<'a> {
    pub arguments: Vec<Expr>,
    pub options: AggregateOptions,
    pub context: FunctionContextInput<'a>,
}

type AggregateKernel =
    Arc<dyn Fn(&FunctionContextInput<'_>) -> PlanResult<Arc<AggregateUDF>> + Send + Sync>;

type AggregateResolver = fn(
    &mut FunctionContextInput<'_>,
    &str,
    Arc<AggregateUDF>,
    Vec<Expr>,
    AggregateOptions,
) -> PlanResult<Expr>;

#[derive(Clone)]
pub(crate) struct AggregateFunction {
    name: String,
    resolver: AggregateResolver,
    kernel: AggregateKernel,
}

impl AggregateFunction {
    fn with_resolver(mut self, resolver: AggregateResolver) -> Self {
        self.resolver = resolver;
        self
    }

    pub(crate) fn kernel(
        &self,
        context: &FunctionContextInput<'_>,
    ) -> PlanResult<Arc<AggregateUDF>> {
        (self.kernel)(context)
    }

    pub(crate) fn call(&self, mut input: AggregateFunctionInput<'_>) -> PlanResult<Expr> {
        let kernel = self.kernel(&input.context)?;
        (self.resolver)(
            &mut input.context,
            &self.name,
            kernel,
            input.arguments,
            input.options,
        )
    }
}

pub(crate) struct AggregateFunctionBuilder;
impl AggregateFunctionBuilder {
    pub fn custom(
        name: impl Into<String>,
        kernel: impl Fn(&FunctionContextInput<'_>) -> PlanResult<Arc<AggregateUDF>>
        + Send
        + Sync
        + 'static,
    ) -> AggregateFunction {
        AggregateFunction {
            name: name.into(),
            resolver: resolve_aggregate,
            kernel: Arc::new(kernel),
        }
    }
    pub fn aggregate_udf(
        name: impl Into<String>,
        kernel: impl Fn() -> Arc<AggregateUDF> + Send + Sync + 'static,
    ) -> AggregateFunction {
        Self::custom(name, move |_| Ok(kernel()))
    }
    pub fn kernel(name: &'static str) -> AggregateFunction {
        use datafusion_expr::registry::FunctionRegistry;
        Self::custom(name, move |context| Ok(context.session.udaf(name)?))
    }
}

pub(crate) fn list_built_in_aggregate_functions() -> Vec<(&'static str, AggregateFunction)> {
    vec![
        ("kll_merge_agg_bigint", F::kernel("kll_merge_agg_bigint")),
        ("kll_merge_agg_double", F::kernel("kll_merge_agg_double")),
        ("kll_merge_agg_float", F::kernel("kll_merge_agg_float")),
        ("measure", F::kernel("measure")),
        (
            "tuple_intersection_agg_double",
            F::kernel("tuple_intersection_agg_double"),
        ),
        (
            "tuple_intersection_agg_integer",
            F::kernel("tuple_intersection_agg_integer"),
        ),
        (
            "tuple_sketch_agg_double",
            F::kernel("tuple_sketch_agg_double"),
        ),
        (
            "tuple_sketch_agg_integer",
            F::kernel("tuple_sketch_agg_integer"),
        ),
        (
            "tuple_union_agg_double",
            F::kernel("tuple_union_agg_double"),
        ),
        (
            "tuple_union_agg_integer",
            F::kernel("tuple_union_agg_integer"),
        ),
        ("vector_avg", F::kernel("vector_avg")),
        ("vector_sum", F::kernel("vector_sum")),
        ("any", F::aggregate_udf("any", bool_and_or::bool_or_udaf)),
        ("some", F::aggregate_udf("some", bool_and_or::bool_or_udaf)),
        (
            "bool_or",
            F::aggregate_udf("bool_or", bool_and_or::bool_or_udaf),
        ),
        (
            "every",
            F::aggregate_udf("every", bool_and_or::bool_and_udaf),
        ),
        (
            "bool_and",
            F::aggregate_udf("bool_and", bool_and_or::bool_and_udaf),
        ),
        (
            "avg",
            F::aggregate_udf("avg", average::avg_udaf).with_resolver(avg),
        ),
        ("mean", F::aggregate_udf("mean", average::avg_udaf)),
        (
            "array_agg",
            F::aggregate_udf("array_agg", array_agg::array_agg_udaf)
                .with_resolver(array_agg_compacted),
        ),
        (
            "collect_list",
            F::aggregate_udf("collect_list", array_agg::array_agg_udaf)
                .with_resolver(array_agg_compacted),
        ),
        (
            "collect_set",
            F::aggregate_udf("collect_set", array_agg::array_agg_udaf)
                .with_resolver(array_agg_compacted),
        ),
        (
            "listagg",
            F::aggregate_udf("listagg", array_agg::array_agg_udaf).with_resolver(listagg),
        ),
        (
            "string_agg",
            F::aggregate_udf("string_agg", array_agg::array_agg_udaf).with_resolver(listagg),
        ),
        (
            "count",
            F::aggregate_udf("count", count::count_udaf).with_resolver(count),
        ),
        (
            "count_if",
            F::aggregate_udf("count_if", count::count_udaf).with_resolver(count_if),
        ),
        (
            "sum",
            F::aggregate_udf("sum", sum::sum_udaf).with_resolver(spark_sum),
        ),
        (
            "first",
            F::aggregate_udf("first", first_last::first_value_udaf).with_resolver(first_value),
        ),
        (
            "first_value",
            F::aggregate_udf("first_value", first_last::first_value_udaf)
                .with_resolver(first_value),
        ),
        (
            "any_value",
            F::aggregate_udf("any_value", first_last::first_value_udaf).with_resolver(first_value),
        ),
        (
            "last",
            F::aggregate_udf("last", first_last::last_value_udaf).with_resolver(first_value),
        ),
        (
            "last_value",
            F::aggregate_udf("last_value", first_last::last_value_udaf).with_resolver(first_value),
        ),
        (
            "min",
            F::aggregate_udf("min", min_max::min_udaf).with_resolver(duplicate_agnostic_extreme),
        ),
        (
            "max",
            F::aggregate_udf("max", min_max::max_udaf).with_resolver(duplicate_agnostic_extreme),
        ),
        (
            "approx_count_distinct",
            F::aggregate_udf("approx_count_distinct", || {
                approx_distinct::approx_distinct_udaf()
            })
            .with_resolver(approx_count_distinct),
        ),
        (
            "approx_percentile",
            F::aggregate_udf("approx_percentile", || {
                approx_percentile_cont::approx_percentile_cont_udaf()
            }),
        ),
        (
            "percentile_approx",
            F::aggregate_udf("percentile_approx", || {
                approx_percentile_cont::approx_percentile_cont_udaf()
            }),
        ),
        (
            "approx_percentile_cont",
            F::aggregate_udf("approx_percentile_cont", || {
                approx_percentile_cont::approx_percentile_cont_udaf()
            }),
        ),
        (
            "approx_median",
            F::aggregate_udf("approx_median", approx_median::approx_median_udaf),
        ),
        (
            "percentile_cont",
            F::aggregate_udf("percentile_cont", || {
                percentile_cont::percentile_cont_udaf()
            })
            .with_resolver(percentile_cont),
        ),
        (
            "bit_and",
            F::aggregate_udf("bit_and", bit_and_or_xor::bit_and_udaf),
        ),
        (
            "bit_or",
            F::aggregate_udf("bit_or", bit_and_or_xor::bit_or_udaf),
        ),
        (
            "bit_xor",
            F::aggregate_udf("bit_xor", bit_and_or_xor::bit_xor_udaf),
        ),
        ("corr", F::aggregate_udf("corr", correlation::corr_udaf)),
        (
            "covar_pop",
            F::aggregate_udf("covar_pop", covariance::covar_pop_udaf),
        ),
        (
            "covar_samp",
            F::aggregate_udf("covar_samp", covariance::covar_samp_udaf),
        ),
        (
            "grouping",
            F::aggregate_udf("grouping", grouping::grouping_udaf),
        ),
        ("std", F::aggregate_udf("std", stddev::stddev_udaf)),
        ("stddev", F::aggregate_udf("stddev", stddev::stddev_udaf)),
        (
            "stddev_samp",
            F::aggregate_udf("stddev_samp", stddev::stddev_udaf),
        ),
        (
            "stddev_pop",
            F::aggregate_udf("stddev_pop", stddev::stddev_pop_udaf),
        ),
        (
            "var_pop",
            F::aggregate_udf("var_pop", variance::var_pop_udaf),
        ),
        (
            "var_samp",
            F::aggregate_udf("var_samp", variance::var_samp_udaf),
        ),
        (
            "variance",
            F::aggregate_udf("variance", variance::var_samp_udaf),
        ),
        (
            "bitmap_and_agg",
            F::aggregate_udf("bitmap_and_agg", || {
                Arc::new(AggregateUDF::from(BitmapAndAggFunction::new()))
            }),
        ),
        (
            "bitmap_construct_agg",
            F::aggregate_udf("bitmap_construct_agg", || {
                Arc::new(AggregateUDF::from(BitmapConstructAggFunction::new()))
            }),
        ),
        (
            "bitmap_or_agg",
            F::aggregate_udf("bitmap_or_agg", || {
                Arc::new(AggregateUDF::from(BitmapOrAggFunction::new()))
            }),
        ),
        (
            "count_min_sketch",
            F::aggregate_udf("count_min_sketch", || {
                Arc::new(AggregateUDF::from(CountMinSketchFunction::new()))
            })
            .with_resolver(count_min_sketch),
        ),
        (
            "grouping_id",
            F::aggregate_udf("grouping_id", || {
                Arc::new(AggregateUDF::from(GroupingIdFunction::new()))
            })
            .with_resolver(grouping_id),
        ),
        (
            "histogram_numeric",
            F::aggregate_udf("histogram_numeric", || {
                Arc::new(AggregateUDF::from(HistogramNumericFunction::new()))
            }),
        ),
        (
            "hll_sketch_agg",
            F::aggregate_udf("hll_sketch_agg", || {
                Arc::new(AggregateUDF::from(HllSketchAggFunction::new()))
            })
            .with_resolver(hll_sketch_agg),
        ),
        (
            "hll_union_agg",
            F::aggregate_udf("hll_union_agg", || {
                Arc::new(AggregateUDF::from(HllUnionAggFunction::new()))
            })
            .with_resolver(hll_sketch_agg),
        ),
        (
            "kurtosis",
            F::aggregate_udf("kurtosis", || {
                Arc::new(AggregateUDF::from(KurtosisFunction::new()))
            })
            .with_resolver(kurtosis),
        ),
        (
            "max_by",
            F::aggregate_udf("max_by", || {
                Arc::new(AggregateUDF::from(MaxByFunction::new()))
            }),
        ),
        (
            "min_by",
            F::aggregate_udf("min_by", || {
                Arc::new(AggregateUDF::from(MinByFunction::new()))
            }),
        ),
        (
            "mode",
            F::aggregate_udf("mode", || Arc::new(AggregateUDF::from(ModeFunction::new())))
                .with_resolver(mode),
        ),
        (
            "percentile",
            F::aggregate_udf("percentile", || {
                Arc::new(AggregateUDF::from(PercentileFunction::new()))
            }),
        ),
        (
            "median",
            F::aggregate_udf("median", || {
                Arc::new(AggregateUDF::from(PercentileFunction::new()))
            })
            .with_resolver(median),
        ),
        (
            "product",
            F::aggregate_udf("product", || {
                Arc::new(AggregateUDF::from(ProductFunction::new()))
            })
            .with_resolver(kurtosis),
        ),
        (
            "schema_of_variant_agg",
            F::aggregate_udf("schema_of_variant_agg", || {
                Arc::new(AggregateUDF::from(SchemaOfVariantAggFunction::new()))
            }),
        ),
        (
            "skewness",
            F::aggregate_udf("skewness", || {
                Arc::new(AggregateUDF::from(SkewnessFunc::new()))
            })
            .with_resolver(kurtosis),
        ),
        (
            "theta_intersection_agg",
            F::aggregate_udf("theta_intersection_agg", || {
                Arc::new(AggregateUDF::from(ThetaIntersectionAggFunction::new()))
            }),
        ),
        (
            "theta_sketch_agg",
            F::aggregate_udf("theta_sketch_agg", || {
                Arc::new(AggregateUDF::from(ThetaSketchAggFunction::new()))
            })
            .with_resolver(hll_sketch_agg),
        ),
        (
            "theta_union_agg",
            F::aggregate_udf("theta_union_agg", || {
                Arc::new(AggregateUDF::from(ThetaUnionAggFunction::new()))
            })
            .with_resolver(hll_sketch_agg),
        ),
        (
            "try_avg",
            F::aggregate_udf("try_avg", || {
                Arc::new(AggregateUDF::from(TryAvgFunction::new()))
            }),
        ),
        (
            "try_sum",
            F::aggregate_udf("try_sum", || {
                Arc::new(AggregateUDF::from(SparkTrySum::new()))
            }),
        ),
        (
            "regr_avgx",
            F::aggregate_udf("regr_avgx", || {
                Arc::new(AggregateUDF::from(Regr::new(RegrType::AvgX, "regr_avgx")))
            }),
        ),
        (
            "regr_avgy",
            F::aggregate_udf("regr_avgy", || {
                Arc::new(AggregateUDF::from(Regr::new(RegrType::AvgY, "regr_avgy")))
            }),
        ),
        (
            "regr_count",
            F::aggregate_udf("regr_count", || {
                Arc::new(AggregateUDF::from(Regr::new(RegrType::Count, "regr_count")))
            }),
        ),
        (
            "regr_intercept",
            F::aggregate_udf("regr_intercept", || {
                Arc::new(AggregateUDF::from(Regr::new(
                    RegrType::Intercept,
                    "regr_intercept",
                )))
            }),
        ),
        (
            "regr_r2",
            F::aggregate_udf("regr_r2", || {
                Arc::new(AggregateUDF::from(Regr::new(RegrType::R2, "regr_r2")))
            }),
        ),
        (
            "regr_slope",
            F::aggregate_udf("regr_slope", || {
                Arc::new(AggregateUDF::from(Regr::new(RegrType::Slope, "regr_slope")))
            }),
        ),
        (
            "regr_sxx",
            F::aggregate_udf("regr_sxx", || {
                Arc::new(AggregateUDF::from(Regr::new(RegrType::Sxx, "regr_sxx")))
            }),
        ),
        (
            "regr_sxy",
            F::aggregate_udf("regr_sxy", || {
                Arc::new(AggregateUDF::from(Regr::new(RegrType::Sxy, "regr_sxy")))
            }),
        ),
        (
            "regr_syy",
            F::aggregate_udf("regr_syy", || {
                Arc::new(AggregateUDF::from(Regr::new(RegrType::Syy, "regr_syy")))
            }),
        ),
        (
            "percentile_disc",
            F::custom("percentile_disc", |context| {
                Ok(percentile_disc_udaf(context.config.ansi_mode))
            })
            .with_resolver(percentile_cont),
        ),
    ]
}
