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

use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::analysis::typing;
use crate::error::{PlanError, PlanResult};

pub(super) fn aggregate_kernel(name: &str, ansi: bool) -> Option<Arc<AggregateUDF>> {
    Some(match name {
        "any" | "some" | "bool_or" => bool_and_or::bool_or_udaf(),
        "every" | "bool_and" => bool_and_or::bool_and_udaf(),
        "avg" | "mean" => average::avg_udaf(),
        "array_agg" | "collect_list" | "collect_set" | "listagg" | "string_agg" => {
            array_agg::array_agg_udaf()
        }
        "count" | "count_if" => count::count_udaf(),
        "sum" => sum::sum_udaf(),
        "first" | "first_value" | "any_value" => first_last::first_value_udaf(),
        "last" | "last_value" => first_last::last_value_udaf(),
        "min" => min_max::min_udaf(),
        "max" => min_max::max_udaf(),
        "approx_count_distinct" => approx_distinct::approx_distinct_udaf(),
        "approx_percentile" | "percentile_approx" | "approx_percentile_cont" => {
            approx_percentile_cont::approx_percentile_cont_udaf()
        }
        "approx_median" => approx_median::approx_median_udaf(),
        "percentile_cont" => percentile_cont::percentile_cont_udaf(),
        "bit_and" => bit_and_or_xor::bit_and_udaf(),
        "bit_or" => bit_and_or_xor::bit_or_udaf(),
        "bit_xor" => bit_and_or_xor::bit_xor_udaf(),
        "corr" => correlation::corr_udaf(),
        "covar_pop" => covariance::covar_pop_udaf(),
        "covar_samp" => covariance::covar_samp_udaf(),
        "grouping" => grouping::grouping_udaf(),
        "std" | "stddev" | "stddev_samp" => stddev::stddev_udaf(),
        "stddev_pop" => stddev::stddev_pop_udaf(),
        "var_pop" => variance::var_pop_udaf(),
        "var_samp" | "variance" => variance::var_samp_udaf(),
        "bitmap_and_agg" => Arc::new(AggregateUDF::from(BitmapAndAggFunction::new())),
        "bitmap_construct_agg" => Arc::new(AggregateUDF::from(BitmapConstructAggFunction::new())),
        "bitmap_or_agg" => Arc::new(AggregateUDF::from(BitmapOrAggFunction::new())),
        "count_min_sketch" => Arc::new(AggregateUDF::from(CountMinSketchFunction::new())),
        "grouping_id" => Arc::new(AggregateUDF::from(GroupingIdFunction::new())),
        "histogram_numeric" => Arc::new(AggregateUDF::from(HistogramNumericFunction::new())),
        "hll_sketch_agg" => Arc::new(AggregateUDF::from(HllSketchAggFunction::new())),
        "hll_union_agg" => Arc::new(AggregateUDF::from(HllUnionAggFunction::new())),
        "kurtosis" => Arc::new(AggregateUDF::from(KurtosisFunction::new())),
        "max_by" => Arc::new(AggregateUDF::from(MaxByFunction::new())),
        "min_by" => Arc::new(AggregateUDF::from(MinByFunction::new())),
        "mode" => Arc::new(AggregateUDF::from(ModeFunction::new())),
        "percentile" => Arc::new(AggregateUDF::from(PercentileFunction::new())),
        "median" => Arc::new(AggregateUDF::from(PercentileFunction::new())),
        "product" => Arc::new(AggregateUDF::from(ProductFunction::new())),
        "schema_of_variant_agg" => Arc::new(AggregateUDF::from(SchemaOfVariantAggFunction::new())),
        "skewness" => Arc::new(AggregateUDF::from(SkewnessFunc::new())),
        "theta_intersection_agg" => {
            Arc::new(AggregateUDF::from(ThetaIntersectionAggFunction::new()))
        }
        "theta_sketch_agg" => Arc::new(AggregateUDF::from(ThetaSketchAggFunction::new())),
        "theta_union_agg" => Arc::new(AggregateUDF::from(ThetaUnionAggFunction::new())),
        "try_avg" => Arc::new(AggregateUDF::from(TryAvgFunction::new())),
        "try_sum" => Arc::new(AggregateUDF::from(SparkTrySum::new())),
        "regr_avgx" => Arc::new(AggregateUDF::from(Regr::new(RegrType::AvgX, "regr_avgx"))),
        "regr_avgy" => Arc::new(AggregateUDF::from(Regr::new(RegrType::AvgY, "regr_avgy"))),
        "regr_count" => Arc::new(AggregateUDF::from(Regr::new(RegrType::Count, "regr_count"))),
        "regr_intercept" => Arc::new(AggregateUDF::from(Regr::new(
            RegrType::Intercept,
            "regr_intercept",
        ))),
        "regr_r2" => Arc::new(AggregateUDF::from(Regr::new(RegrType::R2, "regr_r2"))),
        "regr_slope" => Arc::new(AggregateUDF::from(Regr::new(RegrType::Slope, "regr_slope"))),
        "regr_sxx" => Arc::new(AggregateUDF::from(Regr::new(RegrType::Sxx, "regr_sxx"))),
        "regr_sxy" => Arc::new(AggregateUDF::from(Regr::new(RegrType::Sxy, "regr_sxy"))),
        "regr_syy" => Arc::new(AggregateUDF::from(Regr::new(RegrType::Syy, "regr_syy"))),
        "percentile_disc" => percentile_disc_udaf(ansi),
        _ => return None,
    })
}

#[derive(Default)]
pub(super) struct AggregateOptions {
    pub distinct: bool,
    pub window: bool,
    pub ignore_nulls: Option<bool>,
    pub filter: Option<Box<LogicalExpression>>,
    pub order_by: Vec<SortExpression>,
}

impl Resolver<'_, '_> {
    pub(super) fn aggregate_call_with_options(
        &mut self,
        function: Arc<AggregateUDF>,
        arguments: Vec<LogicalExpression>,
        options: AggregateOptions,
    ) -> PlanResult<LogicalExpression> {
        let fields = typing::argument_fields(&arguments)?;
        let resolved = sail_type_system::aggregate::resolve(&function, &fields)?;
        let arguments =
            typing::coerce_arguments(arguments, &resolved.arguments, &mut self.context.ids)?;
        let filter = options
            .filter
            .map(|value| self.cast(*value, DataType::Boolean).map(Box::new))
            .transpose()?;
        Ok(LogicalExpression {
            id: self.context.ids.expression(),
            node: Deferred::Resolved(ExpressionNode::AggregateFunction {
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
            result_type: Deferred::Resolved(ExpressionType::from_field(&resolved.result)),
        })
    }

    pub(super) fn spark_aggregate(
        &mut self,
        name: &str,
        function: Arc<AggregateUDF>,
        mut args: Vec<LogicalExpression>,
        mut options: AggregateOptions,
    ) -> PlanResult<LogicalExpression> {
        if name == "mode" && args.is_empty() {
            let [sort] = options.order_by.as_slice() else {
                return Err(PlanError::analysis(
                    "mode requires one WITHIN_GROUP ordering expression",
                ));
            };
            args.push(self.copy_expression(&sort.expression)?);
        }
        if matches!(
            name,
            "avg" | "first" | "first_value" | "last" | "last_value" | "any_value"
        ) {
            if args.len() == 2 {
                if options.ignore_nulls.is_some() {
                    return Err(PlanError::invalid(
                        "arguments conflict with IGNORE NULLS clause",
                    ));
                }
                let flag = args
                    .pop()
                    .ok_or_else(|| PlanError::internal("missing null treatment"))?;
                options.ignore_nulls = Some(match flag.node.into_resolved("null treatment")? {
                    ExpressionNode::Literal(ScalarValue::Boolean(Some(flag))) => flag,
                    _ => {
                        return Err(PlanError::invalid(
                            "requires a boolean literal as the second argument",
                        ));
                    }
                });
            }
            sail_type_system::coercion::validate_arity(name, args.len(), 1)?;
            if name == "avg"
                && args[0].result_type.resolved("average argument")?.data_type == DataType::Null
            {
                return Ok(self.literal(ScalarValue::Null));
            }
        }
        if matches!(
            name,
            "first" | "first_value" | "last" | "last_value" | "any_value" | "min" | "max"
        ) && !options.window
        {
            options.distinct = false;
        }
        if name == "grouping_id"
            && (options.distinct
                || options.ignore_nulls.is_some()
                || options.filter.is_some()
                || !options.order_by.is_empty())
        {
            return Err(PlanError::invalid("invalid grouping_id function clause"));
        }
        match name {
            "sum" => {
                args = self.string_sum_arguments(args, options.filter.as_deref())?;
            }
            "kurtosis" | "product" | "skewness" => {
                args = args
                    .into_iter()
                    .map(|a| self.cast(a, DataType::Float64))
                    .collect::<PlanResult<_>>()?;
            }
            "median" if !options.window => args.push(self.literal(ScalarValue::Float64(Some(0.5)))),
            "percentile_cont" | "percentile_disc" => {
                let percentile = args.one()?;
                let sort = options
                    .order_by
                    .first()
                    .filter(|_| options.order_by.len() == 1)
                    .ok_or_else(|| {
                        PlanError::invalid(
                            "percentile requires one WITHIN_GROUP ordering expression",
                        )
                    })?;
                let value = self.copy_expression(&sort.expression)?;
                args = vec![value, percentile];
            }
            "hll_sketch_agg" | "hll_union_agg" | "theta_sketch_agg" | "theta_union_agg" => {
                if args.len() == 1 {
                    args.push(self.literal(match name {
                        "hll_sketch_agg" => ScalarValue::Int32(Some(DEFAULT_HLL_LG_CONFIG_K)),
                        "hll_union_agg" => ScalarValue::Boolean(Some(false)),
                        _ => ScalarValue::Int32(Some(DEFAULT_THETA_LG_NOM_ENTRIES)),
                    }));
                }
                let (value, config) = args.two()?;
                let config = self.cast(
                    config,
                    if name == "hll_union_agg" {
                        DataType::Boolean
                    } else {
                        DataType::Int32
                    },
                )?;
                args = vec![value, config];
            }
            "count_min_sketch" => sail_type_system::coercion::validate_arity(name, args.len(), 4)?,
            "count_if" => {
                let predicate = args.one()?;
                let zero = self.literal(ScalarValue::Int32(Some(0)));
                if options.window {
                    let null = self.literal(ScalarValue::Int32(None));
                    args = vec![self.case(vec![(predicate, zero)], Some(null))?];
                } else {
                    options.filter = Some(Box::new(predicate));
                    args = vec![zero];
                }
            }
            "count" => {
                if args.is_empty() {
                    if !self.context.config.legacy_allow_parameterless_count {
                        return Err(PlanError::invalid(
                            "The count function requires at least one parameter; set spark.sql.legacy.allowParameterlessCount to true to restore the legacy COUNT() result of zero",
                        ));
                    }
                    args.push(self.literal(ScalarValue::Null));
                }
                if args.iter().all(typed_null_literal) {
                    args = vec![self.literal(ScalarValue::Null)];
                    options.distinct = false;
                } else if args.len() > 1 {
                    let mut any_null = None;
                    for value in &args {
                        let value = self.copy_expression(value)?;
                        let test = self.is_null(value);
                        any_null = Some(match any_null {
                            Some(previous) => self.binary(previous, Operator::Or, test)?,
                            None => test,
                        });
                    }
                    let structure = self.scalar_udf(
                        Arc::new(ScalarUDF::from(StructFunction::new(
                            (0..args.len()).map(|i| format!("col{i}")).collect(),
                        ))),
                        args,
                    )?;
                    let null = self.literal(ScalarValue::Null);
                    args =
                        vec![self.case(
                            vec![(
                                any_null.ok_or_else(|| {
                                    PlanError::internal("missing count arguments")
                                })?,
                                null,
                            )],
                            Some(structure),
                        )?];
                }
            }
            "array_agg" | "collect_list" | "collect_set" => {
                let arg = args.one()?;
                let element_type = arg
                    .result_type
                    .resolved("collect argument")?
                    .data_type
                    .clone();
                if options.window && name == "collect_list" {
                    return self.aggregate_call_with_options(function, vec![arg], options);
                }
                options.ignore_nulls = if options.window {
                    Some(true)
                } else {
                    options.ignore_nulls.or(Some(true))
                };
                options.distinct |= name == "collect_set";
                if options.distinct && options.ignore_nulls == Some(true) {
                    let copy = self.copy_expression(&arg)?;
                    let test = self.is_not_null(copy);
                    options.filter = Some(Box::new(match options.filter {
                        Some(previous) => self.binary(*previous, Operator::And, test)?,
                        None => test,
                    }));
                    options.ignore_nulls = if options.window { Some(true) } else { None };
                }
                let window = options.window;
                let agg = self.aggregate_call_with_options(function, vec![arg], options)?;
                if window {
                    return Ok(agg);
                }
                let empty = self.literal(ScalarValue::List(ScalarValue::new_list_nullable(
                    &[],
                    &element_type,
                )));
                return self.scalar("coalesce", vec![agg, empty]);
            }
            "listagg" | "string_agg" => {
                let (value, rest) = args.at_least_one()?;
                let kind = value
                    .result_type
                    .resolved("listagg argument")?
                    .data_type
                    .clone();
                if kind == DataType::Null {
                    return Ok(self.literal(ScalarValue::Null));
                }
                let delimiter = rest
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| self.literal(ScalarValue::Utf8(Some(String::new()))));
                if options.distinct && !options.window {
                    options.order_by = vec![SortExpression {
                        expression: self.copy_expression(&value)?,
                        ascending: true,
                        nulls_first: true,
                    }];
                }
                options.ignore_nulls = Some(true);
                let result = self.aggregate_call_with_options(function, vec![value], options)?;
                let result = self.cast(
                    result,
                    DataType::List(Arc::new(Field::new(
                        SAIL_LIST_FIELD_NAME,
                        DataType::Utf8,
                        true,
                    ))),
                )?;
                let delimiter = self.cast(delimiter, DataType::Utf8)?;
                let result = self.scalar("array_to_string", vec![result, delimiter])?;
                let result = match kind {
                    DataType::Binary | DataType::BinaryView => {
                        self.cast(result, DataType::Binary)?
                    }
                    DataType::LargeBinary => self.cast(result, DataType::LargeBinary)?,
                    _ => result,
                };
                let copy = self.copy_expression(&result)?;
                let test = self.is_not_null(copy);
                let yes = self.literal(ScalarValue::Boolean(Some(true)));
                let null = self.literal(ScalarValue::Null);
                return self.case(vec![(test, result), (yes, null)], None);
            }
            _ => {}
        }
        let window = options.window;
        let result = self.aggregate_call_with_options(function, args, options)?;
        if name == "median" && window {
            return self.cast(result, DataType::Float64);
        }
        if name == "approx_count_distinct" {
            self.cast(result, DataType::Int64)
        } else {
            Ok(result)
        }
    }

    pub(super) fn string_sum_arguments(
        &mut self,
        args: Vec<LogicalExpression>,
        filter: Option<&LogicalExpression>,
    ) -> PlanResult<Vec<LogicalExpression>> {
        args.into_iter()
            .map(|mut arg| {
                let kind = arg.result_type.resolved("sum argument")?.data_type.clone();
                if !kind.is_string() {
                    return Ok(arg);
                }
                if self.context.config.ansi_mode {
                    if let Some(filter) = filter {
                        let filter = self.copy_expression(filter)?;
                        let null = self.literal(ScalarValue::try_from(&kind)?);
                        arg = self.case(vec![(filter, arg)], Some(null))?;
                    }
                    self.cast(arg, DataType::Float64)
                } else {
                    self.try_cast(arg, DataType::Float64)
                }
            })
            .collect()
    }
}

fn typed_null_literal(value: &LogicalExpression) -> bool {
    match &value.node {
        Deferred::Resolved(ExpressionNode::Literal(value)) => value.is_null(),
        Deferred::Resolved(
            ExpressionNode::Cast { expression, .. } | ExpressionNode::WithMetadata(expression),
        ) => typed_null_literal(expression),
        _ => false,
    }
}
