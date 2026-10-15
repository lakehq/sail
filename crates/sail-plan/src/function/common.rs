use std::sync::Arc;

use chrono::{Offset, TimeZone};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use datafusion::logical_expr::expr::NullTreatment;
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{DFSchemaRef, Result as DataFusionResult, ScalarValue};
use datafusion_expr::expr::{
    AggregateFunction, AggregateFunctionParams, HigherOrderFunction, Lambda, LambdaVariable,
    WindowFunctionParams,
};
use datafusion_expr::type_coercion::binary::comparison_coercion;
use datafusion_expr::{
    AggregateUDF, BinaryExpr, ExprSchemable, HigherOrderUDF, Operator, ScalarUDF, ScalarUDFImpl,
    WindowFrame, WindowFunctionDefinition, WindowUDF, cast, expr, lit, try_cast, when,
};
use datafusion_functions::core::get_field;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::spark_array_transform::SparkArrayTransform;
use sail_function::scalar::datetime::convert_tz::ConvertTz;
use sail_function::scalar::datetime::is_fixed_zero_offset_timezone;
use sail_function::scalar::map::spark_transform_keys::SparkTransformKeys;
use sail_function::scalar::map::spark_transform_values::SparkTransformValues;
use sail_function::scalar::update_struct_field::UpdateStructField;
use sail_function::scalar::variant::spark_cast_to_variant::SparkCastToVariant;
use sail_function::sketch::{DEFAULT_HLL_LG_CONFIG_K, DEFAULT_THETA_LG_NOM_ENTRIES};
use sail_python_udf::udf::pyspark_batch_collector::PySparkBatchCollectorUDF;
use sail_python_udf::udf::pyspark_cogroup_map_udf::PySparkCoGroupMapUDF;
use sail_python_udf::udf::pyspark_group_map_udf::PySparkGroupMapUDF;
use sail_python_udf::udf::pyspark_udaf::PySparkGroupAggregateUDF;
use sail_python_udf::udf::pyspark_udf::PySparkUDF;
use sail_python_udf::udf::pyspark_unresolved_udf::PySparkUnresolvedUDF;

use crate::config::PlanConfig;
use crate::error::{IntoPlanResult, PlanError, PlanResult};
use crate::function::is_spark_compatible_arrow_fixed_offset;

/// Cast temporal values using the session zone and Spark's gap/overlap resolution.
pub(crate) fn cast_temporal(
    expression: expr::Expr,
    from: &DataType,
    to: DataType,
    session_timezone: &Arc<str>,
    is_try: bool,
) -> DataFusionResult<expr::Expr> {
    if from == &to {
        return Ok(expression);
    }
    match (from, &to) {
        (DataType::Map(source, _), DataType::Map(target, _)) => {
            let (DataType::Struct(source), DataType::Struct(target)) =
                (source.data_type(), target.data_type())
            else {
                return Ok(cast(expression, to));
            };
            let mut result = expression;
            for (index, (source, target)) in source.iter().zip(target).enumerate() {
                if source.data_type() == target.data_type() {
                    continue;
                }
                let parameters = vec![
                    "__sail_timestamp_key".to_string(),
                    "__sail_timestamp_value".to_string(),
                ];
                let variable = expr::Expr::LambdaVariable(LambdaVariable::new(
                    parameters[index].clone(),
                    Some(Arc::clone(source)),
                ));
                let body = cast_temporal(
                    variable,
                    source.data_type(),
                    target.data_type().clone(),
                    session_timezone,
                    is_try,
                )?;
                let function = if index == 0 {
                    HigherOrderUDF::new_from_impl(
                        SparkTransformKeys::new(false).with_preserve_duplicates(true),
                    )
                } else {
                    HigherOrderUDF::new_from_impl(SparkTransformValues::new())
                };
                result = expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
                    Arc::new(function),
                    vec![result, expr::Expr::Lambda(Lambda::new(parameters, body))],
                ));
            }
            Ok(cast(result, to))
        }
        (
            DataType::List(source) | DataType::LargeList(source),
            DataType::List(target) | DataType::LargeList(target),
        ) if common_temporal_type(
            &[source.data_type().clone(), target.data_type().clone()],
            session_timezone,
        )
        .is_some() =>
        {
            let parameter = "__sail_timestamp_element".to_string();
            let variable = expr::Expr::LambdaVariable(LambdaVariable::new(
                parameter.clone(),
                Some(Arc::clone(source)),
            ));
            let body = cast_temporal(
                variable,
                source.data_type(),
                target.data_type().clone(),
                session_timezone,
                is_try,
            )?;
            let transformed = expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
                Arc::new(HigherOrderUDF::new_from_impl(SparkArrayTransform::new())),
                vec![
                    expression,
                    expr::Expr::Lambda(Lambda::new(vec![parameter], body)),
                ],
            ));
            Ok(cast(transformed, to))
        }
        (DataType::Struct(source), DataType::Struct(target))
            if source.len() == target.len()
                && source
                    .iter()
                    .zip(target)
                    .all(|(source, target)| source.name() == target.name()) =>
        {
            let mut result = expression;
            for (source, target) in source.iter().zip(target) {
                if source.data_type() != target.data_type() {
                    let value = get_field().call(vec![result.clone(), lit(source.name().clone())]);
                    let value = cast_temporal(
                        value,
                        source.data_type(),
                        target.data_type().clone(),
                        session_timezone,
                        is_try,
                    )?;
                    result = ScalarUDF::from(UpdateStructField::new(vec![source.name().clone()]))
                        .call(vec![result, value]);
                }
            }
            Ok(cast(result, to))
        }
        (
            DataType::Timestamp(_, None) | DataType::Date32 | DataType::Date64,
            DataType::Timestamp(_, Some(_)),
        ) => {
            let instant =
                cast_timestamp_ntz_to_ltz(expression, Arc::clone(session_timezone), is_try)?;
            Ok(cast(instant, to))
        }
        (
            DataType::Timestamp(_, Some(_)),
            DataType::Timestamp(_, None) | DataType::Date32 | DataType::Date64,
        ) => {
            let localized = cast(
                shift_timestamp_timezone(expression, session_timezone, is_try, false)?,
                DataType::Timestamp(TimeUnit::Microsecond, None),
            );
            Ok(cast(localized, to))
        }
        _ if is_try => Ok(try_cast(expression, to)),
        _ => Ok(cast(expression, to)),
    }
}

/// The common type for existing date/timestamp operands, including array elements.
/// Other coercions (notably ANSI/legacy string promotion) remain with their callers.
pub(crate) fn common_temporal_type(
    data_types: &[DataType],
    session_timezone: &Arc<str>,
) -> Option<DataType> {
    let types = data_types
        .iter()
        .filter(|data_type| !data_type.is_null())
        .collect::<Vec<_>>();
    if types.is_empty() {
        return None;
    }
    if let DataType::Map(first, _) = types[0] {
        let entries = types
            .iter()
            .map(|data_type| match data_type {
                DataType::Map(entries, _) => Some(entries.data_type().clone()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        let entries = common_temporal_type(&entries, session_timezone)?;
        return Some(DataType::Map(
            Arc::new(first.as_ref().clone().with_data_type(entries)),
            false,
        ));
    }
    if let DataType::Struct(first) = types[0] {
        let structs = types
            .iter()
            .map(|data_type| match data_type {
                DataType::Struct(fields) if fields.len() == first.len() => Some(fields),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        let mut has_temporal = false;
        let fields = first
            .iter()
            .enumerate()
            .map(|(index, first)| {
                if structs
                    .iter()
                    .any(|fields| fields[index].name() != first.name())
                {
                    return None;
                }
                let field_types = structs
                    .iter()
                    .map(|fields| fields[index].data_type().clone())
                    .collect::<Vec<_>>();
                let target =
                    if let Some(target) = common_temporal_type(&field_types, session_timezone) {
                        has_temporal = true;
                        target
                    } else if field_types
                        .iter()
                        .all(|data_type| data_type == first.data_type())
                    {
                        first.data_type().clone()
                    } else if field_types.iter().all(DataType::is_numeric) {
                        field_types
                            .iter()
                            .skip(1)
                            .try_fold(field_types[0].clone(), |previous, current| {
                                comparison_coercion(&previous, current)
                            })?
                    } else {
                        return None;
                    };
                Some(Arc::new(
                    first
                        .as_ref()
                        .clone()
                        .with_data_type(target)
                        .with_nullable(structs.iter().any(|fields| fields[index].is_nullable())),
                ))
            })
            .collect::<Option<Fields>>()?;
        return has_temporal.then_some(DataType::Struct(fields));
    }
    if types
        .iter()
        .all(|data_type| matches!(data_type, DataType::List(_) | DataType::LargeList(_)))
    {
        let nullable = types.iter().any(|data_type| matches!(data_type, DataType::List(field) | DataType::LargeList(field) if field.is_nullable()));
        let elements = types
            .iter()
            .filter_map(|data_type| match data_type {
                DataType::List(field) | DataType::LargeList(field) => {
                    Some(field.data_type().clone())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        return common_temporal_type(&elements, session_timezone)
            .map(|element| DataType::List(Arc::new(Field::new("item", element, nullable))));
    }
    if !types.iter().all(|data_type| {
        matches!(
            data_type,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
        )
    }) {
        return None;
    }
    if types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, Some(_))))
    {
        Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::clone(session_timezone)),
        ))
    } else if types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, None)))
    {
        Some(DataType::Timestamp(TimeUnit::Microsecond, None))
    } else {
        None
    }
}

/// Arrow carries display zones in nested fields; Spark uses the session zone for every LTZ value.
pub(crate) fn with_session_timezone(data_type: &DataType, timezone: &Arc<str>) -> DataType {
    let field = |field: &Arc<Field>| {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(with_session_timezone(field.data_type(), timezone)),
        )
    };
    match data_type {
        DataType::Timestamp(unit, Some(_)) => {
            DataType::Timestamp(*unit, Some(Arc::clone(timezone)))
        }
        DataType::List(value) => DataType::List(field(value)),
        DataType::LargeList(value) => DataType::LargeList(field(value)),
        DataType::FixedSizeList(value, size) => DataType::FixedSizeList(field(value), *size),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(field).collect()),
        DataType::Map(entries, sorted) => DataType::Map(field(entries), *sorted),
        _ => data_type.clone(),
    }
}

pub(crate) fn coerce_temporal_arguments(
    arguments: Vec<expr::Expr>,
    context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let types = arguments
        .iter()
        .map(|argument| argument.get_type(context.schema))
        .collect::<DataFusionResult<Vec<_>>>()?;
    let timezone = &context.plan_config.session_timezone;
    let Some(target) = common_temporal_type(&types, timezone) else {
        return Ok(arguments);
    };
    arguments
        .into_iter()
        .zip(types)
        .map(|(argument, source)| {
            Ok(cast_temporal(
                argument,
                &source,
                target.clone(),
                timezone,
                false,
            )?)
        })
        .collect()
}

pub(crate) fn timestamp_epoch_units(
    value: expr::Expr,
    divisor: i64,
) -> DataFusionResult<expr::Expr> {
    let value = cast(value, DataType::Int64);
    if divisor == 1 {
        return Ok(value);
    }
    // Spark floors epoch conversions; integer division alone truncates toward zero.
    Ok(value.clone() / lit(divisor)
        - when((value % lit(divisor)).lt(lit(0_i64)), lit(1_i64)).otherwise(lit(0_i64))?)
}

pub(crate) fn cast_timestamp_ntz_to_ltz(
    expression: expr::Expr,
    timezone: Arc<str>,
    is_try: bool,
) -> DataFusionResult<expr::Expr> {
    Ok(cast(
        shift_timestamp_timezone(expression, &timezone, is_try, true)?,
        DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
    ))
}

fn shift_timestamp_timezone(
    expression: expr::Expr,
    timezone: &str,
    is_try: bool,
    to_utc: bool,
) -> DataFusionResult<expr::Expr> {
    let timestamp_ntz = cast(expression, DataType::Timestamp(TimeUnit::Microsecond, None));
    // A plain timezone cast can be unwrapped into a numeric comparison by DataFusion,
    // losing the local-time conversion. Keep the conversion explicit before retagging.
    Ok(if is_fixed_zero_offset_timezone(timezone) {
        cast(timestamp_ntz, DataType::Int64)
    } else if is_spark_compatible_arrow_fixed_offset(timezone) {
        let offset = timezone
            .parse::<Tz>()?
            .offset_from_utc_datetime(&chrono::DateTime::UNIX_EPOCH.naive_utc())
            .fix()
            .local_minus_utc();
        // Widen before shifting so the final cast detects i64 overflow instead of wrapping.
        let micros = cast(
            cast(timestamp_ntz, DataType::Int64),
            DataType::Decimal128(20, 0),
        );
        let offset = lit(ScalarValue::Decimal128(
            Some(i128::from(offset) * 1_000_000),
            20,
            0,
        ));
        let shifted = if to_utc {
            micros - offset
        } else {
            micros + offset
        };
        if is_try {
            try_cast(shifted, DataType::Int64)
        } else {
            cast(shifted, DataType::Int64)
        }
    } else {
        // TODO: Support second-resolution fixed offsets rejected by Arrow and ConvertTz.
        let converted = ScalarUDF::from(ConvertTz::new(false)).call(vec![
            lit(if to_utc { timezone } else { "UTC" }),
            lit(if to_utc { "UTC" } else { timezone }),
            timestamp_ntz,
        ]);
        cast(converted, DataType::Int64)
    })
}

pub struct FunctionContextInput<'a> {
    /// The names of function arguments.
    /// Most functions do not need this information, so it is
    /// passed as `&[String]` rather than `Vec<String>` to avoid unnecessary clone.
    /// These are the display names from the [`crate::resolver::expression::NamedExpr`]s,
    /// not to be confused with named function arguments
    /// (e.g., not like encode(charset => "utf-8", expr => "abc")).
    pub argument_display_names: &'a [String],
    pub plan_config: &'a Arc<PlanConfig>,
    pub session_context: &'a SessionContext,
    pub schema: &'a DFSchemaRef,
}

pub struct ScalarFunctionInput<'a> {
    pub arguments: Vec<expr::Expr>,
    pub function_context: FunctionContextInput<'a>,
}

pub(crate) type ScalarFunction =
    Arc<dyn Fn(ScalarFunctionInput) -> PlanResult<expr::Expr> + Send + Sync>;

pub(crate) struct ScalarFunctionBuilder;

impl ScalarFunctionBuilder {
    pub fn nullary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn() -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                arguments.zero()?;
                f().into_plan_result()
            },
        )
    }

    pub fn unary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(expr::Expr) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| f(arguments.one()?).into_plan_result(),
        )
    }

    pub fn binary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(expr::Expr, expr::Expr) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                let (left, right) = arguments.two()?;
                f(left, right).into_plan_result()
            },
        )
    }

    pub fn ternary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(expr::Expr, expr::Expr, expr::Expr) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                let (first, second, third) = arguments.three()?;
                f(first, second, third).into_plan_result()
            },
        )
    }

    pub fn quaternary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(expr::Expr, expr::Expr, expr::Expr, expr::Expr) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                let (first, second, third, fourth) = arguments.four()?;
                f(first, second, third, fourth).into_plan_result()
            },
        )
    }

    pub fn var_arg<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(Vec<expr::Expr>) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| f(arguments).into_plan_result(),
        )
    }

    pub fn binary_op(op: Operator) -> ScalarFunction {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                let (left, right) = arguments.two()?;
                Ok(expr::Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                }))
            },
        )
    }

    pub fn cast(data_type: DataType) -> ScalarFunction {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| { Ok(cast(arguments.one()?, data_type.clone())) },
        )
    }

    pub fn udf<F>(f: F) -> ScalarFunction
    where
        F: ScalarUDFImpl + Send + Sync + 'static,
    {
        let func = ScalarUDF::from(f);
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| { Ok(func.call(arguments)) },
        )
    }

    #[expect(dead_code)]
    pub fn scalar_udf<F>(f: F) -> ScalarFunction
    where
        F: Fn() -> Arc<ScalarUDF> + Send + Sync + 'static,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| { Ok(f().call(arguments)) },
        )
    }

    pub fn custom<F>(f: F) -> ScalarFunction
    where
        F: Fn(ScalarFunctionInput) -> PlanResult<expr::Expr> + Send + Sync + 'static,
    {
        Arc::new(f)
    }

    pub fn unknown(name: &str) -> ScalarFunction {
        let name = name.to_string();
        Arc::new(move |_| Err(PlanError::todo(format!("function: {name}"))))
    }
}

/// Aggregate function input components, excluding the function name.
///
/// Populated by the resolver from `spec::UnresolvedFunction` after resolving spec expressions
/// to DataFusion expressions. Used by aggregate function builders to produce the final
/// DataFusion aggregate expression.
pub struct AggFunctionInput<'a> {
    pub arguments: Vec<expr::Expr>,
    pub distinct: bool,
    pub ignore_nulls: Option<bool>,
    pub filter: Option<Box<expr::Expr>>,
    pub order_by: Vec<expr::Sort>,
    pub preserve_count_argument_columns: bool,
    pub function_context: FunctionContextInput<'a>,
}

/// Builds a DataFusion aggregate expression from resolved function components.
///
/// Takes the resolved arguments, modifiers (DISTINCT, FILTER, ORDER BY), and context,
/// and produces a `datafusion_expr::Expr::AggregateFunction` ready for inclusion in the logical plan.
pub(crate) type AggFunction = Arc<dyn Fn(AggFunctionInput) -> PlanResult<expr::Expr> + Send + Sync>;

/// Factory methods for creating `AggFunction`s.
///
/// Provides different ways to build aggregate function handlers:
/// - `default`: Use an existing DataFusion UDAF directly
/// - `custom`: Provide custom logic for building the expression
/// - `unknown`: Placeholder for unimplemented functions
pub(crate) struct AggFunctionBuilder;

impl AggFunctionBuilder {
    /// Converts a DataFusion UDAF factory into an AggFunction.
    ///
    /// Passes all resolved components through directly with no transformation.
    pub fn default<F>(f: F) -> AggFunction
    where
        F: Fn() -> Arc<AggregateUDF> + Send + Sync + 'static,
    {
        Arc::new(move |input| {
            let AggFunctionInput {
                arguments,
                distinct,
                ignore_nulls,
                filter,
                order_by,
                preserve_count_argument_columns: _,
                function_context: _function_context,
            } = input;
            let null_treatment = get_null_treatment(ignore_nulls);
            Ok(expr::Expr::AggregateFunction(AggregateFunction {
                func: f(),
                params: AggregateFunctionParams {
                    args: arguments,
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                },
            }))
        })
    }

    /// Wraps a custom function that builds the aggregate expression with full control.
    ///
    /// Use when the function needs special argument handling that `default` cannot provide.
    pub fn custom<F>(f: F) -> AggFunction
    where
        F: Fn(AggFunctionInput) -> PlanResult<expr::Expr> + Send + Sync + 'static,
    {
        Arc::new(f)
    }

    pub fn unknown(name: &str) -> AggFunction {
        let name = name.to_string();
        Arc::new(move |_| Err(PlanError::todo(format!("function: {name}"))))
    }
}

pub struct WinFunctionInput<'a> {
    pub arguments: Vec<expr::Expr>,
    pub partition_by: Vec<expr::Expr>,
    pub order_by: Vec<expr::Sort>,
    pub window_frame: WindowFrame,
    pub ignore_nulls: Option<bool>,
    pub distinct: bool,
    pub function_context: FunctionContextInput<'a>,
}

pub(crate) type WinFunction = Arc<dyn Fn(WinFunctionInput) -> PlanResult<expr::Expr> + Send + Sync>;

pub(crate) struct WinFunctionBuilder;

impl WinFunctionBuilder {
    pub fn aggregate<F>(f: F) -> WinFunction
    where
        F: Fn() -> Arc<AggregateUDF> + Send + Sync + 'static,
    {
        Arc::new(move |input| {
            let WinFunctionInput {
                arguments,
                partition_by,
                order_by,
                window_frame,
                ignore_nulls,
                distinct,
                function_context: _function_context,
            } = input;
            let null_treatment = get_null_treatment(ignore_nulls);
            Ok(expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
                fun: WindowFunctionDefinition::AggregateUDF(f()),
                params: WindowFunctionParams {
                    args: arguments,
                    partition_by,
                    order_by,
                    window_frame,
                    filter: None,
                    null_treatment,
                    distinct,
                },
            })))
        })
    }

    pub fn window<F>(f: F) -> WinFunction
    where
        F: Fn() -> Arc<WindowUDF> + Send + Sync + 'static,
    {
        Arc::new(move |input| {
            let WinFunctionInput {
                arguments,
                partition_by,
                order_by,
                window_frame,
                ignore_nulls,
                distinct,
                function_context,
            } = input;
            let null_treatment = get_null_treatment(ignore_nulls);
            let win_func_expr = expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
                fun: WindowFunctionDefinition::WindowUDF(f()),
                params: WindowFunctionParams {
                    args: arguments,
                    partition_by,
                    order_by,
                    window_frame,
                    filter: None,
                    null_treatment,
                    distinct,
                },
            }));
            Ok(match win_func_expr.get_type(function_context.schema)? {
                DataType::UInt64 => cast(win_func_expr.clone(), DataType::Int32),
                _ => win_func_expr,
            })
        })
    }

    pub fn custom<F>(f: F) -> WinFunction
    where
        F: Fn(WinFunctionInput) -> PlanResult<expr::Expr> + Send + Sync + 'static,
    {
        Arc::new(f)
    }

    pub fn unknown(name: &str) -> WinFunction {
        let name = name.to_string();
        Arc::new(move |_| Err(PlanError::todo(format!("unknown window function: {name}"))))
    }
}

pub(crate) fn get_null_treatment(ignore_nulls: Option<bool>) -> Option<NullTreatment> {
    match ignore_nulls {
        Some(true) => Some(NullTreatment::IgnoreNulls),
        Some(false) => Some(NullTreatment::RespectNulls),
        None => None,
    }
}

pub(crate) fn get_arguments_and_null_treatment(
    args: Vec<expr::Expr>,
    ignore_nulls: Option<bool>,
) -> PlanResult<(Vec<expr::Expr>, Option<NullTreatment>)> {
    if args.len() == 1 {
        let expr = args.one()?;
        Ok((vec![expr], get_null_treatment(ignore_nulls)))
    } else if args.len() == 2 {
        if ignore_nulls.is_some() {
            return Err(PlanError::invalid(
                "arguments conflict with IGNORE NULLS clause",
            ));
        }
        let (expr, ignore_nulls) = args.two()?;
        let null_treatment = match ignore_nulls {
            expr::Expr::Literal(ScalarValue::Boolean(Some(ignore_nulls)), _metadata) => {
                if ignore_nulls {
                    Some(NullTreatment::IgnoreNulls)
                } else {
                    Some(NullTreatment::RespectNulls)
                }
            }
            _ => {
                return Err(PlanError::invalid(
                    "requires a boolean literal as the second argument",
                ));
            }
        };
        Ok((vec![expr], null_treatment))
    } else {
        Err(PlanError::invalid("requires 1 or 2 arguments"))
    }
}

pub(crate) fn hll_args_with_default_lg(
    arguments: Vec<expr::Expr>,
    function_name: &str,
) -> PlanResult<Vec<expr::Expr>> {
    match arguments.len() {
        1 => {
            let value = arguments.one()?;
            Ok(vec![value, lit(DEFAULT_HLL_LG_CONFIG_K)])
        }
        2 => {
            let (value, lg_config_k) = arguments.two()?;
            Ok(vec![value, cast(lg_config_k, DataType::Int32)])
        }
        count => Err(PlanError::invalid(format!(
            "{function_name} requires 1 or 2 arguments, got {count}"
        ))),
    }
}

pub(crate) fn hll_union_args_with_default_allow_different_lg(
    arguments: Vec<expr::Expr>,
) -> PlanResult<Vec<expr::Expr>> {
    match arguments.len() {
        1 => {
            let value = arguments.one()?;
            Ok(vec![value, lit(false)])
        }
        2 => {
            let (value, allow_different_lg_config_k) = arguments.two()?;
            Ok(vec![
                value,
                cast(allow_different_lg_config_k, DataType::Boolean),
            ])
        }
        count => Err(PlanError::invalid(format!(
            "hll_union_agg requires 1 or 2 arguments, got {count}"
        ))),
    }
}

pub(crate) fn count_min_sketch_args(arguments: Vec<expr::Expr>) -> PlanResult<Vec<expr::Expr>> {
    match arguments.len() {
        4 => {
            let (value, eps, confidence, seed) = arguments.four()?;
            // Spark requires `eps` and `confidence` to be DOUBLE literals and rejects
            // other types (including DECIMAL/FLOAT). Pass them through unchanged so the
            // aggregate's type validation enforces the same rule, instead of casting and
            // silently accepting types Spark rejects.
            Ok(vec![value, eps, confidence, seed])
        }
        count => Err(PlanError::invalid(format!(
            "count_min_sketch requires 4 arguments, got {count}"
        ))),
    }
}

pub(crate) fn theta_args_with_default_lg(
    arguments: Vec<expr::Expr>,
    function_name: &str,
) -> PlanResult<Vec<expr::Expr>> {
    match arguments.len() {
        1 => {
            let value = arguments.one()?;
            Ok(vec![value, lit(DEFAULT_THETA_LG_NOM_ENTRIES)])
        }
        2 => {
            let (value, lg_nom_entries) = arguments.two()?;
            Ok(vec![value, cast(lg_nom_entries, DataType::Int32)])
        }
        count => Err(PlanError::invalid(format!(
            "{function_name} requires 1 or 2 arguments, got {count}"
        ))),
    }
}

pub fn expr_contains_python_udf(body: &expr::Expr) -> PlanResult<bool> {
    Ok(body.exists(|expression| {
        Ok(match expression {
            expr::Expr::ScalarFunction(function) => {
                let f = function.func.inner();
                f.is::<PySparkUDF>()
                    || f.is::<PySparkUnresolvedUDF>()
                    || f.is::<PySparkCoGroupMapUDF>()
            }
            expr::Expr::AggregateFunction(function) => {
                let f = function.func.inner();
                f.is::<PySparkGroupAggregateUDF>()
                    || f.is::<PySparkGroupMapUDF>()
                    || f.is::<PySparkBatchCollectorUDF>()
            }
            expr::Expr::WindowFunction(window) => matches!(
                &window.fun,
                WindowFunctionDefinition::AggregateUDF(udf)
                    if udf.inner().is::<PySparkGroupAggregateUDF>()
            ),
            _ => false,
        })
    })?)
}

// TODO: Match Catalyst constant folding and NullPropagation before extracting opaque
//  Python or Variant scalar calls, so a foldable NULL can eliminate them.
pub fn expr_contains_spark_cast_to_variant(body: &expr::Expr) -> PlanResult<bool> {
    Ok(body.exists(|expression| {
        Ok(matches!(
            expression,
            expr::Expr::ScalarFunction(function)
                if function.func.inner().is::<SparkCastToVariant>()
        ))
    })?)
}
