use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::functions_window::{cume_dist, lead_lag, nth_value, rank, row_number};
use datafusion_common::ScalarValue;
use datafusion_expr::expr::NullTreatment;
use datafusion_expr::{WindowFrame, WindowFunctionDefinition, WindowUDF};
use sail_common_datafusion::utils::items::ItemTaker;

use self::WindowFunctionBuilder as F;
use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::typing;
use crate::error::{PlanError, PlanResult};
use crate::function::aggregate::AggregateOptions;
use crate::function::common::FunctionContextInput;

pub(crate) struct WindowOptions {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<SortKey>,
    pub frame: WindowFrame,
    pub aggregate: AggregateOptions,
}

pub(crate) fn spark_window(
    context: &mut FunctionContextInput<'_>,
    name: &str,
    function: WindowFunctionDefinition,
    arguments: Vec<Expr>,
    options: WindowOptions,
) -> PlanResult<Expr> {
    match function {
        WindowFunctionDefinition::AggregateUDF(function) => {
            let name = match name {
                "array_join" => "listagg",
                _ => name,
            };
            let expression = crate::function::aggregate::spark_aggregate(
                context,
                name,
                function,
                arguments,
                options.aggregate,
            )?;
            aggregate_to_window(
                context,
                expression,
                &options.partition_by,
                &options.order_by,
                &options.frame,
            )
        }
        WindowFunctionDefinition::WindowUDF(function) => {
            let resolver = crate::function::get_built_in_window_function(name)
                .map(|function| function.resolver)
                .unwrap_or(resolve_window);
            resolver(context, name, function, arguments, options)
        }
    }
}
fn resolve_window(
    context: &mut FunctionContextInput<'_>,
    _name: &str,
    function: Arc<WindowUDF>,
    arguments: Vec<Expr>,
    options: WindowOptions,
) -> PlanResult<Expr> {
    let function = WindowFunctionDefinition::WindowUDF(function);

    let resolved = crate::analyzer::typing::aggregate::resolve_window(
        &function,
        &typing::argument_fields(&arguments)?,
    )?;
    let arguments = typing::coerce_arguments(arguments, &resolved.arguments, context.ids)?;
    let result = Expr {
        id: context.ids.expression(),
        result_type: Deferred::Resolved(ExprType::from_field(&resolved.result)),
        kind: Deferred::Resolved(ExprKind::WindowFunction {
            function,
            arguments,
            partition_by: options.partition_by,
            order_by: options.order_by,
            frame: options.frame,
            filter: options.aggregate.filter,
            distinct: options.aggregate.distinct,
            null_treatment: options.aggregate.ignore_nulls.map(|ignore| {
                if ignore {
                    NullTreatment::IgnoreNulls
                } else {
                    NullTreatment::RespectNulls
                }
            }),
        }),
    };
    if resolved.result.data_type() == &DataType::UInt64 {
        context.cast(result, DataType::Int32)
    } else {
        Ok(result)
    }
}
fn resolve_first(
    context: &mut FunctionContextInput<'_>,
    name: &str,
    mut function: Arc<WindowUDF>,
    mut arguments: Vec<Expr>,
    mut options: WindowOptions,
) -> PlanResult<Expr> {
    if arguments.len() == 2 {
        if options.aggregate.ignore_nulls.is_some() {
            return Err(PlanError::invalid(
                "arguments conflict with IGNORE NULLS clause",
            ));
        }
        let (value, flag) = arguments.two()?;
        options.aggregate.ignore_nulls = Some(match flag.kind.into_resolved("null treatment")? {
            ExprKind::Literal(ScalarValue::Boolean(Some(flag))) => flag,
            _ => {
                return Err(PlanError::invalid(
                    "requires a boolean literal as the second argument",
                ));
            }
        });
        arguments = vec![value];
    }
    crate::analyzer::typing::coercion::validate_arity(name, arguments.len(), 1)?;
    if options.aggregate.ignore_nulls == Some(true) {
        function = match name {
            "last" | "last_value" => sail_function::window::spark_last_value_udwf(true),
            _ => sail_function::window::spark_first_value_udwf(true),
        };
    }

    resolve_window(context, name, function, arguments, options)
}
fn resolve_nth(
    context: &mut FunctionContextInput<'_>,
    name: &str,
    function: Arc<WindowUDF>,
    mut arguments: Vec<Expr>,
    mut options: WindowOptions,
) -> PlanResult<Expr> {
    if arguments.len() == 3 {
        let flag = arguments
            .pop()
            .ok_or_else(|| PlanError::internal("missing nth_value flag"))?;
        options.aggregate.ignore_nulls =
            match flag.kind.into_resolved("nth_value null treatment")? {
                ExprKind::Literal(ScalarValue::Boolean(flag)) => flag,
                _ => {
                    return Err(PlanError::invalid(
                        "nth_value third argument should be boolean scalar",
                    ));
                }
            };
    }
    resolve_window(context, name, function, arguments, options)
}

pub(crate) fn aggregate_to_window(
    context: &mut FunctionContextInput<'_>,
    expression: Expr,
    partition: &[Expr],
    order: &[SortKey],
    frame: &WindowFrame,
) -> PlanResult<Expr> {
    if let Deferred::Resolved(ExprKind::AggregateFunction {
        function,
        arguments,
        distinct,
        filter,
        null_treatment,
        ..
    }) = expression.kind
    {
        let partition_by = partition
            .iter()
            .map(|e| context.copy_expression(e))
            .collect::<PlanResult<_>>()?;
        let order_by = order
            .iter()
            .map(|s| {
                Ok(SortKey {
                    expression: context.copy_expression(&s.expression)?,
                    ascending: s.ascending,
                    nulls_first: s.nulls_first,
                })
            })
            .collect::<PlanResult<_>>()?;
        Ok(Expr {
            id: expression.id,
            result_type: expression.result_type,
            kind: Deferred::Resolved(ExprKind::WindowFunction {
                function: WindowFunctionDefinition::AggregateUDF(function),
                arguments,
                partition_by,
                order_by,
                frame: frame.clone(),
                distinct,
                filter,
                null_treatment,
            }),
        })
    } else {
        expression.map_children(&mut |e| aggregate_to_window(context, e, partition, order, frame))
    }
}

pub(crate) struct WindowFunctionInput<'a> {
    pub arguments: Vec<Expr>,
    pub options: WindowOptions,
    pub context: FunctionContextInput<'a>,
}

type WindowResolver = fn(
    &mut FunctionContextInput<'_>,
    &str,
    Arc<WindowUDF>,
    Vec<Expr>,
    WindowOptions,
) -> PlanResult<Expr>;

#[derive(Clone)]
pub(crate) struct WindowFunction {
    name: &'static str,
    resolver: WindowResolver,
    function: Arc<WindowUDF>,
}
impl WindowFunction {
    fn with_resolver(mut self, resolver: WindowResolver) -> Self {
        self.resolver = resolver;
        self
    }

    pub(crate) fn udf(&self) -> Arc<WindowUDF> {
        Arc::clone(&self.function)
    }
    pub(crate) fn call(&self, mut input: WindowFunctionInput<'_>) -> PlanResult<Expr> {
        (self.resolver)(
            &mut input.context,
            self.name,
            self.udf(),
            input.arguments,
            input.options,
        )
    }
}
pub(crate) struct WindowFunctionBuilder;
impl WindowFunctionBuilder {
    pub fn window_udf(
        name: &'static str,
        function: impl FnOnce() -> Arc<WindowUDF>,
    ) -> WindowFunction {
        WindowFunction {
            name,
            resolver: resolve_window,
            function: function(),
        }
    }
}

pub(crate) fn list_built_in_window_functions() -> Vec<(&'static str, WindowFunction)> {
    vec![
        (
            "cume_dist",
            F::window_udf("cume_dist", cume_dist::cume_dist_udwf),
        ),
        (
            "dense_rank",
            F::window_udf("dense_rank", rank::dense_rank_udwf),
        ),
        (
            "first",
            F::window_udf("first", nth_value::first_value_udwf).with_resolver(resolve_first),
        ),
        (
            "first_value",
            F::window_udf("first_value", nth_value::first_value_udwf).with_resolver(resolve_first),
        ),
        ("lag", F::window_udf("lag", lead_lag::lag_udwf)),
        (
            "last",
            F::window_udf("last", nth_value::last_value_udwf).with_resolver(resolve_first),
        ),
        (
            "last_value",
            F::window_udf("last_value", nth_value::last_value_udwf).with_resolver(resolve_first),
        ),
        ("lead", F::window_udf("lead", lead_lag::lead_udwf)),
        (
            "nth_value",
            F::window_udf("nth_value", nth_value::nth_value_udwf).with_resolver(resolve_nth),
        ),
        (
            "ntile",
            F::window_udf("ntile", sail_function::window::spark_ntile_udwf),
        ),
        ("rank", F::window_udf("rank", rank::rank_udwf)),
        (
            "row_number",
            F::window_udf("row_number", row_number::row_number_udwf),
        ),
        (
            "percent_rank",
            F::window_udf("percent_rank", rank::percent_rank_udwf),
        ),
        (
            "any_value",
            F::window_udf("any_value", nth_value::first_value_udwf).with_resolver(resolve_first),
        ),
    ]
}
