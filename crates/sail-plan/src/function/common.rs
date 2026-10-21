use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, IntervalUnit};
use datafusion::logical_expr::expr::NullTreatment;
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchemaRef, ScalarValue, plan_datafusion_err, plan_err};
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams, WindowFunctionParams};
use datafusion_expr::{
    AggregateUDF, BinaryExpr, ExprSchemable, Operator, ScalarUDF, ScalarUDFImpl, WindowFrame,
    WindowFunctionDefinition, WindowUDF, cast, expr, lit,
};
use sail_common::spec::{self, SAIL_SPARK_INTERVAL_METADATA_KEY, SparkIntervalMetadata};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::spark_struct_rename::SparkStructRename;
use sail_function::scalar::variant::spark_cast_to_variant::SparkCastToVariant;
use sail_function::sketch::{DEFAULT_HLL_LG_CONFIG_K, DEFAULT_THETA_LG_NOM_ENTRIES};

use crate::config::PlanConfig;
use crate::error::{IntoPlanResult, PlanError, PlanResult};

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

pub(crate) fn preserve_aggregate_window_fields(
    expression: expr::Expr,
    schema: &DFSchemaRef,
) -> PlanResult<expr::Expr> {
    Ok(expression
        .transform_up(|mut expression| {
            let (args, order_by) = match &expression {
                expr::Expr::AggregateFunction(function) => {
                    // Grouping markers must retain the exact GROUP BY expression for resolution.
                    if matches!(function.func.name(), "grouping" | "grouping_id") {
                        return Ok(Transformed::no(expression));
                    }
                    (&function.params.args, &function.params.order_by)
                }
                expr::Expr::WindowFunction(function) => {
                    (&function.params.args, &function.params.order_by)
                }
                _ => return Ok(Transformed::no(expression)),
            };
            let types = args
                .iter()
                .chain(order_by.iter().map(|sort| &sort.expr))
                .map(|arg| arg.get_type(schema))
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            let normalized: Vec<_> = types.iter().map(normalize_list_child_metadata).collect();
            if types == normalized {
                return Ok(Transformed::no(expression));
            }
            if let expr::Expr::WindowFunction(function) = &expression
                && matches!(function.fun.name(), "lead" | "lag")
                && let [input, _, default, ..] = args.as_slice()
            {
                check_window_default_intervals(
                    &input.get_type(schema)?,
                    &default.get_type(schema)?,
                )?;
            }
            let output_type = expression.get_type(schema)?;
            let (args, order_by) = match &mut expression {
                expr::Expr::AggregateFunction(function) => {
                    (&mut function.params.args, &mut function.params.order_by)
                }
                expr::Expr::WindowFunction(function) => {
                    (&mut function.params.args, &mut function.params.order_by)
                }
                _ => return Ok(Transformed::no(expression)),
            };
            // DataFusion drops list child metadata when storing scalar aggregate/window state.
            for ((arg, source), target) in args
                .iter_mut()
                .chain(order_by.iter_mut().map(|sort| &mut sort.expr))
                .zip(types)
                .zip(normalized)
            {
                if source != target {
                    *arg = ScalarUDF::from(SparkStructRename::new(target)).call(vec![arg.clone()]);
                }
            }
            if expression.get_type(schema)? != output_type {
                expression =
                    ScalarUDF::from(SparkStructRename::new(output_type)).call(vec![expression]);
            }
            Ok(Transformed::yes(expression))
        })?
        .data)
}

fn normalize_list_child_metadata(data_type: &DataType) -> DataType {
    let field = |field: &FieldRef, clear: bool| {
        let mut field = field
            .as_ref()
            .clone()
            .with_data_type(normalize_list_child_metadata(field.data_type()));
        if clear {
            field.metadata_mut().clear();
        }
        Arc::new(field)
    };
    match data_type {
        DataType::List(child) => DataType::List(field(child, true)),
        DataType::LargeList(child) => DataType::LargeList(field(child, true)),
        DataType::ListView(child) => DataType::ListView(field(child, true)),
        DataType::LargeListView(child) => DataType::LargeListView(field(child, true)),
        DataType::FixedSizeList(child, size) => DataType::FixedSizeList(field(child, true), *size),
        DataType::Struct(fields) => {
            DataType::Struct(fields.iter().map(|child| field(child, false)).collect())
        }
        DataType::Map(child, sorted) => DataType::Map(field(child, false), *sorted),
        _ => data_type.clone(),
    }
}

fn check_window_default_intervals(
    input: &DataType,
    default: &DataType,
) -> datafusion_common::Result<()> {
    let qualifier = |field: &Field| {
        let unit = match field.data_type() {
            DataType::Interval(IntervalUnit::YearMonth) => spec::IntervalUnit::YearMonth,
            DataType::Interval(IntervalUnit::DayTime) => spec::IntervalUnit::DayTime,
            _ => return Ok(None),
        };
        match field.metadata().get(SAIL_SPARK_INTERVAL_METADATA_KEY) {
            Some(value) => SparkIntervalMetadata::from_json(value).map(Some),
            None => SparkIntervalMetadata::try_new(unit, None, None),
        }
        .map_err(|error| plan_datafusion_err!("{error}"))
    };
    let field = |input: &FieldRef, default: &FieldRef| {
        if matches!(input.data_type(), DataType::Interval(_))
            && matches!(default.data_type(), DataType::Interval(_))
            && qualifier(input)? != qualifier(default)?
        {
            return plan_err!("lead/lag default interval type must match the input interval type");
        }
        check_window_default_intervals(input.data_type(), default.data_type())
    };
    match (input, default) {
        (
            DataType::List(input)
            | DataType::LargeList(input)
            | DataType::ListView(input)
            | DataType::LargeListView(input)
            | DataType::FixedSizeList(input, _),
            DataType::List(default)
            | DataType::LargeList(default)
            | DataType::ListView(default)
            | DataType::LargeListView(default)
            | DataType::FixedSizeList(default, _),
        )
        | (DataType::Map(input, _), DataType::Map(default, _)) => field(input, default),
        (DataType::Struct(input), DataType::Struct(default)) => {
            input.iter().zip(default).try_for_each(|(i, d)| field(i, d))
        }
        _ => Ok(()),
    }
}

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
