use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::expr::NullTreatment;
use datafusion::prelude::SessionContext;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams, WindowFunctionParams};
use datafusion_expr::{
    cast, expr, AggregateUDF, BinaryExpr, ExprSchemable, Operator, ScalarUDF, ScalarUDFImpl,
    WindowFrame, WindowFunctionDefinition, WindowUDF,
};
use sail_common_datafusion::utils::items::ItemTaker;

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

    #[allow(dead_code)]
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
        Arc::new(move |_| {
            Err(PlanError::todo(format!(
                "unknown aggregate function: {name}"
            )))
        })
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

    #[allow(dead_code)]
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
                ))
            }
        };
        Ok((vec![expr], null_treatment))
    } else {
        Err(PlanError::invalid("requires 1 or 2 arguments"))
    }
}
