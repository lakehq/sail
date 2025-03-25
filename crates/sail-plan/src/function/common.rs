use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::NullTreatment;
use datafusion_common::DFSchemaRef;
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams, WindowFunctionParams};
use datafusion_expr::{
    expr, AggregateUDF, BinaryExpr, Operator, ScalarUDF, ScalarUDFImpl, WindowFrame,
    WindowFunctionDefinition, WindowUDF,
};

use crate::config::PlanConfig;
use crate::error::{PlanError, PlanResult};
use crate::utils::ItemTaker;

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
    pub fn nullary<F>(f: F) -> ScalarFunction
    where
        F: Fn() -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                arguments.zero()?;
                Ok(f())
            },
        )
    }

    pub fn unary<F>(f: F) -> ScalarFunction
    where
        F: Fn(expr::Expr) -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| Ok(f(arguments.one()?)),
        )
    }

    pub fn binary<F>(f: F) -> ScalarFunction
    where
        F: Fn(expr::Expr, expr::Expr) -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                let (left, right) = arguments.two()?;
                Ok(f(left, right))
            },
        )
    }

    pub fn ternary<F>(f: F) -> ScalarFunction
    where
        F: Fn(expr::Expr, expr::Expr, expr::Expr) -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                let (first, second, third) = arguments.three()?;
                Ok(f(first, second, third))
            },
        )
    }

    pub fn var_arg<F>(f: F) -> ScalarFunction
    where
        F: Fn(Vec<expr::Expr>) -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| Ok(f(arguments)),
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
                  }| {
                Ok(expr::Expr::Cast(expr::Cast {
                    expr: Box::new(arguments.one()?),
                    data_type: data_type.clone(),
                }))
            },
        )
    }

    pub fn udf<F>(f: F) -> ScalarFunction
    where
        F: ScalarUDFImpl + Send + Sync + 'static,
    {
        let func = Arc::new(ScalarUDF::from(f));
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                    func: func.clone(),
                    args: arguments,
                }))
            },
        )
    }

    pub fn scalar_udf<F>(f: F) -> ScalarFunction
    where
        F: Fn() -> Arc<ScalarUDF> + Send + Sync + 'static,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                    func: f(),
                    args: arguments,
                }))
            },
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

pub struct AggFunctionInput<'a> {
    pub arguments: Vec<expr::Expr>,
    pub distinct: bool,
    pub ignore_nulls: Option<bool>,
    pub filter: Option<Box<expr::Expr>>,
    pub order_by: Option<Vec<expr::Sort>>,
    pub function_context: FunctionContextInput<'a>,
}

pub(crate) type AggFunction = Arc<dyn Fn(AggFunctionInput) -> PlanResult<expr::Expr> + Send + Sync>;

pub(crate) struct AggFunctionBuilder;

impl AggFunctionBuilder {
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
                function_context: _function_context,
            } = input;
            let null_treatment = get_null_treatment(ignore_nulls);
            Ok(expr::Expr::WindowFunction(expr::WindowFunction {
                fun: WindowFunctionDefinition::AggregateUDF(f()),
                params: WindowFunctionParams {
                    args: arguments,
                    partition_by,
                    order_by,
                    window_frame,
                    null_treatment,
                },
            }))
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
                function_context: _function_context,
            } = input;
            let null_treatment = get_null_treatment(ignore_nulls);
            Ok(expr::Expr::WindowFunction(expr::WindowFunction {
                fun: WindowFunctionDefinition::WindowUDF(f()),
                params: WindowFunctionParams {
                    args: arguments,
                    partition_by,
                    order_by,
                    window_frame,
                    null_treatment,
                },
            }))
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
