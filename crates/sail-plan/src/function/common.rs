use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::NullTreatment;
use datafusion_common::DFSchemaRef;
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::{expr, AggregateUDF, BinaryExpr, Operator, ScalarUDF, ScalarUDFImpl};

use crate::config::PlanConfig;
use crate::error::{PlanError, PlanResult};
use crate::utils::ItemTaker;

pub struct FunctionInput<'a> {
    pub arguments: Vec<expr::Expr>,
    /// The names of function arguments.
    /// Most functions do not need this information, so it is
    /// passed as `&[String]` rather than `Vec<String>` to avoid unnecessary clone.
    pub argument_names: &'a [String],
    pub plan_config: &'a Arc<PlanConfig>,
    pub session_context: &'a SessionContext,
    pub schema: &'a DFSchemaRef,
}

pub(crate) type Function = Arc<dyn Fn(FunctionInput) -> PlanResult<expr::Expr> + Send + Sync>;

pub(crate) struct FunctionBuilder;

impl FunctionBuilder {
    pub fn nullary<F>(f: F) -> Function
    where
        F: Fn() -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(move |FunctionInput { arguments, .. }| {
            arguments.zero()?;
            Ok(f())
        })
    }

    pub fn unary<F>(f: F) -> Function
    where
        F: Fn(expr::Expr) -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(move |FunctionInput { arguments, .. }| Ok(f(arguments.one()?)))
    }

    pub fn binary<F>(f: F) -> Function
    where
        F: Fn(expr::Expr, expr::Expr) -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(move |FunctionInput { arguments, .. }| {
            let (left, right) = arguments.two()?;
            Ok(f(left, right))
        })
    }

    pub fn ternary<F>(f: F) -> Function
    where
        F: Fn(expr::Expr, expr::Expr, expr::Expr) -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(move |FunctionInput { arguments, .. }| {
            let (first, second, third) = arguments.three()?;
            Ok(f(first, second, third))
        })
    }

    pub fn var_arg<F>(f: F) -> Function
    where
        F: Fn(Vec<expr::Expr>) -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(move |FunctionInput { arguments, .. }| Ok(f(arguments)))
    }

    pub fn binary_op(op: Operator) -> Function {
        Arc::new(move |FunctionInput { arguments, .. }| {
            let (left, right) = arguments.two()?;
            Ok(expr::Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }))
        })
    }

    pub fn cast(data_type: DataType) -> Function {
        Arc::new(move |FunctionInput { arguments, .. }| {
            Ok(expr::Expr::Cast(expr::Cast {
                expr: Box::new(arguments.one()?),
                data_type: data_type.clone(),
            }))
        })
    }

    pub fn udf<F>(f: F) -> Function
    where
        F: ScalarUDFImpl + Send + Sync + 'static,
    {
        let func = Arc::new(ScalarUDF::from(f));
        Arc::new(move |FunctionInput { arguments, .. }| {
            Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func: func.clone(),
                args: arguments,
            }))
        })
    }

    pub fn scalar_udf<F>(f: F) -> Function
    where
        F: Fn() -> Arc<ScalarUDF> + Send + Sync + 'static,
    {
        Arc::new(move |FunctionInput { arguments, .. }| {
            Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func: f(),
                args: arguments,
            }))
        })
    }

    pub fn custom<F>(f: F) -> Function
    where
        F: Fn(FunctionInput) -> PlanResult<expr::Expr> + Send + Sync + 'static,
    {
        Arc::new(f)
    }

    pub fn unknown(name: &str) -> Function {
        let name = name.to_string();
        Arc::new(move |_| Err(PlanError::todo(format!("function: {name}"))))
    }
}

pub struct AggFunctionInput {
    pub arguments: Vec<expr::Expr>,
    pub distinct: bool,
    pub ignore_nulls: Option<bool>,
    pub filter: Option<Box<expr::Expr>>,
    pub order_by: Option<Vec<expr::Sort>>,
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
            } = input;
            let null_treatment = get_null_treatment(ignore_nulls);
            Ok(expr::Expr::AggregateFunction(AggregateFunction {
                func: f(),
                args: arguments,
                distinct,
                filter,
                order_by,
                null_treatment,
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

pub(crate) fn get_null_treatment(ignore_nulls: Option<bool>) -> Option<NullTreatment> {
    match ignore_nulls {
        Some(true) => Some(NullTreatment::IgnoreNulls),
        Some(false) => Some(NullTreatment::RespectNulls),
        None => None,
    }
}
