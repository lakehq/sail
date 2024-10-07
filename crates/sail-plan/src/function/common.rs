use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::prelude::SessionContext;
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::{expr, AggregateUDF, BinaryExpr, Operator, ScalarUDF, ScalarUDFImpl};

use crate::config::PlanConfig;
use crate::error::{PlanError, PlanResult};
use crate::utils::ItemTaker;

pub struct FunctionContext<'a> {
    plan_config: Arc<PlanConfig>,
    ctx: &'a SessionContext,
}

impl<'a> FunctionContext<'a> {
    pub fn new(plan_config: Arc<PlanConfig>, ctx: &'a SessionContext) -> Self {
        Self { plan_config, ctx }
    }

    pub fn plan_config(&self) -> &Arc<PlanConfig> {
        &self.plan_config
    }

    pub fn session_context(&self) -> &SessionContext {
        self.ctx
    }
}

pub(crate) type Function =
    Arc<dyn Fn(Vec<expr::Expr>, &FunctionContext) -> PlanResult<expr::Expr> + Send + Sync>;

pub(crate) struct FunctionBuilder;

impl FunctionBuilder {
    pub fn nullary<F>(f: F) -> Function
    where
        F: Fn() -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(move |args, _config| {
            args.zero()?;
            Ok(f())
        })
    }

    pub fn unary<F>(f: F) -> Function
    where
        F: Fn(expr::Expr) -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(move |args, _config| Ok(f(args.one()?)))
    }

    pub fn binary<F>(f: F) -> Function
    where
        F: Fn(expr::Expr, expr::Expr) -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(move |args, _config| {
            let (left, right) = args.two()?;
            Ok(f(left, right))
        })
    }

    pub fn ternary<F>(f: F) -> Function
    where
        F: Fn(expr::Expr, expr::Expr, expr::Expr) -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(move |args, _config| {
            let (first, second, third) = args.three()?;
            Ok(f(first, second, third))
        })
    }

    pub fn var_arg<F>(f: F) -> Function
    where
        F: Fn(Vec<expr::Expr>) -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(move |args, _config| Ok(f(args)))
    }

    pub fn binary_op(op: Operator) -> Function {
        Arc::new(move |args, _config| {
            let (left, right) = args.two()?;
            Ok(expr::Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }))
        })
    }

    pub fn cast(data_type: DataType) -> Function {
        Arc::new(move |args, _config| {
            Ok(expr::Expr::Cast(expr::Cast {
                expr: Box::new(args.one()?),
                data_type: data_type.clone(),
            }))
        })
    }

    pub fn udf<F>(f: F) -> Function
    where
        F: ScalarUDFImpl + Send + Sync + 'static,
    {
        let func = Arc::new(ScalarUDF::from(f));
        Arc::new(move |args, _config| {
            Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func: func.clone(),
                args,
            }))
        })
    }

    pub fn scalar_udf<F>(f: F) -> Function
    where
        F: Fn() -> Arc<ScalarUDF> + Send + Sync + 'static,
    {
        Arc::new(move |args, _config| {
            Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func: f(),
                args,
            }))
        })
    }

    pub fn dynamic_udf<F, U>(f: F) -> Function
    where
        F: Fn(Vec<expr::Expr>) -> PlanResult<U> + Send + Sync + 'static,
        U: ScalarUDFImpl + Send + Sync + 'static,
    {
        Arc::new(move |args, _config| {
            Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func: Arc::new(ScalarUDF::from(f(args.clone())?)),
                args,
            }))
        })
    }

    pub fn custom<F>(f: F) -> Function
    where
        F: Fn(Vec<expr::Expr>, &FunctionContext) -> PlanResult<expr::Expr> + Send + Sync + 'static,
    {
        Arc::new(f)
    }

    pub fn unknown(name: &str) -> Function {
        let name = name.to_string();
        Arc::new(move |_, _| Err(PlanError::todo(format!("function: {name}"))))
    }
}

pub struct AggFunctionContext {
    distinct: bool,
}

impl AggFunctionContext {
    pub fn new(distinct: bool) -> Self {
        Self { distinct }
    }

    pub fn distinct(&self) -> bool {
        self.distinct
    }
}

pub(crate) type AggFunction =
    Arc<dyn Fn(Vec<expr::Expr>, AggFunctionContext) -> PlanResult<expr::Expr> + Send + Sync>;

pub(crate) struct AggFunctionBuilder;

impl AggFunctionBuilder {
    pub fn default<F>(f: F) -> AggFunction
    where
        F: Fn() -> Arc<AggregateUDF> + Send + Sync + 'static,
    {
        Arc::new(move |args, agg_function_context| {
            Ok(expr::Expr::AggregateFunction(AggregateFunction {
                func: f(),
                args,
                distinct: agg_function_context.distinct(),
                filter: None,
                order_by: None,
                null_treatment: None,
            }))
        })
    }

    pub fn custom<F>(f: F) -> AggFunction
    where
        F: Fn(Vec<expr::Expr>, AggFunctionContext) -> PlanResult<expr::Expr>
            + Send
            + Sync
            + 'static,
    {
        Arc::new(f)
    }

    pub fn unknown(name: &str) -> AggFunction {
        let name = name.to_string();
        Arc::new(move |_, _| {
            Err(PlanError::todo(format!(
                "unknown aggregate function: {name}"
            )))
        })
    }
}
