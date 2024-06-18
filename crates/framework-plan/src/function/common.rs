use crate::error::{PlanError, PlanResult};
use crate::utils::ItemTaker;
use arrow::datatypes::DataType;
use datafusion_expr::{
    expr, BinaryExpr, Operator, ScalarFunctionDefinition, ScalarUDF, ScalarUDFImpl,
};
use std::sync::Arc;

pub(crate) type Function = Arc<dyn Fn(Vec<expr::Expr>) -> PlanResult<expr::Expr> + Send + Sync>;

pub(crate) struct FunctionBuilder;

impl FunctionBuilder {
    pub fn nullary<F>(f: F) -> Function
    where
        F: Fn() -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(move |args| {
            if args.len() != 0 {
                return Err(PlanError::invalid("nullary: Zero arguments expected."));
            }
            Ok(f())
        })
    }

    pub fn unary<F>(f: F) -> Function
    where
        F: Fn(expr::Expr) -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(move |args| Ok(f(args.one()?)))
    }

    pub fn binary<F>(f: F) -> Function
    where
        F: Fn(expr::Expr, expr::Expr) -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(move |args| {
            let (left, right) = args.two()?;
            Ok(f(left, right))
        })
    }

    pub fn ternary<F>(f: F) -> Function
    where
        F: Fn(expr::Expr, expr::Expr, expr::Expr) -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(move |args| {
            let (first, second, third) = args.three()?;
            Ok(f(first, second, third))
        })
    }

    pub fn var_arg<F>(f: F) -> Function
    where
        F: Fn(Vec<expr::Expr>) -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(move |args| Ok(f(args)))
    }

    pub fn binary_op(op: Operator) -> Function {
        Arc::new(move |args| {
            let (left, right) = args.two()?;
            Ok(expr::Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }))
        })
    }

    pub fn cast(data_type: DataType) -> Function {
        Arc::new(move |args| {
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
        let func_def = ScalarFunctionDefinition::UDF(Arc::new(ScalarUDF::from(f)));
        Arc::new(move |args| {
            Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func_def: func_def.clone(),
                args,
            }))
        })
    }

    pub fn dynamic_udf<F, U>(f: F) -> Function
    where
        F: Fn(Vec<expr::Expr>) -> PlanResult<U> + Send + Sync + 'static,
        U: ScalarUDFImpl + Send + Sync + 'static,
    {
        Arc::new(move |args| {
            Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(Arc::new(ScalarUDF::from(
                    f(args.clone())?,
                ))),
                args,
            }))
        })
    }

    pub fn custom<F>(f: F) -> Function
    where
        F: Fn(Vec<expr::Expr>) -> PlanResult<expr::Expr> + Send + Sync + 'static,
    {
        Arc::new(move |args| f(args))
    }

    pub fn unknown(name: &str) -> Function {
        let name = name.to_string();
        Arc::new(move |_| Err(PlanError::todo(format!("function: {name}"))))
    }
}
