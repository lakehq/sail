use crate::error::{PlanError, PlanResult};
use arrow::datatypes::DataType;
use datafusion_expr::{
    expr, BinaryExpr, Operator, ScalarFunctionDefinition, ScalarUDF, ScalarUDFImpl,
};
use std::sync::Arc;

pub(crate) type Function = Arc<dyn Fn(Vec<expr::Expr>) -> PlanResult<expr::Expr> + Send + Sync>;

pub(crate) fn get_one_argument(mut args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
    if args.len() != 1 {
        return Err(PlanError::invalid("one argument expected"));
    }
    Ok(args.pop().unwrap())
}

pub(crate) fn get_two_arguments(mut args: Vec<expr::Expr>) -> PlanResult<(expr::Expr, expr::Expr)> {
    if args.len() != 2 {
        return Err(PlanError::invalid("two arguments expected"));
    }
    let right = args.pop().unwrap();
    let left = args.pop().unwrap();
    Ok((left, right))
}

pub(crate) fn get_three_arguments(
    mut args: Vec<expr::Expr>,
) -> PlanResult<(expr::Expr, expr::Expr, expr::Expr)> {
    if args.len() != 3 {
        return Err(PlanError::invalid("three arguments expected"));
    }
    let first = args.pop().unwrap();
    let second = args.pop().unwrap();
    let third = args.pop().unwrap();
    Ok((first, second, third))
}

pub(crate) struct FunctionBuilder;

impl FunctionBuilder {
    pub fn unary<F>(f: F) -> Function
    where
        F: Fn(expr::Expr) -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(move |args| {
            let arg = get_one_argument(args)?;
            Ok(f(arg))
        })
    }

    pub fn binary<F>(f: F) -> Function
    where
        F: Fn(expr::Expr, expr::Expr) -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(move |args| {
            let (left, right) = get_two_arguments(args)?;
            Ok(f(left, right))
        })
    }

    pub fn ternary<F>(f: F) -> Function
    where
        F: Fn(expr::Expr, expr::Expr, expr::Expr) -> expr::Expr + Send + Sync + 'static,
    {
        Arc::new(move |args| {
            let (first, second, third) = get_three_arguments(args)?;
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
            let (left, right) = get_two_arguments(args)?;
            Ok(expr::Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }))
        })
    }

    pub fn cast(data_type: DataType) -> Function {
        Arc::new(move |args| {
            let arg = get_one_argument(args)?;
            Ok(expr::Expr::Cast(expr::Cast {
                expr: Box::new(arg),
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

    pub fn dynamic_udf<F, U, E>(f: F) -> Function
    where
        F: Fn(Vec<expr::Expr>) -> Result<U, E> + Send + Sync + 'static,
        E: Into<PlanError>,
        U: ScalarUDFImpl + Send + Sync + 'static,
    {
        Arc::new(move |args| {
            Ok(expr::Expr::ScalarFunction(expr::ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(Arc::new(ScalarUDF::from(
                    f(args.clone()).map_err(|e| e.into())?,
                ))),
                args,
            }))
        })
    }

    pub fn custom<F, E>(f: F) -> Function
    where
        F: Fn(Vec<expr::Expr>) -> Result<expr::Expr, E> + Send + Sync + 'static,
        E: Into<PlanError>,
    {
        Arc::new(move |args| f(args).map_err(|e| e.into()))
    }

    pub fn unknown(name: &str) -> Function {
        let name = name.to_string();
        Arc::new(move |_| Err(PlanError::todo(format!("function: {name}"))))
    }
}
