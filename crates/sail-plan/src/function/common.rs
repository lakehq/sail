use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{Operator, ScalarUDF};

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::typing;
use crate::error::{PlanError, PlanResult};

/// Shared inputs for constructing typed analyzer expressions.
/// Function implementations borrow the ID allocator without depending on an analyzer layer.
pub struct FunctionContextInput<'a> {
    pub session: &'a datafusion::prelude::SessionContext,
    pub config: &'a Arc<crate::config::PlanConfig>,
    pub ids: &'a mut IdGenerator,
}

impl FunctionContextInput<'_> {
    pub(crate) fn cast(&mut self, expression: Expr, target: DataType) -> PlanResult<Expr> {
        typing::coerce(expression, &target, self.ids)
    }

    pub(crate) fn case(
        &mut self,
        branches: Vec<(Expr, Expr)>,
        otherwise: Option<Expr>,
    ) -> PlanResult<Expr> {
        typing::resolve_case(self.ids.expression(), None, branches, otherwise, self.ids)
    }

    pub(crate) fn not(&mut self, expression: Expr) -> PlanResult<Expr> {
        let expression = self.cast(expression, DataType::Boolean)?;
        Ok(Expr {
            id: self.ids.expression(),
            result_type: expression.result_type.clone(),
            kind: Deferred::Resolved(ExprKind::Not(Box::new(expression))),
        })
    }

    pub(crate) fn is_not_null(&mut self, expression: Expr) -> Expr {
        Expr {
            id: self.ids.expression(),
            kind: Deferred::Resolved(ExprKind::IsNotNull(Box::new(expression))),
            result_type: Deferred::Resolved(ExprType::new(DataType::Boolean, false)),
        }
    }

    pub(crate) fn is_null(&mut self, expression: Expr) -> Expr {
        Expr {
            id: self.ids.expression(),
            kind: Deferred::Resolved(ExprKind::IsNull(Box::new(expression))),
            result_type: Deferred::Resolved(ExprType::new(DataType::Boolean, false)),
        }
    }

    pub(crate) fn copy_expression(&mut self, expression: &Expr) -> PlanResult<Expr> {
        use sail_common_plan::rewrite::{QueryRewriter, walk_expression};
        struct Copy<'a>(&'a mut IdGenerator);
        impl QueryRewriter for Copy<'_> {
            type Error = PlanError;
            fn rewrite_expression(&mut self, expression: Expr) -> PlanResult<Expr> {
                let mut expression = walk_expression(self, expression)?;
                expression.id = self.0.expression();
                if let Deferred::Resolved(ExprKind::HigherOrderFunction { arguments, .. }) =
                    &mut expression.kind
                {
                    for argument in arguments {
                        if let FunctionArg::Lambda { id, .. } = argument {
                            *id = self.0.expression();
                        }
                    }
                }
                Ok(expression)
            }
        }
        Copy(self.ids).rewrite_expression(expression.clone())
    }

    pub(crate) fn literal(&mut self, value: ScalarValue) -> Expr {
        Expr::literal(self.ids.expression(), value)
    }

    pub(crate) fn binary(
        &mut self,
        left: Expr,
        operator: Operator,
        right: Expr,
    ) -> PlanResult<Expr> {
        let (left, right) =
            crate::function::scalar::predicate::predicate_pair(self, left, operator, right)?;
        typing::resolve_binary(self.ids.expression(), left, operator, right, self.ids)
    }

    pub(crate) fn scalar_kernel(&self, name: &str) -> PlanResult<Arc<ScalarUDF>> {
        self.session
            .udf(name)
            .ok()
            .or_else(|| crate::function::scalar::kernel::default_scalar_kernel(name))
            .ok_or_else(|| {
                crate::error::PlanError::unsupported(format!("unknown scalar kernel: {name}"))
            })
    }

    pub(crate) fn aggregate_kernel(
        &self,
        name: &str,
    ) -> PlanResult<Arc<datafusion_expr::AggregateUDF>> {
        crate::function::get_built_in_aggregate_function(name)
            .map(|function| function.kernel(self))
            .transpose()?
            .or_else(|| self.session.udaf(name).ok())
            .ok_or_else(|| {
                crate::error::PlanError::unsupported(format!("unknown aggregate kernel: {name}"))
            })
    }

    pub(crate) fn scalar(&mut self, name: &str, arguments: Vec<Expr>) -> PlanResult<Expr> {
        self.scalar_udf(self.scalar_kernel(name)?, arguments)
    }

    pub(crate) fn scalar_udf(
        &mut self,
        function: Arc<ScalarUDF>,
        arguments: Vec<Expr>,
    ) -> PlanResult<Expr> {
        typing::resolve_scalar_call(self.ids.expression(), function, arguments, self.ids)
    }

    pub(crate) fn aggregate(&mut self, name: &str, arguments: Vec<Expr>) -> PlanResult<Expr> {
        self.aggregate_call(self.aggregate_kernel(name)?, arguments)
    }

    pub(crate) fn aggregate_call(
        &mut self,
        function: Arc<datafusion_expr::AggregateUDF>,
        arguments: Vec<Expr>,
    ) -> PlanResult<Expr> {
        crate::function::aggregate::aggregate_call_with_options(
            self,
            function,
            arguments,
            crate::function::aggregate::AggregateOptions::default(),
        )
    }

    pub(crate) fn try_cast(&mut self, expression: Expr, data_type: DataType) -> PlanResult<Expr> {
        let field = expression
            .result_type
            .resolved("try cast input")?
            .to_field("");
        if !arrow::compute::can_cast_types(field.data_type(), &data_type) {
            return Err(PlanError::analysis(format!(
                "cannot cast {} to {data_type}",
                field.data_type()
            )));
        }
        let mut target = ExprType::from_field(&crate::analyzer::typing::coercion::cast_field(
            &field, &data_type,
        ));
        target.nullable = true;
        Ok(Expr {
            id: self.ids.expression(),
            result_type: Deferred::Resolved(target.clone()),
            kind: Deferred::Resolved(ExprKind::Cast {
                expression: Box::new(expression),
                target,
                safe: true,
            }),
        })
    }
}

impl FunctionContextInput<'_> {
    pub(crate) fn attribute(&mut self, name: String, result_type: ExprType) -> Attribute {
        let id = self.ids.attribute();
        Attribute {
            id,
            origin: id,
            name,
            qualifier: vec![],
            qualified_position: 0,
            plan_ids: vec![],
            hidden: false,
            result_type: Deferred::Resolved(result_type),
        }
    }
}

/// Resolved analyzer arguments and the context used to allocate typed expressions.
pub struct ScalarFunctionInput<'a> {
    pub arguments: Vec<Expr>,
    pub argument_display_names: &'a [String],
    pub context: FunctionContextInput<'a>,
}

pub type ScalarFunction = Arc<dyn Fn(ScalarFunctionInput<'_>) -> PlanResult<Expr> + Send + Sync>;

pub(crate) struct ScalarFunctionBuilder;

impl ScalarFunctionBuilder {
    pub fn custom<F>(function: F) -> ScalarFunction
    where
        F: Fn(ScalarFunctionInput<'_>) -> PlanResult<Expr> + Send + Sync + 'static,
    {
        Arc::new(function)
    }

    pub fn scalar_udf<F>(function: F) -> ScalarFunction
    where
        F: Fn() -> Arc<ScalarUDF> + Send + Sync + 'static,
    {
        Self::custom(move |mut input| input.context.scalar_udf(function(), input.arguments))
    }

    pub fn udf(function: impl datafusion_expr::ScalarUDFImpl + 'static) -> ScalarFunction {
        let function = Arc::new(ScalarUDF::from(function));
        Self::scalar_udf(move || Arc::clone(&function))
    }

    pub fn kernel(name: &'static str) -> ScalarFunction {
        Self::custom(move |mut input| input.context.scalar(name, input.arguments))
    }

    pub fn cast(data_type: DataType) -> ScalarFunction {
        use sail_common_datafusion::utils::items::ItemTaker;
        Self::custom(move |mut input| {
            input
                .context
                .cast(input.arguments.one()?, data_type.clone())
        })
    }

    pub fn binary_op(operator: Operator) -> ScalarFunction {
        use sail_common_datafusion::utils::items::ItemTaker;
        Self::custom(move |mut input| {
            let (left, right) = input.arguments.two()?;
            input.context.binary(left, operator, right)
        })
    }
}
