use std::sync::Arc;

use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{ExprSchema, Result, ScalarValue, internal_err};
use datafusion_expr::expr::Expr;
use datafusion_expr::{
    ExprSchemable, ExpressionPlacement, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};

/// Selects the physical execution strategy for a left-to-right lazy scalar call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LazyScalarEvaluationPolicy {
    /// Evaluate each child over the currently active rows and invoke the function once.
    ActiveRows,
    /// Try active-row evaluation, then replay row by row if evaluation fails.
    TryActiveRows,
    /// Evaluate every row from left to right without replaying child expressions.
    RowMajor,
}

/// Whether the scalar function itself can fail after its arguments have been evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LazyScalarFunctionFallibility {
    Infallible,
    Fallible,
}

/// Logical marker for a regular scalar UDF with left-to-right, NULL-short-circuit semantics.
///
/// The marker protects the evaluation contract during logical optimization and carries the
/// execution policy selected from the function and child-expression properties. It is lowered to
/// a physical lazy scalar expression at the logical-to-physical planning boundary.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LazyScalarUDF {
    function: Arc<ScalarUDF>,
    signature: Signature,
    policy: LazyScalarEvaluationPolicy,
}

impl LazyScalarUDF {
    pub fn new(function: Arc<ScalarUDF>, policy: LazyScalarEvaluationPolicy) -> Self {
        // Data-source filter pushdown cannot preserve lazy child evaluation. The logical marker
        // therefore acts as a volatility barrier until it is replaced by `LazyScalarExpr`, which
        // reports the wrapped function's actual volatility and uses `policy` for execution.
        let mut signature = function.signature().clone();
        signature.volatility = Volatility::Volatile;
        Self {
            function,
            signature,
            policy,
        }
    }

    pub fn call(
        function: Arc<ScalarUDF>,
        arguments: Vec<Expr>,
        schema: &dyn ExprSchema,
        fallibility: LazyScalarFunctionFallibility,
    ) -> Result<Expr> {
        let policy = select_evaluation_policy(&function, &arguments, fallibility);
        let marker = ScalarUDF::from(Self::new(function, policy));
        let expression = marker.call(arguments.clone());

        if let Some(null_index) = arguments.iter().position(is_constant_null)
            && arguments[..null_index].iter().all(is_proven_infallible)
        {
            let result_type = expression.get_type(schema)?;
            return Ok(Expr::Literal(
                ScalarValue::try_new_null(&result_type)?,
                None,
            ));
        }

        Ok(expression)
    }

    pub fn call_fallible(
        function: Arc<ScalarUDF>,
        arguments: Vec<Expr>,
        schema: &dyn ExprSchema,
    ) -> Result<Expr> {
        Self::call(
            function,
            arguments,
            schema,
            LazyScalarFunctionFallibility::Fallible,
        )
    }

    pub fn function(&self) -> &Arc<ScalarUDF> {
        &self.function
    }

    pub fn policy(&self) -> LazyScalarEvaluationPolicy {
        self.policy
    }
}

fn select_evaluation_policy(
    function: &ScalarUDF,
    arguments: &[Expr],
    fallibility: LazyScalarFunctionFallibility,
) -> LazyScalarEvaluationPolicy {
    let replay_safe = function.signature().volatility != Volatility::Volatile
        && arguments.iter().all(is_replay_safe);
    if !replay_safe {
        return LazyScalarEvaluationPolicy::RowMajor;
    }

    if fallibility == LazyScalarFunctionFallibility::Infallible
        && arguments.iter().all(is_proven_infallible)
    {
        LazyScalarEvaluationPolicy::ActiveRows
    } else {
        LazyScalarEvaluationPolicy::TryActiveRows
    }
}

fn is_replay_safe(expression: &Expr) -> bool {
    let mut replay_safe = true;
    let traversal = expression.apply(|node| {
        let known_pure_node = match node {
            Expr::Alias(_)
            | Expr::Column(_)
            | Expr::Literal(_, _)
            | Expr::BinaryExpr(_)
            | Expr::Like(_)
            | Expr::SimilarTo(_)
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Negative(_)
            | Expr::Between(_)
            | Expr::Case(_)
            | Expr::Cast(_)
            | Expr::TryCast(_)
            | Expr::InList(_) => true,
            Expr::ScalarFunction(function) => function
                .func
                .inner()
                .downcast_ref::<LazyScalarUDF>()
                .map_or(
                    function.func.signature().volatility != Volatility::Volatile,
                    |lazy| lazy.policy != LazyScalarEvaluationPolicy::RowMajor,
                ),
            _ => false,
        };
        if known_pure_node && !node.is_volatile_node() {
            Ok(TreeNodeRecursion::Continue)
        } else {
            replay_safe = false;
            Ok(TreeNodeRecursion::Stop)
        }
    });
    traversal.is_ok() && replay_safe
}

pub fn is_constant_null(expression: &Expr) -> bool {
    match expression {
        Expr::Literal(value, _) => value.is_null(),
        Expr::Alias(alias) => is_constant_null(alias.expr.as_ref()),
        Expr::Cast(cast) => is_constant_null(cast.expr.as_ref()),
        Expr::TryCast(cast) => is_constant_null(cast.expr.as_ref()),
        _ => false,
    }
}

pub fn is_proven_infallible(expression: &Expr) -> bool {
    let mut infallible = true;
    let traversal = expression.apply(|node| {
        let infallible_node = match node {
            Expr::Alias(_)
            | Expr::Column(_)
            | Expr::Literal(_, _)
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Between(_)
            | Expr::Case(_)
            | Expr::TryCast(_) => true,
            Expr::BinaryExpr(binary) => matches!(
                binary.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::IsDistinctFrom
                    | Operator::IsNotDistinctFrom
                    | Operator::And
                    | Operator::Or
            ),
            _ => false,
        };
        if infallible_node {
            Ok(TreeNodeRecursion::Continue)
        } else {
            infallible = false;
            Ok(TreeNodeRecursion::Stop)
        }
    });
    traversal.is_ok() && infallible
}

impl ScalarUDFImpl for LazyScalarUDF {
    fn name(&self) -> &str {
        self.function.name()
    }

    fn aliases(&self) -> &[String] {
        self.function.aliases()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        arg_types: &[datafusion::arrow::datatypes::DataType],
    ) -> Result<datafusion::arrow::datatypes::DataType> {
        self.function.return_type(arg_types)
    }

    fn return_field_from_args(
        &self,
        args: ReturnFieldArgs,
    ) -> Result<datafusion::arrow::datatypes::FieldRef> {
        let arguments_nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        let return_field = self.function.return_field_from_args(args)?;
        let nullable = return_field.is_nullable() || arguments_nullable;
        Ok(Arc::new(
            return_field.as_ref().clone().with_nullable(nullable),
        ))
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> Result<datafusion_expr::ColumnarValue> {
        internal_err!(
            "logical lazy scalar marker {} reached eager evaluation",
            self.function.name()
        )
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn coerce_types(
        &self,
        arg_types: &[datafusion::arrow::datatypes::DataType],
    ) -> Result<Vec<datafusion::arrow::datatypes::DataType>> {
        self.function.coerce_types(arg_types)
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        self.function.placement(args)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion_common::{DFSchema, Result, ScalarValue};
    use datafusion_expr::{Expr, ScalarUDF, ScalarUDFImpl, Volatility, cast, col, lit};

    use super::{LazyScalarEvaluationPolicy, LazyScalarFunctionFallibility, LazyScalarUDF};

    #[test]
    fn folds_constant_null_after_infallible_prefix() -> Result<()> {
        let schema = DFSchema::new_with_metadata(
            vec![(None, Arc::new(Field::new("value", DataType::Int64, true)))],
            Default::default(),
        )?;
        let function = Arc::new(ScalarUDF::new_from_impl(
            datafusion::functions::math::abs::AbsFunc::new(),
        ));
        let expression = LazyScalarUDF::call(
            function,
            vec![lit(ScalarValue::Int64(None))],
            &schema,
            LazyScalarFunctionFallibility::Infallible,
        )?;
        assert!(matches!(expression, Expr::Literal(value, _) if value.is_null()));
        Ok(())
    }

    #[test]
    fn volatile_child_selects_row_major() -> Result<()> {
        let function = Arc::new(ScalarUDF::new_from_impl(
            datafusion::functions::math::abs::AbsFunc::new(),
        ));
        let volatile =
            ScalarUDF::new_from_impl(datafusion::functions::math::random::RandomFunc::new())
                .call(vec![]);
        let policy = super::select_evaluation_policy(
            function.as_ref(),
            &[volatile],
            LazyScalarFunctionFallibility::Fallible,
        );
        let marker = LazyScalarUDF::new(function, policy);
        assert_eq!(marker.policy(), LazyScalarEvaluationPolicy::RowMajor);
        Ok(())
    }

    #[test]
    fn cast_child_is_replay_safe_but_not_proven_infallible() {
        let expression = cast(col("value"), DataType::Int64);
        assert!(super::is_replay_safe(&expression));
        assert!(!super::is_proven_infallible(&expression));
    }

    #[test]
    fn fallible_child_selects_replay_for_an_infallible_function() {
        let function = ScalarUDF::new_from_impl(datafusion::functions::math::abs::AbsFunc::new());
        let policy = super::select_evaluation_policy(
            &function,
            &[cast(col("value"), DataType::Int64)],
            LazyScalarFunctionFallibility::Infallible,
        );
        assert_eq!(policy, LazyScalarEvaluationPolicy::TryActiveRows);
    }

    #[test]
    fn logical_marker_blocks_unsafe_expression_movement() {
        let function = Arc::new(ScalarUDF::new_from_impl(
            datafusion::functions::math::abs::AbsFunc::new(),
        ));
        let marker = LazyScalarUDF::new(function, LazyScalarEvaluationPolicy::ActiveRows);
        assert_eq!(marker.signature().volatility, Volatility::Volatile);
    }
}
