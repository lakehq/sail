use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Subquery,
    Volatility,
};
use sail_common_datafusion::utils::items::ItemTaker;

/// Identity wrapper that gives a scalar subquery Spark's nullability.
///
/// Spark: `ScalarSubquery.nullable = true`, unconditional, because a subquery that
/// returns no rows evaluates to NULL whatever its projected field declares.
/// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/subquery.scala#L426>
///
/// DataFusion 55 derives `Expr::ScalarSubquery` nullability from the subquery's
/// projected field instead, so the expression simplifier, the schema reported to
/// clients, and the physical planner can all treat the empty-result NULL as
/// impossible. Wrapping the subquery expression in this function makes the
/// nullable field the only one those consumers see; the value passes through
/// unchanged. The function is deliberately not declared strict: DataFusion's
/// predicate bounds analysis would then look through it at the bare subquery
/// and conclude the wrapper can never be NULL. DataFusion 56 fixes this
/// upstream (apache/datafusion#24516 and apache/datafusion#24534), after which
/// the wrapper can be removed.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkScalarSubquery {
    signature: Signature,
}

impl Default for SparkScalarSubquery {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkScalarSubquery {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    /// Returns the argument of this function when `expr` is such a call.
    pub fn argument(expr: &Expr) -> Option<&Expr> {
        match expr {
            Expr::ScalarFunction(function) if function.func.inner().is::<Self>() => {
                match function.args.as_slice() {
                    [argument] => Some(argument),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// Returns the scalar subquery wrapped by this function when `expr` is such a call.
    pub fn subquery(expr: &Expr) -> Option<&Subquery> {
        match Self::argument(expr) {
            Some(Expr::ScalarSubquery(subquery)) => Some(subquery),
            _ => None,
        }
    }
}

impl ScalarUDFImpl for SparkScalarSubquery {
    fn name(&self) -> &str {
        "spark_scalar_subquery"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [input] = args.arg_fields else {
            return internal_err!("spark_scalar_subquery expects exactly one argument");
        };
        Ok(Arc::new(input.as_ref().clone().with_nullable(true)))
    }

    /// Decorrelation (`ScalarSubqueryToJoin`) replaces the subquery with a column of
    /// the LEFT JOIN it builds. Once that column is nullable on its own the wrapper
    /// adds nothing and would only turn equi-join keys into expressions, so it
    /// unwraps itself; a column that is still non-nullable keeps the wrapper.
    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        // A column the simplifier cannot resolve (no schema in the context) keeps the wrapper.
        if matches!(args.as_slice(), [column @ Expr::Column(_)] if info.nullable(column).unwrap_or(false))
        {
            Ok(ExprSimplifyResult::Simplified(args.one()?))
        } else {
            Ok(ExprSimplifyResult::Original(args))
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        args.args.one()
    }
}
