use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::utils::items::ItemTaker;

/// Removes provisional hints while preserving their original expressions.
pub fn erase_conditional_type_hints(expr: Expr) -> Result<Expr> {
    Ok(expr
        .transform_up(|expr| match expr {
            Expr::ScalarFunction(function)
                if function.func.inner().is::<SparkConditionalTypeHint>() =>
            {
                let (value, _, _) = function.args.three()?;
                Ok(Transformed::yes(value))
            }
            expr => Ok(Transformed::no(expr)),
        })?
        .data)
}

/// Identifies user conditionals without changing their provisional value type.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkConditionalTypeHint {
    signature: Signature,
}

impl Default for SparkConditionalTypeHint {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkConditionalTypeHint {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkConditionalTypeHint {
    fn name(&self) -> &str {
        "spark_conditional_type_hint"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let [value, _, _] = args else {
            return plan_err!("{} requires 3 arguments, got {}", self.name(), args.len());
        };
        Ok(value.schema_name().to_string())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [value, _, hint] = arg_types else {
            return plan_err!(
                "{} requires 3 arguments, got {}",
                self.name(),
                arg_types.len()
            );
        };
        // Coercing only the phase marker leaves the original expression's coercion unchanged.
        Ok(vec![value.clone(), DataType::Boolean, hint.clone()])
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [value, _, _] = arg_types else {
            return plan_err!(
                "{} requires 3 arguments, got {}",
                self.name(),
                arg_types.len()
            );
        };
        Ok(value.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [value, _, _] = args.arg_fields else {
            return plan_err!(
                "{} requires 3 arguments, got {}",
                self.name(),
                args.arg_fields.len()
            );
        };
        // The original CASE type is required by existing value builders. Only
        // observation-only expression copies may expose the common branch type.
        Ok(Arc::clone(value))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value, _, _] = args.args.as_slice() else {
            return exec_err!(
                "{} requires 3 arguments, got {}",
                self.name(),
                args.args.len()
            );
        };
        Ok(value.clone())
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let [value, marker, _] = args.as_slice() else {
            return plan_err!("{} requires 3 arguments, got {}", self.name(), args.len());
        };
        if matches!(marker, Expr::Literal(ScalarValue::Null, _)) {
            Ok(ExprSimplifyResult::Original(args))
        } else {
            Ok(ExprSimplifyResult::Simplified(value.clone()))
        }
    }
}
