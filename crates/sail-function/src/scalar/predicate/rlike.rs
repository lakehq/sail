// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::regex::regexp_like;

/// Use the regex kernel directly. DataFusion 56 can rewrite a literal backslash
/// in a regular expression to a LIKE escape and change which strings match.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RLike {
    signature: Signature,
}

impl Default for RLike {
    fn default() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for RLike {
    fn name(&self) -> &str {
        "spark_rlike"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Boolean,
            args.arg_fields.iter().any(|f| f.is_nullable()),
        )))
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        // A backslash can be spelled as an escape or selected by a character class.
        // Keep pushdown for literal patterns that cannot introduce a LIKE escape.
        if let [_, Expr::Literal(pattern, _)] = args.as_slice()
            && let Some(Some(pattern)) = pattern.try_as_str()
            && !pattern.contains(['\\', '['])
        {
            regexp_like().simplify(args, info)
        } else {
            Ok(ExprSimplifyResult::Original(args))
        }
    }

    fn should_evaluate_const(&self, args: &[&ScalarValue]) -> bool {
        regexp_like().should_evaluate_const(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        regexp_like().invoke_with_args(args)
    }
}
