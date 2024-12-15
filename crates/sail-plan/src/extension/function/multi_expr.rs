use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{plan_err, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

/// A placeholder UDF used to represent a list of expressions that come from the output
/// of generator expressions or wildcard expressions.
/// The UDF must only exist as a top-level expression in projection nodes,
/// otherwise a planning error will be raised during logical plan analysis.
#[derive(Debug)]
pub struct MultiExpr {
    signature: Signature,
}

impl Default for MultiExpr {
    fn default() -> Self {
        Self::new()
    }
}

impl MultiExpr {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for MultiExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "multi_expr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        plan_err!(
            "{} should be rewritten during logical plan analysis",
            self.name()
        )
    }

    fn invoke(&self, _: &[ColumnarValue]) -> Result<ColumnarValue> {
        plan_err!(
            "{} should be rewritten during logical plan analysis",
            self.name()
        )
    }
}
