use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{plan_err, Result};
use datafusion_expr::{ColumnarValue, LogicalPlan, ScalarUDFImpl, Signature, Volatility};

/// A placeholder UDF used to represent a table input in UDTF arguments.
#[derive(Debug)]
pub struct TableInput {
    plan: Arc<LogicalPlan>,
    signature: Signature,
}

impl TableInput {
    pub fn new(plan: Arc<LogicalPlan>) -> Self {
        Self {
            plan,
            signature: Signature::exact(vec![], Volatility::Immutable),
        }
    }

    pub fn plan(&self) -> &Arc<LogicalPlan> {
        &self.plan
    }
}

impl ScalarUDFImpl for TableInput {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "table_input"
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
