use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, plan_err};
use datafusion_expr::{
    ColumnarValue, LogicalPlan, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// A placeholder UDF used to represent a table input in UDTF arguments.
#[derive(Debug, PartialEq, Eq, Hash)]
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
    fn name(&self) -> &str {
        "table_input"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        sail_type_system::policies::table_input::return_type(arg_types)
    }

    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        plan_err!(
            "{} should be rewritten during logical plan analysis",
            self.name()
        )
    }
}
