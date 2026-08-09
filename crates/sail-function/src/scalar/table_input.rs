use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, plan_err};
use datafusion_expr::{
    ColumnarValue, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::udf_utils::{any_arg_nullable, arg_data_types};

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

    fn output_type(&self, _: &[DataType]) -> Result<DataType> {
        plan_err!(
            "{} should be rewritten during logical plan analysis",
            self.name()
        )
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        self.output_type(&[])
    }

    // Internal plumbing; nullability follows the inputs.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let data_type = self.output_type(&arg_data_types(&args))?;
        Ok(Arc::new(Field::new(
            self.name(),
            data_type,
            any_arg_nullable(&args),
        )))
    }

    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        plan_err!(
            "{} should be rewritten during logical plan analysis",
            self.name()
        )
    }
}
