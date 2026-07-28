use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err, plan_err};
use datafusion_expr::{
    ColumnarValue, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
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
        internal_err!(
            "{}: `return_type` should not be called; `return_field_from_args` is used instead",
            self.name()
        )
    }

    // Internal plumbing; nullability follows the inputs.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let arg_types = arg_types.as_slice();
        let data_type = self.output_type(arg_types)?;
        Ok(Arc::new(Field::new(
            self.name(),
            data_type,
            args.arg_fields.iter().any(|field| field.is_nullable()),
        )))
    }

    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        plan_err!(
            "{} should be rewritten during logical plan analysis",
            self.name()
        )
    }
}
