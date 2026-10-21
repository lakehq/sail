use std::sync::Arc;

use arrow::compute::DatePart;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};

use crate::scalar::datetime::spark_date_trunc::invoke_value;

/// The earliest valid local midnight used by Spark's daysToMicros.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkStartOfDay {
    signature: Signature,
}

impl Default for SparkStartOfDay {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkStartOfDay {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Timestamp)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkStartOfDay {
    fn name(&self) -> &str {
        "spark_start_of_day"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = &args.arg_fields[0];
        Ok(Arc::new(Field::new(
            self.name(),
            field.data_type().clone(),
            field.is_nullable(),
        )))
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_value(DatePart::Day, "day", &args.args[0], true)
    }
}
