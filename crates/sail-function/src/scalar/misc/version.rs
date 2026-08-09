use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::Result;
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};

use crate::functions_utils::make_scalar_function;

/// <https://spark.apache.org/docs/latest/api/sql/index.html#version>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkVersion {
    signature: Signature,
}

impl Default for SparkVersion {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkVersion {
    const SAIL_VERSION: &'static str = env!("CARGO_PKG_VERSION");
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkVersion {
    fn name(&self) -> &str {
        "version"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    crate::unused_return_type!();

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_version, vec![])(&args)
    }
}

fn spark_version(_args: &[ArrayRef]) -> Result<ArrayRef> {
    Ok(Arc::new(StringArray::from(vec![Some(SparkVersion::SAIL_VERSION)])) as ArrayRef)
}
