use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "version"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_version, vec![])(&args)
    }
}

fn spark_version(_args: &[ArrayRef]) -> Result<ArrayRef> {
    Ok(Arc::new(StringArray::from(vec![Some(SparkVersion::SAIL_VERSION)])) as ArrayRef)
}
