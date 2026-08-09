use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, exec_err};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};

/// The actual evaluation is implemented as a plan rewrite + partition-aware
/// physical operator. This UDF exists as a *marker* so the logical rewriter can detect
/// and replace it before physical planning.
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#monotonically_increasing_id>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMonotonicallyIncreasingId {
    signature: Signature,
}

impl Default for SparkMonotonicallyIncreasingId {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMonotonicallyIncreasingId {
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for SparkMonotonicallyIncreasingId {
    fn name(&self) -> &str {
        "monotonically_increasing_id"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    crate::unused_return_type!();

    // Spark: `MonotonicallyIncreasingID` declares `override def nullable: Boolean = false`
    // (MonotonicallyIncreasingID.scala).
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Int64, false)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("monotonically_increasing_id() was not rewritten into a partition-aware operator")
    }
}
