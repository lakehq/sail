use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, exec_err};
use datafusion_common::internal_err;
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};

/// The actual evaluation is implemented as a plan rewrite + partition-aware
/// physical operator. This UDF exists as a *marker* so the logical rewriter can detect
/// and replace it before physical planning.
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#spark_partition_id>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkPartitionId {
    signature: Signature,
}

impl Default for SparkPartitionId {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkPartitionId {
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for SparkPartitionId {
    fn name(&self) -> &str {
        "spark_partition_id"
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

    // Spark: `SparkPartitionID` declares `override def nullable: Boolean = false`
    // (SparkPartitionID.scala:39).
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Int32, false)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("spark_partition_id() was not rewritten into a partition-aware operator")
    }
}
