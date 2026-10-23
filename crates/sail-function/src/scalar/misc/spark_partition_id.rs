use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, exec_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};

/// Marker for partition-aware evaluation. Plan rewrites materialize the value
/// as a column, or a stateful filter binds it to its execution partition.
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
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("spark_partition_id() was not rewritten into a partition-aware operator")
    }
}
