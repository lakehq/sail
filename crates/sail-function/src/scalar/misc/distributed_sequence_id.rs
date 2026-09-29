use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, exec_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, TypeSignature, Volatility};

/// Marker rewritten to a partition-aware operator before physical planning.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDistributedSequenceId {
    signature: Signature,
}

impl Default for SparkDistributedSequenceId {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDistributedSequenceId {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Nullary,
                    TypeSignature::Exact(vec![DataType::Boolean]),
                ],
                Volatility::Volatile,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkDistributedSequenceId {
    fn name(&self) -> &str {
        "distributed_sequence_id"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("distributed_sequence_id() was not rewritten into a partition-aware operator")
    }
}
