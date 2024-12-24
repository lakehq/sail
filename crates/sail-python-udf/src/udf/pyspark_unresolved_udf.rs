use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion_common::internal_err;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use sail_common::spec;

#[derive(Debug, Clone)]
pub struct PySparkUnresolvedUDF {
    signature: Signature,
    name: String,
    python_version: String,
    eval_type: spec::PySparkUdfType,
    command: Vec<u8>,
    output_type: DataType,
    deterministic: bool,
}

impl PySparkUnresolvedUDF {
    pub fn new(
        name: String,
        python_version: String,
        eval_type: spec::PySparkUdfType,
        command: Vec<u8>,
        output_type: DataType,
        deterministic: bool,
    ) -> Self {
        Self {
            signature: Signature::variadic_any(match deterministic {
                true => Volatility::Immutable,
                false => Volatility::Volatile,
            }),
            name,
            python_version,
            eval_type,
            command,
            output_type,
            deterministic,
        }
    }

    pub fn python_version(&self) -> &str {
        &self.python_version
    }

    pub fn eval_type(&self) -> spec::PySparkUdfType {
        self.eval_type
    }

    pub fn command(&self) -> &[u8] {
        &self.command
    }

    pub fn output_type(&self) -> &DataType {
        &self.output_type
    }

    pub fn deterministic(&self) -> bool {
        self.deterministic
    }
}

impl ScalarUDFImpl for PySparkUnresolvedUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.output_type.clone())
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
        internal_err!("unresolved UDF {} cannot be invoked", self.name())
    }
}
