use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use sail_common::spec::FunctionDefinition;

#[derive(Debug, Clone)]
pub struct UnresolvedPySparkUDF {
    signature: Signature,
    function_name: String,
    python_function_definition: FunctionDefinition,
    output_type: DataType,
    deterministic: bool,
}

impl UnresolvedPySparkUDF {
    pub fn new(
        function_name: String,
        python_function_definition: FunctionDefinition,
        output_type: DataType,
        deterministic: bool,
    ) -> Self {
        Self {
            signature: Signature::variadic_any(
                // TODO: Check if this is correct. There is also `Volatility::Stable`
                match deterministic {
                    true => Volatility::Immutable,
                    false => Volatility::Volatile,
                },
            ),
            function_name,
            python_function_definition,
            output_type,
            deterministic,
        }
    }

    pub fn python_function_definition(&self) -> Result<&FunctionDefinition> {
        Ok(&self.python_function_definition)
    }

    pub fn deterministic(&self) -> Result<bool> {
        Ok(self.deterministic)
    }
}

impl ScalarUDFImpl for UnresolvedPySparkUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.function_name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.output_type.clone())
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
        Err(DataFusionError::Internal(format!(
            "{} Unresolved UDF cannot be invoked",
            self.name()
        )))
    }
}
