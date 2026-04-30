use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::Result;
use datafusion_common::internal_err;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common::spec;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PySparkUnresolvedUDF {
    signature: Signature,
    name: String,
    python_version: String,
    eval_type: spec::PySparkUdfType,
    command: Vec<u8>,
    /// The output type of the UDF. `None` for UDTFs that use an `analyze` static method
    /// to determine the return type dynamically at query analysis time.
    output_type: Option<DataType>,
    /// UDT metadata to attach to the output field (e.g., udt.python_class).
    output_metadata: Vec<(String, String)>,
    deterministic: bool,
}

impl PySparkUnresolvedUDF {
    pub fn new(
        name: String,
        python_version: String,
        eval_type: spec::PySparkUdfType,
        command: Vec<u8>,
        output_type: Option<DataType>,
        output_metadata: HashMap<String, String>,
        deterministic: bool,
    ) -> Self {
        let mut output_metadata_vec: Vec<(String, String)> = output_metadata.into_iter().collect();
        output_metadata_vec.sort();
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
            output_metadata: output_metadata_vec,
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

    pub fn output_type(&self) -> Option<&DataType> {
        self.output_type.as_ref()
    }

    pub fn output_metadata(&self) -> &[(String, String)] {
        &self.output_metadata
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
        match &self.output_type {
            Some(t) => Ok(t.clone()),
            None => internal_err!(
                "unresolved UDF {} has no scalar return type; \
                 dynamic-return UDTFs must be resolved during query analysis before scalar use",
                self.name()
            ),
        }
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let data_type = self.return_type(&[])?;
        let metadata: HashMap<String, String> = self.output_metadata.iter().cloned().collect();
        let field = Field::new(self.name(), data_type, true).with_metadata(metadata);
        Ok(Arc::new(field))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("unresolved UDF {} cannot be invoked", self.name())
    }
}
