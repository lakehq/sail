use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub(crate) struct Explode {
    signature: Signature,
    name: String,
    output_names: Option<Vec<String>>,
}

impl Explode {
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            name: name.into(),
            output_names: None,
        }
    }

    pub(crate) fn with_output_names(&self, output_names: Vec<String>) -> Result<Self> {
        let mut f = Self::new(&self.name);
        f.output_names = Some(output_names);
        Ok(f)
    }

    pub(crate) fn output_names(&self) -> Option<&Vec<String>> {
        self.output_names.as_ref()
    }
}

impl ScalarUDFImpl for Explode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types {
            &[DataType::List(f)]
            | &[DataType::LargeList(f)]
            | &[DataType::FixedSizeList(f, _)]
            | &[DataType::Map(f, _)] => Ok(f.data_type().clone()),
            _ => Err(DataFusionError::Internal(format!(
                "{} should only be called with a list or map",
                self.name()
            ))),
        }
    }

    fn invoke(&self, _: &[ColumnarValue]) -> Result<ColumnarValue> {
        Err(DataFusionError::Internal(format!(
            "{} should be rewritten during logical plan analysis",
            self.name()
        )))
    }
}
