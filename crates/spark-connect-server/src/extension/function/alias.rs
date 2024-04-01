use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub(crate) struct MultiAlias {
    signature: Signature,
    names: Vec<String>,
}

impl MultiAlias {
    pub(crate) fn new(names: impl Into<Vec<String>>) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            names: names.into(),
        }
    }

    pub(crate) fn names(&self) -> &Vec<String> {
        &self.names
    }

    pub(crate) fn with_names(&self, names: Vec<String>) -> Result<Self> {
        if names.len() != self.names.len() {
            return Err(DataFusionError::Internal(format!(
                "expected {} names, found {}",
                self.names.len(),
                names.len()
            )));
        }
        Ok(Self::new(names))
    }
}

impl ScalarUDFImpl for MultiAlias {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "multi_alias"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types {
            &[x] => Ok(x.clone()),
            _ => Err(DataFusionError::Internal(format!(
                "{} should only be called with a single argument",
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
