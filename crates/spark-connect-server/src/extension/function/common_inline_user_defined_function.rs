use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub(crate) struct CommonInlineUserDefinedFunction {
    signature: Signature,
    aliases: Vec<String>,
}
