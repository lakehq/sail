use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::Date32Array;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use sail_common_datafusion::datetime::date::string_to_date;

use crate::utils::ItemTaker;

#[derive(Debug)]
pub struct SparkDate {
    signature: Signature,
}

impl Default for SparkDate {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDate {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Native(
                    logical_string(),
                ))],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkDate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let arg = args.one()?;
        match arg {
            ColumnarValue::Array(array) => {
                let array = match array.data_type() {
                    DataType::Utf8 => as_string_array(&array)?
                        .iter()
                        .map(|x| x.map(string_to_date).transpose())
                        .collect::<Result<Date32Array>>()?,
                    DataType::LargeUtf8 => as_large_string_array(&array)?
                        .iter()
                        .map(|x| x.map(string_to_date).transpose())
                        .collect::<Result<Date32Array>>()?,
                    DataType::Utf8View => as_string_view_array(&array)?
                        .iter()
                        .map(|x| x.map(string_to_date).transpose())
                        .collect::<Result<Date32Array>>()?,
                    _ => return exec_err!("expected string array for `date`"),
                };
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            ColumnarValue::Scalar(scalar) => {
                let value = match scalar.try_as_str() {
                    Some(x) => x.map(string_to_date).transpose()?,
                    _ => {
                        return exec_err!("expected string scalar for `date`");
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(value)))
            }
        }
    }
}
