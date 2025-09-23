use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use sail_common_datafusion::utils::items::ItemTaker;

use crate::scalar::datetime::utils::spark_datetime_format_to_chrono_strftime;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkToChronoFmt {
    signature: Signature,
}

impl Default for SparkToChronoFmt {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkToChronoFmt {
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

impl ScalarUDFImpl for SparkToChronoFmt {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_to_chrono_fmt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let arg = args.one()?;
        match arg {
            ColumnarValue::Array(array) => {
                let array = match array.data_type() {
                    DataType::Utf8 => as_string_array(&array)?
                        .iter()
                        .map(|x| x.map(spark_datetime_format_to_chrono_strftime).transpose())
                        .collect::<Result<StringArray>>()?,
                    DataType::LargeUtf8 => as_large_string_array(&array)?
                        .iter()
                        .map(|x| x.map(spark_datetime_format_to_chrono_strftime).transpose())
                        .collect::<Result<StringArray>>()?,
                    DataType::Utf8View => as_string_view_array(&array)?
                        .iter()
                        .map(|x| x.map(spark_datetime_format_to_chrono_strftime).transpose())
                        .collect::<Result<StringArray>>()?,
                    _ => return exec_err!("expected string array for `spark_to_chrono_fmt`"),
                };
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            ColumnarValue::Scalar(scalar) => {
                let value = match scalar.try_as_str() {
                    Some(x) => x
                        .map(spark_datetime_format_to_chrono_strftime)
                        .transpose()?,
                    _ => {
                        return exec_err!("expected string scalar for `spark_to_chrono_fmt`");
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(value)))
            }
        }
    }
}
