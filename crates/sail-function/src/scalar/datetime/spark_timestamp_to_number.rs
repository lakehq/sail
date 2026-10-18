use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::compute::try_binary;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::arrow::error::ArrowError;
use datafusion_common::cast::as_int64_array;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::functions_nested_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimestampToNumber {
    signature: Signature,
}

impl Default for SparkTimestampToNumber {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTimestampToNumber {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Int64, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkTimestampToNumber {
    fn name(&self) -> &str {
        "spark_timestamp_to_number"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Int64,
            args.arg_fields.iter().any(|field| field.is_nullable()),
        )))
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(|values| {
            let [value, divisor] = values else {
                return internal_err!("timestamp epoch conversion requires two arguments");
            };
            let result: Int64Array = try_binary(
                as_int64_array(value)?,
                as_int64_array(divisor)?,
                |value, divisor| {
                    if divisor <= 0 {
                        return Err(ArrowError::InvalidArgumentError(
                            "timestamp epoch divisor must be positive".to_string(),
                        ));
                    }
                    Ok(value.div_euclid(divisor))
                },
            )?;
            Ok(Arc::new(result))
        })(&args.args)
    }
}
