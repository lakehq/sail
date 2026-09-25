use std::sync::Arc;

use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::invalid_arg_count_exec_err;

/// Strict casts introduced by ANSI conditional branch coercion. Keeping the cast
/// in a UDF lets DataFusion defer invalid literals in unselected CASE branches.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkConditionalCast {
    signature: Signature,
    target_type: DataType,
}

impl SparkConditionalCast {
    pub fn new(target_type: DataType) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            target_type,
        }
    }

    pub fn target_type(&self) -> &DataType {
        &self.target_type
    }
}

impl ScalarUDFImpl for SparkConditionalCast {
    fn name(&self) -> &str {
        "spark_conditional_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.target_type.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [source] = args.arg_fields else {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (1, 1),
                args.arg_fields.len(),
            ));
        };
        let nullable = source.is_nullable()
            || (source.data_type().is_string() && self.target_type.is_numeric());
        Ok(Arc::new(Field::new(
            self.name(),
            self.target_type.clone(),
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (1, 1),
                args.args.len(),
            ));
        };
        // TODO: Match Spark's numeric STRING grammar when shared cast support is available:
        // control-character trimming, floating-point suffixes/hex literals, and DECIMAL exponents.
        // Arrow's parser currently rejects these forms, as it does for ordinary CAST expressions.
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        match arg {
            ColumnarValue::Scalar(value) => Ok(ColumnarValue::Scalar(
                value.cast_to_with_options(&self.target_type, &options)?,
            )),
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(cast_with_options(
                array,
                &self.target_type,
                &options,
            )?)),
        }
    }
}
