use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::invalid_arg_count_exec_err;

/// The value of a UDT read as its storage type: the input unchanged, under a field that carries no
/// metadata. A Spark cast yields its target type, never the UDT it was applied to, but DataFusion
/// copies the source field's metadata through a cast -- the UDT marker included -- so the resolver
/// puts this under a cast whose input is a UDT.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkUdtStorage {
    signature: Signature,
}

impl Default for SparkUdtStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkUdtStorage {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkUdtStorage {
    fn name(&self) -> &str {
        "spark_udt_storage"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = args.arg_fields else {
            return Err(invalid_arg_count_exec_err(
                "spark_udt_storage",
                (1, 1),
                args.arg_fields.len(),
            ));
        };
        Ok(Arc::new(Field::new(
            self.name(),
            field.data_type().clone(),
            field.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let [value] = <[ColumnarValue; 1]>::try_from(args)
            .map_err(|args| invalid_arg_count_exec_err("spark_udt_storage", (1, 1), args.len()))?;
        Ok(value)
    }
}
