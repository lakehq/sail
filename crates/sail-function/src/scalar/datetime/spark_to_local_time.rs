use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};

use crate::error::invalid_arg_count_exec_err;

/// DataFusion's `to_local_time` kernel with the input's nullability preserved in its field.
///
/// `to_local_time` is null-intolerant, but its default UDF field is nullable. Spark's
/// `SubtractTimestamps` is also null-intolerant, so the wrapper is needed only to retain that
/// fact while Sail plans timestamp subtraction.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkToLocalTime {
    signature: Signature,
}

impl Default for SparkToLocalTime {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkToLocalTime {
    pub fn new() -> Self {
        Self {
            signature: datafusion_functions::datetime::to_local_time()
                .signature()
                .clone(),
        }
    }
}

impl ScalarUDFImpl for SparkToLocalTime {
    fn name(&self) -> &str {
        "spark_to_local_time"
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
        let [argument] = args.arg_fields else {
            return Err(invalid_arg_count_exec_err(
                "spark_to_local_time",
                (1, 1),
                args.arg_fields.len(),
            ));
        };
        Ok(Arc::new(Field::new(
            self.name(),
            datafusion_functions::datetime::to_local_time()
                .return_type(&[argument.data_type().clone()])?,
            argument.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion_functions::datetime::to_local_time().invoke_with_args(args)
    }
}
