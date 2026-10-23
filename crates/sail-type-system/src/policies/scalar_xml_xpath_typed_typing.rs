//! Shared function type policies.

use arrow_schema::DataType;
use datafusion_common::{Result, internal_err};

pub fn xpath_typed_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    internal_err!("`return_type` should not be called; `return_field_from_args` is used instead")
}
