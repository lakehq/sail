//! Shared function type policies.

use std::sync::Arc;

use arrow_schema::{DataType, Field};
use datafusion_common::Result;

pub fn spark_sentences_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::List(Arc::new(Field::new_list_field(
        DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
        true,
    ))))
}
