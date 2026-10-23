//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::{DataType, Field};
use datafusion_common::Result;
use datafusion_expr::ReturnFieldArgs;

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Binary)
}

pub fn return_field_from_args(_args: ReturnFieldArgs) -> Result<Arc<Field>> {
    // st_asbinary strips metadata, so return a plain Binary field
    Ok(Arc::new(Field::new("st_asbinary", DataType::Binary, true)))
}
