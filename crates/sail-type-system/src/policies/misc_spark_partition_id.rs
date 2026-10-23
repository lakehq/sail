//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::DataType;
use datafusion_common::Result;

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Int32)
}

pub fn return_field(args: datafusion_expr::ReturnFieldArgs) -> Result<arrow_schema::FieldRef> {
    if !args.arg_fields.is_empty() {
        return datafusion_common::plan_err!("row identity functions take no arguments");
    }
    Ok(std::sync::Arc::new(arrow_schema::Field::new(
        "",
        return_type(&[])?,
        false,
    )))
}
