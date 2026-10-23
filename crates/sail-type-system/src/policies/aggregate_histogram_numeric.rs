//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::{DataType, Field};
use datafusion_common::Result;

pub fn return_type(arg_types: &[DataType]) -> Result<DataType> {
    let x_type = arg_types[0].clone();
    Ok(DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(
            vec![
                Field::new("x", x_type, true),
                Field::new("y", DataType::Float64, true),
            ]
            .into(),
        ),
        true,
    ))))
}
