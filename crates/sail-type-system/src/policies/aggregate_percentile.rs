//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::{DataType, Field};
use datafusion_common::Result;

pub fn return_type(arg_types: &[DataType]) -> Result<DataType> {
    // Check if second argument is an array (multiple percentiles)
    let is_array_percentiles = matches!(arg_types.get(1), Some(DataType::List(_)));

    let base_type = match &arg_types[0] {
        DataType::Utf8 => DataType::Utf8,
        DataType::Utf8View => DataType::Utf8View,
        DataType::LargeUtf8 => DataType::LargeUtf8,
        dt @ DataType::Interval(_) => dt.clone(),
        dt @ DataType::Duration(_) => dt.clone(),
        _ => DataType::Float64,
    };

    // If percentiles is an array, return List of base_type
    if is_array_percentiles {
        Ok(DataType::List(Arc::new(Field::new(
            "item", base_type, true,
        ))))
    } else {
        Ok(base_type)
    }
}
