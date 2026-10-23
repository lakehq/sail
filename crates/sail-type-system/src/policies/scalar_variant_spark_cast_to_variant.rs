//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields};
use datafusion_common::Result;
use datafusion_expr::ReturnFieldArgs;
use parquet_variant_compute::VariantType;
use sail_common_datafusion::variant::{VARIANT_VALUE_FIELD_NAME, variant_metadata_field};

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Struct(Fields::from(vec![
        Field::new(VARIANT_VALUE_FIELD_NAME, DataType::Binary, false),
        variant_metadata_field(DataType::Binary, false),
    ])))
}

pub fn return_field_from_args(args: ReturnFieldArgs) -> Result<Arc<Field>> {
    let data_type = return_type(
        args.arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>()
            .as_slice(),
    )?;
    Ok(Arc::new(
        Field::new("spark_cast_to_variant", data_type, true).with_extension_type(VariantType),
    ))
}
