use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Fields};
use datafusion_common::{DataFusionError, Result};
pub fn return_type(arg_types: &[DataType]) -> Result<DataType> {
    let out = match arg_types {
        [DataType::List(f)] => DataType::List(Arc::new(Field::new_list_field(
            item_type(f, DataType::Int32),
            f.is_nullable(),
        ))),
        [DataType::LargeList(f)] => DataType::LargeList(Arc::new(Field::new_list_field(
            item_type(f, DataType::Int64),
            f.is_nullable(),
        ))),
        [DataType::FixedSizeList(f, n)] => DataType::FixedSizeList(
            Arc::new(Field::new_list_field(
                item_type(f, DataType::Int32),
                f.is_nullable(),
            )),
            *n,
        ),
        _ => {
            return Err(DataFusionError::Internal(format!(
                "{} should only be called with a list",
                "array_item_with_position"
            )));
        }
    };
    Ok(out)
}
pub fn item_fields(field: &FieldRef, position_type: DataType) -> Fields {
    Fields::from(vec![
        Arc::new(Field::new("pos", position_type, field.is_nullable())),
        Arc::new(Field::new(
            "col",
            field.data_type().clone(),
            field.is_nullable(),
        )),
    ])
}
pub fn item_type(field: &FieldRef, position_type: DataType) -> DataType {
    DataType::Struct(item_fields(field, position_type))
}
