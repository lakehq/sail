//! Shared type utilities.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields};
use sail_common::spec::{SAIL_MAP_FIELD_NAME, SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME};

pub fn map_type_from_key_value_types(key_type: &DataType, value_type: &DataType) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            SAIL_MAP_FIELD_NAME,
            DataType::Struct(Fields::from(vec![
                // the key must not be nullable
                Field::new(SAIL_MAP_KEY_FIELD_NAME, key_type.clone(), false),
                Field::new(SAIL_MAP_VALUE_FIELD_NAME, value_type.clone(), true),
            ])),
            false, // the entry is not nullable
        )),
        false, // the keys are not sorted
    )
}
