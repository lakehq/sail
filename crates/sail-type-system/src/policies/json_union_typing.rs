//! Shared type utilities.
// https://github.com/datafusion-contrib/datafusion-functions-json/blob/cb1ba7a80a84e10a4d658f3100eae8f6bca2ced9/LICENSE
//
// [Credit]: https://github.com/datafusion-contrib/datafusion-functions-json/blob/78c5abbf7222510ff221517f5d2e3c344969da98/src/common_union.rs

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, Field, UnionFields, UnionMode};
const TYPE_ID_NULL: i8 = 0;
const TYPE_ID_BOOL: i8 = 1;
const TYPE_ID_INT: i8 = 2;
const TYPE_ID_FLOAT: i8 = 3;
const TYPE_ID_STR: i8 = 4;
const TYPE_ID_ARRAY: i8 = 5;
const TYPE_ID_OBJECT: i8 = 6;

pub fn is_json_union(data_type: &DataType) -> bool {
    match data_type {
        DataType::Union(fields, UnionMode::Sparse) => fields == &union_fields(),
        _ => false,
    }
}

pub fn union_fields() -> UnionFields {
    static FIELDS: OnceLock<UnionFields> = OnceLock::new();
    FIELDS
        .get_or_init(|| {
            let json_metadata: HashMap<String, String> =
                HashMap::from_iter(vec![("is_json".to_string(), "true".to_string())]);
            UnionFields::from_iter([
                (
                    TYPE_ID_NULL,
                    Arc::new(Field::new("null", DataType::Null, true)),
                ),
                (
                    TYPE_ID_BOOL,
                    Arc::new(Field::new("bool", DataType::Boolean, false)),
                ),
                (
                    TYPE_ID_INT,
                    Arc::new(Field::new("int", DataType::Int64, false)),
                ),
                (
                    TYPE_ID_FLOAT,
                    Arc::new(Field::new("float", DataType::Float64, false)),
                ),
                (
                    TYPE_ID_STR,
                    Arc::new(Field::new("str", DataType::Utf8, false)),
                ),
                (
                    TYPE_ID_ARRAY,
                    Arc::new(
                        Field::new("array", DataType::Utf8, false)
                            .with_metadata(json_metadata.clone()),
                    ),
                ),
                (
                    TYPE_ID_OBJECT,
                    Arc::new(
                        Field::new("object", DataType::Utf8, false)
                            .with_metadata(json_metadata.clone()),
                    ),
                ),
            ])
        })
        .clone()
}
