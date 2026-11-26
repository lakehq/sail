// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/7e7ae72a820e2bf87da1205b96639fba334ed2f7/datafusion/datasource-parquet/src/schema_coercion.rs#L266-L428
// Find the value type that can represent both sides without narrowing offsets
// or crossing string/binary families.
fn common_dictionary_value_type(
    field_type: &DataType,
    dictionary_value_type: &DataType,
) -> Option<DataType> {
    let field_type = match field_type {
        DataType::Dictionary(_, field_value_type) => field_value_type.as_ref(),
        _ => field_type,
    };

    match (field_type, dictionary_value_type) {
        (DataType::Utf8, DataType::Utf8) => Some(DataType::Utf8),
        (DataType::Utf8 | DataType::LargeUtf8, DataType::Utf8 | DataType::LargeUtf8) => {
            Some(DataType::LargeUtf8)
        }
        (DataType::Binary, DataType::Binary) => Some(DataType::Binary),
        (DataType::Binary | DataType::LargeBinary, DataType::Binary | DataType::LargeBinary) => {
            Some(DataType::LargeBinary)
        }
        _ => None,
    }
}

// Same family (both signed or both unsigned): return the wider member.
// Mixed: return the type with the larger capacity. For the UInt64+Int64 case
// this is UInt64 since arrow-rs supports both signed and unsigned dictionary keys.
// Returns None only for non-integer key types.
fn common_dictionary_key_type(a: &DataType, b: &DataType) -> Option<DataType> {
    fn key_capacity(dt: &DataType) -> Option<u128> {
        match dt {
            DataType::Int8 => Some(1u128 << 7),
            DataType::Int16 => Some(1u128 << 15),
            DataType::Int32 => Some(1u128 << 31),
            DataType::Int64 => Some(1u128 << 63),
            DataType::UInt8 => Some(1u128 << 8),
            DataType::UInt16 => Some(1u128 << 16),
            DataType::UInt32 => Some(1u128 << 32),
            DataType::UInt64 => Some(1u128 << 64),
            _ => None,
        }
    }
    fn is_signed(dt: &DataType) -> bool {
        matches!(
            dt,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        )
    }
    let cap_a = key_capacity(a)?;
    let cap_b = key_capacity(b)?;
    if is_signed(a) == is_signed(b) {
        return Some(if cap_a >= cap_b { a.clone() } else { b.clone() });
    }
    let max_cap = cap_a.max(cap_b);
    Some(match max_cap {
        c if c <= (1u128 << 7) => DataType::Int8,
        c if c <= (1u128 << 15) => DataType::Int16,
        c if c <= (1u128 << 31) => DataType::Int32,
        c if c <= (1u128 << 63) => DataType::Int64,
        _ => DataType::UInt64,
    })
}

fn can_promote_to_dictionary_type(
    file_field_type: &DataType,
    table_dictionary_type: &DataType,
) -> bool {
    let DataType::Dictionary(table_key, table_value) = table_dictionary_type else {
        return false;
    };
    if common_dictionary_value_type(file_field_type, table_value)
        .is_none_or(|common| &common != table_value.as_ref())
    {
        return false;
    }
    if let DataType::Dictionary(file_key, _) = file_field_type {
        return common_dictionary_key_type(file_key, table_key)
            .is_some_and(|common| &common == table_key.as_ref());
    }
    // Plain file field: only allow promotion if the table key is at least Int32 wide.
    // A plain column can have any number of distinct values, so narrow keys (Int8, Int16)
    // are unsafe — uniform_dict_schemas widens to Int32 when a plain file is present, so
    // this guard handles the case where can_promote_to_dictionary_type is called directly.
    common_dictionary_key_type(&DataType::Int32, table_key)
        .is_some_and(|common| &common == table_key.as_ref())
}

/// Normalize per-file schemas so that a column promoted to `Dictionary` in
/// *any* file is promoted to the same `Dictionary` type in *all* files.
///
/// This lets [`Schema::try_merge`] accept directories that mix dictionary and
/// plain encodings for the same column.
pub(crate) fn uniform_dict_schemas(schemas: Vec<Schema>) -> Vec<Schema> {
    // First pass: record the dictionary type for every column that is Dictionary in
    // at least one schema.
    let mut dict_types: HashMap<String, DataType> = HashMap::new();
    for schema in &schemas {
        for field in schema.fields() {
            if matches!(field.data_type(), DataType::Dictionary(_, _)) {
                dict_types
                    .entry(field.name().clone())
                    .or_insert_with(|| field.data_type().clone());
            }
        }
    }
    if dict_types.is_empty() {
        return schemas;
    }

    for schema in &schemas {
        for field in schema.fields() {
            let Some(dict_type) = dict_types.get_mut(field.name()) else {
                continue;
            };
            let DataType::Dictionary(key_type, value_type) = dict_type else {
                continue;
            };
            let key_type = key_type.clone();
            let value_type = value_type.as_ref().clone();
            let new_key = if let DataType::Dictionary(file_key, _) = field.data_type() {
                common_dictionary_key_type(&key_type, file_key)
                    .map(Box::new)
                    .unwrap_or_else(|| key_type.clone())
            } else {
                // Plain field: treat as Int32 key capacity since we don't know how
                // many distinct values it has. This prevents a narrow key (e.g. Int8)
                // from being selected as the common type when one file uses a narrow
                // dictionary and another uses a plain encoding with potentially more
                // values than the key can represent.
                common_dictionary_key_type(&key_type, &DataType::Int32)
                    .map(Box::new)
                    .unwrap_or_else(|| Box::new(DataType::Int32))
            };
            if let Some(common_type) = common_dictionary_value_type(field.data_type(), &value_type)
            {
                *dict_type = DataType::Dictionary(new_key, Box::new(common_type));
            }
        }
    }

    // Only promote fields whose type family matches the dictionary value type,
    // so schema normalization does not reinterpret binary bytes as UTF-8.
    schemas
        .into_iter()
        .map(|schema| {
            let fields: Vec<Arc<Field>> = schema
                .fields()
                .iter()
                .map(|field| {
                    if let Some(dict_type) = dict_types.get(field.name())
                        && can_promote_to_dictionary_type(field.data_type(), dict_type)
                    {
                        return field_with_new_type(field, dict_type.clone());
                    }
                    Arc::clone(field)
                })
                .collect();
            Schema::new_with_metadata(fields, schema.metadata().clone())
        })
        .collect()
}
// SPDX-SnippetEnd

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/7e7ae72a820e2bf87da1205b96639fba334ed2f7/datafusion/datasource-parquet/src/schema_coercion.rs#L716-L720
/// Create a new field with the specified data type, copying the other
/// properties from the input field
fn field_with_new_type(field: &FieldRef, new_type: DataType) -> FieldRef {
    Arc::new(field.as_ref().clone().with_data_type(new_type))
}
// SPDX-SnippetEnd
