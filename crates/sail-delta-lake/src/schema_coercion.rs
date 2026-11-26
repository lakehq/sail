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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};

    use super::uniform_dict_schemas;

    const I8: DataType = DataType::Int8;
    const I16: DataType = DataType::Int16;
    const I32: DataType = DataType::Int32;
    const I64: DataType = DataType::Int64;
    const U8: DataType = DataType::UInt8;
    const U16: DataType = DataType::UInt16;
    const U32: DataType = DataType::UInt32;
    const U64: DataType = DataType::UInt64;

    fn dict(key: DataType, value: DataType) -> DataType {
        DataType::Dictionary(Box::new(key), Box::new(value))
    }

    /// Normalizes one single-column schema per input type and returns the
    /// normalized column types along with the merged type, if `Schema::try_merge`
    /// succeeds on the normalized schemas.
    fn normalize(types: &[DataType]) -> (Vec<DataType>, Option<DataType>) {
        let schemas = types
            .iter()
            .map(|t| Schema::new(vec![Field::new("c", t.clone(), true)]))
            .collect();
        let schemas = uniform_dict_schemas(schemas);
        let normalized = schemas
            .iter()
            .map(|s| s.field(0).data_type().clone())
            .collect();
        let merged = Schema::try_merge(schemas)
            .ok()
            .map(|s| s.field(0).data_type().clone());
        (normalized, merged)
    }

    /// Checks the inputs in the given order and in reverse order.
    fn assert_both_orders(
        name: &str,
        inputs: &[DataType],
        expected: &[DataType],
        merged: Option<DataType>,
    ) {
        assert_eq!(
            normalize(inputs),
            (expected.to_vec(), merged.clone()),
            "{name}"
        );
        let reversed = |v: &[DataType]| v.iter().rev().cloned().collect::<Vec<_>>();
        assert_eq!(
            normalize(&reversed(inputs)),
            (reversed(expected), merged),
            "{name} (reversed)"
        );
    }

    #[test]
    fn dictionary_plain_mixtures_are_promoted() {
        let utf8 = dict(I32, DataType::Utf8);
        let binary = dict(I32, DataType::Binary);
        let cases = [
            (
                "utf8",
                vec![utf8.clone(), DataType::Utf8],
                vec![utf8.clone(), utf8.clone()],
                Some(utf8.clone()),
            ),
            (
                "binary",
                vec![binary.clone(), DataType::Binary],
                vec![binary.clone(), binary.clone()],
                Some(binary.clone()),
            ),
            (
                "plain files around a dictionary file",
                vec![DataType::Utf8, utf8.clone(), DataType::Utf8],
                vec![utf8.clone(), utf8.clone(), utf8.clone()],
                Some(utf8.clone()),
            ),
            (
                "identical dictionaries",
                vec![utf8.clone(), utf8.clone()],
                vec![utf8.clone(), utf8.clone()],
                Some(utf8.clone()),
            ),
            (
                // `Null` has no value family, so it is left alone, but
                // `Schema::try_merge` accepts it next to any type.
                "null column",
                vec![dict(I8, DataType::Utf8), DataType::Null],
                vec![dict(I8, DataType::Utf8), DataType::Null],
                Some(dict(I8, DataType::Utf8)),
            ),
            (
                // An incompatible file does not prevent promoting the others,
                // but the merge still fails because of it.
                "incompatible file among compatible files",
                vec![dict(I8, DataType::Utf8), DataType::Binary, DataType::Utf8],
                vec![utf8.clone(), DataType::Binary, utf8.clone()],
                None,
            ),
        ];
        for (name, inputs, expected, merged) in cases {
            assert_both_orders(name, &inputs, &expected, merged);
        }
    }

    #[test]
    fn dictionary_key_types_widen_to_common_type() {
        let keys = [I8, I16, I32, I64, U8, U16, U32, U64];
        // Common key type for each pair of `keys` (rows and columns in the same order).
        let expected = [
            [I8, I16, I32, I64, I16, I32, I64, U64],
            [I16, I16, I32, I64, I16, I32, I64, U64],
            [I32, I32, I32, I64, I32, I32, I64, U64],
            [I64, I64, I64, I64, I64, I64, I64, U64],
            [I16, I16, I32, I64, U8, U16, U32, U64],
            [I32, I32, I32, I64, U16, U16, U32, U64],
            [I64, I64, I64, I64, U32, U32, U32, U64],
            [U64, U64, U64, U64, U64, U64, U64, U64],
        ];
        for (a, row) in keys.iter().zip(expected) {
            for (b, common) in keys.iter().zip(row) {
                let common = dict(common, DataType::Utf8);
                assert_eq!(
                    normalize(&[
                        dict(a.clone(), DataType::Utf8),
                        dict(b.clone(), DataType::Utf8)
                    ]),
                    (vec![common.clone(), common.clone()], Some(common)),
                    "{a} + {b}"
                );
            }
        }
    }

    #[test]
    fn plain_field_widens_dictionary_key_to_at_least_int32() {
        // A plain column may have any number of distinct values, so its key
        // capacity is treated as `Int32`.
        for (key, common) in [
            (I8, I32),
            (I16, I32),
            (I32, I32),
            (I64, I64),
            (U8, I32),
            (U16, I32),
            (U32, I64),
            (U64, U64),
        ] {
            let common = dict(common, DataType::Utf8);
            assert_both_orders(
                &format!("{key} + plain"),
                &[dict(key.clone(), DataType::Utf8), DataType::Utf8],
                &[common.clone(), common.clone()],
                Some(common),
            );
        }
    }

    #[test]
    fn dictionary_value_types_widen_to_large_offsets() {
        let large_utf8 = dict(I32, DataType::LargeUtf8);
        let large_binary = dict(I32, DataType::LargeBinary);
        let cases = [
            (
                "dictionary utf8 + plain large utf8",
                vec![dict(I32, DataType::Utf8), DataType::LargeUtf8],
                large_utf8.clone(),
            ),
            (
                "dictionary large utf8 + plain utf8",
                vec![large_utf8.clone(), DataType::Utf8],
                large_utf8.clone(),
            ),
            (
                "dictionary utf8 + dictionary large utf8",
                vec![dict(I32, DataType::Utf8), large_utf8.clone()],
                large_utf8.clone(),
            ),
            (
                "dictionary binary + plain large binary",
                vec![dict(I32, DataType::Binary), DataType::LargeBinary],
                large_binary.clone(),
            ),
            (
                "dictionary large binary + plain binary",
                vec![large_binary.clone(), DataType::Binary],
                large_binary.clone(),
            ),
            (
                "dictionary binary + dictionary large binary",
                vec![dict(I32, DataType::Binary), large_binary.clone()],
                large_binary.clone(),
            ),
            (
                "key and value widening together",
                vec![dict(I8, DataType::Utf8), dict(U16, DataType::LargeUtf8)],
                large_utf8.clone(),
            ),
            (
                "three files",
                vec![
                    dict(U8, DataType::Binary),
                    DataType::LargeBinary,
                    dict(I16, DataType::Binary),
                ],
                large_binary.clone(),
            ),
        ];
        for (name, inputs, common) in cases {
            let expected = vec![common.clone(); inputs.len()];
            assert_both_orders(name, &inputs, &expected, Some(common));
        }
    }

    #[test]
    fn incompatible_types_are_not_promoted() {
        let nested = |value: DataType| Fields::from(vec![Field::new("v", value, true)]);
        let cases = [
            (
                "utf8 dictionary + binary",
                vec![dict(I32, DataType::Utf8), DataType::Binary],
            ),
            (
                // The key is not widened for an incompatible plain field.
                "binary dictionary + utf8",
                vec![dict(I8, DataType::Binary), DataType::Utf8],
            ),
            (
                "utf8 dictionary + binary dictionary",
                vec![dict(I32, DataType::Utf8), dict(I32, DataType::Binary)],
            ),
            (
                "large utf8 dictionary + large binary",
                vec![dict(I32, DataType::LargeUtf8), DataType::LargeBinary],
            ),
            (
                "utf8 dictionary + utf8 view",
                vec![dict(I32, DataType::Utf8), DataType::Utf8View],
            ),
            (
                "utf8 view dictionary + utf8 view",
                vec![dict(I32, DataType::Utf8View), DataType::Utf8View],
            ),
            (
                "binary dictionary + binary view",
                vec![dict(I32, DataType::Binary), DataType::BinaryView],
            ),
            (
                "binary view dictionary + binary view",
                vec![dict(I32, DataType::BinaryView), DataType::BinaryView],
            ),
            (
                "fixed size binary dictionary + fixed size binary",
                vec![
                    dict(I32, DataType::FixedSizeBinary(4)),
                    DataType::FixedSizeBinary(4),
                ],
            ),
            (
                "int64 dictionary + int64",
                vec![dict(I32, DataType::Int64), DataType::Int64],
            ),
            (
                // Keys are only widened for string and binary values.
                "int64 dictionaries with different keys",
                vec![dict(I8, DataType::Int64), dict(I32, DataType::Int64)],
            ),
            (
                // Only top-level fields are normalized.
                "struct with dictionary child + struct with plain child",
                vec![
                    DataType::Struct(nested(dict(I32, DataType::Utf8))),
                    DataType::Struct(nested(DataType::Utf8)),
                ],
            ),
            (
                "list of dictionary + list of plain",
                vec![
                    DataType::new_list(dict(I32, DataType::Utf8), true),
                    DataType::new_list(DataType::Utf8, true),
                ],
            ),
            (
                // Without any dictionary, plain types are not widened.
                "plain utf8 + plain large utf8",
                vec![DataType::Utf8, DataType::LargeUtf8],
            ),
        ];
        for (name, inputs) in cases {
            assert_both_orders(name, &inputs, &inputs, None);
        }
    }

    #[test]
    fn first_dictionary_value_family_wins() {
        // When a column has dictionaries of both the string and binary families,
        // only files compatible with the first dictionary seen are normalized.
        // The merge fails either way.
        let inputs = [
            dict(I8, DataType::Utf8),
            dict(I32, DataType::Binary),
            DataType::Utf8,
        ];
        assert_eq!(
            normalize(&inputs),
            (
                vec![
                    dict(I32, DataType::Utf8),
                    dict(I32, DataType::Binary),
                    dict(I32, DataType::Utf8),
                ],
                None
            )
        );
        let reversed: Vec<_> = inputs.iter().rev().cloned().collect();
        assert_eq!(normalize(&reversed), (reversed.clone(), None));
    }

    #[test]
    fn multiple_columns_preserve_field_and_schema_properties() {
        let schema_metadata = HashMap::from([("k".to_string(), "v".to_string())]);
        let field_metadata = HashMap::from([("m".to_string(), "1".to_string())]);
        let schemas = vec![
            Schema::new_with_metadata(
                vec![
                    Field::new("a", dict(I8, DataType::Utf8), true)
                        .with_metadata(field_metadata.clone()),
                    Field::new("b", DataType::Utf8, false),
                ],
                schema_metadata.clone(),
            ),
            Schema::new_with_metadata(
                vec![
                    Field::new("a", DataType::Utf8, false).with_metadata(field_metadata.clone()),
                    Field::new("b", DataType::Utf8, true),
                    Field::new("d", dict(U16, DataType::LargeBinary), true),
                ],
                schema_metadata.clone(),
            ),
            Schema::new(vec![
                Field::new("a", DataType::LargeUtf8, true),
                Field::new("d", DataType::Binary, false),
                Field::new("e", DataType::Int64, true),
            ]),
        ];
        let a = dict(I32, DataType::LargeUtf8);
        let d = dict(I32, DataType::LargeBinary);
        let expected = vec![
            Schema::new_with_metadata(
                vec![
                    Field::new("a", a.clone(), true).with_metadata(field_metadata.clone()),
                    Field::new("b", DataType::Utf8, false),
                ],
                schema_metadata.clone(),
            ),
            Schema::new_with_metadata(
                vec![
                    Field::new("a", a.clone(), false).with_metadata(field_metadata.clone()),
                    Field::new("b", DataType::Utf8, true),
                    Field::new("d", d.clone(), true),
                ],
                schema_metadata.clone(),
            ),
            Schema::new(vec![
                Field::new("a", a.clone(), true),
                Field::new("d", d.clone(), false),
                Field::new("e", DataType::Int64, true),
            ]),
        ];
        let normalized = uniform_dict_schemas(schemas);
        assert_eq!(normalized, expected);
        assert_eq!(
            Schema::try_merge(normalized).ok(),
            Some(Schema::new_with_metadata(
                vec![
                    Field::new("a", a, true).with_metadata(field_metadata),
                    Field::new("b", DataType::Utf8, true),
                    Field::new("d", d, true),
                    Field::new("e", DataType::Int64, true),
                ],
                schema_metadata,
            ))
        );
    }
}
