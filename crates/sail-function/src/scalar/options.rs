use std::ops::Range;

use datafusion::arrow::array::{Array, MapArray, StringArray};
use datafusion_common::{Result, exec_err};

pub(super) fn first_row_entries(
    map: &MapArray,
) -> Option<(&StringArray, &StringArray, Range<usize>)> {
    if map.is_empty() || map.is_null(0) {
        return None;
    }
    let keys = map.keys().as_any().downcast_ref::<StringArray>()?;
    let values = map.values().as_any().downcast_ref::<StringArray>()?;
    let offsets = map.value_offsets();
    let start = *offsets.first()? as usize;
    let end = *offsets.get(1)? as usize;
    Some((keys, values, start..end))
}

/// Returns the effective option value from the first map row.
///
/// Spark uses `CaseInsensitiveMap` for CSV, JSON, and XML options. When case variants of the same
/// key are present, the later entry shadows the earlier one.
pub(super) fn find_option<'a>(map: &'a MapArray, key: &str) -> Option<&'a str> {
    let (keys, values, entries) = first_row_entries(map)?;
    keys.iter()
        .zip(values.iter())
        .take(entries.end)
        .skip(entries.start)
        .filter_map(|(entry_key, entry_value)| match entry_key {
            Some(entry_key) if entry_key.eq_ignore_ascii_case(key) => Some(entry_value),
            _ => None,
        })
        .next_back()
        .flatten()
}

/// Rejects a null key or value before any option lookup.
///
/// Spark's `CreateMap` rejects null keys, and `ExprUtils.convertToMapData` eagerly calls `toString`
/// on every value, including unknown options, before the format-specific options object is built.
pub(super) fn reject_null_options(map: &MapArray, function_name: &str) -> Result<()> {
    let Some((keys, values, entries)) = first_row_entries(map) else {
        return Ok(());
    };
    for (key, value) in keys
        .iter()
        .zip(values.iter())
        .take(entries.end)
        .skip(entries.start)
    {
        if key.is_none() {
            return exec_err!("[NULL_MAP_KEY] Cannot use null as map key. SQLSTATE: 2200E");
        }
        if value.is_none() {
            return exec_err!(
                "[FAILED_FUNCTION_CALL] Failed preparing of the function `{function_name}` for call. Please, double check function's arguments. SQLSTATE: 38000"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, StructArray};
    use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::{DataType, Field, Fields};
    use sail_common::spec::{SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME};

    use super::*;

    fn options_map(
        keys: Vec<Option<&str>>,
        values: Vec<Option<&str>>,
        offsets: Vec<i32>,
    ) -> MapArray {
        assert_eq!(keys.len(), values.len());
        let fields = Fields::from(vec![
            // Nullable only so the null-key error path can be exercised with a malformed map.
            Arc::new(Field::new(SAIL_MAP_KEY_FIELD_NAME, DataType::Utf8, true)),
            Arc::new(Field::new(SAIL_MAP_VALUE_FIELD_NAME, DataType::Utf8, true)),
        ]);
        let entries = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(StringArray::from(keys)) as ArrayRef,
                Arc::new(StringArray::from(values)) as ArrayRef,
            ],
            None,
        );
        MapArray::new(
            Arc::new(Field::new("entries", DataType::Struct(fields), false)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            entries,
            None,
            false,
        )
    }

    #[test]
    fn spark_options_use_first_row_case_insensitive_last_wins_lookup() {
        let map = options_map(
            vec![
                Some("dateFormat"),
                Some("DATEFORMAT"),
                Some("unrelated"),
                Some("dateFormat"),
            ],
            vec![
                Some("yyyy"),
                Some("MM"),
                Some("x"),
                Some("ignored second row"),
            ],
            vec![0, 3, 4],
        );

        assert_eq!(find_option(&map, "DaTeFoRmAt"), Some("MM"));
        assert_eq!(find_option(&map, "unset"), None);
    }

    #[test]
    fn spark_options_reject_every_null_key_and_value_before_lookup() -> Result<()> {
        let null_key = options_map(vec![None], vec![Some("ignored")], vec![0, 1]);
        let Err(error) = reject_null_options(&null_key, "from_json") else {
            return exec_err!("null map key should fail");
        };
        assert!(
            error
                .to_string()
                .contains("[NULL_MAP_KEY] Cannot use null as map key. SQLSTATE: 2200E"),
            "{error}"
        );

        let null_unknown_value = options_map(vec![Some("unknown")], vec![None], vec![0, 1]);
        let Err(error) = reject_null_options(&null_unknown_value, "from_json") else {
            return exec_err!("null map value should fail");
        };
        assert!(
            error.to_string().contains(
                "[FAILED_FUNCTION_CALL] Failed preparing of the function `from_json` for call. Please, double check function's arguments. SQLSTATE: 38000"
            ),
            "{error}"
        );
        Ok(())
    }
}
