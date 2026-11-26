// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};

use datafusion::common::{DataFusionError, Result, exec_err, not_impl_err};
use serde::Deserialize;

use crate::io::{ManifestReadBudget, StoreContext};
use crate::spec::manifest::deserialize_optional_vec;
use crate::spec::{
    DataContentType, Datum, FormatVersion, ManifestEntry, ManifestFile, ManifestMetadata,
    ManifestStatus, PrimitiveLiteral, PrimitiveType,
};
use crate::table::files::{EntrySelection, inherit_manifest_entries};

// Coordinator procedures retain manifest records and encoded output in memory.
pub(super) const MAX_INPUT_BYTES: u64 = 256 * 1024 * 1024;
pub(super) const MAX_ENTRIES: usize = 1_000_000;
pub(super) const MAX_OUTPUT_BYTES: usize = 256 * 1024 * 1024;

pub(super) fn missing_counts(descriptor: &ManifestFile) -> bool {
    descriptor.added_files_count.is_none()
        || descriptor.existing_files_count.is_none()
        || descriptor.deleted_files_count.is_none()
        || descriptor.added_rows_count.is_none()
        || descriptor.existing_rows_count.is_none()
        || descriptor.deleted_rows_count.is_none()
}

pub(super) fn populate_counts(
    descriptor: &mut ManifestFile,
    entries: &[ManifestEntry],
) -> Result<()> {
    let mut counts = [(0i32, 0i64); 3];
    for entry in entries {
        let index = match entry.status {
            ManifestStatus::Added => 0,
            ManifestStatus::Existing => 1,
            ManifestStatus::Deleted => 2,
        };
        let (files, rows) = &mut counts[index];
        *files = files
            .checked_add(1)
            .ok_or_else(|| DataFusionError::Execution("Manifest file count overflow".into()))?;
        *rows = i64::try_from(entry.data_file.record_count)
            .ok()
            .and_then(|count| rows.checked_add(count))
            .ok_or_else(|| DataFusionError::Execution("Manifest row count overflow".into()))?;
    }
    descriptor.added_files_count.get_or_insert(counts[0].0);
    descriptor.existing_files_count.get_or_insert(counts[1].0);
    descriptor.deleted_files_count.get_or_insert(counts[2].0);
    descriptor.added_rows_count.get_or_insert(counts[0].1);
    descriptor.existing_rows_count.get_or_insert(counts[1].1);
    descriptor.deleted_rows_count.get_or_insert(counts[2].1);
    Ok(())
}

#[derive(Deserialize)]
struct RawEntry {
    data_file: RawBounds,
}

#[derive(Deserialize)]
struct RawBounds {
    #[serde(default, deserialize_with = "deserialize_optional_vec")]
    lower_bounds: Option<Vec<RawBound>>,
    #[serde(default, deserialize_with = "deserialize_optional_vec")]
    upper_bounds: Option<Vec<RawBound>>,
}

#[derive(Deserialize)]
struct RawBound {
    key: i32,
    value: serde_bytes::ByteBuf,
}

fn restore_bounds(raw: Option<Vec<RawBound>>, decoded: &mut HashMap<i32, Datum>) -> Result<()> {
    // Bounds are opaque bytes in Iceberg. The scan reader drops bounds it cannot type
    // (e.g. dropped columns or position-delete metadata). Keep these bytes on rewrite.
    let mut seen = HashSet::new();
    for bound in raw.unwrap_or_default() {
        if !seen.insert(bound.key) {
            return exec_err!("Duplicate Iceberg metric bound for field {}", bound.key);
        }
        let bytes = decoded
            .get(&bound.key)
            .map(Datum::to_bytes)
            .transpose()
            .map_err(DataFusionError::Execution)?;
        if bytes.as_deref() != Some(bound.value.as_ref()) {
            decoded.insert(
                bound.key,
                Datum::new(
                    PrimitiveType::Binary,
                    PrimitiveLiteral::Binary(bound.value.into_vec()),
                ),
            );
        }
    }
    Ok(())
}

pub(super) async fn read_entries(
    store: &StoreContext,
    descriptor: &ManifestFile,
    budget: &mut ManifestReadBudget,
) -> Result<(ManifestMetadata, Vec<ManifestEntry>, bool)> {
    if descriptor.key_metadata.is_some() {
        return not_impl_err!("Cannot rewrite encrypted Iceberg manifests");
    }
    let (manifest_metadata, decoded, _) = budget
        .read_manifest(store, &descriptor.manifest_path, |entry, value| {
            apache_avro::from_value::<RawEntry>(value)
                .map(|raw| (entry, raw))
                .map_err(|error| format!("Avro decode manifest bounds error: {error}"))
        })
        .await?;
    let (mut entries, raw_entries): (Vec<_>, Vec<_>) = decoded.into_iter().unzip();
    let needs_normalization = entries.iter().any(|entry| {
        entry.status != ManifestStatus::Existing
            || entry.snapshot_id.is_none()
            || (manifest_metadata.format_version != FormatVersion::V1
                && (entry.sequence_number.is_none() || entry.file_sequence_number.is_none()))
            || (entry.data_file.content == DataContentType::Data
                && entry.data_file.first_row_id.is_none()
                && descriptor.first_row_id.is_some())
    });
    // Inherit using the source format, before upgrading or discarding deleted entries.
    inherit_manifest_entries(descriptor, manifest_metadata.format_version, &mut entries)?;
    for (entry, raw) in entries.iter_mut().zip(raw_entries) {
        if EntrySelection::live().matches(entry) {
            restore_bounds(
                raw.data_file.lower_bounds,
                &mut entry.data_file.lower_bounds,
            )?;
            restore_bounds(
                raw.data_file.upper_bounds,
                &mut entry.data_file.upper_bounds,
            )?;
        }
    }
    Ok((manifest_metadata, entries, needs_normalization))
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn raw_bounds_accept_optional_avro_encodings() {
        use apache_avro::types::Value;

        let bound = Value::Record(vec![
            ("key".into(), Value::Int(999)),
            ("value".into(), Value::Bytes(vec![0xff, 0, 0x80])),
        ]);
        let array = Value::Array(vec![bound.clone()]);
        for value in [
            Some(array.clone()),
            Some(Value::Union(1, Box::new(array))),
            Some(Value::Null),
            Some(Value::Union(0, Box::new(Value::Null))),
            None,
        ] {
            let fields = value
                .clone()
                .map(|value| {
                    vec![
                        ("lower_bounds".into(), value.clone()),
                        ("upper_bounds".into(), value),
                    ]
                })
                .unwrap_or_default();
            let raw: RawBounds = apache_avro::from_value(&Value::Record(fields)).expect("bounds");
            for bounds in [raw.lower_bounds, raw.upper_bounds] {
                let mut decoded = HashMap::new();
                restore_bounds(bounds, &mut decoded).expect("restore");
                if matches!(value, Some(Value::Array(_) | Value::Union(1, _))) {
                    assert_eq!(
                        decoded[&999].to_bytes().expect("bytes"),
                        vec![0xff, 0, 0x80]
                    );
                } else {
                    assert!(decoded.is_empty());
                }
            }
        }
        for field in ["lower_bounds", "upper_bounds"] {
            for value in [
                Value::String("invalid".into()),
                Value::Array(vec![Value::Record(vec![("key".into(), Value::Int(1))])]),
                Value::Array(vec![Value::Record(vec![
                    ("key".into(), Value::String("1".into())),
                    ("value".into(), Value::Bytes(vec![1])),
                ])]),
            ] {
                assert!(
                    apache_avro::from_value::<RawBounds>(&Value::Record(vec![(
                        field.into(),
                        value
                    ),]))
                    .is_err()
                );
            }
            let raw: RawBounds = apache_avro::from_value(&Value::Record(vec![(
                field.into(),
                Value::Array(vec![bound.clone(), bound.clone()]),
            )]))
            .expect("duplicate bounds decode");
            assert!(
                restore_bounds(raw.lower_bounds.or(raw.upper_bounds), &mut HashMap::new()).is_err()
            );
        }
    }

    #[test]
    fn opaque_bounds_survive_typed_reader_projection() {
        let raw = vec![0xff, 0, 0x80];
        let mut decoded = HashMap::new();
        restore_bounds(
            Some(vec![RawBound {
                key: 2147483546,
                value: raw.clone().into(),
            }]),
            &mut decoded,
        )
        .expect("restore raw bytes");
        assert_eq!(decoded[&2147483546].to_bytes().expect("bytes"), raw);
        assert!(
            restore_bounds(
                Some(vec![
                    RawBound {
                        key: 1,
                        value: vec![1].into()
                    },
                    RawBound {
                        key: 1,
                        value: vec![2].into()
                    },
                ]),
                &mut decoded
            )
            .is_err()
        );
    }
}
