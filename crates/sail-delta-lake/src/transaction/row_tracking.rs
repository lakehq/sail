// SPDX-License-Identifier: Apache-2.0

//! Writer support for the Delta `rowTracking` table feature.
//!
//! See <https://github.com/delta-io/delta/blob/fddd2ed2da1c79a857ec24f0c3038706280f346d/PROTOCOL.md#writer-requirements-for-row-tracking>.

use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::Value;

use super::conflict_checker::WinningCommitSummary;
use crate::spec::{
    Add, ColumnMappingMode, CommitAction, CommitConflictError, DeltaError, DeltaOperation,
    DeltaResult, DomainMetadata, Metadata, Protocol, SaveMode, StructType, TableFeature,
    TransactionError,
};
use crate::table::DeltaSnapshot;

pub(crate) const ROW_TRACKING_DOMAIN: &str = "delta.rowTracking";
const ENABLE_ROW_TRACKING: &str = "delta.enableRowTracking";
const ROW_TRACKING_SUSPENDED: &str = "delta.rowTrackingSuspended";
const MATERIALIZED_ROW_ID_COLUMN_NAME: &str = "delta.rowTracking.materializedRowIdColumnName";
const MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME: &str =
    "delta.rowTracking.materializedRowCommitVersionColumnName";
const PRESERVED_TAG: &str = "delta.rowTracking.preserved";

fn property_enabled(configuration: &HashMap<String, String>, key: &str) -> bool {
    configuration
        .get(key)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

fn supports_row_tracking(protocol: &Protocol) -> bool {
    protocol.min_writer_version() >= 7 && protocol.has_writer_feature(&TableFeature::RowTracking)
}

/// Whether the table configuration enables row tracking.
pub(crate) fn row_tracking_enabled(configuration: &HashMap<String, String>) -> bool {
    property_enabled(configuration, ENABLE_ROW_TRACKING)
}

/// Derives the protocol that the new metadata of an existing table requires. A table that
/// sets `delta.enableRowTracking` without the feature, such as one that an earlier version of
/// Sail created, does not get the feature from a commit that keeps the property.
pub(crate) fn protocol_for_existing_table_metadata(
    existing_protocol: &Protocol,
    existing_configuration: &HashMap<String, String>,
    metadata: &Metadata,
) -> DeltaResult<Protocol> {
    if !supports_row_tracking(existing_protocol)
        && row_tracking_enabled(existing_configuration)
        && row_tracking_enabled(metadata.configuration())
    {
        let metadata = metadata.clone().remove_config_key(ENABLE_ROW_TRACKING);
        return crate::schema::protocol_for_metadata(&metadata);
    }
    crate::schema::protocol_for_metadata(metadata)
}

/// Adds the names of the materialized row tracking columns to the configuration of a new
/// table that enables row tracking. The names follow the ones that Delta Spark generates.
pub(crate) fn add_materialized_column_names(configuration: &mut HashMap<String, String>) {
    if !row_tracking_enabled(configuration) {
        return;
    }
    for (key, prefix) in [
        (MATERIALIZED_ROW_ID_COLUMN_NAME, "_row-id-col-"),
        (
            MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME,
            "_row-commit-version-col-",
        ),
    ] {
        configuration
            .entry(key.to_string())
            .or_insert_with(|| format!("{prefix}{}", uuid::Uuid::new_v4()));
    }
}

/// Carries the row tracking configuration of a table that is replaced to the configuration
/// of the new table, as Delta Spark does. If the replaced table supports row tracking, the
/// new table keeps the names of its materialized row tracking columns, and keeps row
/// tracking enabled unless the new configuration sets `delta.enableRowTracking`.
pub(crate) fn retain_for_replace(
    existing_protocol: &Protocol,
    existing_configuration: &HashMap<String, String>,
    configuration: &mut HashMap<String, String>,
) {
    if !supports_row_tracking(existing_protocol) {
        return;
    }
    for key in [
        MATERIALIZED_ROW_ID_COLUMN_NAME,
        MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME,
    ] {
        if let Some(name) = existing_configuration.get(key) {
            configuration.insert(key.to_string(), name.clone());
        }
    }
    if !configuration.contains_key(ENABLE_ROW_TRACKING)
        && let Some(value) = existing_configuration.get(ENABLE_ROW_TRACKING)
    {
        configuration.insert(ENABLE_ROW_TRACKING.to_string(), value.clone());
    }
}

/// Whether two table configurations are the same, except for the generated names of the
/// materialized row tracking columns.
pub(crate) fn same_configuration_for_create(
    left: &HashMap<String, String>,
    right: &HashMap<String, String>,
) -> bool {
    let without_generated_names = |configuration: &HashMap<String, String>| {
        let mut configuration = configuration.clone();
        configuration.remove(MATERIALIZED_ROW_ID_COLUMN_NAME);
        configuration.remove(MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME);
        configuration
    };
    left == right || without_generated_names(left) == without_generated_names(right)
}

fn effective_protocol_and_metadata<'a>(
    actions: &'a [CommitAction],
    read_snapshot: Option<&'a Arc<DeltaSnapshot>>,
) -> Option<(&'a Protocol, &'a Metadata)> {
    let protocol = actions
        .iter()
        .rev()
        .find_map(|action| match action {
            CommitAction::Protocol(protocol) => Some(protocol),
            _ => None,
        })
        .or_else(|| read_snapshot.map(|snapshot| snapshot.protocol()))?;
    let metadata = actions
        .iter()
        .rev()
        .find_map(|action| match action {
            CommitAction::Metadata(metadata) => Some(metadata),
            _ => None,
        })
        .or_else(|| read_snapshot.map(|snapshot| snapshot.metadata()))?;
    Some((protocol, metadata))
}

fn num_records(add: &Add) -> DeltaResult<i64> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct NumRecords {
        num_records: Option<i64>,
    }

    add.stats
        .as_deref()
        .and_then(|stats| serde_json::from_str::<NumRecords>(stats).ok())
        .and_then(|stats| stats.num_records)
        .filter(|count| *count >= 0)
        .ok_or_else(|| {
            DeltaError::generic(format!(
                "Cannot assign row IDs to Add action `{}` without stats.numRecords",
                add.path
            ))
        })
}

/// Assigns fresh row IDs and row commit versions to the files that the commit adds to a
/// table that supports row tracking, and records the new row ID high-water mark.
///
/// This runs for each commit attempt on the latest snapshot of the table, so that a commit
/// that is retried after another writer won gets row IDs above the ones of the winner.
/// Files that already carry row tracking information, such as a file that is committed
/// again with a new deletion vector, keep it.
pub(super) fn assign_row_tracking(
    actions: &mut Vec<CommitAction>,
    read_snapshot: Option<&Arc<DeltaSnapshot>>,
    version: i64,
    operation: &DeltaOperation,
) -> DeltaResult<()> {
    let Some((protocol, metadata)) = effective_protocol_and_metadata(actions, read_snapshot) else {
        return Ok(());
    };
    if !supports_row_tracking(protocol)
        || property_enabled(metadata.configuration(), ROW_TRACKING_SUSPENDED)
    {
        return Ok(());
    }
    let enabled = row_tracking_enabled(metadata.configuration());
    // A table that did not support row tracking before this commit has no row IDs yet.
    let committed_high_water_mark = read_snapshot
        .filter(|snapshot| supports_row_tracking(snapshot.protocol()))
        .map(|snapshot| snapshot.row_tracking_high_water_mark())
        .transpose()?
        .flatten()
        .unwrap_or(-1);

    let mut high_water_mark = committed_high_water_mark;
    let mut adds_new_rows = false;
    for action in actions.iter_mut() {
        let CommitAction::Add(add) = action else {
            continue;
        };
        if add.base_row_id.is_none() {
            let count = num_records(add)?;
            add.base_row_id = Some(high_water_mark + 1);
            high_water_mark += count;
            adds_new_rows = true;
        }
        if add.default_row_commit_version.is_none() {
            add.default_row_commit_version = Some(version);
        }
    }

    if high_water_mark > committed_high_water_mark {
        actions.push(CommitAction::DomainMetadata(DomainMetadata {
            domain: ROW_TRACKING_DOMAIN.to_string(),
            configuration: serde_json::json!({ "rowIdHighWaterMark": high_water_mark }).to_string(),
            removed: false,
        }));
    }

    if enabled {
        // Rows that an operation copies to a new file get fresh row IDs, since the
        // materialized row tracking columns are not written. An overwrite with a
        // predicate, such as `replaceWhere`, may copy the rows of the removed files that
        // do not match the predicate.
        let copies_rows = adds_new_rows
            && actions
                .iter()
                .any(|action| matches!(action, CommitAction::Remove(_)))
            && !matches!(
                operation,
                DeltaOperation::Write {
                    predicate: None,
                    ..
                } | DeltaOperation::DynamicPartitionOverwrite { .. }
            );
        if let Some(CommitAction::CommitInfo(info)) = actions.first_mut() {
            let tags = info
                .info
                .entry("tags".to_string())
                .or_insert_with(|| Value::Object(Default::default()));
            if let Value::Object(tags) = tags {
                tags.insert(
                    PRESERVED_TAG.to_string(),
                    Value::String((!copies_rows).to_string()),
                );
            }
        }
    }
    Ok(())
}

/// Fails if a winning commit added files while the commit turns row tracking on for a
/// table that did not support it. Such a commit assigns row IDs to the files that it adds
/// and removes the files of the table that it read, so a file that the winning commit
/// added would stay in the table without row IDs.
pub(super) fn check_files_added_concurrently(
    read_snapshot: &DeltaSnapshot,
    actions: &[CommitAction],
    winning_commit: &WinningCommitSummary,
) -> Result<(), CommitConflictError> {
    if supports_row_tracking(read_snapshot.protocol()) {
        return Ok(());
    }
    let protocol = actions.iter().rev().find_map(|action| match action {
        CommitAction::Protocol(protocol) => Some(protocol),
        _ => None,
    });
    let metadata = actions
        .iter()
        .rev()
        .find_map(|action| match action {
            CommitAction::Metadata(metadata) => Some(metadata),
            _ => None,
        })
        .unwrap_or_else(|| read_snapshot.metadata());
    if protocol.is_some_and(supports_row_tracking)
        && row_tracking_enabled(metadata.configuration())
        && !winning_commit.added_files().is_empty()
    {
        return Err(CommitConflictError::ConcurrentAppend);
    }
    Ok(())
}

/// Validates the row tracking state of the table that a commit results in.
pub(super) fn validate_row_tracking(
    read_snapshot: Option<&Arc<DeltaSnapshot>>,
    protocol: &Protocol,
    metadata: &Metadata,
    schema: &StructType,
    operation: &DeltaOperation,
) -> DeltaResult<()> {
    let configuration = metadata.configuration();
    let supported = supports_row_tracking(protocol);
    // A table that sets `delta.enableRowTracking` without the feature, such as one that an
    // earlier version of Sail created, is written without row IDs, as before. Only a commit
    // that enables row tracking on the table requires the feature.
    let enabled = row_tracking_enabled(configuration)
        && (supported
            || read_snapshot.is_none_or(|snapshot| {
                supports_row_tracking(snapshot.protocol())
                    || !row_tracking_enabled(snapshot.metadata().configuration())
            }));
    let suspended = property_enabled(configuration, ROW_TRACKING_SUSPENDED);
    if enabled && suspended {
        return Err(DeltaError::generic(format!(
            "Table properties {ENABLE_ROW_TRACKING} and {ROW_TRACKING_SUSPENDED} cannot both be true"
        )));
    }
    if supported {
        // Readers take the values of the materialized row tracking columns as row IDs and
        // row commit versions, so the columns must not be table columns.
        for key in [
            MATERIALIZED_ROW_ID_COLUMN_NAME,
            MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME,
        ] {
            if let Some(name) = configuration.get(key)
                && schema.fields().any(|field| {
                    field.name() == name || field.physical_name(ColumnMappingMode::Name) == name
                })
            {
                return Err(DeltaError::generic(format!(
                    "The materialized row tracking column name `{name}` in {key} conflicts with a table column"
                )));
            }
        }
    }
    if enabled {
        if !supported {
            return Err(TransactionError::TableFeaturesRequired(TableFeature::RowTracking).into());
        }
        if !protocol.has_writer_feature(&TableFeature::DomainMetadata) {
            return Err(
                TransactionError::TableFeaturesRequired(TableFeature::DomainMetadata).into(),
            );
        }
    }
    // The files of an existing table need row IDs before the table can track rows. A table
    // that is replaced, or overwritten as a whole, removes all of its files.
    let replaces_table = matches!(
        operation,
        DeltaOperation::Create {
            mode: SaveMode::Overwrite,
            ..
        } | DeltaOperation::Write {
            mode: SaveMode::Overwrite,
            predicate: None,
            ..
        }
    );
    if let Some(snapshot) = read_snapshot
        && !replaces_table
        && supported
        && (!supports_row_tracking(snapshot.protocol())
            || (enabled && !row_tracking_enabled(snapshot.metadata().configuration())))
    {
        return Err(DeltaError::Unsupported(
            "Enabling Delta row tracking on an existing table is not supported".to_string(),
        ));
    }
    Ok(())
}
