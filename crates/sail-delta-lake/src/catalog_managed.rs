use sail_common_datafusion::catalog::delta::{
    unity_table_id_value, DELTA_TABLE_TYPE_VALUE, DELTA_UNITY_TABLE_ID_KEY,
};
use sail_common_datafusion::catalog::{CatalogPartitionField, TableColumnStatus, TableKind};

use crate::spec::{CommitAction, DeltaError, DeltaResult, Metadata, Protocol, TableFeature};

#[derive(Debug)]
pub(crate) struct CatalogManagedDeltaTable {
    pub(crate) columns: Vec<TableColumnStatus>,
    pub(crate) comment: Option<String>,
    pub(crate) location: Option<String>,
    pub(crate) partition_by: Vec<CatalogPartitionField>,
    pub(crate) properties: Vec<(String, String)>,
    pub(crate) table_id: String,
}

pub(crate) fn catalog_managed_delta_table(kind: TableKind) -> Option<CatalogManagedDeltaTable> {
    let TableKind::Table {
        columns,
        comment,
        location,
        format,
        partition_by,
        properties,
        is_external,
        ..
    } = kind
    else {
        return None;
    };
    if !format.eq_ignore_ascii_case(DELTA_TABLE_TYPE_VALUE) {
        return None;
    }
    let is_managed = !is_external
        || properties.iter().any(|(key, value)| {
            key.eq_ignore_ascii_case("table_type") && value.eq_ignore_ascii_case("MANAGED")
        });
    if !is_managed {
        return None;
    }
    // The Unity table id is the explicit marker that this table participates in
    // Unity coordinated commits. Other managed Delta catalogs may not use UC rules.
    let table_id = unity_table_id_value(
        properties
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )?
    .to_string();

    Some(CatalogManagedDeltaTable {
        columns,
        comment,
        location,
        partition_by,
        properties,
        table_id,
    })
}

pub(crate) fn protocol_with_catalog_managed(protocol: &Protocol) -> Protocol {
    let mut reader_features = protocol
        .reader_features()
        .map(|features| features.to_vec())
        .unwrap_or_default();
    let mut writer_features = protocol
        .writer_features()
        .map(|features| features.to_vec())
        .unwrap_or_default();
    if !reader_features.contains(&TableFeature::CatalogManaged) {
        reader_features.push(TableFeature::CatalogManaged);
    }
    for feature in [
        TableFeature::CatalogManaged,
        TableFeature::InCommitTimestamp,
    ] {
        if !writer_features.contains(&feature) {
            writer_features.push(feature);
        }
    }
    Protocol::new(
        protocol.min_reader_version().max(3),
        protocol.min_writer_version().max(7),
        Some(reader_features),
        Some(writer_features),
    )
}

pub(crate) fn metadata_with_catalog_managed(metadata: Metadata, table_id: &str) -> Metadata {
    metadata
        .add_config_key(
            "delta.feature.catalogManaged".to_string(),
            "supported".to_string(),
        )
        .add_config_key(
            "delta.enableInCommitTimestamps".to_string(),
            "true".to_string(),
        )
        .add_config_key(DELTA_UNITY_TABLE_ID_KEY.to_string(), table_id.to_string())
}

pub(crate) fn enable_catalog_managed_create_actions(
    final_actions: &mut [CommitAction],
    table_id: &str,
) -> DeltaResult<()> {
    let mut saw_protocol = false;
    let mut saw_metadata = false;
    for action in final_actions {
        match action {
            CommitAction::Protocol(protocol) => {
                *protocol = protocol_with_catalog_managed(protocol);
                saw_protocol = true;
            }
            CommitAction::Metadata(metadata) => {
                *metadata = metadata_with_catalog_managed(metadata.clone(), table_id);
                saw_metadata = true;
            }
            _ => {}
        }
    }
    if !saw_protocol || !saw_metadata {
        return Err(DeltaError::InternalError(
            "catalog-managed Delta table creation requires Protocol and Metadata actions"
                .to_string(),
        ));
    }
    Ok(())
}
