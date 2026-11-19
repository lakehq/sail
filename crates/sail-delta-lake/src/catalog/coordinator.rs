// SPDX-License-Identifier: Apache-2.0

use datafusion_common::{DataFusionError, Result};
use log::warn;
use object_store::ObjectStoreExt;
use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::lakehouse::{
    DeltaRatifiedCommitRequest, DeltaRatifiedCommitResponse, LakehouseCommitOutcome,
    LakehouseCommitRequest,
};
use sail_catalog::manager::CatalogManager;
use sail_common::spec::{SAIL_SPARK_UDT_METADATA_KEY, SPARK_METADATA_JSON_KEY};
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::delta_log::LogStoreRef;
use crate::spec::{
    CommitAction, DataType as DeltaDataType, Metadata, MetadataValue, PrimitiveType, Protocol,
    StructField,
};
use crate::table::DeltaSnapshot;
use crate::transaction::CatalogManagedStagedCommit;

#[derive(Debug, Clone)]
pub(crate) struct DeltaCatalogManagedTable {
    pub(crate) table_id: String,
    pub(crate) table_uri: String,
}

#[derive(Debug)]
struct UnityCommitColumnType {
    type_text: String,
    type_json: serde_json::Value,
    type_name: &'static str,
    type_precision: Option<i32>,
    type_scale: Option<i32>,
}

// TODO: Extend catalog-managed Delta coordination to cover inline ratified
// commits, provider protocol profiles, latest-ratified read bounds, published
// commit consistency checks, and commit-state-unknown reconciliation.
pub(crate) struct DeltaCatalogCommitCoordinator<'a, C: SessionExtensionAccessor + ?Sized> {
    context: &'a C,
    catalog_table: &'a [String],
}

impl<'a, C: SessionExtensionAccessor + ?Sized> DeltaCatalogCommitCoordinator<'a, C> {
    pub(crate) fn new(context: &'a C, catalog_table: &'a [String]) -> Self {
        Self {
            context,
            catalog_table,
        }
    }

    pub(crate) async fn get_ratified_commits(
        &self,
        lakehouse_table: &LakehouseExecutionContext,
        table_uri: impl Into<String>,
        start_version: i64,
        end_version: Option<i64>,
    ) -> Result<DeltaRatifiedCommitResponse> {
        let manager = self.context.extension::<CatalogManager>()?;
        manager
            .get_delta_ratified_commits(
                self.catalog_table,
                DeltaRatifiedCommitRequest {
                    context: lakehouse_table.clone(),
                    table_uri: table_uri.into(),
                    start_version,
                    end_version,
                },
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    pub(crate) async fn latest_table_version(
        &self,
        lakehouse_table: &LakehouseExecutionContext,
        table: &DeltaCatalogManagedTable,
    ) -> Result<i64> {
        self.get_ratified_commits(lakehouse_table, table.table_uri.clone(), 1, None)
            .await
            .map(|response| response.latest_table_version)
    }

    /// Commits the staged commit to the catalog. The `base` is the snapshot of the table
    /// that the commit is based on. The staged commit file is deleted when the catalog
    /// rejects the commit, and kept when the outcome is not known.
    pub(crate) async fn commit_staged(
        &self,
        lakehouse_table: &LakehouseExecutionContext,
        table: &DeltaCatalogManagedTable,
        log_store: &LogStoreRef,
        base: Option<&DeltaSnapshot>,
        staged: &CatalogManagedStagedCommit,
        actions: &[CommitAction],
        latest_backfilled_version: Option<i64>,
    ) -> Result<()> {
        let manager = self.context.extension::<CatalogManager>()?;
        let update = match delta_catalog_managed_commit_update(
            table,
            base.map(|base| (base.metadata(), base.protocol())),
            staged,
            actions,
            latest_backfilled_version,
        ) {
            Ok(update) => update,
            Err(error) => {
                delete_staged_commit(log_store, staged).await;
                return Err(error);
            }
        };
        let outcome = manager
            .commit_lakehouse_table(
                self.catalog_table,
                LakehouseCommitRequest {
                    context: lakehouse_table.clone(),
                    format: "delta".to_string(),
                    requirements: vec![],
                    updates: vec![update],
                    payload: None,
                },
            )
            .await;
        if commit_is_rejected(&outcome) {
            delete_staged_commit(log_store, staged).await;
        }
        map_commit_outcome(outcome)
    }
}

/// Whether the catalog has answered that the commit is not ratified. A failure without such
/// an answer (a server error, a timeout, or a network error) may follow a ratified commit.
fn commit_is_rejected(outcome: &CatalogResult<LakehouseCommitOutcome>) -> bool {
    match outcome {
        Ok(
            LakehouseCommitOutcome::RetryableConflict { .. }
            | LakehouseCommitOutcome::Rejected { .. },
        ) => true,
        Ok(_) => false,
        Err(
            CatalogError::InvalidArgument(_)
            | CatalogError::NotFound(_, _)
            | CatalogError::AlreadyExists(_, _)
            | CatalogError::Conflict(_)
            | CatalogError::NotSupported(_)
            | CatalogError::Unauthorized(_)
            | CatalogError::Forbidden(_)
            | CatalogError::AuthExpired(_)
            | CatalogError::RateLimited(_)
            | CatalogError::ResourceExhausted(_)
            | CatalogError::ReadOnly(_)
            | CatalogError::UnsupportedCapability(_)
            | CatalogError::StaleMetadata(_)
            | CatalogError::CommitRejected(_),
        ) => true,
        Err(_) => false,
    }
}

/// Deletes a staged commit that is not ratified, so that it is never read as a commit of the
/// table. A failure to delete does not change the outcome of the commit.
async fn delete_staged_commit(log_store: &LogStoreRef, staged: &CatalogManagedStagedCommit) {
    if let Err(error) = log_store
        .object_store(None)
        .delete(&staged.staged_path)
        .await
    {
        warn!(
            "Failed to delete the rejected staged Delta commit `{}`: {error}",
            staged.staged_path
        );
    }
}

fn map_commit_outcome(outcome: CatalogResult<LakehouseCommitOutcome>) -> Result<()> {
    match outcome {
        Ok(LakehouseCommitOutcome::Committed { .. } | LakehouseCommitOutcome::Noop { .. }) => {
            Ok(())
        }
        Ok(LakehouseCommitOutcome::RetryableConflict { message }) => Err(
            DataFusionError::Execution(format!("Delta catalog commit conflict: {message}")),
        ),
        Ok(LakehouseCommitOutcome::StateUnknown { message }) => Err(DataFusionError::External(
            Box::new(CatalogError::CommitStateUnknown(message.into())),
        )),
        Ok(LakehouseCommitOutcome::Rejected { message }) => Err(DataFusionError::External(
            Box::new(CatalogError::CommitRejected(message)),
        )),
        Err(CatalogError::Conflict(message)) => Err(DataFusionError::Execution(format!(
            "Delta catalog commit conflict: {message}"
        ))),
        Err(other) => Err(DataFusionError::External(Box::new(other))),
    }
}

fn delta_catalog_managed_commit_update(
    table: &DeltaCatalogManagedTable,
    base: Option<(&Metadata, &Protocol)>,
    staged: &CatalogManagedStagedCommit,
    actions: &[CommitAction],
    latest_backfilled_version: Option<i64>,
) -> Result<serde_json::Value> {
    let mut update = serde_json::json!({
        "table_id": table.table_id,
        "table_uri": table.table_uri,
        "commit_info": {
            "version": staged.version,
            "timestamp": staged.in_commit_timestamp,
            "file_name": staged.file_name,
            "file_size": staged.file_size,
            "file_modification_timestamp": staged.file_modification_timestamp,
        }
    });
    if let Some(latest_backfilled_version) = latest_backfilled_version {
        update["latest_backfilled_version"] = serde_json::json!(latest_backfilled_version);
    }
    match unity_commit_metadata(actions) {
        Ok(Some(metadata)) => update["metadata"] = metadata,
        Ok(None) => {}
        // The metadata of the legacy commit API is not used by a catalog with the Delta
        // REST API, so the catalog reports the error when it needs the metadata.
        Err(error) => update["metadata_error"] = serde_json::json!(error.to_string()),
    }
    if let Some(changes) = delta_log_changes(base, actions)? {
        update["delta_log"] = changes;
    }
    Ok(update)
}

/// Returns the metadata action and the protocol action of the commit that differ from
/// the metadata and the protocol of the base snapshot, in the form of the Delta log.
/// The metadata action comes with the metadata of the base snapshot, so that the catalog
/// can tell what the commit changes.
fn delta_log_changes(
    base: Option<(&Metadata, &Protocol)>,
    actions: &[CommitAction],
) -> Result<Option<serde_json::Value>> {
    let json = |value: serde_json::Result<serde_json::Value>| {
        value.map_err(|e| DataFusionError::External(Box::new(e)))
    };
    let mut changes = serde_json::Map::new();
    let metadata = actions.iter().rev().find_map(|action| match action {
        CommitAction::Metadata(metadata) => Some(metadata),
        _ => None,
    });
    if let Some(metadata) = metadata.filter(|x| base.map(|(metadata, _)| metadata) != Some(x)) {
        changes.insert(
            "metaData".to_string(),
            json(serde_json::to_value(metadata))?,
        );
        if let Some((base, _)) = base {
            changes.insert(
                "baseMetaData".to_string(),
                json(serde_json::to_value(base))?,
            );
        }
    }
    let protocol = actions.iter().rev().find_map(|action| match action {
        CommitAction::Protocol(protocol) => Some(protocol),
        _ => None,
    });
    if let Some(protocol) = protocol.filter(|x| base.map(|(_, protocol)| protocol) != Some(x)) {
        changes.insert(
            "protocol".to_string(),
            json(serde_json::to_value(protocol))?,
        );
    }
    Ok((!changes.is_empty()).then_some(serde_json::Value::Object(changes)))
}

fn unity_commit_metadata(actions: &[CommitAction]) -> Result<Option<serde_json::Value>> {
    let Some(metadata) = actions.iter().rev().find_map(|action| match action {
        CommitAction::Metadata(metadata) => Some(metadata),
        _ => None,
    }) else {
        return Ok(None);
    };

    let schema = metadata
        .parse_schema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let columns = schema
        .fields()
        .enumerate()
        .map(|(position, field)| unity_column_info(metadata, field, position))
        .collect::<Result<Vec<_>>>()?;

    let mut out = serde_json::Map::new();
    if let Some(description) = metadata.description() {
        out.insert(
            "description".to_string(),
            serde_json::Value::String(description.to_string()),
        );
    }
    out.insert(
        "schema".to_string(),
        serde_json::json!({
            "columns": columns,
        }),
    );
    if !metadata.configuration().is_empty() {
        out.insert(
            "properties".to_string(),
            serde_json::json!({
                "properties": metadata.configuration(),
            }),
        );
    }

    Ok(Some(serde_json::Value::Object(out)))
}

fn unity_column_info(
    metadata: &Metadata,
    field: &StructField,
    position: usize,
) -> Result<serde_json::Value> {
    let column_type = unity_column_type(field.data_type())?;
    let mut column = serde_json::json!({
        "name": field.name(),
        "nullable": field.is_nullable(),
        "position": i32::try_from(position).unwrap_or(i32::MAX),
        "type_text": column_type.type_text,
        "type_json": unity_struct_field_type_json(field)?.to_string(),
        "type_name": column_type.type_name,
    });

    if let Some(precision) = column_type.type_precision {
        column["type_precision"] = serde_json::json!(precision);
    }
    if let Some(scale) = column_type.type_scale {
        column["type_scale"] = serde_json::json!(scale);
    }
    if let Some(partition_index) = metadata
        .partition_columns()
        .iter()
        .position(|column| column.eq_ignore_ascii_case(field.name()))
    {
        column["partition_index"] =
            serde_json::json!(i32::try_from(partition_index).unwrap_or(i32::MAX));
    }
    if let Some(MetadataValue::String(comment)) = field.metadata().get("comment") {
        column["comment"] = serde_json::Value::String(comment.clone());
    }

    Ok(column)
}

fn unity_struct_field_type_json(field: &StructField) -> Result<serde_json::Value> {
    Ok(serde_json::json!({
        "name": field.name(),
        "type": spark_delta_data_type_json(field.data_type())?,
        "nullable": field.is_nullable(),
        "metadata": unity_field_metadata(field)?,
    }))
}

fn spark_delta_data_type_json(data_type: &DeltaDataType) -> Result<serde_json::Value> {
    match data_type {
        DeltaDataType::Primitive(primitive) => Ok(spark_delta_primitive_json(primitive)),
        DeltaDataType::Array(array) => Ok(serde_json::json!({
            "type": "array",
            "elementType": spark_delta_data_type_json(array.element_type())?,
            "containsNull": array.contains_null(),
        })),
        DeltaDataType::Struct(struct_type) => {
            let fields = struct_type
                .fields()
                .map(unity_struct_field_type_json)
                .collect::<Result<Vec<_>>>()?;
            Ok(serde_json::json!({
                "type": "struct",
                "fields": fields,
            }))
        }
        DeltaDataType::Map(map) => Ok(serde_json::json!({
            "type": "map",
            "keyType": spark_delta_data_type_json(map.key_type())?,
            "valueType": spark_delta_data_type_json(map.value_type())?,
            "valueContainsNull": map.value_contains_null(),
        })),
        DeltaDataType::Variant(_) => Err(DataFusionError::NotImplemented(
            "Unity Catalog metadata payloads for Delta variant columns".to_string(),
        )),
    }
}

fn spark_delta_primitive_json(primitive: &PrimitiveType) -> serde_json::Value {
    let type_name = match primitive {
        PrimitiveType::String => "string".to_string(),
        PrimitiveType::Long => "long".to_string(),
        PrimitiveType::Integer => "integer".to_string(),
        PrimitiveType::Short => "short".to_string(),
        PrimitiveType::Byte => "byte".to_string(),
        PrimitiveType::Float => "float".to_string(),
        PrimitiveType::Double => "double".to_string(),
        PrimitiveType::Boolean => "boolean".to_string(),
        PrimitiveType::Binary => "binary".to_string(),
        PrimitiveType::Date => "date".to_string(),
        PrimitiveType::Timestamp => "timestamp".to_string(),
        PrimitiveType::TimestampNtz => "timestamp_ntz".to_string(),
        PrimitiveType::Decimal(decimal) => {
            format!("decimal({},{})", decimal.precision(), decimal.scale())
        }
    };
    serde_json::Value::String(type_name)
}

fn unity_column_type(data_type: &DeltaDataType) -> Result<UnityCommitColumnType> {
    match data_type {
        DeltaDataType::Primitive(primitive) => Ok(unity_primitive_column_type(primitive)),
        DeltaDataType::Array(array) => {
            let element_type = unity_column_type(array.element_type())?;
            Ok(UnityCommitColumnType {
                type_text: format!("array<{}>", element_type.type_text),
                type_json: serde_json::json!({
                    "type": {
                        "type": "array",
                        "elementType": element_type.type_json,
                        "containsNull": array.contains_null(),
                    },
                }),
                type_name: "ARRAY",
                type_precision: None,
                type_scale: None,
            })
        }
        DeltaDataType::Struct(struct_type) => {
            let mut type_text_parts = Vec::new();
            let mut fields = Vec::new();
            for field in struct_type.fields() {
                let field_type = unity_column_type(field.data_type())?;
                type_text_parts.push(format!("{}:{}", field.name(), field_type.type_text));
                fields.push(serde_json::json!({
                    "name": field.name(),
                    "type": field_type.type_json,
                    "nullable": field.is_nullable(),
                    "metadata": unity_field_metadata(field)?,
                }));
            }
            Ok(UnityCommitColumnType {
                type_text: format!("struct<{}>", type_text_parts.join(",")),
                type_json: serde_json::json!({
                    "type": {
                        "type": "struct",
                        "fields": fields,
                    },
                }),
                type_name: "STRUCT",
                type_precision: None,
                type_scale: None,
            })
        }
        DeltaDataType::Map(map) => {
            let key_type = unity_column_type(map.key_type())?;
            let value_type = unity_column_type(map.value_type())?;
            Ok(UnityCommitColumnType {
                type_text: format!("map<{},{}>", key_type.type_text, value_type.type_text),
                type_json: serde_json::json!({
                    "type": {
                        "type": "map",
                        "keyType": key_type.type_json,
                        "valueType": value_type.type_json,
                        "valueContainsNull": map.value_contains_null(),
                    },
                }),
                type_name: "MAP",
                type_precision: None,
                type_scale: None,
            })
        }
        DeltaDataType::Variant(_) => Err(DataFusionError::NotImplemented(
            "Unity Catalog metadata payloads for Delta variant columns".to_string(),
        )),
    }
}

fn unity_primitive_column_type(primitive: &PrimitiveType) -> UnityCommitColumnType {
    let (type_text, type_name) = match primitive {
        PrimitiveType::String => ("string".to_string(), "STRING"),
        PrimitiveType::Long => ("long".to_string(), "LONG"),
        PrimitiveType::Integer => ("int".to_string(), "INT"),
        PrimitiveType::Short => ("short".to_string(), "SHORT"),
        PrimitiveType::Byte => ("byte".to_string(), "BYTE"),
        PrimitiveType::Float => ("float".to_string(), "FLOAT"),
        PrimitiveType::Double => ("double".to_string(), "DOUBLE"),
        PrimitiveType::Boolean => ("boolean".to_string(), "BOOLEAN"),
        PrimitiveType::Binary => ("binary".to_string(), "BINARY"),
        PrimitiveType::Date => ("date".to_string(), "DATE"),
        PrimitiveType::Timestamp => ("timestamp".to_string(), "TIMESTAMP"),
        PrimitiveType::TimestampNtz => ("timestamp_ntz".to_string(), "TIMESTAMP_NTZ"),
        PrimitiveType::Decimal(decimal) => {
            return UnityCommitColumnType {
                type_text: format!("decimal({},{})", decimal.precision(), decimal.scale()),
                type_json: serde_json::json!({
                    "type": {
                        "type": "decimal",
                        "precision": decimal.precision(),
                        "scale": decimal.scale(),
                    },
                }),
                type_name: "DECIMAL",
                type_precision: Some(i32::from(decimal.precision())),
                type_scale: Some(i32::from(decimal.scale())),
            };
        }
    };

    UnityCommitColumnType {
        type_json: serde_json::Value::String(type_text.clone()),
        type_text,
        type_name,
        type_precision: None,
        type_scale: None,
    }
}

fn unity_field_metadata(field: &StructField) -> Result<serde_json::Value> {
    let mut out = if let Some(value) = field.metadata().get(SPARK_METADATA_JSON_KEY) {
        match spark_metadata_value_json(value)? {
            serde_json::Value::Object(object) => object,
            other => {
                return Err(DataFusionError::External(Box::new(
                    CatalogError::InvalidArgument(format!(
                        "{SPARK_METADATA_JSON_KEY} must contain a JSON object, got {other:?}",
                    )),
                )));
            }
        }
    } else {
        serde_json::Map::new()
    };

    for (key, value) in field.metadata() {
        if key == SPARK_METADATA_JSON_KEY || key == SAIL_SPARK_UDT_METADATA_KEY {
            continue;
        }
        out.entry(key.clone())
            .or_insert(metadata_value_json(value)?);
    }

    Ok(serde_json::Value::Object(out))
}

fn spark_metadata_value_json(value: &MetadataValue) -> Result<serde_json::Value> {
    match value {
        MetadataValue::String(value) => serde_json::from_str(value).map_err(|e| {
            DataFusionError::External(Box::new(CatalogError::InvalidArgument(format!(
                "{SPARK_METADATA_JSON_KEY} contains invalid JSON: {e}",
            ))))
        }),
        other => metadata_value_json(other),
    }
}

fn metadata_value_json(value: &MetadataValue) -> Result<serde_json::Value> {
    match value {
        MetadataValue::Number(value) => Ok(serde_json::Value::Number((*value).into())),
        MetadataValue::String(value) => Ok(serde_json::Value::String(value.clone())),
        MetadataValue::Boolean(value) => Ok(serde_json::Value::Bool(*value)),
        MetadataValue::Other(value) => Ok(value.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::StructType;

    #[test]
    fn delta_log_changes_are_relative_to_the_base_snapshot() -> Result<()> {
        let external = |e| DataFusionError::External(Box::new(e));
        let schema = |columns: &[&str]| {
            StructType::try_new(
                columns
                    .iter()
                    .map(|column| StructField::nullable(*column, DeltaDataType::LONG)),
            )
        };
        let base_metadata = Metadata::try_new(
            None,
            None,
            schema(&["id"]).map_err(external)?,
            vec![],
            0,
            [("owner".to_string(), "a".to_string())].into(),
        )
        .map_err(external)?;
        let base_protocol = Protocol::new(3, 7, Some(vec![]), Some(vec![]));
        let base = Some((&base_metadata, &base_protocol));

        // The actions of the base snapshot written again are not changes.
        let actions = vec![
            CommitAction::Protocol(base_protocol.clone()),
            CommitAction::Metadata(base_metadata.clone()),
        ];
        assert_eq!(delta_log_changes(base, &actions)?, None);
        assert_eq!(delta_log_changes(base, &[])?, None);

        let metadata = base_metadata
            .clone()
            .with_schema(&schema(&["id", "value"]).map_err(external)?)
            .map_err(external)?;
        let protocol = Protocol::new(3, 7, Some(vec![]), None);
        let actions = vec![
            CommitAction::Protocol(protocol.clone()),
            CommitAction::Metadata(metadata.clone()),
        ];
        assert_eq!(
            delta_log_changes(base, &actions)?,
            Some(serde_json::json!({
                "metaData": metadata,
                "baseMetaData": base_metadata,
                "protocol": protocol,
            }))
        );
        assert_eq!(
            delta_log_changes(None, &actions)?,
            Some(serde_json::json!({
                "metaData": metadata,
                "protocol": protocol,
            }))
        );
        Ok(())
    }

    #[test]
    fn commit_update_defers_legacy_metadata_errors() -> Result<()> {
        let external = |e| DataFusionError::External(Box::new(e));
        let metadata = |data_type: DeltaDataType| {
            Metadata::try_new(
                None,
                None,
                StructType::try_new([StructField::nullable("value", data_type)])?,
                vec![],
                0,
                Default::default(),
            )
        };
        let table = DeltaCatalogManagedTable {
            table_id: "id".to_string(),
            table_uri: "memory:///t".to_string(),
        };
        let staged = CatalogManagedStagedCommit {
            version: 1,
            file_name: "00000000000000000001.uuid.json".to_string(),
            file_size: 1,
            file_modification_timestamp: 1,
            in_commit_timestamp: 1,
            staged_path: object_store::path::Path::from("t/_delta_log/_staged_commits/1.json"),
            metrics: Default::default(),
        };
        let base_metadata = metadata(DeltaDataType::LONG).map_err(external)?;
        let base_protocol = Protocol::new(3, 7, Some(vec![]), Some(vec![]));
        let base = Some((&base_metadata, &base_protocol));

        let variant = metadata(DeltaDataType::unshredded_variant()).map_err(external)?;
        let actions = vec![CommitAction::Metadata(variant.clone())];
        let update = delta_catalog_managed_commit_update(&table, base, &staged, &actions, None)?;
        assert_eq!(update.get("metadata"), None);
        assert!(
            update["metadata_error"]
                .as_str()
                .is_some_and(|error| error.contains("variant columns")),
            "{update}"
        );
        assert_eq!(update["delta_log"]["metaData"], serde_json::json!(variant));

        let actions = vec![CommitAction::Metadata(base_metadata.clone())];
        let update = delta_catalog_managed_commit_update(&table, base, &staged, &actions, None)?;
        assert!(update["metadata"].is_object(), "{update}");
        assert_eq!(update.get("metadata_error"), None);
        Ok(())
    }

    #[test]
    fn definite_commit_rejection_preserves_its_typed_outcome() -> Result<()> {
        match map_commit_outcome(Ok(LakehouseCommitOutcome::Rejected {
            message: "policy denied".to_string(),
        })) {
            Err(DataFusionError::External(error)) => {
                let error = error.downcast_ref::<CatalogError>().ok_or_else(|| {
                    DataFusionError::Internal("expected typed catalog error".to_string())
                })?;
                assert!(
                    matches!(error, CatalogError::CommitRejected(message) if message == "policy denied")
                );
                Ok(())
            }
            Err(error) => Err(error),
            Ok(()) => Err(DataFusionError::Internal(
                "definite commit rejection unexpectedly succeeded".to_string(),
            )),
        }
    }
}
