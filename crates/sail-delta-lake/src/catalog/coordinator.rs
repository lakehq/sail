use datafusion_common::{DataFusionError, Result};
use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::lakehouse::{
    DeltaRatifiedCommitRequest, DeltaRatifiedCommitResponse, LakehouseCommitOutcome,
    LakehouseCommitRequest,
};
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::kernel::transaction::CatalogManagedStagedCommit;
use crate::spec::{
    CommitAction, DataType as DeltaDataType, Metadata, MetadataValue, PrimitiveType, StructField,
};

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

    pub(crate) async fn commit_staged(
        &self,
        lakehouse_table: &LakehouseExecutionContext,
        table: &DeltaCatalogManagedTable,
        staged: &CatalogManagedStagedCommit,
        actions: &[CommitAction],
        latest_backfilled_version: Option<i64>,
    ) -> Result<()> {
        let manager = self.context.extension::<CatalogManager>()?;
        let update =
            delta_catalog_managed_commit_update(table, staged, actions, latest_backfilled_version)?;
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
        map_commit_outcome(outcome)
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
            Box::new(CatalogError::CommitStateUnknown(message)),
        )),
        Ok(LakehouseCommitOutcome::Rejected { message }) => Err(DataFusionError::Execution(
            format!("Delta catalog commit rejected: {message}"),
        )),
        Err(CatalogError::Conflict(message)) => Err(DataFusionError::Execution(format!(
            "Delta catalog commit conflict: {message}"
        ))),
        Err(other) => Err(DataFusionError::External(Box::new(other))),
    }
}

fn delta_catalog_managed_commit_update(
    table: &DeltaCatalogManagedTable,
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
    if let Some(metadata) = unity_commit_metadata(actions)? {
        update["metadata"] = metadata;
    }
    Ok(update)
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
        "type_json": column_type.type_json.to_string(),
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
                    "metadata": unity_field_metadata(field),
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

fn unity_field_metadata(field: &StructField) -> serde_json::Value {
    let metadata = field
        .metadata()
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                serde_json::to_value(value).unwrap_or(serde_json::Value::Null),
            )
        })
        .collect();
    serde_json::Value::Object(metadata)
}
