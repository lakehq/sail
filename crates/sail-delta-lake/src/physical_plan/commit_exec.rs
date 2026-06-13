// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use futures::stream::{self, StreamExt};
use log::warn;
use object_store::{Error as ObjectStoreError, ObjectStoreExt, PutMode, PutOptions};
use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::{CommitTableOptions, GetTableCommitsOptions};
use sail_common_datafusion::datasource::PhysicalSinkMode;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use url::Url;

use crate::catalog_managed::{catalog_managed_delta_table, enable_catalog_managed_create_actions};
use crate::kernel::transaction::{
    CatalogManagedStagedCommit, CommitBuilder, CommitProperties, FinalizedCommit,
    Metrics as CommitFinalMetrics, OperationMetrics,
};
use crate::kernel::{DeltaOperation, DeltaSnapshotConfig, SaveMode};
use crate::physical_plan::action_schema::ExecCommitMeta;
use crate::physical_plan::catalog_location::resolve_catalog_table_url;
use crate::physical_plan::{decode_actions_and_meta_from_batch, DeltaCommitContext, COL_ACTION};
use crate::schema::{
    metadata_for_create_with_struct_type, normalize_delta_schema, protocol_for_create,
    schema_has_column_defaults, schema_has_generated_columns, schema_has_identity_columns,
};
use crate::spec::{
    commit_path, contains_timestampntz_arrow, contains_variant_arrow, ColumnMetadataKey,
    CommitAction, DataType as DeltaDataType, Metadata, MetadataValue, PrimitiveType, StatValue,
    Stats, StructField, StructType, TableFeature,
};
use crate::storage::{get_object_store_from_context, LogStoreRef, StorageConfig};
use crate::table::{
    create_delta_table_with_object_store, load_catalog_managed_commits_for_snapshot,
    open_table_with_object_store_and_table_config,
    open_table_with_object_store_and_table_config_at_version,
};

const METRIC_NUM_COMMIT_RETRIES: &str = "num_commit_retries";
const METRIC_CHECKPOINT_CREATED: &str = "checkpoint_created";
const METRIC_LOG_FILES_CLEANED: &str = "log_files_cleaned";

#[derive(Debug, Clone)]
struct IdentityColumnCommitInfo {
    name: String,
    stats_name: String,
    start: i64,
    step: i64,
    high_water_mark: Option<i64>,
}

#[derive(Debug)]
struct UnityCommitColumnType {
    type_text: String,
    type_json: serde_json::Value,
    type_name: &'static str,
    type_precision: Option<i32>,
    type_scale: Option<i32>,
}

/// Physical execution node for Delta Lake commit operations
#[derive(Debug)]
pub struct DeltaCommitExec {
    /// The plan that produces action metadata (Add and Remove actions).
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    partition_columns: Vec<String>,
    table_exists: bool,
    sink_schema: SchemaRef,
    sink_mode: PhysicalSinkMode,
    /// Per-commit user-defined metadata to record in `commitInfo.userMetadata`.
    user_metadata: Option<String>,
    commit_context: DeltaCommitContext,
    catalog_table: Option<Vec<String>>,
    metrics: ExecutionPlanMetricsSet,
    cache: Arc<PlanProperties>,
}

impl DeltaCommitExec {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        partition_columns: Vec<String>,
        table_exists: bool,
        sink_schema: SchemaRef,
        sink_mode: PhysicalSinkMode,
        user_metadata: Option<String>,
        commit_context: DeltaCommitContext,
        catalog_table: Option<Vec<String>>,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            true,
        )]));
        let cache = Self::compute_properties(schema);
        Self {
            input,
            table_url,
            partition_columns,
            table_exists,
            sink_schema,
            sink_mode,
            user_metadata,
            commit_context,
            catalog_table,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }

    fn compute_properties(schema: SchemaRef) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ))
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    pub fn table_exists(&self) -> bool {
        self.table_exists
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn sink_schema(&self) -> &SchemaRef {
        &self.sink_schema
    }

    pub fn sink_mode(&self) -> &PhysicalSinkMode {
        &self.sink_mode
    }

    pub fn user_metadata(&self) -> Option<&str> {
        self.user_metadata.as_deref()
    }

    pub fn commit_context(&self) -> &DeltaCommitContext {
        &self.commit_context
    }

    pub fn catalog_table(&self) -> Option<&[String]> {
        self.catalog_table.as_deref()
    }

    async fn load_catalog_managed_table(
        context: &Arc<TaskContext>,
        catalog_table: &[String],
        table_url: &Url,
    ) -> Result<Option<DeltaCatalogManagedTable>> {
        let manager = context.extension::<CatalogManager>()?;
        let status = manager
            .get_table(catalog_table)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let Some(table) = catalog_managed_delta_table(status.kind) else {
            return Ok(None);
        };

        Ok(Some(DeltaCatalogManagedTable {
            table_id: table.table_id,
            table_uri: table.location.unwrap_or_else(|| table_url.to_string()),
        }))
    }

    async fn latest_catalog_managed_table_version(
        context: &Arc<TaskContext>,
        catalog_table: &[String],
        table: &DeltaCatalogManagedTable,
    ) -> Result<i64> {
        let manager = context.extension::<CatalogManager>()?;
        manager
            .get_table_commits(
                catalog_table,
                GetTableCommitsOptions {
                    format: "delta".to_string(),
                    table_uri: table.table_uri.clone(),
                    start_version: 1,
                    end_version: None,
                },
            )
            .await
            .map(|response| response.latest_table_version)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn refresh_catalog_managed_reference(
        context: &Arc<TaskContext>,
        catalog_table: &[String],
        table_url: &Url,
        log_store: &LogStoreRef,
        reference: Option<Arc<crate::table::DeltaSnapshot>>,
        latest_catalog_version: i64,
    ) -> Result<Option<Arc<crate::table::DeltaSnapshot>>> {
        let Some(snapshot) = reference else {
            return Ok(None);
        };
        if latest_catalog_version < 0 || snapshot.version() == latest_catalog_version {
            return Ok(Some(snapshot));
        }
        if snapshot.version() > latest_catalog_version {
            return Err(DataFusionError::Internal(format!(
                "catalog-managed Delta commit snapshot version {} is newer than catalog latest ratified version {latest_catalog_version}",
                snapshot.version()
            )));
        }

        let catalog_managed_commits = load_catalog_managed_commits_for_snapshot(
            context.as_ref(),
            catalog_table,
            table_url,
            log_store.clone(),
            Some(latest_catalog_version),
        )
        .await?;
        let snapshot = crate::table::DeltaSnapshot::try_new(
            log_store.as_ref(),
            DeltaSnapshotConfig {
                require_files: false,
                catalog_managed_commits,
                ..Default::default()
            },
            Some(latest_catalog_version),
            None,
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Some(Arc::new(snapshot)))
    }

    fn is_catalog_managed_commit(
        reference: Option<&Arc<crate::table::DeltaSnapshot>>,
        actions: &[CommitAction],
    ) -> bool {
        let protocol = actions
            .iter()
            .rev()
            .find_map(|action| match action {
                CommitAction::Protocol(protocol) => Some(protocol),
                _ => None,
            })
            .or_else(|| reference.map(|snapshot| snapshot.protocol()));
        protocol.is_some_and(|protocol| {
            protocol.has_reader_feature(&TableFeature::CatalogManaged)
                && protocol.has_writer_feature(&TableFeature::CatalogManaged)
        })
    }

    fn split_create_actions_for_catalog_managed_commit(
        actions: Vec<CommitAction>,
    ) -> (Vec<CommitAction>, Vec<CommitAction>) {
        let mut bootstrap_actions = Vec::new();
        let mut commit_actions = Vec::new();
        for action in actions {
            match action {
                CommitAction::Protocol(_) | CommitAction::Metadata(_) => {
                    bootstrap_actions.push(action);
                }
                _ => commit_actions.push(action),
            }
        }
        (bootstrap_actions, commit_actions)
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
            .map(|(position, field)| Self::unity_column_info(metadata, field, position))
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
        let column_type = Self::unity_column_type(field.data_type())?;
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
            DeltaDataType::Primitive(primitive) => Ok(Self::unity_primitive_column_type(primitive)),
            DeltaDataType::Array(array) => {
                let element_type = Self::unity_column_type(array.element_type())?;
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
                    let field_type = Self::unity_column_type(field.data_type())?;
                    type_text_parts.push(format!("{}:{}", field.name(), field_type.type_text));
                    fields.push(serde_json::json!({
                        "name": field.name(),
                        "type": field_type.type_json,
                        "nullable": field.is_nullable(),
                        "metadata": Self::unity_field_metadata(field),
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
                let key_type = Self::unity_column_type(map.key_type())?;
                let value_type = Self::unity_column_type(map.value_type())?;
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

    fn write_operation_for_sink_mode(
        partition_columns: &[String],
        sink_mode: &PhysicalSinkMode,
    ) -> DeltaOperation {
        let partition_by = (!partition_columns.is_empty()).then(|| partition_columns.to_vec());
        match sink_mode {
            PhysicalSinkMode::Overwrite
            | PhysicalSinkMode::OverwriteIf { .. }
            | PhysicalSinkMode::OverwritePartitions => DeltaOperation::Write {
                mode: SaveMode::Overwrite,
                partition_by,
                predicate: None,
            },
            PhysicalSinkMode::IgnoreIfExists => DeltaOperation::Write {
                mode: SaveMode::Ignore,
                partition_by,
                predicate: None,
            },
            PhysicalSinkMode::Append | PhysicalSinkMode::ErrorIfExists => DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by,
                predicate: None,
            },
        }
    }

    async fn commit_catalog_managed_table(
        context: &Arc<TaskContext>,
        catalog_table: &[String],
        table: &DeltaCatalogManagedTable,
        staged: &CatalogManagedStagedCommit,
        actions: &[CommitAction],
        latest_backfilled_version: Option<i64>,
    ) -> Result<()> {
        let manager = context.extension::<CatalogManager>()?;
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
        if let Some(metadata) = Self::unity_commit_metadata(actions)? {
            update["metadata"] = metadata;
        }
        manager
            .commit_table(
                catalog_table,
                CommitTableOptions {
                    format: "delta".to_string(),
                    requirements: vec![],
                    updates: vec![update],
                },
            )
            .await
            .map(|_| ())
            .map_err(|err| match err {
                CatalogError::Conflict(message) => {
                    DataFusionError::Execution(format!("Delta catalog commit conflict: {message}"))
                }
                other => DataFusionError::External(Box::new(other)),
            })
    }

    async fn publish_staged_commit(
        log_store: &LogStoreRef,
        staged: &CatalogManagedStagedCommit,
    ) -> Result<()> {
        let store = log_store.object_store(None);
        let bytes = store
            .get(&staged.staged_path)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .bytes()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        match store
            .put_opts(
                &commit_path(staged.version),
                bytes.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(ObjectStoreError::AlreadyExists { .. }) => Ok(()),
            Err(err) => Err(DataFusionError::External(Box::new(err))),
        }
    }

    async fn latest_published_backfilled_version(
        log_store: &LogStoreRef,
        end_version: i64,
    ) -> Result<Option<i64>> {
        if end_version < 0 {
            return Ok(None);
        }

        let store = log_store.object_store(None);
        let mut latest = None;
        for version in 0..=end_version {
            match store.head(&commit_path(version)).await {
                Ok(_) => latest = Some(version),
                Err(ObjectStoreError::NotFound { .. }) => break,
                Err(err) => return Err(DataFusionError::External(Box::new(err))),
            }
        }
        Ok(latest)
    }
}

#[derive(Debug, Clone)]
struct DeltaCatalogManagedTable {
    table_id: String,
    table_uri: String,
}

#[async_trait]
impl ExecutionPlan for DeltaCommitExec {
    fn name(&self) -> &'static str {
        "DeltaCommitExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("DeltaCommitExec requires exactly one child");
        }

        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.table_url.clone(),
            self.partition_columns.clone(),
            self.table_exists,
            self.sink_schema.clone(),
            self.sink_mode.clone(),
            self.user_metadata.clone(),
            self.commit_context.clone(),
            self.catalog_table.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("DeltaCommitExec can only be executed in a single partition");
        }

        let input_partitions = self.input.output_partitioning().partition_count();
        if input_partitions != 1 {
            return internal_err!(
                "DeltaCommitExec requires exactly one input partition, got {}",
                input_partitions
            );
        }

        let input_stream = self.input.execute(0, Arc::clone(&context))?;

        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let elapsed_compute = MetricBuilder::new(&self.metrics).elapsed_compute(partition);
        let plan_metrics = self.metrics.clone();

        let table_url = self.table_url.clone();
        let partition_columns = self.partition_columns.clone();
        let table_exists = self.table_exists;
        let sink_schema = self.sink_schema.clone();
        let sink_mode = self.sink_mode.clone();
        let user_metadata = self.user_metadata.clone();
        let commit_context = self.commit_context.clone();
        let catalog_table = self.catalog_table.clone();
        let schema = self.schema();
        let future = async move {
            let _elapsed_compute_timer = elapsed_compute.timer();
            let storage_config = StorageConfig;
            let table_url =
                resolve_catalog_table_url(&context, catalog_table.as_deref(), &table_url).await?;
            let object_store = get_object_store_from_context(&context, &table_url)?;

            let full_snapshot_task = if table_exists {
                let open_url = table_url.clone();
                let open_store = Arc::clone(&object_store);
                let open_storage = storage_config.clone();
                let base_version = commit_context.base_version();
                Some(SpawnedTask::spawn(async move {
                    match base_version {
                        Some(version) => {
                            open_table_with_object_store_and_table_config_at_version(
                                open_url,
                                open_store,
                                open_storage,
                                DeltaSnapshotConfig::default(),
                                version,
                            )
                            .await
                        }
                        None => {
                            open_table_with_object_store_and_table_config(
                                open_url,
                                open_store,
                                open_storage,
                                DeltaSnapshotConfig::default(),
                            )
                            .await
                        }
                    }
                }))
            } else {
                None
            };

            let mut total_rows = 0u64;
            let mut has_data = false;
            // "data" actions (Add/Remove/other) and "initial" actions (Protocol/Metadata)
            // are kept separate so we can preserve the required action ordering on commit.
            let mut actions: Vec<CommitAction> = Vec::new();
            let mut initial_actions: Vec<CommitAction> = Vec::new();
            let mut operation: Option<DeltaOperation> = None;
            let mut operation_metrics = OperationMetrics::default();
            let mut data = input_stream;

            while let Some(batch_result) = data.next().await {
                let batch = batch_result?;

                // Arrow-native action rows + optional CommitMeta row only.
                if batch.column_by_name(COL_ACTION).is_some() {
                    let (decoded_actions, decoded_meta) =
                        decode_actions_and_meta_from_batch(&batch)?;
                    for a in decoded_actions {
                        // Convert from the broad Action type (used for log replay) to
                        // CommitAction, rejecting any checkpoint-only variants at the
                        // boundary.  In practice decode_actions_and_meta_from_batch only
                        // produces Metadata/Protocol/Add/Remove/Cdc/Txn actions.
                        let ca = CommitAction::try_from(a).map_err(|e| {
                            DataFusionError::Plan(format!(
                                "unsupported action in commit batch: {e}"
                            ))
                        })?;
                        match ca {
                            CommitAction::Protocol(_) | CommitAction::Metadata(_) => {
                                initial_actions.push(ca)
                            }
                            _ => actions.push(ca),
                        }
                    }
                    if let Some(ExecCommitMeta {
                        row_count,
                        operation: op,
                        operation_metrics: metrics,
                    }) = decoded_meta
                    {
                        total_rows = total_rows.saturating_add(row_count);
                        if operation.is_none() {
                            operation = op;
                        }
                        operation_metrics.merge(metrics);
                    }
                    has_data = has_data || batch.num_rows() > 0;
                } else {
                    return Err(DataFusionError::Plan(
                        "DeltaCommitExec input must be delta action rows (action_type: UInt8)"
                            .to_string(),
                    ));
                }
            }

            if !has_data {
                let array = Arc::new(UInt64Array::from(vec![0]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }

            // Prepend initial actions
            let mut final_actions = initial_actions;
            final_actions.extend(actions);
            if !table_exists {
                final_actions = apply_identity_high_water_marks(final_actions, None)?;
            }
            let kinds: Vec<&'static str> = final_actions
                .iter()
                .map(|a| match a {
                    CommitAction::Protocol(_) => "Protocol",
                    CommitAction::Metadata(_) => "Metadata",
                    CommitAction::Add(_) => "Add",
                    CommitAction::Remove(_) => "Remove",
                    _ => "Other",
                })
                .collect();
            log::trace!(
                "final_actions_len: {}, final_action_kinds: {:?}",
                final_actions.len(),
                &kinds
            );

            if final_actions.is_empty() && !table_exists {
                // For new tables, add protocol and metadata even if no data
                let array = Arc::new(UInt64Array::from(vec![0]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            } else if final_actions.is_empty() {
                // For existing tables, no actions means no changes
                let array = Arc::new(UInt64Array::from(vec![0]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }

            let catalog_managed_table = match catalog_table.as_deref() {
                Some(catalog_table) => {
                    Self::load_catalog_managed_table(&context, catalog_table, &table_url).await?
                }
                None => None,
            };
            // For new tables, always ensure Protocol + Metadata are present and use Create.
            // Even if the writer supplied an operation (e.g., Overwrite), the first commit
            // must initialize the table with protocol and metadata.
            let (operation, mut final_actions) = if !table_exists {
                let mut create_actions = final_actions;
                let has_protocol = create_actions
                    .iter()
                    .any(|action| matches!(action, CommitAction::Protocol(_)));
                let has_metadata = create_actions
                    .iter()
                    .any(|action| matches!(action, CommitAction::Metadata(_)));
                if !has_protocol || !has_metadata {
                    // Construct minimal protocol/metadata and insert them
                    let normalized_sink = normalize_delta_schema(&sink_schema);
                    let kernel_schema = StructType::try_from(normalized_sink.as_ref())
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    let protocol = protocol_for_create(
                        false,
                        contains_timestampntz_arrow(normalized_sink.as_ref()),
                        false,
                        schema_has_generated_columns(&kernel_schema),
                        schema_has_column_defaults(&kernel_schema),
                        schema_has_identity_columns(&kernel_schema),
                        contains_variant_arrow(normalized_sink.as_ref()),
                        &HashMap::new(),
                    )
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    let metadata = metadata_for_create_with_struct_type(
                        kernel_schema,
                        partition_columns.to_vec(),
                        Utc::now().timestamp_millis(),
                        HashMap::new(),
                    )
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    // Insert in order: Protocol, then Metadata
                    create_actions.insert(0, CommitAction::Metadata(metadata));
                    create_actions.insert(0, CommitAction::Protocol(protocol));
                }
                if let Some(table) = catalog_managed_table.as_ref() {
                    enable_catalog_managed_create_actions(&mut create_actions, &table.table_id)
                        .map_err(DataFusionError::from)?;
                }
                let protocol = create_actions
                    .iter()
                    .find_map(|action| match action {
                        CommitAction::Protocol(protocol) => Some(protocol.clone()),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "Delta table creation requires a Protocol action".to_string(),
                        )
                    })?;
                let metadata = create_actions
                    .iter()
                    .find_map(|action| match action {
                        CommitAction::Metadata(metadata) => Some(metadata.clone()),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "Delta table creation requires a Metadata action".to_string(),
                        )
                    })?;
                (
                    DeltaOperation::Create {
                        mode: SaveMode::ErrorIfExists,
                        location: table_url.to_string(),
                        protocol: Box::new(protocol),
                        metadata: Box::new(metadata),
                    },
                    create_actions,
                )
            } else {
                (
                    operation.clone().unwrap_or(DeltaOperation::Write {
                        mode: SaveMode::Append,
                        partition_by: if partition_columns.is_empty() {
                            None
                        } else {
                            Some(partition_columns.to_vec())
                        },
                        predicate: None,
                    }),
                    final_actions,
                )
            };

            let needs_full_snapshot = final_actions
                .iter()
                .any(|action| matches!(action, CommitAction::Remove(_)))
                || operation.read_whole_table();

            let table = create_delta_table_with_object_store(
                table_url.clone(),
                object_store.clone(),
                storage_config.clone(),
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let log_store = table.log_store();

            let reference = if table_exists {
                if needs_full_snapshot {
                    let table = full_snapshot_task
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "Delta full snapshot task missing for existing table".to_string(),
                            )
                        })?
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    Some(
                        table
                            .snapshot()
                            .map_err(|e| DataFusionError::External(Box::new(e)))?
                            .clone(),
                    )
                } else {
                    drop(full_snapshot_task);
                    if let Some(snapshot_context) = commit_context.base_snapshot.as_ref() {
                        Some(Arc::new(
                            snapshot_context
                                .to_snapshot(
                                    log_store.as_ref(),
                                    DeltaSnapshotConfig {
                                        require_files: false,
                                        ..Default::default()
                                    },
                                )
                                .map_err(|e| DataFusionError::External(Box::new(e)))?,
                        ))
                    } else {
                        let table = match commit_context.base_version() {
                            Some(version) => {
                                open_table_with_object_store_and_table_config_at_version(
                                    table_url.clone(),
                                    object_store.clone(),
                                    storage_config.clone(),
                                    DeltaSnapshotConfig {
                                        require_files: false,
                                        ..Default::default()
                                    },
                                    version,
                                )
                                .await
                            }
                            None => {
                                open_table_with_object_store_and_table_config(
                                    table_url.clone(),
                                    object_store.clone(),
                                    storage_config.clone(),
                                    DeltaSnapshotConfig {
                                        require_files: false,
                                        ..Default::default()
                                    },
                                )
                                .await
                            }
                        }
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        Some(
                            table
                                .snapshot()
                                .map_err(|e| DataFusionError::External(Box::new(e)))?
                                .clone(),
                        )
                    }
                }
            } else {
                None
            };

            if table_exists {
                final_actions = apply_identity_high_water_marks(
                    final_actions,
                    reference.as_deref().map(|snapshot| snapshot.metadata()),
                )?;
            }

            let commit_uses_catalog_managed_protocol =
                Self::is_catalog_managed_commit(reference.as_ref(), &final_actions);
            if commit_uses_catalog_managed_protocol && catalog_managed_table.is_none() {
                return Err(DataFusionError::Plan(
                    "catalog-managed Delta writes require a managed catalog table reference"
                        .to_string(),
                ));
            }
            let use_catalog_managed_commit =
                catalog_managed_table.is_some() && commit_uses_catalog_managed_protocol;

            let finalized_commit = if let Some(table) = catalog_managed_table
                .as_ref()
                .filter(|_| use_catalog_managed_commit)
            {
                let catalog_table = catalog_table.as_deref().ok_or_else(|| {
                    DataFusionError::Internal(
                        "catalog-managed Delta commit missing catalog table reference".to_string(),
                    )
                })?;
                let (reference, final_actions, operation, operation_metrics) = if !table_exists {
                    let (bootstrap_actions, commit_actions) =
                        Self::split_create_actions_for_catalog_managed_commit(final_actions);
                    if commit_actions.is_empty() {
                        (
                            reference.clone(),
                            bootstrap_actions,
                            operation,
                            operation_metrics,
                        )
                    } else {
                        let bootstrap_commit = CommitBuilder::from(
                            CommitProperties::default().with_user_metadata(user_metadata.clone()),
                        )
                        .with_actions(bootstrap_actions)
                        .build(None, log_store.clone(), operation)
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        let operation =
                            Self::write_operation_for_sink_mode(&partition_columns, &sink_mode);
                        (
                            bootstrap_commit.snapshot,
                            commit_actions,
                            operation,
                            operation_metrics,
                        )
                    }
                } else {
                    (
                        reference.clone(),
                        final_actions,
                        operation,
                        operation_metrics,
                    )
                };
                let mut operation_metrics = operation_metrics;
                if !table_exists
                    && final_actions.iter().all(|action| {
                        matches!(
                            action,
                            CommitAction::Protocol(_) | CommitAction::Metadata(_)
                        )
                    })
                {
                    operation_metrics.finalize_for(&operation);
                    CommitBuilder::from(
                        CommitProperties::default()
                            .with_operation_metrics(operation_metrics)
                            .with_user_metadata(user_metadata),
                    )
                    .with_actions(final_actions)
                    .build(reference, log_store.clone(), operation)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                } else {
                    operation_metrics.finalize_for(&operation);
                    let latest_catalog_version =
                        Self::latest_catalog_managed_table_version(&context, catalog_table, table)
                            .await?;
                    let reference = Self::refresh_catalog_managed_reference(
                        &context,
                        catalog_table,
                        &table_url,
                        &log_store,
                        reference,
                        latest_catalog_version,
                    )
                    .await?;
                    let pre_commit = CommitBuilder::from(
                        CommitProperties::default()
                            .with_operation_metrics(operation_metrics)
                            .with_user_metadata(user_metadata),
                    )
                    .with_actions(final_actions.clone())
                    .build(reference, log_store.clone(), operation);
                    let staged = pre_commit
                        .into_staged_commit_future_with_catalog_latest_version(
                            latest_catalog_version,
                        )
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    let latest_backfilled_version =
                        Self::latest_published_backfilled_version(&log_store, staged.version - 1)
                            .await?;
                    Self::commit_catalog_managed_table(
                        &context,
                        catalog_table,
                        table,
                        &staged,
                        &final_actions,
                        latest_backfilled_version,
                    )
                    .await?;
                    if let Err(err) = Self::publish_staged_commit(&log_store, &staged).await {
                        warn!(
                            "Failed to publish catalog-managed Delta commit version {} after catalog ratification: {}",
                            staged.version, err
                        );
                    }
                    FinalizedCommit {
                        snapshot: None,
                        metrics: CommitFinalMetrics {
                            num_retries: staged.metrics.num_retries,
                            new_checkpoint_created: false,
                            num_log_files_cleaned_up: 0,
                        },
                    }
                }
            } else {
                operation_metrics.finalize_for(&operation);
                let pre_commit = CommitBuilder::from(
                    CommitProperties::default()
                        .with_operation_metrics(operation_metrics)
                        .with_user_metadata(user_metadata),
                )
                .with_actions(final_actions)
                .build(reference.clone(), log_store.clone(), operation);
                pre_commit
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            };

            let retries =
                usize::try_from(finalized_commit.metrics.num_retries).unwrap_or(usize::MAX);
            MetricBuilder::new(&plan_metrics)
                .global_counter(METRIC_NUM_COMMIT_RETRIES)
                .add(retries);

            if finalized_commit.metrics.new_checkpoint_created {
                MetricBuilder::new(&plan_metrics)
                    .global_counter(METRIC_CHECKPOINT_CREATED)
                    .add(1);
            }

            let cleaned = usize::try_from(finalized_commit.metrics.num_log_files_cleaned_up)
                .unwrap_or(usize::MAX);
            MetricBuilder::new(&plan_metrics)
                .global_counter(METRIC_LOG_FILES_CLEANED)
                .add(cleaned);

            // Expose row count through execution metrics as well.
            output_rows.add(usize::try_from(total_rows).unwrap_or(usize::MAX));

            let array = Arc::new(UInt64Array::from(vec![total_rows]));
            let batch = RecordBatch::try_new(schema, vec![array])?;
            Ok(batch)
        };

        let stream = stream::once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

fn apply_identity_high_water_marks(
    mut actions: Vec<CommitAction>,
    reference_metadata: Option<&Metadata>,
) -> Result<Vec<CommitAction>> {
    let metadata_index = actions
        .iter()
        .rposition(|action| matches!(action, CommitAction::Metadata(_)));
    let Some(base_metadata) = metadata_index
        .and_then(|idx| match &actions[idx] {
            CommitAction::Metadata(metadata) => Some(metadata.clone()),
            _ => None,
        })
        .or_else(|| reference_metadata.cloned())
    else {
        return Ok(actions);
    };

    let schema = base_metadata
        .parse_schema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let identity_columns = collect_identity_columns_for_commit(&schema)?;
    if identity_columns.is_empty() {
        return Ok(actions);
    }

    let candidates = collect_identity_high_water_mark_candidates(&actions, &identity_columns)?;
    if candidates.is_empty() {
        return Ok(actions);
    }

    let (updated_schema, changed) =
        update_identity_high_water_marks_in_schema(schema, &identity_columns, &candidates)?;
    if !changed {
        return Ok(actions);
    }

    let updated_metadata = base_metadata
        .with_schema(&updated_schema)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    if let Some(idx) = metadata_index {
        actions[idx] = CommitAction::Metadata(updated_metadata);
    } else {
        actions.insert(0, CommitAction::Metadata(updated_metadata));
    }
    Ok(actions)
}

fn collect_identity_columns_for_commit(
    schema: &StructType,
) -> Result<Vec<IdentityColumnCommitInfo>> {
    schema
        .fields()
        .filter_map(|field| {
            let start = metadata_i64(field, &ColumnMetadataKey::IdentityStart)?;
            let step = metadata_i64(field, &ColumnMetadataKey::IdentityStep)?;
            let _allow_explicit =
                metadata_bool(field, &ColumnMetadataKey::IdentityAllowExplicitInsert)?;
            let high_water_mark = metadata_i64(field, &ColumnMetadataKey::IdentityHighWaterMark);
            let stats_name = metadata_string(field, &ColumnMetadataKey::ColumnMappingPhysicalName)
                .unwrap_or_else(|| field.name.clone());
            Some(if step == 0 {
                Err(DataFusionError::Plan(format!(
                    "identity column `{}` has invalid step 0",
                    field.name
                )))
            } else {
                Ok(IdentityColumnCommitInfo {
                    name: field.name.clone(),
                    stats_name,
                    start,
                    step,
                    high_water_mark,
                })
            })
        })
        .collect()
}

fn collect_identity_high_water_mark_candidates(
    actions: &[CommitAction],
    identity_columns: &[IdentityColumnCommitInfo],
) -> Result<HashMap<String, i64>> {
    let mut candidates: HashMap<String, i64> = HashMap::new();
    for action in actions {
        let CommitAction::Add(add) = action else {
            continue;
        };
        let Some(stats_json) = add.stats.as_deref() else {
            continue;
        };
        let stats = Stats::from_json_str(stats_json).map_err(|e| {
            DataFusionError::Plan(format!("failed to parse Delta AddFile stats: {e}"))
        })?;
        for identity in identity_columns {
            let stat = if identity.step > 0 {
                stats.max_value(&identity.stats_name)
            } else {
                stats.min_value(&identity.stats_name)
            };
            let Some(value) = stat.and_then(stat_value_i64) else {
                continue;
            };
            candidates
                .entry(identity.name.clone())
                .and_modify(|current| {
                    if identity.step > 0 {
                        *current = (*current).max(value);
                    } else {
                        *current = (*current).min(value);
                    }
                })
                .or_insert(value);
        }
    }
    Ok(candidates)
}

fn update_identity_high_water_marks_in_schema(
    schema: StructType,
    identity_columns: &[IdentityColumnCommitInfo],
    candidates: &HashMap<String, i64>,
) -> Result<(StructType, bool)> {
    let identity_by_name = identity_columns
        .iter()
        .map(|identity| (identity.name.as_str(), identity))
        .collect::<HashMap<_, _>>();
    let mut changed = false;
    let fields = schema
        .into_fields()
        .map(|field| {
            let Some(identity) = identity_by_name.get(field.name.as_str()) else {
                return Ok(field);
            };
            let Some(candidate) = candidates.get(&identity.name).copied() else {
                return Ok(field);
            };
            let before_start =
                identity_value_is_before_start(candidate, identity.start, identity.step);
            let rounded = round_identity_high_water_mark(identity.start, identity.step, candidate)?;
            let new_high_water_mark = match identity.high_water_mark {
                Some(old) if identity.step > 0 => old.max(rounded),
                Some(old) => old.min(rounded),
                None => rounded,
            };
            let old_bad = identity.high_water_mark.is_some_and(|old| {
                identity_value_is_before_start(old, identity.start, identity.step)
            });
            if old_bad || (!before_start && identity.high_water_mark != Some(new_high_water_mark)) {
                changed = true;
                let StructField {
                    name,
                    data_type,
                    nullable,
                    mut metadata,
                } = field;
                metadata.insert(
                    ColumnMetadataKey::IdentityHighWaterMark
                        .as_ref()
                        .to_string(),
                    MetadataValue::Number(new_high_water_mark),
                );
                Ok(StructField {
                    name,
                    data_type,
                    nullable,
                    metadata,
                })
            } else {
                Ok(field)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((
        StructType::try_new(fields).map_err(|e| DataFusionError::External(Box::new(e)))?,
        changed,
    ))
}

fn identity_value_is_before_start(value: i64, start: i64, step: i64) -> bool {
    if step > 0 {
        value < start
    } else {
        value > start
    }
}

fn round_identity_high_water_mark(start: i64, step: i64, value: i64) -> Result<i64> {
    let value_offset = value.checked_sub(start).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "identity high water mark `{value}` cannot be compared with start `{start}` without overflowing BIGINT"
        ))
    })?;
    if value_offset % step == 0 {
        return Ok(value);
    }
    let quotient = value_offset / step;
    let same_direction = (value_offset > 0 && step > 0) || (value_offset < 0 && step < 0);
    let step_multiple = if same_direction {
        quotient.checked_add(1).ok_or_else(|| {
            DataFusionError::Plan("identity high water mark rounding overflowed BIGINT".to_string())
        })?
    } else {
        quotient
    };
    let delta = step.checked_mul(step_multiple).ok_or_else(|| {
        DataFusionError::Plan("identity high water mark rounding overflowed BIGINT".to_string())
    })?;
    start.checked_add(delta).ok_or_else(|| {
        DataFusionError::Plan("identity high water mark rounding overflowed BIGINT".to_string())
    })
}

fn metadata_i64(field: &StructField, key: &ColumnMetadataKey) -> Option<i64> {
    match field.metadata.get(key.as_ref())? {
        MetadataValue::Number(value) => Some(*value),
        MetadataValue::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn metadata_bool(field: &StructField, key: &ColumnMetadataKey) -> Option<bool> {
    match field.metadata.get(key.as_ref())? {
        MetadataValue::Boolean(value) => Some(*value),
        MetadataValue::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn metadata_string(field: &StructField, key: &ColumnMetadataKey) -> Option<String> {
    match field.metadata.get(key.as_ref())? {
        MetadataValue::String(value) => Some(value.clone()),
        _ => None,
    }
}

fn stat_value_i64(value: &StatValue) -> Option<i64> {
    match value {
        StatValue::Number(value) => value.as_i64(),
        StatValue::String(value) => value.parse().ok(),
        _ => None,
    }
}

impl DisplayAs for DeltaCommitExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeltaCommitExec(table_path={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: delta")?;
                write!(f, "table_path={}", self.table_url)
            }
        }
    }
}
