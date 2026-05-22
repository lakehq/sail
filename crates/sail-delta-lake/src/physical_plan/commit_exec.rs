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

use std::any::Any;
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
use object_store::{Error as ObjectStoreError, ObjectStoreExt, PutMode, PutOptions};
use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::CommitTableOptions;
use sail_common_datafusion::catalog::delta::{unity_table_id_value, DELTA_UNITY_TABLE_ID_KEY};
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::datasource::PhysicalSinkMode;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use url::Url;

use crate::kernel::transaction::{
    CatalogManagedStagedCommit, CommitBuilder, CommitProperties, FinalizedCommit,
    Metrics as CommitFinalMetrics, OperationMetrics,
};
use crate::kernel::{DeltaOperation, DeltaSnapshotConfig, SaveMode};
use crate::physical_plan::action_schema::ExecCommitMeta;
use crate::physical_plan::{decode_actions_and_meta_from_batch, DeltaCommitContext, COL_ACTION};
use crate::schema::{
    metadata_for_create_with_struct_type, normalize_delta_schema, protocol_for_create,
};
use crate::spec::{
    commit_path, contains_variant_arrow, CommitAction, Protocol, StructType, TableFeature,
};
use crate::storage::{get_object_store_from_context, LogStoreRef, StorageConfig};
use crate::table::{
    create_delta_table_with_object_store, open_table_with_object_store_and_table_config,
    open_table_with_object_store_and_table_config_at_version,
};

const METRIC_NUM_COMMIT_RETRIES: &str = "num_commit_retries";
const METRIC_CHECKPOINT_CREATED: &str = "checkpoint_created";
const METRIC_LOG_FILES_CLEANED: &str = "log_files_cleaned";

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
        let TableKind::Table {
            location,
            format,
            properties,
            is_external,
            ..
        } = status.kind
        else {
            return Ok(None);
        };
        if !format.eq_ignore_ascii_case("delta") {
            return Ok(None);
        }
        let is_managed = !is_external
            || properties.iter().any(|(key, value)| {
                key.eq_ignore_ascii_case("table_type") && value.eq_ignore_ascii_case("MANAGED")
            });
        if !is_managed {
            return Ok(None);
        }
        // Managed Delta tables from non-Unity catalogs do not participate in
        // Unity coordinated commits, so the UC table id is the opt-in marker.
        let table_id = match unity_table_id_value(
            properties
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        ) {
            Some(table_id) => table_id.to_string(),
            None => return Ok(None),
        };

        Ok(Some(DeltaCatalogManagedTable {
            table_id,
            table_uri: location.unwrap_or_else(|| table_url.to_string()),
        }))
    }

    fn enable_catalog_managed_create_actions(
        final_actions: &mut [CommitAction],
        table_id: &str,
    ) -> Result<()> {
        let mut saw_protocol = false;
        let mut saw_metadata = false;
        for action in final_actions {
            match action {
                CommitAction::Protocol(protocol) => {
                    *protocol = Self::protocol_with_catalog_managed(protocol);
                    saw_protocol = true;
                }
                CommitAction::Metadata(metadata) => {
                    *metadata = metadata
                        .clone()
                        .add_config_key(
                            "delta.feature.catalogManaged".to_string(),
                            "supported".to_string(),
                        )
                        .add_config_key(
                            "delta.enableInCommitTimestamps".to_string(),
                            "true".to_string(),
                        )
                        .add_config_key(DELTA_UNITY_TABLE_ID_KEY.to_string(), table_id.to_string());
                    saw_metadata = true;
                }
                _ => {}
            }
        }
        if !saw_protocol || !saw_metadata {
            return Err(DataFusionError::Internal(
                "catalog-managed Delta table creation requires Protocol and Metadata actions"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn protocol_with_catalog_managed(protocol: &Protocol) -> Protocol {
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

    async fn commit_catalog_managed_table(
        context: &Arc<TaskContext>,
        catalog_table: &[String],
        table: &DeltaCatalogManagedTable,
        staged: &CatalogManagedStagedCommit,
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
        if staged.version > 0 {
            update["latest_backfilled_version"] = serde_json::json!(staged.version - 1);
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

    fn as_any(&self) -> &dyn Any {
        self
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
        let user_metadata = self.user_metadata.clone();
        let commit_context = self.commit_context.clone();
        let catalog_table = self.catalog_table.clone();
        let schema = self.schema();
        let future = async move {
            let _elapsed_compute_timer = elapsed_compute.timer();
            let storage_config = StorageConfig;
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
            let (operation, final_actions) = if !table_exists {
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
                    let protocol = protocol_for_create(
                        false,
                        false,
                        false,
                        false,
                        contains_variant_arrow(normalized_sink.as_ref()),
                        &HashMap::new(),
                    )
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    let metadata = metadata_for_create_with_struct_type(
                        StructType::try_from(normalized_sink.as_ref())
                            .map_err(|e| DataFusionError::External(Box::new(e)))?,
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
                    Self::enable_catalog_managed_create_actions(
                        &mut create_actions,
                        &table.table_id,
                    )?;
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

            operation_metrics.finalize_for(&operation);

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

            let pre_commit = CommitBuilder::from(
                CommitProperties::default()
                    .with_operation_metrics(operation_metrics)
                    .with_user_metadata(user_metadata),
            )
            .with_actions(final_actions)
            .build(reference.clone(), log_store.clone(), operation);

            let finalized_commit = if let Some(table) = catalog_managed_table
                .as_ref()
                .filter(|_| use_catalog_managed_commit)
            {
                let catalog_table = catalog_table.as_deref().ok_or_else(|| {
                    DataFusionError::Internal(
                        "catalog-managed Delta commit missing catalog table reference".to_string(),
                    )
                })?;
                let staged = pre_commit
                    .into_staged_commit_future()
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Self::commit_catalog_managed_table(&context, catalog_table, table, &staged).await?;
                Self::publish_staged_commit(&log_store, &staged).await?;
                FinalizedCommit {
                    snapshot: None,
                    version: staged.version,
                    metrics: CommitFinalMetrics {
                        num_retries: staged.metrics.num_retries,
                        new_checkpoint_created: false,
                        num_log_files_cleaned_up: 0,
                    },
                }
            } else {
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
