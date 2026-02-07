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
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use futures::stream::once;
use futures::StreamExt;
use url::Url;

use crate::io::StoreContext;
use crate::operations::bootstrap::{
    bootstrap_first_snapshot, bootstrap_new_table, PersistStrategy,
};
use crate::operations::{SnapshotProduceOperation, Transaction, TransactionAction};
use crate::physical_plan::action_schema::decode_actions_and_meta_from_batch;
use crate::physical_plan::commit::IcebergCommitInfo;
use crate::spec::catalog::TableUpdate;
use crate::spec::metadata::table_metadata::SnapshotLog;
use crate::spec::snapshots::MAIN_BRANCH;
use crate::spec::{PartitionSpec, Schema as IcebergSchema, TableMetadata, TableRequirement};
use crate::utils::get_object_store_from_context;

const MAX_COMMIT_RETRIES: usize = 5;

#[derive(Debug)]
pub struct IcebergCommitExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    cache: PlanProperties,
}

impl IcebergCommitExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, table_url: Url) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            true,
        )]));
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            input,
            table_url,
            cache,
        }
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    fn apply_schema_update(table_meta: &mut TableMetadata, new_schema: IcebergSchema) {
        let schema_id = new_schema.schema_id();
        let highest_field_id = new_schema.highest_field_id();

        let mut replaced = false;
        for schema in table_meta.schemas.iter_mut() {
            if schema.schema_id() == schema_id {
                *schema = new_schema.clone();
                replaced = true;
                break;
            }
        }
        if !replaced {
            table_meta.schemas.push(new_schema.clone());
        }

        table_meta.current_schema_id = schema_id;
        table_meta.last_column_id = table_meta.last_column_id.max(highest_field_id);
    }

    fn apply_partition_spec_update(table_meta: &mut TableMetadata, new_spec: PartitionSpec) {
        let spec_id = new_spec.spec_id();
        let mut replaced = false;
        for spec in table_meta.partition_specs.iter_mut() {
            if spec.spec_id() == spec_id {
                *spec = new_spec.clone();
                replaced = true;
                break;
            }
        }
        if !replaced {
            table_meta.partition_specs.push(new_spec.clone());
        }
        table_meta.default_spec_id = spec_id;
        if let Some(highest) = new_spec.highest_field_id() {
            table_meta.last_partition_id = table_meta.last_partition_id.max(highest);
        }
    }

    fn validate_requirements(
        table_meta: Option<&TableMetadata>,
        requirements: &[TableRequirement],
    ) -> Result<()> {
        for requirement in requirements {
            match requirement {
                TableRequirement::NotExist => {
                    if table_meta.is_some() {
                        return Err(DataFusionError::Plan(
                            "Iceberg table already exists but commit asserted non-existence."
                                .to_string(),
                        ));
                    }
                }
                TableRequirement::LastAssignedFieldIdMatch {
                    last_assigned_field_id,
                } => {
                    let meta = table_meta.ok_or_else(|| {
                        DataFusionError::Plan(
                            "Iceberg table metadata missing while validating field id requirement"
                                .to_string(),
                        )
                    })?;
                    if &meta.last_column_id != last_assigned_field_id {
                        return Err(DataFusionError::Plan(format!(
                            "Iceberg commit failed: expected last assigned field id {} but found {}. Reload table metadata and retry.",
                            last_assigned_field_id, meta.last_column_id
                        )));
                    }
                }
                TableRequirement::CurrentSchemaIdMatch { current_schema_id } => {
                    let meta = table_meta.ok_or_else(|| {
                        DataFusionError::Plan(
                            "Iceberg table metadata missing while validating schema requirement"
                                .to_string(),
                        )
                    })?;
                    if &meta.current_schema_id != current_schema_id {
                        return Err(DataFusionError::Plan(format!(
                            "Iceberg commit failed: expected current schema id {} but found {}. Reload table metadata and retry.",
                            current_schema_id, meta.current_schema_id
                        )));
                    }
                }
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: reference,
                    snapshot_id,
                } => {
                    let meta = table_meta.ok_or_else(|| {
                        DataFusionError::Plan(
                            "Iceberg table metadata missing while validating snapshot requirement"
                                .to_string(),
                        )
                    })?;
                    let actual = if reference == MAIN_BRANCH {
                        meta.current_snapshot_id
                    } else {
                        meta.refs
                            .get(reference)
                            .map(|ref_entry| ref_entry.snapshot_id)
                    };
                    if &actual != snapshot_id {
                        return Err(DataFusionError::Plan(format!(
                            "Iceberg commit failed: reference '{}' expected snapshot {:?} but found {:?}",
                            reference, snapshot_id, actual
                        )));
                    }
                }
                other => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Table requirement '{other:?}' is not supported in local commits"
                    )));
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ExecutionPlan for IcebergCommitExec {
    fn name(&self) -> &'static str {
        "IcebergCommitExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("IcebergCommitExec requires exactly one child");
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.table_url.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("IcebergCommitExec can only be executed in a single partition");
        }

        let input_partitions = self.input.output_partitioning().partition_count();
        if input_partitions != 1 {
            return internal_err!(
                "IcebergCommitExec requires exactly one input partition, got {input_partitions}"
            );
        }

        let input_stream = self.input.execute(0, Arc::clone(&context))?;

        let table_url = self.table_url.clone();
        let schema = self.schema();
        let future = async move {
            let object_store = get_object_store_from_context(&context, &table_url)?;
            let store_ctx = StoreContext::new(object_store.clone(), &table_url)?;

            // Read writer result as Arrow-native action batches (may be empty for IgnoreIfExists).
            let mut data = input_stream;
            let mut added_data_files = Vec::new();
            let mut commit_meta = None;
            while let Some(batch_result) = data.next().await {
                let batch = batch_result?;
                if batch.num_rows() == 0 {
                    continue;
                }
                let (adds, _deletes, meta) = decode_actions_and_meta_from_batch(&batch)?;
                added_data_files.extend(adds);
                if meta.is_some() {
                    commit_meta = meta;
                }
            }

            // No-op path (e.g. IgnoreIfExists on existing table): no rows, no meta.
            if commit_meta.is_none() && added_data_files.is_empty() {
                let array = Arc::new(UInt64Array::from(vec![0u64]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }

            let commit_meta = commit_meta.ok_or_else(|| {
                DataFusionError::Internal(
                    "missing commit_meta action from writer output".to_string(),
                )
            })?;

            let commit_info = IcebergCommitInfo {
                table_uri: commit_meta.table_uri,
                row_count: commit_meta.row_count,
                data_files: added_data_files,
                manifest_path: String::new(),
                manifest_list_path: String::new(),
                updates: vec![],
                requirements: commit_meta.requirements,
                operation: commit_meta.operation,
                schema: commit_meta.schema,
                partition_spec: commit_meta.partition_spec,
            };

            // Load table metadata JSON if exists; for overwrite on new table we bootstrap
            let latest_meta_res =
                crate::table::find_latest_metadata_file(&object_store, &table_url).await;

            if latest_meta_res.is_err()
                && (matches!(commit_info.operation, crate::spec::Operation::Overwrite)
                    || matches!(commit_info.operation, crate::spec::Operation::Append))
            {
                Self::validate_requirements(None, &commit_info.requirements)?;
                // Bootstrap a new table using the unified bootstrap helper
                bootstrap_new_table(&table_url, &store_ctx, &commit_info).await?;

                let array = Arc::new(UInt64Array::from(vec![commit_info.row_count]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }

            let initial_latest_meta =
                latest_meta_res.map_err(|e| DataFusionError::External(Box::new(e)))?;

            let mut attempt = 0;
            loop {
                attempt += 1;
                let latest_meta = if attempt == 1 {
                    initial_latest_meta.clone()
                } else {
                    crate::table::find_latest_metadata_file(&object_store, &table_url)
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?
                };

                let meta_path = object_store::path::Path::from(latest_meta.as_str());
                let bytes = store_ctx
                    .base
                    .get(&meta_path)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .bytes()
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let mut table_meta = TableMetadata::from_json(&bytes)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Self::validate_requirements(Some(&table_meta), &commit_info.requirements)?;
                if let Some(new_schema) = commit_info.schema.clone() {
                    Self::apply_schema_update(&mut table_meta, new_schema);
                }
                let mut partition_spec_for_commit = table_meta
                    .default_partition_spec()
                    .cloned()
                    .unwrap_or_else(PartitionSpec::unpartitioned_spec);
                if let Some(new_spec) = commit_info.partition_spec.clone() {
                    let spec = if new_spec.spec_id() == 0 && table_meta.default_spec_id != 0 {
                        new_spec.with_spec_id(table_meta.default_spec_id)
                    } else {
                        new_spec
                    };
                    Self::apply_partition_spec_update(&mut table_meta, spec.clone());
                    partition_spec_for_commit = spec;
                }
                let maybe_snapshot = table_meta.current_snapshot().cloned();
                let schema_iceberg = table_meta.current_schema().cloned().ok_or_else(|| {
                    DataFusionError::Plan("No current schema in table metadata".to_string())
                })?;

                // If metadata exists but there is no current snapshot (e.g. from a CREATE TABLE),
                // bootstrap the first snapshot into the existing metadata using InPlace strategy
                // (per user preference to keep external SQL catalogs in sync).
                if maybe_snapshot.is_none()
                    && (matches!(commit_info.operation, crate::spec::Operation::Overwrite)
                        || matches!(commit_info.operation, crate::spec::Operation::Append))
                {
                    bootstrap_first_snapshot(
                        &table_url,
                        &store_ctx,
                        &commit_info,
                        table_meta,
                        &latest_meta,
                        PersistStrategy::InPlace,
                    )
                    .await?;

                    let array = Arc::new(UInt64Array::from(vec![commit_info.row_count]));
                    let batch = RecordBatch::try_new(schema, vec![array])?;
                    return Ok(batch);
                }

                let snapshot = maybe_snapshot.ok_or_else(|| {
                    DataFusionError::Plan("No current snapshot in table metadata".to_string())
                })?;

                let current_version = parse_version_from_path(&latest_meta).unwrap_or(0);
                let next_version = current_version + 1;

                let existing_for_next =
                    metadata_files_for_version(&store_ctx, next_version).await?;
                if !existing_for_next.is_empty() {
                    log::warn!(
                        "Detected existing metadata files for version {}: {:?}. Retrying attempt {}",
                        next_version,
                        existing_for_next,
                        attempt
                    );
                    if attempt >= MAX_COMMIT_RETRIES {
                        return Err(commit_conflict_error());
                    }
                    continue;
                }

                // Build transaction and action based on operation
                let tx = Transaction::new(table_url.to_string(), snapshot);
                let manifest_meta =
                    tx.default_manifest_metadata(&schema_iceberg, &partition_spec_for_commit);
                let action_commit = match commit_info.operation {
                    crate::spec::Operation::Append => {
                        let mut action = tx
                            .fast_append()
                            .with_store_context(store_ctx.clone())
                            .with_manifest_metadata(manifest_meta);
                        for df in commit_info.data_files.clone().into_iter() {
                            action.add_file(df);
                        }
                        Arc::new(action)
                            .commit(&tx)
                            .await
                            .map_err(DataFusionError::Execution)?
                    }
                    crate::spec::Operation::Overwrite => {
                        let producer = crate::operations::SnapshotProducer::new(
                            &tx,
                            commit_info.data_files.clone(),
                            Some(store_ctx.clone()),
                            Some(manifest_meta),
                        );
                        struct LocalOverwriteOperation;
                        impl SnapshotProduceOperation for LocalOverwriteOperation {
                            fn operation(&self) -> &'static str {
                                "overwrite"
                            }
                        }
                        producer
                            .commit(LocalOverwriteOperation)
                            .await
                            .map_err(DataFusionError::Execution)?
                    }
                    _ => {
                        return Err(DataFusionError::NotImplemented(
                            "Unsupported Iceberg operation in commit".to_string(),
                        ));
                    }
                };

                // Apply updates (only handle the ones we emit: AddSnapshot, SetSnapshotRef)
                let action_requirements = action_commit.requirements().to_vec();
                Self::validate_requirements(Some(&table_meta), &action_requirements)?;
                let updates = action_commit.into_updates();
                log::trace!("commit_exec: applying updates: {:?}", &updates);
                let mut newest_snapshot_seq: Option<i64> = None;
                let timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();
                for upd in updates {
                    match upd {
                        TableUpdate::AddSnapshot { snapshot } => {
                            newest_snapshot_seq = Some(snapshot.sequence_number());
                            table_meta.snapshots.push(snapshot.clone());
                            table_meta.current_snapshot_id = Some(snapshot.snapshot_id());
                            table_meta.snapshot_log.push(SnapshotLog {
                                timestamp_ms,
                                snapshot_id: snapshot.snapshot_id(),
                            });
                        }
                        TableUpdate::SetSnapshotRef {
                            ref_name,
                            reference,
                        } => {
                            table_meta.refs.insert(ref_name, reference);
                        }
                        _ => {}
                    }
                }
                if let Some(seq) = newest_snapshot_seq {
                    if seq > table_meta.last_sequence_number {
                        table_meta.last_sequence_number = seq;
                    }
                }
                table_meta.last_updated_ms = timestamp_ms;

                // Add metadata_log entry referencing previous metadata file
                table_meta
                    .metadata_log
                    .push(crate::spec::metadata::table_metadata::MetadataLog {
                        timestamp_ms,
                        metadata_file: latest_meta.clone(),
                    });

                let new_meta_bytes = table_meta
                    .to_json()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let new_meta_rel = format!(
                    "metadata/{:05}-{}.metadata.json",
                    next_version,
                    uuid::Uuid::new_v4()
                );

                log::trace!(
                    "Writing metadata: {} snapshot_id={:?} table_url={}",
                    &new_meta_rel,
                    table_meta.current_snapshot_id,
                    &table_url
                );

                let new_meta_path = object_store::path::Path::from(new_meta_rel.as_str());
                let put_opts = object_store::PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                };
                let payload = object_store::PutPayload::from(Bytes::from(new_meta_bytes));
                match store_ctx
                    .prefixed
                    .put_opts(&new_meta_path, payload, put_opts)
                    .await
                {
                    Ok(_) => {}
                    Err(object_store::Error::AlreadyExists { .. }) => {
                        log::warn!(
                            "Metadata file {} already exists for version {}. Retrying attempt {}",
                            new_meta_rel,
                            next_version,
                            attempt
                        );
                        if attempt >= MAX_COMMIT_RETRIES {
                            return Err(commit_conflict_error());
                        }
                        continue;
                    }
                    Err(e) => return Err(DataFusionError::External(Box::new(e))),
                }
                let version_files = metadata_files_for_version(&store_ctx, next_version).await?;
                let conflict_after_write = version_files.iter().any(|path| path != &new_meta_rel);
                if conflict_after_write {
                    log::warn!(
                        "Concurrent metadata writes detected for version {}: {:?}. Retrying attempt {}",
                        next_version,
                        version_files,
                        attempt
                    );
                    if let Err(err) = store_ctx.prefixed.delete(&new_meta_path).await {
                        log::warn!(
                            "Failed to delete conflicted metadata file {}: {:?}",
                            new_meta_rel,
                            err
                        );
                    }
                    if attempt >= MAX_COMMIT_RETRIES {
                        return Err(commit_conflict_error());
                    }
                    continue;
                }
                log::trace!("Metadata written successfully");

                let metadata_filename = if let Some(fname) = new_meta_rel.rsplit('/').next() {
                    fname.to_string()
                } else {
                    new_meta_rel.clone()
                };
                let hint_bytes = Bytes::from(metadata_filename.into_bytes());
                let hint_path = object_store::path::Path::from("metadata/version-hint.text");
                store_ctx
                    .prefixed
                    .put(&hint_path, object_store::PutPayload::from(hint_bytes))
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let array = Arc::new(UInt64Array::from(vec![commit_info.row_count]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }
        };

        let stream = once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DisplayAs for IcebergCommitExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "IcebergCommitExec(table_path={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: iceberg")?;
                write!(f, "table_path={}", self.table_url)
            }
        }
    }
}

fn parse_version_from_path(p: &str) -> Option<i32> {
    if let Some(fname) = p.rsplit('/').next() {
        if let Some(num) = fname
            .strip_prefix('v')
            .and_then(|s| s.strip_suffix(".metadata.json"))
        {
            return num.parse::<i32>().ok();
        }
        if let Some((num, _)) = fname.split_once('-') {
            return num.parse::<i32>().ok();
        }
    }
    None
}

async fn metadata_files_for_version(store_ctx: &StoreContext, version: i32) -> Result<Vec<String>> {
    let prefix = object_store::path::Path::from("metadata/");
    let mut stream = store_ctx.prefixed.list(Some(&prefix));
    let mut matches = Vec::new();
    while let Some(meta) = stream.next().await {
        let meta = meta.map_err(|e| DataFusionError::External(Box::new(e)))?;
        if parse_version_from_path(meta.location.as_ref()) == Some(version) {
            matches.push(meta.location.to_string());
        }
    }
    Ok(matches)
}

fn commit_conflict_error() -> DataFusionError {
    DataFusionError::Execution(format!(
        "Iceberg commit failed after {MAX_COMMIT_RETRIES} retries due to concurrent metadata updates"
    ))
}
