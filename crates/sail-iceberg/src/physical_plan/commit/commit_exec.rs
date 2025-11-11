use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::{Array, StringArray, UInt64Array};
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
use crate::physical_plan::commit::bootstrap::{
    bootstrap_first_snapshot, bootstrap_new_table, PersistStrategy,
};
use crate::physical_plan::commit::IcebergCommitInfo;
use crate::spec::catalog::TableUpdate;
use crate::spec::metadata::table_metadata::SnapshotLog;
use crate::spec::TableMetadata;
use crate::transaction::{SnapshotProduceOperation, Transaction, TransactionAction};
use crate::utils::get_object_store_from_context;

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

            // Read writer result (first row, string JSON)
            let mut data = input_stream;
            let arr = if let Some(batch_result) = data.next().await {
                let batch = batch_result?;
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .cloned()
                    .ok_or_else(|| {
                        DataFusionError::Plan("Invalid writer output schema".to_string())
                    })?
            } else {
                let array = Arc::new(UInt64Array::from(vec![0u64]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            };
            if arr.is_empty() {
                let array = Arc::new(UInt64Array::from(vec![0u64]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }
            let commit_info: IcebergCommitInfo = serde_json::from_str(arr.value(0))
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Load table metadata JSON if exists; for overwrite on new table we bootstrap
            let latest_meta_res =
                crate::table_format::find_latest_metadata_file(&object_store, &table_url).await;

            if latest_meta_res.is_err()
                && (matches!(commit_info.operation, crate::spec::Operation::Overwrite)
                    || matches!(commit_info.operation, crate::spec::Operation::Append))
            {
                // Bootstrap a new table using the unified bootstrap helper
                bootstrap_new_table(&table_url, &store_ctx, &commit_info).await?;

                let array = Arc::new(UInt64Array::from(vec![commit_info.row_count]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }

            let latest_meta =
                latest_meta_res.map_err(|e| DataFusionError::External(Box::new(e)))?;
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

            // Build transaction and action based on operation
            let tx = Transaction::new(table_url.to_string(), snapshot);
            let manifest_meta = tx.default_manifest_metadata(&schema_iceberg);
            let commit = match commit_info.operation {
                crate::spec::Operation::Append => {
                    let mut action = tx
                        .fast_append()
                        .with_store_context(store_ctx.clone())
                        .with_manifest_metadata(manifest_meta);
                    for df in commit_info.data_files.into_iter() {
                        action.add_file(df);
                    }
                    Arc::new(action)
                        .commit(&tx)
                        .await
                        .map_err(DataFusionError::Execution)?
                }
                crate::spec::Operation::Overwrite => {
                    let producer = super::super::super::transaction::SnapshotProducer::new(
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
            let updates = commit.into_updates();
            log::trace!("commit_exec: applying updates: {:?}", &updates);
            let mut newest_snapshot_seq: Option<i64> = None;
            let timestamp_ms = chrono::Utc::now().timestamp_millis();
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

            // Persist updated metadata.json with incremented version
            // Derive next version number from latest metadata file path
            fn parse_version_from_path(p: &str) -> Option<i32> {
                // support both v123.metadata.json and 00001-uuid.metadata.json
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
            let current_version = parse_version_from_path(&latest_meta).unwrap_or(0);
            let next_version = current_version + 1;

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
            store_ctx
                .prefixed
                .put(
                    &new_meta_path,
                    object_store::PutPayload::from(Bytes::from(new_meta_bytes)),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            log::trace!("Metadata written successfully");

            // Update version-hint with the metadata filename (not version number)
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
            Ok(batch)
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
