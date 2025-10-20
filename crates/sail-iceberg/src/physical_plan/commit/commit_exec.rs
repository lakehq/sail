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

use crate::io::IcebergObjectStore;
use crate::physical_plan::commit::types::IcebergCommitInfo;
use crate::spec::catalog::TableUpdate;
use crate::spec::metadata::table_metadata::{MetadataLog, SnapshotLog};
use crate::spec::TableMetadata;
use crate::transaction::{Transaction, TransactionAction};
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
            let table_path = table_url
                .path()
                .strip_prefix('/')
                .unwrap_or(table_url.path());
            let store = IcebergObjectStore::new(
                object_store.clone(),
                object_store::path::Path::from(table_path),
            );

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

            // Load table metadata JSON to obtain current snapshot and schema
            let latest_meta =
                crate::table_format::find_latest_metadata_file(&object_store, &table_url)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let meta_path = object_store::path::Path::from(latest_meta.as_str());
            let bytes = object_store
                .get(&meta_path)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .bytes()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let table_meta = crate::spec::TableMetadata::from_json(&bytes)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let snapshot = table_meta.current_snapshot().cloned().ok_or_else(|| {
                DataFusionError::Plan("No current snapshot in table metadata".to_string())
            })?;
            let schema_iceberg = table_meta.current_schema().cloned().ok_or_else(|| {
                DataFusionError::Plan("No current schema in table metadata".to_string())
            })?;

            // Build transaction and fast-append action
            let tx = Transaction::new(table_url.to_string(), snapshot);
            let manifest_meta = tx.default_manifest_metadata(&schema_iceberg);
            let mut action = tx
                .fast_append()
                .with_store(store)
                .with_manifest_metadata(manifest_meta);
            for df in commit_info.data_files.into_iter() {
                action.add_file(df);
            }
            let commit = Arc::new(action)
                .commit(&tx)
                .await
                .map_err(DataFusionError::Execution)?;

            // Apply ActionCommit to table metadata and persist a new metadata.json and version-hint
            // 1) Load current table metadata again to ensure we update the freshest state
            let latest_meta =
                crate::table_format::find_latest_metadata_file(&object_store, &table_url)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let meta_path = object_store::path::Path::from(latest_meta.as_str());
            let bytes = object_store
                .get(&meta_path)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .bytes()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let mut table_meta: TableMetadata = TableMetadata::from_json(&bytes)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            log::trace!(
                "commit_exec: loaded table metadata with snapshot id: {:?}",
                table_meta.current_snapshot_id
            );

            // 2) Apply updates (only handle the ones we emit: AddSnapshot, SetSnapshotRef)
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

            // 3) Persist updated metadata.json with incremented version
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
            table_meta.metadata_log.push(MetadataLog {
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

            let _full_path = IcebergObjectStore::new(
                object_store.clone(),
                object_store::path::Path::from(table_path),
            )
            .put_rel(&new_meta_rel, Bytes::from(new_meta_bytes))
            .await
            .map_err(DataFusionError::Execution)?;
            log::trace!("Metadata written successfully");

            // 4) Update version-hint
            let hint_bytes = Bytes::from(next_version.to_string().into_bytes());
            IcebergObjectStore::new(
                object_store.clone(),
                object_store::path::Path::from(table_url.path()),
            )
            .put_rel("metadata/version-hint.text", hint_bytes)
            .await
            .map_err(DataFusionError::Execution)?;

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
