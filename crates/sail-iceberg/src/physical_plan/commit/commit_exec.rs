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
use crate::physical_plan::commit::IcebergCommitInfo;
use crate::spec::catalog::TableUpdate;
use crate::spec::metadata::table_metadata::SnapshotLog;
use crate::spec::TableMetadata;
use crate::transaction::{SnapshotProduceOperation, Transaction, TransactionAction};
use crate::utils::{get_object_store_from_context, join_table_uri, WritePathMode};

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
            let store_ctx = StoreContext::new(object_store.clone(), &table_url);

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
                && matches!(commit_info.operation, crate::spec::Operation::Overwrite)
            {
                // Bootstrap a new table metadata for overwrite when no table exists
                use crate::spec::metadata::format::FormatVersion;
                use crate::spec::snapshots::{
                    SnapshotBuilder, SnapshotReference, SnapshotRetention,
                };
                use crate::spec::{ManifestContentType, PartitionSpec, Schema};

                let iceberg_schema: Schema = commit_info.schema.clone().ok_or_else(|| {
                    DataFusionError::Plan("Missing schema for bootstrap overwrite".to_string())
                })?;
                let partition_spec: PartitionSpec = commit_info
                    .partition_spec
                    .clone()
                    .unwrap_or_else(PartitionSpec::unpartitioned_spec);

                // Build a manifest for the new data files
                let metadata = crate::spec::manifest::ManifestMetadata::new(
                    std::sync::Arc::new(iceberg_schema.clone()),
                    iceberg_schema.schema_id(),
                    partition_spec.clone(),
                    FormatVersion::V2,
                    ManifestContentType::Data,
                );
                let mut writer =
                    crate::spec::manifest::ManifestWriterBuilder::new(None, None, metadata.clone())
                        .build();
                for df in &commit_info.data_files {
                    writer.add(df.clone());
                }
                let manifest = writer.finish();
                let manifest_bytes = manifest
                    .to_avro_bytes_v2()
                    .map_err(DataFusionError::Execution)?;
                let manifest_len = manifest_bytes.len() as i64;
                let manifest_rel = format!("metadata/manifest-{}.avro", uuid::Uuid::new_v4());
                let manifest_path = object_store::path::Path::from(manifest_rel.as_str());
                store_ctx
                    .prefixed
                    .put(
                        &manifest_path,
                        object_store::PutPayload::from(Bytes::from(manifest_bytes)),
                    )
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                // Create manifest list referencing the manifest
                let mut list_writer = crate::spec::manifest_list::ManifestListWriter::new();
                list_writer.append(
                    crate::spec::manifest_list::ManifestFile::builder()
                        .with_manifest_path(format!("{}{}", table_url, manifest_rel))
                        .with_manifest_length(manifest_len)
                        .with_partition_spec_id(partition_spec.spec_id())
                        .with_content(crate::spec::ManifestContentType::Data)
                        .with_sequence_number(
                            crate::spec::manifest_list::UNASSIGNED_SEQUENCE_NUMBER,
                        )
                        .with_min_sequence_number(
                            crate::spec::manifest_list::UNASSIGNED_SEQUENCE_NUMBER,
                        )
                        .with_added_snapshot_id(0)
                        .with_file_counts(commit_info.data_files.len() as i32, 0, 0)
                        .with_row_counts(commit_info.row_count as i64, 0, 0)
                        .build()
                        .map_err(DataFusionError::Execution)?,
                );
                let list_bytes = list_writer
                    .to_bytes(FormatVersion::V2)
                    .map_err(DataFusionError::Execution)?;
                let new_snapshot_id = chrono::Utc::now().timestamp_millis();
                let list_rel = format!("metadata/snap-{}.avro", new_snapshot_id);
                let list_path = object_store::path::Path::from(list_rel.as_str());
                store_ctx
                    .prefixed
                    .put(
                        &list_path,
                        object_store::PutPayload::from(Bytes::from(list_bytes)),
                    )
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                // Build initial snapshot
                let write_mode = WritePathMode::Absolute;
                let snapshot = SnapshotBuilder::new()
                    .with_snapshot_id(new_snapshot_id)
                    .with_sequence_number(1)
                    .with_manifest_list(join_table_uri(table_url.as_ref(), &list_rel, &write_mode))
                    .with_summary(crate::spec::snapshots::Summary::new(
                        crate::spec::Operation::Append,
                    ))
                    .with_schema_id(iceberg_schema.schema_id())
                    .build()
                    .map_err(DataFusionError::Execution)?;

                // Build minimal TableMetadata V2
                let table_meta = TableMetadata {
                    format_version: crate::spec::metadata::format::FormatVersion::V2,
                    table_uuid: None,
                    location: table_url.to_string(),
                    last_sequence_number: 1,
                    last_updated_ms: chrono::Utc::now().timestamp_millis(),
                    last_column_id: iceberg_schema.highest_field_id(),
                    schemas: vec![iceberg_schema.clone()],
                    current_schema_id: iceberg_schema.schema_id(),
                    partition_specs: vec![partition_spec.clone()],
                    default_spec_id: partition_spec.spec_id(),
                    last_partition_id: partition_spec.highest_field_id().unwrap_or(0),
                    properties: std::collections::HashMap::new(),
                    current_snapshot_id: Some(new_snapshot_id),
                    snapshots: vec![snapshot.clone()],
                    snapshot_log: vec![SnapshotLog {
                        timestamp_ms: snapshot.timestamp_ms,
                        snapshot_id: new_snapshot_id,
                    }],
                    metadata_log: vec![],
                    sort_orders: vec![],
                    default_sort_order_id: None,
                    refs: std::iter::once((
                        crate::spec::snapshots::MAIN_BRANCH.to_string(),
                        SnapshotReference {
                            snapshot_id: new_snapshot_id,
                            retention: SnapshotRetention::Branch {
                                min_snapshots_to_keep: None,
                                max_snapshot_age_ms: None,
                                max_ref_age_ms: None,
                            },
                        },
                    ))
                    .collect(),
                    statistics: vec![],
                    partition_statistics: vec![],
                };

                // Write metadata v00001
                let new_meta_bytes = table_meta
                    .to_json()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let new_meta_rel =
                    format!("metadata/{:05}-{}.metadata.json", 1, uuid::Uuid::new_v4());
                let meta_path = object_store::path::Path::from(new_meta_rel.as_str());
                store_ctx
                    .prefixed
                    .put(
                        &meta_path,
                        object_store::PutPayload::from(Bytes::from(new_meta_bytes)),
                    )
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                // Write version-hint
                let hint_path = object_store::path::Path::from("metadata/version-hint.text");
                store_ctx
                    .prefixed
                    .put(
                        &hint_path,
                        object_store::PutPayload::from(Bytes::from("1".as_bytes().to_vec())),
                    )
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

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
            let mut table_meta = crate::spec::TableMetadata::from_json(&bytes)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let maybe_snapshot = table_meta.current_snapshot().cloned();
            let schema_iceberg = table_meta.current_schema().cloned().ok_or_else(|| {
                DataFusionError::Plan("No current schema in table metadata".to_string())
            })?;

            // If metadata exists but there is no current snapshot and this is an overwrite,
            // bootstrap the first snapshot into the existing metadata and persist a new version.
            if maybe_snapshot.is_none()
                && matches!(commit_info.operation, crate::spec::Operation::Overwrite)
            {
                use crate::spec::manifest_list::UNASSIGNED_SEQUENCE_NUMBER;
                use crate::spec::metadata::format::FormatVersion;

                let partition_spec = table_meta
                    .default_partition_spec()
                    .cloned()
                    .unwrap_or_else(crate::spec::partition::PartitionSpec::unpartitioned_spec);

                // Build manifest containing new data files
                let metadata = crate::spec::manifest::ManifestMetadata::new(
                    std::sync::Arc::new(schema_iceberg.clone()),
                    schema_iceberg.schema_id(),
                    partition_spec.clone(),
                    FormatVersion::V2,
                    crate::spec::ManifestContentType::Data,
                );
                let mut writer =
                    crate::spec::manifest::ManifestWriterBuilder::new(None, None, metadata.clone())
                        .build();
                for df in &commit_info.data_files {
                    writer.add(df.clone());
                }
                let manifest = writer.finish();
                let manifest_bytes = manifest
                    .to_avro_bytes_v2()
                    .map_err(DataFusionError::Execution)?;
                let manifest_len = manifest_bytes.len() as i64;
                let manifest_rel = format!("metadata/manifest-{}.avro", uuid::Uuid::new_v4());
                let manifest_path = object_store::path::Path::from(manifest_rel.as_str());
                store_ctx
                    .prefixed
                    .put(
                        &manifest_path,
                        object_store::PutPayload::from(Bytes::from(manifest_bytes)),
                    )
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                // Create manifest list
                let mut list_writer = crate::spec::manifest_list::ManifestListWriter::new();
                list_writer.append(
                    crate::spec::manifest_list::ManifestFile::builder()
                        .with_manifest_path(format!("{}{}", table_url, manifest_rel))
                        .with_manifest_length(manifest_len)
                        .with_partition_spec_id(partition_spec.spec_id())
                        .with_content(crate::spec::ManifestContentType::Data)
                        .with_sequence_number(UNASSIGNED_SEQUENCE_NUMBER)
                        .with_min_sequence_number(UNASSIGNED_SEQUENCE_NUMBER)
                        .with_added_snapshot_id(0)
                        .with_file_counts(commit_info.data_files.len() as i32, 0, 0)
                        .with_row_counts(commit_info.row_count as i64, 0, 0)
                        .build()
                        .map_err(DataFusionError::Execution)?,
                );
                let list_bytes = list_writer
                    .to_bytes(FormatVersion::V2)
                    .map_err(DataFusionError::Execution)?;
                let new_snapshot_id = chrono::Utc::now().timestamp_millis();
                let list_rel = format!("metadata/snap-{}.avro", new_snapshot_id);
                let list_path = object_store::path::Path::from(list_rel.as_str());
                store_ctx
                    .prefixed
                    .put(
                        &list_path,
                        object_store::PutPayload::from(Bytes::from(list_bytes)),
                    )
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                // Build initial snapshot and update metadata
                let write_mode = WritePathMode::Absolute;
                let snapshot = crate::spec::snapshots::SnapshotBuilder::new()
                    .with_snapshot_id(new_snapshot_id)
                    .with_sequence_number(1)
                    .with_manifest_list(join_table_uri(table_url.as_ref(), &list_rel, &write_mode))
                    .with_summary(crate::spec::snapshots::Summary::new(
                        crate::spec::Operation::Append,
                    ))
                    .with_schema_id(schema_iceberg.schema_id())
                    .build()
                    .map_err(DataFusionError::Execution)?;

                table_meta.snapshots.push(snapshot.clone());
                table_meta.current_snapshot_id = Some(new_snapshot_id);
                let timestamp_ms = chrono::Utc::now().timestamp_millis();
                table_meta.snapshot_log.push(SnapshotLog {
                    timestamp_ms,
                    snapshot_id: new_snapshot_id,
                });
                if table_meta.last_sequence_number < 1 {
                    table_meta.last_sequence_number = 1;
                }
                table_meta.last_updated_ms = timestamp_ms;

                // Persist updated metadata IN PLACE at the existing metadata path (keeps SQL catalog in sync)
                let new_meta_bytes = table_meta
                    .to_json()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                // Derive relative metadata filename (e.g., metadata/00000-<uuid>.metadata.json)
                let rel_name = if let Some(idx) = latest_meta.rfind("/metadata/") {
                    let (_, suffix) = latest_meta.split_at(idx + 1);
                    suffix.to_string()
                } else if let Some(pos) = latest_meta.rfind("/metadata/") {
                    latest_meta[(pos + 1)..].to_string()
                } else if let Some(fname) = latest_meta.rsplit('/').next() {
                    format!("metadata/{}", fname)
                } else {
                    "metadata/00000.metadata.json".to_string()
                };
                let rel_path = object_store::path::Path::from(rel_name.as_str());
                store_ctx
                    .prefixed
                    .put(
                        &rel_path,
                        object_store::PutPayload::from(Bytes::from(new_meta_bytes)),
                    )
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                // Extract metadata filename (without "metadata/" prefix) for version-hint
                let metadata_filename = if let Some(fname) = rel_name.rsplit('/').next() {
                    fname.to_string()
                } else {
                    rel_name.clone()
                };

                // Write version-hint
                let hint_path = object_store::path::Path::from("metadata/version-hint.text");
                store_ctx
                    .prefixed
                    .put(
                        &hint_path,
                        object_store::PutPayload::from(Bytes::from(
                            metadata_filename.as_bytes().to_vec(),
                        )),
                    )
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

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
