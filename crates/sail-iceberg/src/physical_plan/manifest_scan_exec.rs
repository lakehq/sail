use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result};
use futures::stream::{self, TryStreamExt};
use url::Url;

use crate::io::{
    load_manifest as io_load_manifest, load_manifest_list as io_load_manifest_list, StoreContext,
};
use crate::spec::{ManifestContentType, ManifestFile, ManifestStatus, Snapshot};

/// Column names for the metadata batch produced by `IcebergManifestScanExec`.
pub const COL_FILE_PATH: &str = "file_path";
pub const COL_FILE_FORMAT: &str = "file_format";
pub const COL_RECORD_COUNT: &str = "record_count";
pub const COL_FILE_SIZE_IN_BYTES: &str = "file_size_in_bytes";
pub const COL_PARTITION_SPEC_ID: &str = "partition_spec_id";
pub const COL_CONTENT_TYPE: &str = "content_type";

/// Returns the Arrow schema used by the manifest scan metadata batch.
pub fn manifest_scan_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COL_FILE_PATH, DataType::Utf8, false),
        Field::new(COL_FILE_FORMAT, DataType::Utf8, false),
        Field::new(COL_RECORD_COUNT, DataType::UInt64, false),
        Field::new(COL_FILE_SIZE_IN_BYTES, DataType::UInt64, false),
        Field::new(COL_PARTITION_SPEC_ID, DataType::Int32, false),
        Field::new(COL_CONTENT_TYPE, DataType::Utf8, false),
    ]))
}

/// Physical execution plan that scans Iceberg manifests from the snapshot's manifest list
/// and produces data file metadata as an Arrow `RecordBatch`.
#[derive(Debug)]
pub struct IcebergManifestScanExec {
    table_url: String,
    snapshot: Snapshot,
    output_schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl IcebergManifestScanExec {
    pub fn new(table_url: String, snapshot: Snapshot) -> Self {
        let output_schema = manifest_scan_schema();
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            table_url,
            snapshot,
            output_schema,
            cache,
        }
    }

    pub fn table_url(&self) -> &str {
        &self.table_url
    }

    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }
}

impl DisplayAs for IcebergManifestScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "IcebergManifestScanExec: table_url={}, snapshot_id={}",
                    self.table_url,
                    self.snapshot.snapshot_id()
                )
            }
        }
    }
}

#[async_trait]
impl ExecutionPlan for IcebergManifestScanExec {
    fn name(&self) -> &str {
        "IcebergManifestScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Plan(
                "IcebergManifestScanExec does not accept children".to_string(),
            ))
        }
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![]
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let table_url = self.table_url.clone();
        let snapshot = self.snapshot.clone();
        let schema = self.output_schema.clone();

        // Phase 1: Load the manifest list (a single lightweight metadata file).
        let init = async move {
            let table_url_parsed =
                Url::parse(&table_url).map_err(|e| DataFusionError::External(Box::new(e)))?;
            let object_store = context
                .runtime_env()
                .object_store_registry
                .get_store(&table_url_parsed)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let store_ctx = StoreContext::new(object_store, &table_url_parsed)?;

            let manifest_list_path = snapshot.manifest_list();
            log::trace!(
                "IcebergManifestScanExec: loading manifest list from {}",
                manifest_list_path
            );
            let manifest_list = io_load_manifest_list(&store_ctx, manifest_list_path).await?;

            // Collect data manifests only (skip delete manifests).
            let data_manifests: Vec<ManifestFile> = manifest_list
                .entries()
                .iter()
                .filter(|m| m.content == ManifestContentType::Data)
                .cloned()
                .collect();

            Ok::<_, DataFusionError>((store_ctx, data_manifests, schema))
        };

        // Phase 2: Stream one RecordBatch per manifest file to cap peak memory usage
        // and improve time-to-first-row for large tables.
        let stream = stream::once(init)
            .map_ok(|(store_ctx, manifests, schema)| {
                stream::try_unfold(
                    (store_ctx, manifests, 0usize, schema),
                    |(store_ctx, manifests, mut idx, schema)| async move {
                        while idx < manifests.len() {
                            let manifest_path = manifests[idx].manifest_path.clone();
                            let partition_spec_id = manifests[idx].partition_spec_id;
                            idx += 1;

                            let manifest =
                                io_load_manifest(&store_ctx, manifest_path.as_str()).await?;

                            let mut file_paths = Vec::new();
                            let mut file_formats = Vec::new();
                            let mut record_counts = Vec::new();
                            let mut file_sizes = Vec::new();
                            let mut partition_spec_ids = Vec::new();
                            let mut content_types = Vec::new();

                            for entry_ref in manifest.entries() {
                                let entry = entry_ref.as_ref();
                                if !matches!(
                                    entry.status,
                                    ManifestStatus::Added | ManifestStatus::Existing
                                ) {
                                    continue;
                                }

                                let df = &entry.data_file;
                                file_paths.push(df.file_path().to_string());
                                file_formats.push(df.file_format().as_action_str().to_string());
                                record_counts.push(df.record_count());
                                file_sizes.push(df.file_size_in_bytes());
                                partition_spec_ids.push(partition_spec_id);
                                content_types.push(df.content_type().as_action_str().to_string());
                            }

                            if file_paths.is_empty() {
                                continue;
                            }

                            let batch = RecordBatch::try_new(
                                schema.clone(),
                                vec![
                                    Arc::new(StringArray::from(file_paths)),
                                    Arc::new(StringArray::from(file_formats)),
                                    Arc::new(UInt64Array::from(record_counts)),
                                    Arc::new(UInt64Array::from(file_sizes)),
                                    Arc::new(Int32Array::from(partition_spec_ids)),
                                    Arc::new(StringArray::from(content_types)),
                                ],
                            )?;
                            return Ok(Some((batch, (store_ctx, manifests, idx, schema))));
                        }
                        Ok(None)
                    },
                )
            })
            .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_scan_schema_has_expected_columns() {
        let schema = manifest_scan_schema();
        assert_eq!(schema.fields().len(), 6);
        assert!(schema.field_with_name(COL_FILE_PATH).is_ok());
        assert!(schema.field_with_name(COL_FILE_FORMAT).is_ok());
        assert!(schema.field_with_name(COL_RECORD_COUNT).is_ok());
        assert!(schema.field_with_name(COL_FILE_SIZE_IN_BYTES).is_ok());
        assert!(schema.field_with_name(COL_PARTITION_SPEC_ID).is_ok());
        assert!(schema.field_with_name(COL_CONTENT_TYPE).is_ok());
    }
}
