use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::{PartitionedFile, TableSchema};
use futures::StreamExt;
use object_store::ObjectStore;

use crate::formats::binary::reader::{BinaryFileMetadata, BinaryFileReader};

#[derive(Debug, Clone)]
pub struct BinarySource {
    table_schema: TableSchema,
    path_glob_filter: Option<String>,
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    projection: SplitProjection,
}

impl BinarySource {
    pub fn new(table_schema: impl Into<TableSchema>, path_glob_filter: Option<String>) -> Self {
        let table_schema = table_schema.into();
        Self {
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            path_glob_filter,
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn with_path_glob_filter(mut self, path_glob_filter: Option<String>) -> Self {
        self.path_glob_filter = path_glob_filter;
        self
    }

    pub fn path_glob_filter(&self) -> Option<&String> {
        self.path_glob_filter.as_ref()
    }
}

impl From<BinarySource> for Arc<dyn FileSource> {
    fn from(source: BinarySource) -> Self {
        Arc::new(source)
    }
}

impl FileSource for BinarySource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let opener = Arc::new(BinaryOpener::new(Arc::new(self.clone()), object_store))
            as Arc<dyn FileOpener>;

        let opener = ProjectionOpener::try_new(
            self.projection.clone(),
            opener,
            self.table_schema.file_schema(),
        )?;

        Ok(opener)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let new_projection = self.projection.source.try_merge(projection)?;
        source.projection = SplitProjection::new(self.table_schema.file_schema(), &new_projection);
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn file_type(&self) -> &str {
        "binary"
    }
}

pub struct BinaryOpener {
    config: Arc<BinarySource>,
    object_store: Arc<dyn ObjectStore>,
}

impl BinaryOpener {
    pub fn new(config: Arc<BinarySource>, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            config,
            object_store,
        }
    }
}

impl FileOpener for BinaryOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        if let Some(ref glob_pattern) = self.config.path_glob_filter {
            let file_name = file.object_meta.location.filename().unwrap_or("");
            let pattern = glob::Pattern::new(glob_pattern)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            if !pattern.matches(file_name) {
                return Ok(Box::pin(
                    async move { Ok(futures::stream::empty().boxed()) },
                ));
            }
        }

        let store = Arc::clone(&self.object_store);
        let location = file.object_meta.location.clone();
        let last_modified = file.object_meta.last_modified;
        let size = file.object_meta.size as i64;
        let schema = Arc::new(
            self.config
                .table_schema
                .file_schema()
                .project(&self.config.projection.file_indices)?,
        );

        Ok(Box::pin(async move {
            if schema.fields().is_empty() {
                let empty_batch = RecordBatch::try_new_with_options(
                    schema,
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(1)),
                )
                .map_err(DataFusionError::from)?;
                return Ok(futures::stream::once(async move { Ok(empty_batch) }).boxed());
            }

            let get_result = store.get(&location).await?;
            let content = get_result.bytes().await?;
            let modification_time = last_modified.timestamp_micros();
            let metadata = BinaryFileMetadata {
                path: location.to_string(),
                modification_time,
                length: size,
            };
            // `content.into()` moves `Bytes` into `Vec<u8>` without copy.
            // `content.to_vec()` would copy the data since this is a method on the slice.
            let reader = BinaryFileReader::new(metadata, content.into(), schema.clone());

            let stream = futures::stream::once(async move {
                let batch = reader.read().map_err(DataFusionError::from)?;
                Ok(batch)
            })
            .boxed();

            Ok(stream)
        }))
    }
}
