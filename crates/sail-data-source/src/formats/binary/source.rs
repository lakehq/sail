use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_common::{internal_err, DataFusionError, Result, Statistics};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
use datafusion_datasource::{PartitionedFile, TableSchema};
use futures::StreamExt;
use object_store::ObjectStore;

use crate::formats::binary::reader::{BinaryFileMetadata, BinaryFileReader};

#[derive(Debug, Clone, Default)]
pub struct BinarySource {
    path_glob_filter: Option<String>,
    batch_size: Option<usize>,
    file_schema: Option<SchemaRef>,
    file_projection: Option<Vec<usize>>,
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

impl BinarySource {
    pub fn new(path_glob_filter: Option<String>) -> Self {
        Self {
            path_glob_filter,
            ..Self::default()
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
    ) -> Arc<dyn FileOpener> {
        Arc::new(BinaryOpener::new(Arc::new(self.clone()), object_store))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn with_schema(&self, schema: TableSchema) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.file_schema = Some(schema.file_schema().clone());
        Arc::new(conf)
    }

    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.file_projection = config.file_column_projection_indices();
        Arc::new(conf)
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projected_statistics = Some(statistics);
        Arc::new(conf)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> Result<Statistics> {
        let statistics = &self.projected_statistics;
        statistics.clone().ok_or_else(|| {
            DataFusionError::Internal(
                "projected_statistics must be set before calling statistics()".to_string(),
            )
        })
    }

    fn file_type(&self) -> &str {
        "binary"
    }

    fn with_schema_adapter_factory(
        &self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        Ok(Arc::new(Self {
            schema_adapter_factory: Some(schema_adapter_factory),
            ..self.clone()
        }))
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
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
        let projection = self.config.file_projection.clone();
        let schema = if let Some(schema) = &self.config.file_schema {
            Arc::clone(schema)
        } else {
            return internal_err!("schema must be set before open the file");
        };

        Ok(Box::pin(async move {
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
                let batch = reader.read()?;
                match &projection {
                    Some(proj) => {
                        if !proj.is_empty() {
                            // Project the batch to only include requested columns
                            let projected_columns: Vec<_> =
                                proj.iter().map(|&i| batch.column(i).clone()).collect();
                            let projected_fields: Vec<_> =
                                proj.iter().map(|&i| schema.field(i).clone()).collect();
                            let projected_schema = Arc::new(Schema::new(projected_fields));
                            RecordBatch::try_new(projected_schema, projected_columns)
                                .map_err(DataFusionError::from)
                        } else {
                            // Empty projection - return empty batch with row count preserved
                            let empty_schema = Arc::new(Schema::empty());
                            RecordBatch::try_new_with_options(
                                empty_schema,
                                vec![],
                                &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                            )
                            .map_err(DataFusionError::from)
                        }
                    }
                    None => Ok(batch),
                }
            })
            .boxed();

            Ok(stream)
        }))
    }
}
