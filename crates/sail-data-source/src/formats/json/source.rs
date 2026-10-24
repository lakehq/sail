use std::io::Read;
use std::sync::Arc;

use bytes::Buf;
use datafusion::arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions, StringArray};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::json::ReaderBuilder;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_plan::apply_expression_roots;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::{PartitionedFile, TableSchema};
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::Map;

use super::records::{JsonRecord, JsonRecordOptions, ParseMode};

/// A JSON file source that finds and parses the records of each file as Spark does,
/// for the reader options that the newline-delimited JSON source of DataFusion does not
/// support: `lineSep`, `multiLine`, `encoding`, `primitivesAsString`, `mode`, and the
/// column for corrupt records. Each file is read as a whole.
#[derive(Debug, Clone)]
pub struct SparkJsonSource {
    record_options: JsonRecordOptions,
    primitives_as_string: bool,
    batch_size: Option<usize>,
    table_schema: TableSchema,
    projection: SplitProjection,
    metrics: ExecutionPlanMetricsSet,
}

impl SparkJsonSource {
    pub(super) fn new(
        table_schema: impl Into<TableSchema>,
        record_options: JsonRecordOptions,
        primitives_as_string: bool,
    ) -> Self {
        let table_schema = table_schema.into();
        Self {
            record_options,
            primitives_as_string,
            batch_size: None,
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Decodes the records into batches of the file schema.
    fn decode(&self, records: Vec<JsonRecord>) -> Result<Vec<RecordBatch>> {
        let schema = self.table_schema.file_schema();
        let batch_size = self.batch_size.unwrap_or(8192).max(1);
        // Like Spark, the corrupt record column is only filled when it is a string field.
        let corrupt_index = schema
            .index_of(&self.record_options.column_name_of_corrupt_record)
            .ok()
            .filter(|i| schema.field(*i).data_type() == &DataType::Utf8);
        let decode_schema = match corrupt_index {
            Some(index) => {
                let mut fields = schema.fields().to_vec();
                fields.remove(index);
                Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
            }
            None => Arc::clone(schema),
        };
        let mut decoder = ReaderBuilder::new(Arc::clone(&decode_schema))
            .with_batch_size(batch_size)
            .with_coerce_primitive(self.primitives_as_string)
            .build_decoder()?;

        let mut batches = vec![];
        let mut corrupt: Vec<Option<String>> = vec![];
        let mut flush = |decoder: &mut datafusion::arrow::json::reader::Decoder,
                         corrupt: &mut Vec<Option<String>>|
         -> Result<()> {
            if corrupt.is_empty() {
                return Ok(());
            }
            let rows = corrupt.len();
            let mut columns = match decoder.flush()? {
                Some(batch) => batch.columns().to_vec(),
                None => vec![],
            };
            if let Some(index) = corrupt_index {
                let values = StringArray::from(std::mem::take(corrupt));
                columns.insert(index, Arc::new(values) as ArrayRef);
            } else {
                corrupt.clear();
            }
            batches.push(RecordBatch::try_new_with_options(
                Arc::clone(schema),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )?);
            Ok(())
        };
        let empty = Map::new();
        for record in records {
            match record {
                JsonRecord::Valid(object) => {
                    decoder.serialize(&[object])?;
                    corrupt.push(None);
                }
                JsonRecord::Corrupt(text) => match self.record_options.mode {
                    ParseMode::FailFast => {
                        return Err(self.record_options.malformed_record_error(&text));
                    }
                    ParseMode::DropMalformed => continue,
                    ParseMode::Permissive => {
                        // The other fields of a corrupt record are null.
                        decoder.serialize(std::slice::from_ref(&empty))?;
                        corrupt.push(Some(text));
                    }
                },
            }
            if corrupt.len() >= batch_size {
                flush(&mut decoder, &mut corrupt)?;
            }
        }
        flush(&mut decoder, &mut corrupt)?;
        Ok(batches)
    }
}

impl From<SparkJsonSource> for Arc<dyn FileSource> {
    fn from(source: SparkJsonSource) -> Self {
        Arc::new(source)
    }
}

impl FileSource for SparkJsonSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let opener = Arc::new(SparkJsonOpener {
            config: Arc::new(self.clone()),
            file_compression_type: base_config.file_compression_type,
            object_store,
            partition,
        }) as Arc<dyn FileOpener>;

        ProjectionOpener::try_new(
            self.projection.clone(),
            opener,
            self.table_schema.file_schema(),
        )
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.batch_size = Some(batch_size);
        Arc::new(source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let projection = self.projection.source.try_merge(projection)?;
        source.projection = SplitProjection::new(self.table_schema.file_schema(), &projection);
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(self.projection.source.iter(), f)
    }

    fn file_type(&self) -> &str {
        "json"
    }

    fn supports_repartitioning(&self) -> bool {
        // A record may span file ranges, so each file is read as a whole.
        false
    }
}

struct SparkJsonOpener {
    config: Arc<SparkJsonSource>,
    file_compression_type: FileCompressionType,
    object_store: Arc<dyn ObjectStore>,
    partition: usize,
}

impl FileOpener for SparkJsonOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let config = Arc::clone(&self.config);
        let file_compression_type = self.file_compression_type;
        let object_store = Arc::clone(&self.object_store);
        let baseline_metrics = BaselineMetrics::new(&self.config.metrics, self.partition);
        Ok(Box::pin(async move {
            let location = partitioned_file.object_meta.location;
            let data = object_store.get(&location).await?.bytes().await?;
            let timer = baseline_metrics.elapsed_compute().timer();
            let mut content = vec![];
            file_compression_type
                .convert_read(data.reader())?
                .read_to_end(&mut content)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let records = config.record_options.read_records(&content)?;
            let batches = config.decode(records)?;
            timer.done();
            Ok(futures::stream::iter(batches.into_iter().map(Ok)).boxed())
        }))
    }
}

/// Returns whether the schema has the column for corrupt records.
pub(super) fn has_corrupt_record_column(schema: &SchemaRef, options: &JsonRecordOptions) -> bool {
    schema
        .field_with_name(&options.column_name_of_corrupt_record)
        .is_ok_and(|field| field.data_type() == &DataType::Utf8)
}
