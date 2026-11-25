use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow_avro::reader::ReaderBuilder;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::physical_expr_adapter::BatchAdapterFactory;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::projection::ProjectionExprs;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::buffered::BufReader;
use sail_common_datafusion::schema_evolution::{
    SchemaEvolutionPhysicalExprAdapterFactoryWithMatching, StructFieldMatching,
};
use tokio_util::io::SyncIoBridge;

/// Streaming reader for externally imported Avro data.
#[derive(Clone)]
pub struct IcebergImportedFileSource {
    schema: TableSchema,
    batch_size: usize,
    projection: SplitProjection,
    metrics: ExecutionPlanMetricsSet,
}

impl IcebergImportedFileSource {
    pub fn new(schema: TableSchema) -> Self {
        Self {
            projection: SplitProjection::unprojected(&schema),
            schema,
            batch_size: 8192,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub(crate) fn with_schema(&self, schema: TableSchema) -> Result<Arc<dyn FileSource>> {
        let source = Self::new(schema);
        source
            .try_pushdown_projection(&self.projection.source)?
            .ok_or_else(|| DataFusionError::Internal("Cannot project imported file".into()))
    }
}

impl FileSource for IcebergImportedFileSource {
    fn create_file_opener(
        &self,
        store: Arc<dyn ObjectStore>,
        _config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let target = Arc::new(
            self.schema
                .file_schema()
                .project(&self.projection.file_indices)?,
        );
        let opener: Arc<dyn FileOpener> = Arc::new(ImportedOpener {
            store,
            target,
            batch_size: self.batch_size,
        });
        ProjectionOpener::try_new(self.projection.clone(), opener, self.schema.file_schema())
    }
    fn table_schema(&self) -> &TableSchema {
        &self.schema
    }
    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.batch_size = batch_size;
        Arc::new(source)
    }
    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let merged = self.projection.source.try_merge(projection)?;
        source.projection = SplitProjection::new(self.schema.file_schema(), &merged);
        Ok(Some(Arc::new(source)))
    }
    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }
    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }
    fn file_type(&self) -> &str {
        "avro"
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> Result<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion::common::tree_node::TreeNodeRecursion> {
        datafusion::physical_plan::apply_expression_roots(self.projection.source.iter(), f)
    }
    fn supports_repartitioning(&self) -> bool {
        false
    }
}

struct ImportedOpener {
    store: Arc<dyn ObjectStore>,
    target: SchemaRef,
    batch_size: usize,
}

impl FileOpener for ImportedOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let store = self.store.clone();
        let target = self.target.clone();
        let batch_size = self.batch_size;
        Ok(Box::pin(async move {
            let input = BufReader::new(store, &file.object_meta);
            let reader = tokio::task::spawn_blocking(move || {
                ReaderBuilder::new()
                    .with_batch_size(batch_size)
                    .build(SyncIoBridge::new(input))
            })
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))??;
            let schema = reader.schema();
            // Poll one batch at a time so dropping the scan stops further reads.
            let batches = futures::stream::try_unfold(reader, |mut reader| async move {
                tokio::task::spawn_blocking(move || {
                    let batch = reader.next().transpose()?;
                    Ok::<_, DataFusionError>(batch.map(|batch| (batch, reader)))
                })
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
            });
            let adapter = BatchAdapterFactory::new(target)
                .with_adapter_factory(Arc::new(
                    SchemaEvolutionPhysicalExprAdapterFactoryWithMatching::new(
                        StructFieldMatching::FieldId,
                    ),
                ))
                .make_adapter(&schema)?;
            Ok(batches
                .map(move |batch| batch.and_then(|batch| adapter.adapt_batch(&batch)))
                .boxed())
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow_avro::writer::WriterBuilder;
    use datafusion::arrow_avro::writer::format::AvroOcfFormat;
    use datafusion_datasource::file_stream::FileOpener;
    use futures::TryStreamExt;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use sail_common_datafusion::schema_evolution::FIELD_ALIASES_METADATA_KEY;

    use super::ImportedOpener;
    use crate::io::test_store::TestStore;

    #[tokio::test]
    async fn first_batch_does_not_read_entire_imported_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let values: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(16 * 1024); 16]));
        let batch = RecordBatch::try_from_iter([("value", values)])?;
        let target_field = batch.schema().field(0).clone().with_metadata(
            [(
                FIELD_ALIASES_METADATA_KEY.to_string(),
                r#"["value"]"#.to_string(),
            )]
            .into(),
        );
        let target = Arc::new(Schema::new(vec![target_field]));
        let mut avro = WriterBuilder::new(batch.schema().as_ref().clone())
            .build::<_, AvroOcfFormat>(Vec::new())?;
        for _ in 0..32 {
            avro.write(&batch)?;
        }
        avro.finish()?;
        let bytes = avro.into_inner();
        let size = bytes.len();
        let inner = Arc::new(InMemory::new());
        let path = "data".into();
        inner.put(&path, bytes.into()).await?;
        let file = inner.head(&path).await?.into();
        let store = Arc::new(TestStore::new(inner));
        let opener = ImportedOpener {
            store: store.clone(),
            target,
            batch_size: 1,
        };
        let mut stream = opener.open(file)?.await?;
        let first = stream.try_next().await?.ok_or("missing first batch")?;
        assert_eq!(first.columns(), batch.slice(0, 1).columns());
        drop(stream);
        assert!(
            store.bytes_read.load(Ordering::Relaxed) < size,
            "read the entire Avro file"
        );
        Ok(())
    }
}
