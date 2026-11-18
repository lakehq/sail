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

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow_avro::reader::ReaderBuilder;
use datafusion::common::{DataFusionError, Result, plan_err};
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
use object_store::{ObjectStore, ObjectStoreExt};
use sail_common_datafusion::schema_evolution::{
    SchemaEvolutionPhysicalExprAdapterFactoryWithMatching, StructFieldMatching,
};

use crate::spec::DataFileFormat;

/// Whole-file readers for externally imported Avro and ORC data.
#[derive(Clone)]
pub struct IcebergImportedFileSource {
    schema: TableSchema,
    format: DataFileFormat,
    batch_size: usize,
    projection: SplitProjection,
    metrics: ExecutionPlanMetricsSet,
}

impl IcebergImportedFileSource {
    pub fn new(schema: TableSchema, format: DataFileFormat) -> Self {
        Self {
            projection: SplitProjection::unprojected(&schema),
            schema,
            format,
            batch_size: 8192,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn format(&self) -> DataFileFormat {
        self.format
    }

    pub(crate) fn with_schema(&self, schema: TableSchema) -> Result<Arc<dyn FileSource>> {
        let source = Self::new(schema, self.format);
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
            format: self.format,
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
        match self.format {
            DataFileFormat::Orc => "orc",
            _ => "avro",
        }
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
    format: DataFileFormat,
    target: SchemaRef,
    batch_size: usize,
}

impl FileOpener for ImportedOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let store = self.store.clone();
        let format = self.format;
        let target = self.target.clone();
        let batch_size = self.batch_size;
        Ok(Box::pin(async move {
            let bytes = store.get(&file.object_meta.location).await?.bytes().await?;
            let (schema, reader): (
                SchemaRef,
                Box<dyn Iterator<Item = Result<RecordBatch>> + Send>,
            ) = match format {
                DataFileFormat::Orc => {
                    let builder = orc_rust::ArrowReaderBuilder::try_new(bytes)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?
                        .with_batch_size(batch_size);
                    (
                        builder.schema(),
                        Box::new(builder.build().map(|batch| {
                            batch.map_err(|e| DataFusionError::External(Box::new(e)))
                        })),
                    )
                }
                DataFileFormat::Avro => {
                    let reader = ReaderBuilder::new()
                        .with_batch_size(batch_size)
                        .build(std::io::Cursor::new(bytes))?;
                    (
                        reader.schema(),
                        Box::new(reader.map(|batch| batch.map_err(Into::into))),
                    )
                }
                _ => return plan_err!("Expected an imported Avro or ORC file"),
            };
            let adapter = BatchAdapterFactory::new(target)
                .with_adapter_factory(Arc::new(
                    SchemaEvolutionPhysicalExprAdapterFactoryWithMatching::new(
                        StructFieldMatching::FieldId,
                    ),
                ))
                .make_adapter(&schema)?;
            Ok(futures::stream::iter(reader)
                .map(move |batch| batch.and_then(|batch| adapter.adapt_batch(&batch)))
                .boxed())
        }))
    }
}
