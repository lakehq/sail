use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::execution::context::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result as DataFusionResult};
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::path::Path as ObjectStorePath;
use serde_arrow::from_record_batch;
use url::Url;

use crate::spec::FileInfo;

/// Physical node that reads DuckLake data by consuming a stream of `FileInfo` record batches.
///
/// This is the DuckLake equivalent of Delta's `DeltaScanByAddsExec`: bridge metadata -> data scan.
#[derive(Debug, Clone)]
pub struct DuckLakeScanExec {
    input: Arc<dyn ExecutionPlan>,
    base_path: String,
    schema_name: String,
    table_name: String,
    table_schema: datafusion::arrow::datatypes::SchemaRef,
    output_schema: datafusion::arrow::datatypes::SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    pushdown_predicate: Option<Arc<dyn PhysicalExpr>>,
    cache: PlanProperties,
}

impl DuckLakeScanExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        base_path: String,
        schema_name: String,
        table_name: String,
        table_schema: datafusion::arrow::datatypes::SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        pushdown_predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Self> {
        let output_schema = if let Some(indices) = projection.as_ref() {
            Arc::new(
                table_schema
                    .as_ref()
                    .project(indices)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            )
        } else {
            table_schema.clone()
        };
        let cache = Self::compute_properties(output_schema.clone());
        Ok(Self {
            input,
            base_path,
            schema_name,
            table_name,
            table_schema,
            output_schema,
            projection,
            limit,
            pushdown_predicate,
            cache,
        })
    }

    fn compute_properties(schema: datafusion::arrow::datatypes::SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn base_path(&self) -> &str {
        &self.base_path
    }

    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn table_schema(&self) -> &datafusion::arrow::datatypes::SchemaRef {
        &self.table_schema
    }

    pub fn projection(&self) -> Option<&[usize]> {
        self.projection.as_deref()
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn pushdown_predicate(&self) -> &Option<Arc<dyn PhysicalExpr>> {
        &self.pushdown_predicate
    }

    fn build_object_store_context(&self) -> DataFusionResult<(ObjectStoreUrl, ObjectStorePath)> {
        let base_url =
            Url::parse(&self.base_path).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut object_store_base = base_url.clone();
        object_store_base.set_query(None);
        object_store_base.set_fragment(None);
        object_store_base.set_path("/");
        let object_store_url = ObjectStoreUrl::parse(object_store_base.as_str())?;

        let mut table_prefix = ObjectStorePath::parse(base_url.path())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        table_prefix = table_prefix
            .child(self.schema_name.as_str())
            .child(self.table_name.as_str());
        Ok((object_store_url, table_prefix))
    }

    fn append_relative_path(
        base_prefix: &ObjectStorePath,
        relative: &ObjectStorePath,
    ) -> ObjectStorePath {
        relative
            .parts()
            .fold(base_prefix.clone(), |acc, part| acc.child(part))
    }

    fn resolve_file_path(
        base_prefix: &ObjectStorePath,
        file: &FileInfo,
    ) -> DataFusionResult<ObjectStorePath> {
        if file.path_is_relative {
            let relative = ObjectStorePath::parse(&file.path)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok(Self::append_relative_path(base_prefix, &relative))
        } else if let Ok(path_url) = Url::parse(&file.path) {
            ObjectStorePath::from_url_path(path_url.path())
                .map_err(|e| DataFusionError::External(Box::new(e)))
        } else {
            ObjectStorePath::parse(&file.path).map_err(|e| DataFusionError::External(Box::new(e)))
        }
    }

    fn aggregate_statistics(
        schema: &datafusion::arrow::datatypes::Schema,
        files: &[FileInfo],
    ) -> Statistics {
        let column_statistics = (0..schema.fields().len())
            .map(|_| ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Absent,
                min_value: Precision::Absent,
                distinct_count: Precision::Absent,
                sum_value: Precision::Absent,
            })
            .collect();
        let total_rows = files
            .iter()
            .fold(0u64, |acc, file| acc.saturating_add(file.record_count));
        let total_bytes = files
            .iter()
            .fold(0u64, |acc, file| acc.saturating_add(file.file_size_bytes));
        let num_rows = total_rows.min(usize::MAX as u64) as usize;
        let total_byte_size = total_bytes.min(usize::MAX as u64) as usize;
        Statistics {
            num_rows: Precision::Exact(num_rows),
            total_byte_size: Precision::Exact(total_byte_size),
            column_statistics,
        }
    }

    async fn create_scan_stream(
        &self,
        context: Arc<TaskContext>,
        files: Vec<FileInfo>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let (object_store_url, table_prefix) = self.build_object_store_context()?;

        let mut partitioned_files = Vec::with_capacity(files.len());
        for file in &files {
            let object_path = Self::resolve_file_path(&table_prefix, file)?;
            partitioned_files.push(PartitionedFile::new(object_path, file.file_size_bytes));
        }

        let file_groups = if partitioned_files.is_empty() {
            vec![FileGroup::from(vec![])]
        } else {
            vec![FileGroup::from(partitioned_files)]
        };

        let table_stats = Self::aggregate_statistics(self.table_schema.as_ref(), &files);

        let parquet_options = TableParquetOptions {
            global: context.session_config().options().execution.parquet.clone(),
            ..Default::default()
        };
        let mut parquet_source = ParquetSource::new(parquet_options);
        if let Some(pred) = self.pushdown_predicate.clone() {
            parquet_source = parquet_source.with_predicate(pred);
        }
        let parquet_source = Arc::new(parquet_source);

        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, self.table_schema.clone(), parquet_source)
                .with_file_groups(file_groups)
                .with_statistics(table_stats)
                .with_projection_indices(self.projection.clone())
                .with_limit(self.limit)
                .build();

        let scan_exec = DataSourceExec::from_data_source(file_scan_config);
        scan_exec.execute(0, context)
    }
}

#[async_trait]
impl ExecutionPlan for DuckLakeScanExec {
    fn name(&self) -> &'static str {
        "DuckLakeScanExec"
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
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("DuckLakeScanExec requires exactly one child");
        }
        Ok(Arc::new(Self::try_new(
            children[0].clone(),
            self.base_path.clone(),
            self.schema_name.clone(),
            self.table_name.clone(),
            self.table_schema.clone(),
            self.projection.clone(),
            self.limit,
            self.pushdown_predicate.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("DuckLakeScanExec only supports a single partition");
        }

        let mut input_stream = self.input.execute(0, context.clone())?;
        let schema = self.schema();
        let schema_clone = schema.clone();
        let self_clone = self.clone();

        let stream_fut = async move {
            let mut files: Vec<FileInfo> = vec![];
            while let Some(batch_result) = input_stream.next().await {
                let batch = batch_result?;
                if batch.num_rows() == 0 {
                    continue;
                }
                let mut batch_files: Vec<FileInfo> = from_record_batch(&batch)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                files.append(&mut batch_files);
            }

            if files.is_empty() {
                let empty_batch = RecordBatch::new_empty(schema_clone.clone());
                let stream = stream::once(async { Ok(empty_batch) });
                let adapter = RecordBatchStreamAdapter::new(schema_clone, stream);
                return Ok(Box::pin(adapter) as SendableRecordBatchStream);
            }

            self_clone.create_scan_stream(context, files).await
        };

        let stream = futures::stream::once(stream_fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.output_schema.clone()
    }
}

impl DisplayAs for DuckLakeScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "DuckLakeScanExec(base_path={}, table={}.{})",
                self.base_path, self.schema_name, self.table_name
            ),
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: ducklake")?;
                write!(
                    f,
                    "base_path={}, table={}.{}",
                    self.base_path, self.schema_name, self.table_name
                )
            }
        }
    }
}
