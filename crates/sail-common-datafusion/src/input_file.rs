use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, SchemaRef};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, ScalarValue, exec_err, internal_err};
use datafusion::datasource::physical_plan::{
    FileOpenFuture, FileOpener, FileScanConfig, FileSource,
};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{
    ColumnarValue, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, Volatility,
};
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::projection::{ProjectionExprs, Projector};
use datafusion::physical_expr_adapter::rewrite::expr_references_scalar_udf;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayFormatType, PhysicalExpr, SendableRecordBatchStream, apply_expression_roots,
};
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::morsel::{Morsel, MorselPlan, MorselPlanner, Morselizer};
use futures::future::FutureExt;
use futures::stream::{BoxStream, StreamExt};
use object_store::ObjectStore;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct InputFileBlockStartFunc {
    signature: Signature,
}

impl Default for InputFileBlockStartFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl InputFileBlockStartFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for InputFileBlockStartFunc {
    fn name(&self) -> &str {
        "input_file_block_start"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types.is_empty() {
            return internal_err!("input_file_block_start expects no arguments");
        }
        Ok(DataType::Int64)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if !args.arg_fields.is_empty() {
            return internal_err!("input_file_block_start expects no arguments");
        }
        Ok(Arc::new(Field::new(self.name(), DataType::Int64, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !args.args.is_empty() {
            return internal_err!("input_file_block_start expects no arguments");
        }
        exec_err!("input_file_block_start() is source dependent and cannot be evaluated directly")
    }

    fn placement(&self, _args: &[ExpressionPlacement]) -> ExpressionPlacement {
        ExpressionPlacement::MoveTowardsLeafNodes
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct InputFileBlockLengthFunc {
    signature: Signature,
}

impl Default for InputFileBlockLengthFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl InputFileBlockLengthFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for InputFileBlockLengthFunc {
    fn name(&self) -> &str {
        "input_file_block_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types.is_empty() {
            return internal_err!("input_file_block_length expects no arguments");
        }
        Ok(DataType::Int64)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if !args.arg_fields.is_empty() {
            return internal_err!("input_file_block_length expects no arguments");
        }
        Ok(Arc::new(Field::new(self.name(), DataType::Int64, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !args.args.is_empty() {
            return internal_err!("input_file_block_length expects no arguments");
        }
        exec_err!("input_file_block_length() is source dependent and cannot be evaluated directly")
    }

    fn placement(&self, _args: &[ExpressionPlacement]) -> ExpressionPlacement {
        ExpressionPlacement::MoveTowardsLeafNodes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputFileMetadata {
    pub name: String,
    pub block_start: i64,
    pub block_length: i64,
}

impl InputFileMetadata {
    pub fn try_new(object_store_url: &ObjectStoreUrl, file: &PartitionedFile) -> Result<Self> {
        let name = format!(
            "{}{}",
            object_store_url.as_str(),
            file.object_meta.location.as_ref().trim_start_matches('/')
        );
        let (block_start, block_length) = match &file.range {
            Some(range) => {
                let block_length = range
                    .end
                    .checked_sub(range.start)
                    .filter(|length| *length >= 0)
                    .ok_or_else(|| {
                        datafusion::common::DataFusionError::Execution(format!(
                            "invalid input file range [{}, {}) for {}",
                            range.start, range.end, file.object_meta.location
                        ))
                    })?;
                (range.start, block_length)
            }
            None => (
                0,
                i64::try_from(file.object_meta.size).map_err(|_| {
                    datafusion::common::DataFusionError::Execution(format!(
                        "input file size exceeds i64::MAX for {}",
                        file.object_meta.location
                    ))
                })?,
            ),
        };
        Ok(Self {
            name,
            block_start,
            block_length,
        })
    }

    pub fn fallback() -> Self {
        Self {
            name: String::new(),
            block_start: -1,
            block_length: -1,
        }
    }
}

pub fn projection_references_input_file_metadata(projection: &ProjectionExprs) -> bool {
    projection
        .iter()
        .any(|projection| expression_references_input_file_metadata(&projection.expr))
}

pub fn expression_references_input_file_metadata(expression: &Arc<dyn PhysicalExpr>) -> bool {
    expr_references_scalar_udf::<datafusion::functions::core::input_file_name::InputFileNameFunc>(
        expression,
    ) || expr_references_scalar_udf::<InputFileBlockStartFunc>(expression)
        || expr_references_scalar_udf::<InputFileBlockLengthFunc>(expression)
}

pub fn rewrite_input_file_metadata_projection(
    projection: ProjectionExprs,
    metadata: &InputFileMetadata,
) -> Result<ProjectionExprs> {
    if !projection_references_input_file_metadata(&projection) {
        return Ok(projection);
    }

    let name = Arc::new(Literal::new(ScalarValue::Utf8(Some(metadata.name.clone()))))
        as Arc<dyn PhysicalExpr>;
    let block_start = Arc::new(Literal::new(ScalarValue::Int64(Some(metadata.block_start))))
        as Arc<dyn PhysicalExpr>;
    let block_length = Arc::new(Literal::new(ScalarValue::Int64(Some(
        metadata.block_length,
    )))) as Arc<dyn PhysicalExpr>;

    projection.try_map_exprs(|expr| {
        expr.transform_up(|node| {
            if ScalarFunctionExpr::try_downcast_func::<
                datafusion::functions::core::input_file_name::InputFileNameFunc,
            >(node.as_ref())
            .is_some()
            {
                Ok(Transformed::yes(Arc::clone(&name)))
            } else if ScalarFunctionExpr::try_downcast_func::<InputFileBlockStartFunc>(
                node.as_ref(),
            )
            .is_some()
            {
                Ok(Transformed::yes(Arc::clone(&block_start)))
            } else if ScalarFunctionExpr::try_downcast_func::<InputFileBlockLengthFunc>(
                node.as_ref(),
            )
            .is_some()
            {
                Ok(Transformed::yes(Arc::clone(&block_length)))
            } else {
                Ok(Transformed::no(node))
            }
        })
        .map(|result| result.data)
    })
}

pub fn project_input_file_metadata_stream(
    input: SendableRecordBatchStream,
    projection: &ProjectionExprs,
    metadata: &InputFileMetadata,
) -> Result<SendableRecordBatchStream> {
    let projection = rewrite_input_file_metadata_projection(projection.clone(), metadata)?;
    let projector = projection.make_projector(input.schema().as_ref())?;
    let output_schema = Arc::clone(projector.output_schema());
    let stream = input.map(move |batch| projector.project_batch(&batch?));
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        output_schema,
        stream,
    )))
}

#[derive(Clone)]
pub struct InputFileMetadataSource {
    file_source: Arc<dyn FileSource>,
    table_schema: TableSchema,
    projection: ProjectionExprs,
}

impl Debug for InputFileMetadataSource {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InputFileMetadataSource")
            .field("file_source", &self.file_source.file_type())
            .field("projection", &self.projection)
            .finish()
    }
}

impl InputFileMetadataSource {
    pub fn try_new(file_source: Arc<dyn FileSource>) -> Result<Self> {
        let input_schema = projected_source_schema(file_source.as_ref())?;
        let projection = ProjectionExprs::from_indices(
            &(0..input_schema.fields().len()).collect::<Vec<_>>(),
            input_schema.as_ref(),
        );
        Self::try_new_with_projection(file_source, projection)
    }

    pub fn try_new_with_projection(
        file_source: Arc<dyn FileSource>,
        projection: ProjectionExprs,
    ) -> Result<Self> {
        let input_schema = projected_source_schema(file_source.as_ref())?;
        projection.project_schema(input_schema.as_ref())?;
        Ok(Self {
            file_source,
            table_schema: TableSchema::from(input_schema),
            projection,
        })
    }

    pub fn file_source(&self) -> &Arc<dyn FileSource> {
        &self.file_source
    }

    pub fn metadata_projection(&self) -> &ProjectionExprs {
        &self.projection
    }

    fn input_schema(&self) -> &SchemaRef {
        self.table_schema.file_schema()
    }

    fn file_scan_config(&self, base_config: &FileScanConfig) -> FileScanConfig {
        let mut config = base_config.clone();
        config.file_source = Arc::clone(&self.file_source);
        config
    }
}

fn projected_source_schema(source: &dyn FileSource) -> Result<SchemaRef> {
    match source.projection() {
        Some(projection) => Ok(Arc::new(
            projection.project_schema(source.table_schema().table_schema())?,
        )),
        None => Ok(Arc::clone(source.table_schema().table_schema())),
    }
}

impl FileSource for InputFileMetadataSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let file_scan_config = self.file_scan_config(base_config);
        let file_opener =
            self.file_source
                .create_file_opener(object_store, &file_scan_config, partition)?;
        Ok(Arc::new(InputFileMetadataOpener {
            file_opener,
            object_store_url: base_config.object_store_url.clone(),
            input_schema: Arc::clone(self.input_schema()),
            projection: self.projection.clone(),
        }))
    }

    fn create_morselizer(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Box<dyn Morselizer>> {
        let file_scan_config = self.file_scan_config(base_config);
        let file_morselizer =
            self.file_source
                .create_morselizer(object_store, &file_scan_config, partition)?;
        Ok(Box::new(InputFileMetadataMorselizer {
            file_morselizer,
            object_store_url: base_config.object_store_url.clone(),
            input_schema: Arc::clone(self.input_schema()),
            projection: self.projection.clone(),
        }))
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            file_source: self.file_source.with_batch_size(batch_size),
            table_schema: self.table_schema.clone(),
            projection: self.projection.clone(),
        })
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.file_source.metrics()
    }

    fn file_type(&self) -> &str {
        self.file_source.file_type()
    }

    fn fmt_extra(&self, format: DisplayFormatType, formatter: &mut Formatter) -> std::fmt::Result {
        self.file_source.fmt_extra(format, formatter)
    }

    fn supports_repartitioning(&self) -> bool {
        self.file_source.supports_repartitioning()
    }

    fn reorder_files(&self, files: Vec<PartitionedFile>) -> Vec<PartitionedFile> {
        self.file_source.reorder_files(files)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        source.projection = self.projection.try_merge(projection)?;
        Ok(Some(Arc::new(source)))
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        if self.file_source.apply_expressions(f)? == TreeNodeRecursion::Stop {
            return Ok(TreeNodeRecursion::Stop);
        }
        apply_expression_roots(self.projection.iter().map(|projection| &projection.expr), f)
    }
}

struct InputFileMetadataOpener {
    file_opener: Arc<dyn FileOpener>,
    object_store_url: ObjectStoreUrl,
    input_schema: SchemaRef,
    projection: ProjectionExprs,
}

impl FileOpener for InputFileMetadataOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let metadata = InputFileMetadata::try_new(&self.object_store_url, &file)?;
        let projection =
            rewrite_input_file_metadata_projection(self.projection.clone(), &metadata)?;
        let projector = projection.make_projector(self.input_schema.as_ref())?;
        let file_open_future = self.file_opener.open(file)?;
        Ok(async move {
            let stream = file_open_future
                .await?
                .map(move |batch| projector.project_batch(&batch?));
            Ok(stream.boxed())
        }
        .boxed())
    }
}

struct InputFileMetadataMorselizer {
    file_morselizer: Box<dyn Morselizer>,
    object_store_url: ObjectStoreUrl,
    input_schema: SchemaRef,
    projection: ProjectionExprs,
}

impl Debug for InputFileMetadataMorselizer {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InputFileMetadataMorselizer")
            .field("file_morselizer", &self.file_morselizer)
            .finish_non_exhaustive()
    }
}

impl Morselizer for InputFileMetadataMorselizer {
    fn plan_file(&self, file: PartitionedFile) -> Result<Box<dyn MorselPlanner>> {
        let metadata = InputFileMetadata::try_new(&self.object_store_url, &file)?;
        let projection =
            rewrite_input_file_metadata_projection(self.projection.clone(), &metadata)?;
        let projector = projection.make_projector(self.input_schema.as_ref())?;
        Ok(Box::new(InputFileMetadataMorselPlanner {
            file_planner: self.file_morselizer.plan_file(file)?,
            projector,
        }))
    }
}

struct InputFileMetadataMorselPlanner {
    file_planner: Box<dyn MorselPlanner>,
    projector: Projector,
}

impl Debug for InputFileMetadataMorselPlanner {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InputFileMetadataMorselPlanner")
            .field("file_planner", &self.file_planner)
            .finish_non_exhaustive()
    }
}

impl MorselPlanner for InputFileMetadataMorselPlanner {
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
        let Self {
            file_planner,
            projector,
        } = *self;
        let Some(mut plan) = file_planner.plan()? else {
            return Ok(None);
        };

        let morsels = plan
            .take_morsels()
            .into_iter()
            .map(|file_morsel| {
                Box::new(InputFileMetadataMorsel {
                    file_morsel,
                    projector: projector.clone(),
                }) as Box<dyn Morsel>
            })
            .collect();
        let planners = plan
            .take_ready_planners()
            .into_iter()
            .map(|file_planner| {
                Box::new(Self {
                    file_planner,
                    projector: projector.clone(),
                }) as Box<dyn MorselPlanner>
            })
            .collect();
        plan = plan.with_morsels(morsels).with_planners(planners);
        if let Some(pending) = plan.take_pending_planner() {
            let pending_projector = projector.clone();
            plan.set_pending_planner(async move {
                Ok(Box::new(Self {
                    file_planner: pending.into_future().await?,
                    projector: pending_projector,
                }) as Box<dyn MorselPlanner>)
            });
        }
        Ok(Some(plan))
    }
}

struct InputFileMetadataMorsel {
    file_morsel: Box<dyn Morsel>,
    projector: Projector,
}

impl Debug for InputFileMetadataMorsel {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InputFileMetadataMorsel")
            .field("file_morsel", &self.file_morsel)
            .finish_non_exhaustive()
    }
}

impl Morsel for InputFileMetadataMorsel {
    fn into_stream(
        self: Box<Self>,
    ) -> BoxStream<'static, Result<datafusion::arrow::record_batch::RecordBatch>> {
        let Self {
            file_morsel,
            projector,
        } = *self;
        file_morsel
            .into_stream()
            .map(move |batch| projector.project_batch(&batch?))
            .boxed()
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion_datasource::{FileRange, PartitionedFile};

    use super::*;

    #[test]
    fn derives_spark_metadata_from_whole_file() {
        let file = PartitionedFile::new("warehouse/table/part-00000.parquet", 4096);
        let metadata =
            InputFileMetadata::try_new(&ObjectStoreUrl::local_filesystem(), &file).unwrap();

        assert_eq!(
            metadata,
            InputFileMetadata {
                name: "file:///warehouse/table/part-00000.parquet".to_string(),
                block_start: 0,
                block_length: 4096,
            }
        );
    }

    #[test]
    fn derives_spark_metadata_from_file_range() {
        let mut file = PartitionedFile::new("path/data.parquet", 4096);
        file.range = Some(FileRange {
            start: 1024,
            end: 3072,
        });
        let metadata =
            InputFileMetadata::try_new(&ObjectStoreUrl::parse("s3://bucket").unwrap(), &file)
                .unwrap();

        assert_eq!(
            metadata,
            InputFileMetadata {
                name: "s3://bucket/path/data.parquet".to_string(),
                block_start: 1024,
                block_length: 2048,
            }
        );
    }

    #[test]
    fn rejects_invalid_file_range() {
        let mut file = PartitionedFile::new("path/data.parquet", 4096);
        file.range = Some(FileRange {
            start: 3072,
            end: 1024,
        });

        assert!(InputFileMetadata::try_new(&ObjectStoreUrl::local_filesystem(), &file).is_err());
    }
}
