use std::collections::HashSet;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, StructArray, make_array};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::physical_plan::parquet::{ParquetAccessPlan, ParquetRowSelection};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfig, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
    ExecutionPlanProperties, Partitioning, PlanProperties, ReplaceChildrenOptions,
    SendableRecordBatchStream,
};
use datafusion_common::stats::Precision;
use datafusion_common::{DataFusionError, Result};
use futures::future::BoxFuture;
use futures::stream::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection, RowSelector};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::schema::types::SchemaDescriptor;
use roaring::RoaringTreemap;
use url::Url;

use crate::io::StoreContext;
use crate::physical_plan::merge_metadata_exec::IcebergMergeMetadataExec;
use crate::spec::Schema as IcebergSchema;
use crate::spec::delete_index::{DeleteFileRef, PositionDeleteFile};

/// Column name used in Iceberg position-delete files for the target data-file path.
const POS_DELETE_FILE_PATH_COL: &str = "file_path";
/// Column name used in Iceberg position-delete files for the row position.
const POS_DELETE_POS_COL: &str = "pos";
const MAX_SELECTION_RUNS: usize = 4096;

/// Bound selector memory for fragmented deletes; the bitmap filter handles the rest.
fn live_row_selection(positions: &RoaringTreemap, rows: usize) -> Option<RowSelection> {
    if positions
        .max()
        .is_some_and(|position| position >= rows as u64)
    {
        return None;
    }
    if positions.len() == rows as u64 {
        return Some(RowSelection::from(vec![RowSelector::skip(rows)]));
    }
    let mut selectors = Vec::new();
    let mut offset = 0;
    let mut deleted = positions.iter().peekable();
    while let Some(start) = deleted.next() {
        let start = start as usize;
        if start > offset {
            selectors.push(RowSelector::select(start - offset));
        }
        let mut end = start + 1;
        while deleted.peek().is_some_and(|next| *next == end as u64) {
            deleted.next();
            end += 1;
        }
        selectors.push(RowSelector::skip(end - start));
        if selectors.len() > MAX_SELECTION_RUNS {
            return None;
        }
        offset = end;
    }
    if offset < rows {
        selectors.push(RowSelector::select(rows - offset));
    }
    Some(RowSelection::from(selectors))
}

fn select_live_parquet_rows(
    input: &Arc<dyn ExecutionPlan>,
    positions: &RoaringTreemap,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // Delegated downcasts expose the wrapped plan, but replacing children acts
    // on the wrapper. Traverse and rebuild its actual child to retain both.
    let child = if let Some(delegate) = input.downcast_delegate() {
        let children = input.children();
        let [child] = children.as_slice() else {
            return Ok(None);
        };
        if !std::ptr::addr_eq(delegate, child.as_ref()) {
            return Ok(None);
        }
        Some(Arc::clone(child))
    } else {
        // Metadata uses the reader's physical positions, including after skips.
        input
            .downcast_ref::<ProjectionExec>()
            .map(|projection| Arc::clone(projection.input()))
            .or_else(|| {
                input
                    .downcast_ref::<IcebergMergeMetadataExec>()
                    .map(|metadata| Arc::clone(metadata.input()))
            })
    };
    if let Some(child) = child {
        return select_live_parquet_rows(&child, positions)?
            .map(|selected| {
                Arc::clone(input).replace_children(
                    vec![selected],
                    ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                )
            })
            .transpose();
    }
    let Some(scan) = input.downcast_ref::<DataSourceExec>() else {
        return Ok(None);
    };
    let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>() else {
        return Ok(None);
    };
    if !config.file_source.is::<ParquetSource>()
        || config.file_source.filter().is_some()
        || config.limit.is_some()
    {
        return Ok(None);
    }
    let [group] = config.file_groups.as_slice() else {
        return Ok(None);
    };
    let [file] = group.files() else {
        return Ok(None);
    };
    if file.range.is_some()
        || file.extensions.get::<ParquetRowSelection>().is_some()
        || file.extensions.get::<ParquetAccessPlan>().is_some()
    {
        return Ok(None);
    }
    let Some(Precision::Exact(rows)) = file.statistics.as_ref().map(|stats| stats.num_rows) else {
        return Ok(None);
    };
    let Some(selection) = live_row_selection(positions, rows) else {
        return Ok(None);
    };
    let mut file = file.clone();
    file.extensions.insert(ParquetRowSelection::new(selection));
    let mut config = config.clone();
    config.file_groups = vec![FileGroup::from(vec![file])];
    Ok(Some(Arc::new(
        scan.clone().with_data_source(Arc::new(config)),
    )))
}

#[derive(Clone)]
struct ObjectStoreParquetReader {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    size: u64,
}

impl ObjectStoreParquetReader {
    fn new(store: Arc<dyn ObjectStore>, path: ObjectPath, size: u64) -> Self {
        Self { store, path, size }
    }
}

impl AsyncFileReader for ObjectStoreParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        Box::pin(async move {
            self.store
                .get_range(&self.path, range)
                .await
                .map_err(parquet_object_store_error)
        })
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        Box::pin(async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(parquet_object_store_error)
        })
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        let size = self.size;
        Box::pin(async move {
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_and_finish(self, size)
                .await?;
            Ok(Arc::new(metadata))
        })
    }
}

fn parquet_object_store_error(error: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

#[derive(Debug, Clone)]
pub struct IcebergDeleteApplyExec {
    /// Child plan: a scan of a single data file.
    input: Arc<dyn ExecutionPlan>,
    /// Absolute path of the data file this node is filtering.
    ///
    /// Used to narrow position-delete rows: `file_path == data_file_path` and, for
    /// partition-scoped position deletes, to identify positions relevant to this file.
    data_file_path: String,
    /// Applicable position-delete file references.
    positional_deletes: Vec<DeleteFileRef>,
    /// Applicable equality-delete file references.
    equality_deletes: Vec<DeleteFileRef>,
    /// Table root URL for resolving delete-file paths via the object store.
    table_url: String,
    /// Iceberg schema used to map equality-delete `equality_ids` (field ids) to
    /// column names.
    iceberg_schema: IcebergSchema,
    /// Cached plan properties (derived from the child's schema).
    cache: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl IcebergDeleteApplyExec {
    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            Partitioning::UnknownPartitioning(1),
            input.pipeline_behavior(),
            input.boundedness(),
        ))
    }

    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        data_file_path: String,
        positional_deletes: Vec<DeleteFileRef>,
        equality_deletes: Vec<DeleteFileRef>,
        table_url: String,
        iceberg_schema: IcebergSchema,
    ) -> Self {
        let input_partitions =
            datafusion::physical_plan::ExecutionPlanProperties::output_partitioning(&input)
                .partition_count();
        if input_partitions != 1 {
            log::warn!(
                "IcebergDeleteApplyExec: child scan has {} partitions; \
                 positional deletes may be incorrect",
                input_partitions
            );
        }
        let cache = Self::compute_properties(&input);
        Self {
            input,
            data_file_path,
            positional_deletes,
            equality_deletes,
            table_url,
            iceberg_schema,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn data_file_path(&self) -> &str {
        &self.data_file_path
    }
    pub fn positional_deletes(&self) -> &[DeleteFileRef] {
        &self.positional_deletes
    }
    pub fn equality_deletes(&self) -> &[DeleteFileRef] {
        &self.equality_deletes
    }
    pub fn table_url(&self) -> &str {
        &self.table_url
    }
    pub fn iceberg_schema(&self) -> &IcebergSchema {
        &self.iceberg_schema
    }
}

impl DisplayAs for IcebergDeleteApplyExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "IcebergDeleteApplyExec: data_file={}, pos_deletes={}, eq_deletes={}",
                    self.data_file_path,
                    self.positional_deletes.len(),
                    self.equality_deletes.len()
                )
            }
        }
    }
}

#[async_trait]
impl ExecutionPlan for IcebergDeleteApplyExec {
    fn name(&self) -> &str {
        "IcebergDeleteApplyExec"
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // Advertising order preservation allows sort pushdown, but positional
        // filtering may fall back to counting rows in physical file order.
        vec![self.positional_deletes.is_empty()]
    }

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "IcebergDeleteApplyExec requires exactly one child".to_string(),
            ));
        }
        let mut cloned = (*self).clone();
        cloned.input = children[0].clone();
        cloned.cache = Self::compute_properties(&cloned.input);
        Ok(Arc::new(cloned))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "IcebergDeleteApplyExec only supports partition 0, got {partition}"
            )));
        }

        let output_schema = self.schema();
        let input = Arc::clone(&self.input);
        let selections =
            MetricBuilder::new(&self.metrics).counter("position_delete_row_selections", partition);
        let fallbacks =
            MetricBuilder::new(&self.metrics).counter("position_delete_fallbacks", partition);
        let data_file_path = self.data_file_path.clone();
        let positional_deletes = self.positional_deletes.clone();
        let equality_deletes = self.equality_deletes.clone();
        let table_url = self.table_url.clone();
        let iceberg_schema = self.iceberg_schema.clone();
        let schema_for_adapter = output_schema.clone();

        let stream = try_stream! {
            let parsed_table_url = Url::parse(&table_url)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let base_store = context
                .runtime_env()
                .object_store_registry
                .get_store(&parsed_table_url)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let store_ctx = StoreContext::new(base_store, &parsed_table_url)?;

            let positional_deletes = positional_deletes.iter()
                .map(|delete| PositionDeleteFile::try_from(&delete.data_file))
                .collect::<Result<Vec<_>>>()?;
            let mut deleted_positions =
                load_deleted_positions(&store_ctx, &positional_deletes, &data_file_path).await?;

            // Materialize selections at the execution site, after worker decoding.
            let input = if !deleted_positions.is_empty() {
                if let Some(selected) = select_live_parquet_rows(&input, &deleted_positions)? {
                    selections.add(1);
                    deleted_positions.clear();
                    selected
                } else {
                    fallbacks.add(1);
                    input
                }
            } else {
                input
            };

            // Equality field IDs may differ between delete files, so each file is
            // loaded and matched independently.
            let loaded_equality_deletes =
                load_equality_deletes(&store_ctx, &equality_deletes, &iceberg_schema).await?;

            let mut row_offset: u64 = 0;
            let mut stream = input.execute(0, Arc::clone(&context))?;
            while let Some(batch) = stream.try_next().await? {
                if deleted_positions.is_empty() && loaded_equality_deletes.is_empty() {
                    yield batch;
                    continue;
                }
                let batch_row_count = batch.num_rows() as u64;
                let mask = compute_delete_mask(
                    &batch,
                    row_offset,
                    &deleted_positions,
                    &loaded_equality_deletes,
                )?;
                row_offset += batch_row_count;
                let filtered_batch = filter_record_batch(&batch, &mask)
                    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
                if filtered_batch.num_rows() > 0 {
                    yield filtered_batch;
                }
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_for_adapter,
            Box::pin(stream),
        )))
    }
}

/// Load all applicable position-delete rows for the target data file.
pub(crate) async fn load_deleted_positions(
    store_ctx: &StoreContext,
    delete_files: &[PositionDeleteFile],
    data_file_path: &str,
) -> Result<RoaringTreemap> {
    if delete_files.is_empty() {
        return Ok(RoaringTreemap::new());
    }

    let mut deleted_positions = RoaringTreemap::new();
    for delete_file in delete_files {
        let (file_path, file_size) = match delete_file {
            PositionDeleteFile::DeletionVector {
                path,
                range,
                cardinality,
            } => {
                deleted_positions |= crate::io::deletion_vector::read_deletion_vector(
                    store_ctx,
                    path,
                    range.clone(),
                    *cardinality,
                )
                .await?;
                continue;
            }
            PositionDeleteFile::Parquet { path, size } => (path, *size),
        };
        let (store, path) = store_ctx.resolve(file_path)?;
        let delete_batches = read_parquet_all(store.clone(), &path, file_size).await?;
        for batch in delete_batches {
            let file_paths = batch
                .column_by_name(POS_DELETE_FILE_PATH_COL)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "position-delete file {} missing '{}' column",
                        file_path, POS_DELETE_FILE_PATH_COL
                    ))
                })?
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "position-delete file {} '{}' column is not Utf8",
                        file_path, POS_DELETE_FILE_PATH_COL
                    ))
                })?
                .clone();
            let positions = batch
                .column_by_name(POS_DELETE_POS_COL)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "position-delete file {} missing '{}' column",
                        file_path, POS_DELETE_POS_COL
                    ))
                })?
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "position-delete file {} '{}' column is not Int64",
                        file_path, POS_DELETE_POS_COL
                    ))
                })?
                .clone();
            for row_index in 0..file_paths.len() {
                if file_paths.is_null(row_index) || positions.is_null(row_index) {
                    continue;
                }
                if file_paths.value(row_index) != data_file_path {
                    // A delete file may reference multiple data files.
                    continue;
                }
                let position = positions.value(row_index);
                if position >= 0 {
                    deleted_positions.insert(position as u64);
                }
            }
        }
    }

    Ok(deleted_positions)
}

/// A fully-loaded equality-delete set for one delete file.
struct LoadedEqualityDelete {
    /// Ordered fields forming the equality key projection.
    key_fields: Vec<EqualityKeyField>,
    /// Converter used to encode rows into sortable byte representations; NULLs
    /// compare equal to NULLs (IS NOT DISTINCT FROM semantics).
    converter: RowConverter,
    /// Encoded rows from the equality-delete file.
    deleted_rows: HashSet<OwnedRow>,
}

struct EqualityKeyField {
    field_id: i32,
    data_column_path: Vec<String>,
    data_type: DataType,
}

/// Resolve the current data-column name and Arrow type for each equality field id.
fn resolve_equality_key_fields(
    iceberg_schema: &IcebergSchema,
    equality_ids: &[i32],
) -> Result<Vec<EqualityKeyField>> {
    let mut key_fields = Vec::with_capacity(equality_ids.len());
    for field_id in equality_ids {
        let field = iceberg_schema.field_by_id(*field_id).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "equality delete references unknown field id {field_id}"
            ))
        })?;
        if !matches!(field.field_type.as_ref(), crate::spec::Type::Primitive(ty) if !matches!(ty, crate::spec::PrimitiveType::Float | crate::spec::PrimitiveType::Double | crate::spec::PrimitiveType::Variant | crate::spec::PrimitiveType::Unknown | crate::spec::PrimitiveType::Geometry { .. } | crate::spec::PrimitiveType::Geography { .. }))
        {
            return Err(DataFusionError::Plan(format!(
                "Unsupported equality-delete key type {} for field {field_id}",
                field.field_type
            )));
        }
        let arrow_type = crate::datasource::type_converter::iceberg_type_to_arrow(
            &field.field_type,
        )
        .map_err(|error| {
            DataFusionError::External(Box::new(std::io::Error::other(format!(
                "failed to translate equality field '{}' to Arrow: {error}",
                field.name
            ))))
        })?;
        key_fields.push(EqualityKeyField {
            field_id: *field_id,
            data_column_path: crate::equality_schema::equality_field_path(
                iceberg_schema.as_struct().fields(),
                *field_id,
            )
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Equality field {field_id} must be a primitive field outside lists and maps"
                ))
            })?
            .iter()
            .map(|field| field.name.clone())
            .collect(),
            data_type: arrow_type,
        });
    }
    Ok(key_fields)
}

async fn load_equality_deletes(
    store_ctx: &StoreContext,
    delete_files: &[DeleteFileRef],
    iceberg_schema: &IcebergSchema,
) -> Result<Vec<LoadedEqualityDelete>> {
    if delete_files.is_empty() {
        return Ok(Vec::new());
    }

    let mut equality_deletes = Vec::with_capacity(delete_files.len());
    for delete_file in delete_files {
        if delete_file.data_file.equality_ids.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "equality delete file {} has empty equality_ids",
                delete_file.data_file.file_path
            )));
        }
        let key_fields =
            resolve_equality_key_fields(iceberg_schema, &delete_file.data_file.equality_ids)?;
        let sort_fields: Vec<SortField> = key_fields
            .iter()
            .map(|field| SortField::new(field.data_type.clone()))
            .collect();
        let converter = RowConverter::new(sort_fields)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;

        let (store, path) = store_ctx.resolve(&delete_file.data_file.file_path)?;
        let size = delete_file.data_file.file_size_in_bytes;
        let key_batches = read_equality_delete_keys(
            store.clone(),
            &path,
            size,
            &key_fields,
            &delete_file.data_file.file_path,
        )
        .await?;
        let mut deleted_rows = HashSet::new();
        for key_columns in key_batches {
            let rows = converter
                .convert_columns(&key_columns)
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
            for row_index in 0..rows.num_rows() {
                deleted_rows.insert(rows.row(row_index).owned());
            }
        }
        equality_deletes.push(LoadedEqualityDelete {
            key_fields,
            converter,
            deleted_rows,
        });
    }

    Ok(equality_deletes)
}

fn equality_key_column(
    batch: &RecordBatch,
    path: &[String],
    expected: &DataType,
) -> std::result::Result<ArrayRef, String> {
    let (root, children) = path.split_first().ok_or("Empty equality key path")?;
    let mut column = batch
        .column_by_name(root)
        .cloned()
        .ok_or_else(|| format!("missing equality column {root}"))?;
    for name in children {
        let parent = column
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| format!("Equality key parent is not a struct: {name}"))?;
        let child = parent
            .column_by_name(name)
            .ok_or_else(|| format!("missing equality key child {name}"))?;
        let nulls = NullBuffer::union(parent.nulls(), child.nulls());
        column = make_array(
            child
                .to_data()
                .into_builder()
                .nulls(nulls)
                .build()
                .map_err(|error| error.to_string())?,
        );
    }
    if column.data_type() == expected {
        return Ok(column);
    }
    let compatible = matches!(
        (column.data_type(), expected),
        (DataType::Int32, DataType::Int64)
            | (DataType::Utf8View | DataType::LargeUtf8, DataType::Utf8)
            | (
                DataType::Binary | DataType::BinaryView | DataType::LargeBinary,
                DataType::Binary | DataType::BinaryView | DataType::LargeBinary
            )
    ) || matches!((column.data_type(), expected), (DataType::Decimal128(p, s), DataType::Decimal128(q, t)) if p <= q && s == t);
    if !compatible {
        return Err(format!(
            "equality column {path:?} has type {}, expected {expected}",
            column.data_type()
        ));
    }
    datafusion::arrow::compute::cast(&column, expected).map_err(|error| error.to_string())
}

fn project_equality_key_columns(
    batch: &RecordBatch,
    key_fields: &[EqualityKeyField],
) -> std::result::Result<Vec<ArrayRef>, String> {
    key_fields
        .iter()
        .map(|field| equality_key_column(batch, &field.data_column_path, &field.data_type))
        .collect()
}

struct EqualityDeleteProjection {
    mask: ProjectionMask,
    key_column_paths: Vec<Vec<String>>,
}

impl EqualityDeleteProjection {
    fn try_new(
        parquet_schema: &SchemaDescriptor,
        key_fields: &[EqualityKeyField],
    ) -> std::result::Result<Self, String> {
        let mut leaf_indices = Vec::with_capacity(key_fields.len());
        let mut key_column_paths = Vec::with_capacity(key_fields.len());
        for key_field in key_fields {
            let mut matching = parquet_schema
                .columns()
                .iter()
                .enumerate()
                .filter(|(_, column)| {
                    let info = column.self_type().get_basic_info();
                    info.has_id() && info.id() == key_field.field_id
                });
            let (index, column) = matching.next().ok_or_else(|| {
                format!(
                    "missing column with Iceberg field id {}",
                    key_field.field_id
                )
            })?;
            if matching.next().is_some() {
                return Err(format!(
                    "multiple columns have Iceberg field id {}",
                    key_field.field_id
                ));
            }
            if column.max_rep_level() != 0 {
                return Err(format!(
                    "Equality key {} cannot be nested in a list or map",
                    key_field.field_id
                ));
            }
            leaf_indices.push(index);
            key_column_paths.push(column.path().parts().to_vec());
        }
        Ok(Self {
            mask: ProjectionMask::leaves(parquet_schema, leaf_indices),
            key_column_paths,
        })
    }

    fn project_key_columns(
        &self,
        batch: &RecordBatch,
        key_fields: &[EqualityKeyField],
    ) -> std::result::Result<Vec<ArrayRef>, String> {
        self.key_column_paths
            .iter()
            .zip(key_fields)
            .map(|(path, field)| equality_key_column(batch, path, &field.data_type))
            .collect()
    }
}

async fn read_equality_delete_keys(
    store: Arc<dyn object_store::ObjectStore>,
    path: &ObjectPath,
    size: u64,
    key_fields: &[EqualityKeyField],
    display_path: &str,
) -> Result<Vec<Vec<ArrayRef>>> {
    let reader = ObjectStoreParquetReader::new(store, path.clone(), size);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let projection = EqualityDeleteProjection::try_new(builder.parquet_schema(), key_fields)
        .map_err(|error| {
            DataFusionError::Internal(format!("equality delete file {display_path}: {error}"))
        })?;
    let mut stream = builder
        .with_projection(projection.mask.clone())
        .build()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let mut key_batches = Vec::new();
    while let Some(batch) = stream
        .try_next()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
    {
        key_batches.push(
            projection
                .project_key_columns(&batch, key_fields)
                .map_err(|error| {
                    DataFusionError::Internal(format!(
                        "equality delete file {display_path}: {error}"
                    ))
                })?,
        );
    }
    Ok(key_batches)
}

/// Read all RecordBatches from a Parquet file on the given store.
async fn read_parquet_all(
    store: Arc<dyn object_store::ObjectStore>,
    path: &ObjectPath,
    size: u64,
) -> Result<Vec<RecordBatch>> {
    let reader = ObjectStoreParquetReader::new(store, path.clone(), size);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let stream = builder
        .build()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let batches: Vec<RecordBatch> = stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(batches)
}

/// Compute a boolean mask where `true` keeps the row, `false` drops it.
fn compute_delete_mask(
    batch: &RecordBatch,
    row_offset: u64,
    deleted_positions: &RoaringTreemap,
    equality_deletes: &[LoadedEqualityDelete],
) -> Result<datafusion::arrow::array::BooleanArray> {
    let row_count = batch.num_rows();
    let mut keep_rows = vec![true; row_count];

    let end_offset = row_offset + row_count as u64;
    let mut positions = deleted_positions.iter();
    positions.advance_to(row_offset);
    for position in positions.take_while(|position| *position < end_offset) {
        keep_rows[(position - row_offset) as usize] = false;
    }

    // Equality deletes: convert data-batch rows once per key set and probe the set.
    for equality_delete in equality_deletes {
        let key_columns = project_equality_key_columns(batch, &equality_delete.key_fields)
            .map_err(|error| {
                DataFusionError::Internal(format!("equality-delete apply: {error}"))
            })?;
        let rows = equality_delete
            .converter
            .convert_columns(&key_columns)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        let key_rows = (0..row_count)
            .map(|row_index| rows.row(row_index).owned())
            .collect::<Vec<OwnedRow>>();
        for (row_index, keep_row) in keep_rows.iter_mut().enumerate() {
            if !*keep_row {
                continue;
            }
            if equality_delete.deleted_rows.contains(&key_rows[row_index]) {
                *keep_row = false;
            }
        }
    }

    Ok(datafusion::arrow::array::BooleanArray::from(keep_rows))
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]
    use std::sync::Arc;

    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::arrow::row::{RowConverter, SortField};
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::FileScanConfigBuilder;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::limit::GlobalLimitExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_common::Statistics;
    use object_store::memory::InMemory;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use parquet::schema::parser::parse_message_type;

    use super::*;

    #[test]
    fn row_selection_preserves_positions_and_bounds_fragmentation() {
        let positions = RoaringTreemap::from_iter([0, 1, 5, 6, 9]);
        assert_eq!(
            live_row_selection(&positions, 12),
            Some(RowSelection::from(vec![
                RowSelector::skip(2),
                RowSelector::select(3),
                RowSelector::skip(2),
                RowSelector::select(2),
                RowSelector::skip(1),
                RowSelector::select(2),
            ]))
        );
        assert!(live_row_selection(&positions, 9).is_none());
        assert_eq!(
            live_row_selection(&RoaringTreemap::from_iter(0..12), 12),
            Some(RowSelection::from(vec![RowSelector::skip(12)]))
        );
        let fragmented = RoaringTreemap::from_iter((0..20_000).step_by(2));
        assert!(live_row_selection(&fragmented, 20_000).is_none());
    }

    async fn scan_vector(
        positions: RoaringTreemap,
        known_rows: bool,
        projection: Vec<usize>,
        limit: Option<usize>,
        merge_metadata: bool,
        sort_rows: bool,
    ) -> Result<(Vec<RecordBatch>, MetricsSet)> {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(17));
        let store = Arc::new(InMemory::new());
        let table_url = Url::parse("memory://iceberg/table/").unwrap();
        ctx.register_object_store(&table_url, store.clone());
        let store_ctx = StoreContext::new(store.clone(), &table_url)?;
        let data_file_path = "memory://iceberg/table/data.parquet";
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values((0..1024).map(
                |position| if sort_rows { 1023 - position } else { position },
            )))],
        )?;
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(128))
            .build();
        let mut writer = ArrowWriter::try_new(Vec::new(), schema.clone(), Some(properties))?;
        writer.write(&batch)?;
        let bytes = writer.into_inner()?;
        let size = bytes.len() as u64;
        store
            .put(&ObjectPath::from("table/data.parquet"), bytes.into())
            .await?;
        let schema = if merge_metadata {
            Arc::new(crate::row_lineage::append_lineage_fields(&schema)?)
        } else {
            schema
        };
        let table_schema =
            datafusion::datasource::table_schema::TableSchema::builder(schema.clone());
        let table_schema = if merge_metadata {
            table_schema.with_virtual_columns(vec![
                crate::row_level_metadata::parquet_row_position_field(&schema),
            ])
        } else {
            table_schema
        };
        let vectors = crate::io::deletion_vector::write_deletion_vectors(
            &store_ctx,
            &table_url,
            vec![crate::io::deletion_vector::DeletionVector {
                referenced_data_file: data_file_path.to_string(),
                partition_spec_id: 0,
                partition: vec![],
                positions,
            }],
            crate::io::deletion_vector::TARGET_PUFFIN_SIZE,
        )
        .await?;
        let mut file = PartitionedFile::new("table/data.parquet", size);
        if known_rows {
            let mut stats = Statistics::new_unknown(&schema);
            stats.num_rows = Precision::Exact(1024);
            file.statistics = Some(Arc::new(stats));
        }
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("memory://iceberg")?,
            Arc::new(ParquetSource::new(table_schema.build())),
        )
        .with_file_groups(vec![FileGroup::from(vec![file])])
        .with_projection_indices(Some(projection))?
        .with_preserve_order(true)
        .build();
        let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
        // Identity/default projections must not prevent selection at the leaf scan.
        let expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = scan
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                (
                    Arc::new(datafusion::physical_expr::expressions::Column::new(
                        field.name(),
                        index,
                    )) as _,
                    if field.name() == "id" {
                        "selected_id".to_string()
                    } else {
                        field.name().clone()
                    },
                )
            })
            .collect();
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(expressions, scan)?);
        let scan = if merge_metadata {
            Arc::new(IcebergMergeMetadataExec::try_new(
                scan,
                data_file_path.to_string(),
                0,
                "{}".to_string(),
                Some("file".to_string()),
                Some("position".to_string()),
                Some(crate::row_lineage::RowLineage {
                    first_row_id: Some(10_000),
                    data_sequence_number: 7,
                }),
            )?) as Arc<dyn ExecutionPlan>
        } else {
            scan
        };
        let apply = Arc::new(IcebergDeleteApplyExec::new(
            scan,
            data_file_path.to_string(),
            vectors
                .into_iter()
                .map(|data_file| DeleteFileRef {
                    data_file,
                    data_sequence_number: 2,
                    partition_spec_id: 0,
                    is_unpartitioned_spec: true,
                })
                .collect(),
            vec![],
            table_url.to_string(),
            IcebergSchema::builder().build().unwrap(),
        ));
        let plan: Arc<dyn ExecutionPlan> = match limit {
            Some(limit) => Arc::new(GlobalLimitExec::new(apply.clone(), 0, Some(limit))),
            None => apply.clone(),
        };
        let plan = if sort_rows {
            let ordering = datafusion::physical_expr::LexOrdering::new(vec![
                datafusion::physical_expr::PhysicalSortExpr::new_default(Arc::new(
                    datafusion::physical_expr::expressions::Column::new("selected_id", 0),
                )),
            ])
            .unwrap();
            datafusion::physical_planner::DefaultPhysicalPlanner::default().optimize_physical_plan(
                Arc::new(SortExec::new(ordering, plan)),
                &ctx.state(),
                |_, _| {},
            )?
        } else {
            plan
        };
        let plan = sail_telemetry::trace_execution_plan(plan, Default::default())?;
        let batches = collect(plan, ctx.task_ctx()).await?;
        Ok((batches, apply.metrics().unwrap()))
    }

    #[tokio::test]
    async fn parquet_selection_preserves_traced_projections_across_row_groups() -> Result<()> {
        let positions = RoaringTreemap::from_iter((0..128).chain([511, 512, 513, 600, 1023]));
        for known_rows in [true, false] {
            let (batches, metrics) =
                scan_vector(positions.clone(), known_rows, vec![0], None, false, false).await?;
            let actual = batches
                .iter()
                .flat_map(|batch| {
                    assert_eq!(batch.schema().field(0).name(), "selected_id");
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                actual,
                (0..1024)
                    .filter(|id| !positions.contains(*id as u64))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                metrics
                    .sum_by_name("position_delete_row_selections")
                    .unwrap()
                    .as_usize(),
                usize::from(known_rows)
            );
            assert_eq!(
                metrics
                    .sum_by_name("position_delete_fallbacks")
                    .unwrap()
                    .as_usize(),
                usize::from(!known_rows)
            );
            let (batches, _) =
                scan_vector(positions.clone(), known_rows, vec![], Some(3), false, false).await?;
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
            assert!(batches.iter().all(|batch| batch.num_columns() == 0));
        }
        let (batches, metrics) = scan_vector(
            RoaringTreemap::from_iter(0..1024),
            true,
            vec![0],
            None,
            false,
            false,
        )
        .await?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        assert_eq!(
            metrics
                .sum_by_name("position_delete_row_selections")
                .unwrap()
                .as_usize(),
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn parquet_selection_retains_merge_positions_and_lineage() -> Result<()> {
        let positions = RoaringTreemap::from_iter((128..256).chain([1, 511, 512, 513, 600, 1023]));
        let expected = (0..1024i64)
            .filter(|id| !positions.contains(*id as u64))
            .collect::<Vec<_>>();
        for known_rows in [true, false] {
            let (batches, metrics) = scan_vector(
                positions.clone(),
                known_rows,
                vec![0, 1, 2, 3],
                None,
                true,
                false,
            )
            .await?;
            for (name, values) in [
                ("selected_id", expected.clone()),
                ("position", expected.clone()),
                (
                    crate::row_lineage::ROW_ID_COLUMN,
                    expected.iter().map(|position| 10_000 + position).collect(),
                ),
                (
                    crate::row_lineage::LAST_UPDATED_SEQUENCE_COLUMN,
                    vec![7; expected.len()],
                ),
            ] {
                let actual = batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column_by_name(name)
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .values()
                            .iter()
                            .copied()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(actual, values, "{name}");
            }
            assert_eq!(
                metrics
                    .sum_by_name("position_delete_row_selections")
                    .unwrap()
                    .as_usize(),
                usize::from(known_rows)
            );
            assert_eq!(
                metrics
                    .sum_by_name("position_delete_fallbacks")
                    .unwrap()
                    .as_usize(),
                usize::from(!known_rows)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn sorting_retains_physical_delete_positions() -> Result<()> {
        let positions = RoaringTreemap::from_iter((128..256).chain([1, 511, 512, 600]));
        let expected = (0..1024)
            .filter(|id| !positions.contains((1023 - id) as u64))
            .collect::<Vec<_>>();
        for known_rows in [true, false] {
            let (batches, metrics) =
                scan_vector(positions.clone(), known_rows, vec![0], None, false, true).await?;
            let actual = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied()
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
            assert_eq!(
                metrics
                    .sum_by_name("position_delete_row_selections")
                    .unwrap()
                    .as_usize(),
                usize::from(known_rows)
            );
            assert_eq!(
                metrics
                    .sum_by_name("position_delete_fallbacks")
                    .unwrap()
                    .as_usize(),
                usize::from(!known_rows)
            );
        }
        Ok(())
    }

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn equality_projection_reads_only_key_fields_and_restores_key_order() {
        let parquet_schema = SchemaDescriptor::new(Arc::new(
            parse_message_type(
                "message test {
                    REQUIRED INT64 extra = 9;
                    REQUIRED BINARY second (UTF8) = 2;
                    REQUIRED INT64 first = 1;
                }",
            )
            .unwrap(),
        ));
        let key_fields = vec![
            EqualityKeyField {
                field_id: 1,
                data_column_path: vec!["first".to_string()],
                data_type: DataType::Int64,
            },
            EqualityKeyField {
                field_id: 2,
                data_column_path: vec!["second".to_string()],
                data_type: DataType::Utf8,
            },
        ];

        let projection = EqualityDeleteProjection::try_new(&parquet_schema, &key_fields).unwrap();

        assert_eq!(
            projection.mask,
            ProjectionMask::roots(&parquet_schema, [1, 2])
        );
        assert_eq!(
            projection.key_column_paths,
            vec![vec!["first"], vec!["second"]]
        );
    }

    #[test]
    fn equality_projection_rejects_duplicate_field_ids() {
        let parquet_schema = SchemaDescriptor::new(Arc::new(
            parse_message_type(
                "message test {
                    REQUIRED INT64 first = 1;
                    REQUIRED INT64 duplicate = 1;
                }",
            )
            .unwrap(),
        ));
        let key_fields = vec![EqualityKeyField {
            field_id: 1,
            data_column_path: vec!["first".to_string()],
            data_type: DataType::Int64,
        }];

        let error = EqualityDeleteProjection::try_new(&parquet_schema, &key_fields)
            .err()
            .unwrap();

        assert!(error.contains("multiple columns have Iceberg field id 1"));
    }

    #[test]
    fn mask_drops_positions_within_range() {
        let batch = make_batch();
        let positions = RoaringTreemap::from_iter([1, 3, 100]);
        let mask = compute_delete_mask(&batch, 0, &positions, &[]).unwrap();
        // Rows 1 and 3 dropped.
        let values = (0..mask.len())
            .map(|index| mask.value(index))
            .collect::<Vec<_>>();
        assert_eq!(values, vec![true, false, true, false, true]);
    }

    #[test]
    fn mask_respects_row_offset_window() {
        let batch = make_batch(); // 5 rows
        // Upstream row offset 10 means this batch spans rows [10, 15).
        let positions = RoaringTreemap::from_iter([9, 11, 14, 20]);
        let mask = compute_delete_mask(&batch, 10, &positions, &[]).unwrap();
        // Positions 11 and 14 drop rows 1 and 4; the other positions are outside the batch.
        let values = (0..mask.len())
            .map(|index| mask.value(index))
            .collect::<Vec<_>>();
        assert_eq!(values, vec![true, false, true, true, false]);
    }

    #[test]
    fn mask_applies_equality_deletes() {
        let batch = make_batch();
        let key_fields = vec![EqualityKeyField {
            field_id: 1,
            data_column_path: vec!["id".to_string()],
            data_type: DataType::Int64,
        }];
        let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
        let delete_rows = converter
            .convert_columns(&[Arc::new(Int64Array::from(vec![2i64, 4])) as _])
            .unwrap();
        let mut deleted_rows = HashSet::new();
        for row_index in 0..delete_rows.num_rows() {
            deleted_rows.insert(delete_rows.row(row_index).owned());
        }
        let equality_deletes = vec![LoadedEqualityDelete {
            key_fields,
            converter,
            deleted_rows,
        }];

        let mask =
            compute_delete_mask(&batch, 0, &RoaringTreemap::new(), &equality_deletes).unwrap();
        let values = (0..mask.len())
            .map(|index| mask.value(index))
            .collect::<Vec<_>>();
        // id 0,1,2,3,4 → keep 0,1,3; drop 2 and 4.
        assert_eq!(values, vec![true, true, false, true, false]);
    }

    #[test]
    fn mask_combines_positions_and_equality() {
        let batch = make_batch();
        let positions = RoaringTreemap::from_iter([0]); // drops row 0

        let key_fields = vec![EqualityKeyField {
            field_id: 1,
            data_column_path: vec!["id".to_string()],
            data_type: DataType::Int64,
        }];
        let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
        let delete_rows = converter
            .convert_columns(&[Arc::new(Int64Array::from(vec![4i64])) as _])
            .unwrap();
        let mut deleted_rows = HashSet::new();
        deleted_rows.insert(delete_rows.row(0).owned());
        let equality_deletes = vec![LoadedEqualityDelete {
            key_fields,
            converter,
            deleted_rows,
        }];

        let mask = compute_delete_mask(&batch, 0, &positions, &equality_deletes).unwrap();
        let values = (0..mask.len())
            .map(|index| mask.value(index))
            .collect::<Vec<_>>();
        assert_eq!(values, vec![false, true, true, true, false]);
    }

    #[test]
    fn mask_noop_when_no_deletes() {
        let batch = make_batch();
        let mask = compute_delete_mask(&batch, 0, &RoaringTreemap::new(), &[]).unwrap();
        assert!((0..mask.len()).all(|i| mask.value(i)));
    }
}
