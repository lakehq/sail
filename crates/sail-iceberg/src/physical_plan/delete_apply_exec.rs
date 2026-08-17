use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result};
use futures::stream::TryStreamExt;
use object_store::path::Path as ObjectPath;
use parquet::arrow::ProjectionMask;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::schema::types::SchemaDescriptor;
use url::Url;

use crate::io::StoreContext;
use crate::spec::Schema as IcebergSchema;
use crate::spec::delete_index::DeleteFileRef;

/// Column name used in Iceberg position-delete files for the target data-file path.
const POS_DELETE_FILE_PATH_COL: &str = "file_path";
/// Column name used in Iceberg position-delete files for the row position.
const POS_DELETE_POS_COL: &str = "pos";

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
}

impl IcebergDeleteApplyExec {
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
        let output_schema = input.schema();
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            input,
            data_file_path,
            positional_deletes,
            equality_deletes,
            table_url,
            iceberg_schema,
            cache,
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

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
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
        cloned.cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(cloned.input.schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(cloned))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
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
        let child = self.input.execute(0, Arc::clone(&context))?;
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

            let deleted_positions =
                load_deleted_positions(&store_ctx, &positional_deletes, &data_file_path).await?;

            // Equality field IDs may differ between delete files, so each file is
            // loaded and matched independently.
            let loaded_equality_deletes =
                load_equality_deletes(&store_ctx, &equality_deletes, &iceberg_schema).await?;

            let mut row_offset: u64 = 0;
            let mut stream = child;
            while let Some(batch) = stream.try_next().await? {
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
async fn load_deleted_positions(
    store_ctx: &StoreContext,
    delete_files: &[DeleteFileRef],
    data_file_path: &str,
) -> Result<Vec<u64>> {
    if delete_files.is_empty() {
        return Ok(Vec::new());
    }

    let mut deleted_positions = Vec::new();
    for delete_file in delete_files {
        let (store, path) = store_ctx.resolve(&delete_file.data_file.file_path)?;
        let file_size = delete_file.data_file.file_size_in_bytes;
        let delete_batches = read_parquet_all(store.clone(), &path, file_size).await?;
        for batch in delete_batches {
            let file_paths = batch
                .column_by_name(POS_DELETE_FILE_PATH_COL)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "position-delete file {} missing '{}' column",
                        delete_file.data_file.file_path, POS_DELETE_FILE_PATH_COL
                    ))
                })?
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "position-delete file {} '{}' column is not Utf8",
                        delete_file.data_file.file_path, POS_DELETE_FILE_PATH_COL
                    ))
                })?
                .clone();
            let positions = batch
                .column_by_name(POS_DELETE_POS_COL)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "position-delete file {} missing '{}' column",
                        delete_file.data_file.file_path, POS_DELETE_POS_COL
                    ))
                })?
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "position-delete file {} '{}' column is not Int64",
                        delete_file.data_file.file_path, POS_DELETE_POS_COL
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
                    deleted_positions.push(position as u64);
                }
            }
        }
    }

    deleted_positions.sort_unstable();
    deleted_positions.dedup();
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
    data_column_name: String,
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
            data_column_name: field.name.clone(),
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

fn project_equality_key_columns(
    batch: &RecordBatch,
    key_fields: &[EqualityKeyField],
) -> std::result::Result<Vec<datafusion::arrow::array::ArrayRef>, String> {
    let mut key_columns = Vec::with_capacity(key_fields.len());
    for field in key_fields {
        let column = batch
            .column_by_name(&field.data_column_name)
            .ok_or_else(|| format!("missing column '{}'", field.data_column_name))?;
        if column.data_type() != &field.data_type {
            return Err(format!(
                "column '{name}' has type {:?}, expected {:?}",
                column.data_type(),
                field.data_type,
                name = field.data_column_name,
            ));
        }
        key_columns.push(column.clone());
    }
    Ok(key_columns)
}

struct EqualityDeleteProjection {
    mask: ProjectionMask,
    key_column_indices: Vec<usize>,
}

impl EqualityDeleteProjection {
    fn try_new(
        parquet_schema: &SchemaDescriptor,
        key_fields: &[EqualityKeyField],
    ) -> std::result::Result<Self, String> {
        let root_fields = parquet_schema.root_schema().get_fields();
        let mut root_indices = Vec::with_capacity(key_fields.len());
        for key_field in key_fields {
            let mut matching_roots = root_fields.iter().enumerate().filter_map(|(index, field)| {
                let basic_info = field.get_basic_info();
                (basic_info.has_id() && basic_info.id() == key_field.field_id).then_some(index)
            });
            let root_index = matching_roots.next().ok_or_else(|| {
                format!(
                    "missing column with Iceberg field id {}",
                    key_field.field_id
                )
            })?;
            if matching_roots.next().is_some() {
                return Err(format!(
                    "multiple columns have Iceberg field id {}",
                    key_field.field_id
                ));
            }
            root_indices.push(root_index);
        }

        let mut projected_roots = root_indices.clone();
        projected_roots.sort_unstable();
        projected_roots.dedup();
        let key_column_indices = root_indices
            .iter()
            .map(|root_index| {
                projected_roots
                    .binary_search(root_index)
                    .map_err(|_| "failed to map projected equality-delete column".to_string())
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(Self {
            mask: ProjectionMask::roots(parquet_schema, projected_roots),
            key_column_indices,
        })
    }

    fn project_key_columns(
        &self,
        batch: &RecordBatch,
        key_fields: &[EqualityKeyField],
    ) -> std::result::Result<Vec<ArrayRef>, String> {
        let mut columns = Vec::with_capacity(key_fields.len());
        for (column_index, key_field) in self.key_column_indices.iter().zip(key_fields) {
            let column = batch.columns().get(*column_index).ok_or_else(|| {
                format!(
                    "missing projected column with Iceberg field id {}",
                    key_field.field_id
                )
            })?;
            if column.data_type() != &key_field.data_type {
                return Err(format!(
                    "column with Iceberg field id {} has type {:?}, expected {:?}",
                    key_field.field_id,
                    column.data_type(),
                    key_field.data_type
                ));
            }
            columns.push(column.clone());
        }
        Ok(columns)
    }
}

async fn read_equality_delete_keys(
    store: Arc<dyn object_store::ObjectStore>,
    path: &ObjectPath,
    size: u64,
    key_fields: &[EqualityKeyField],
    display_path: &str,
) -> Result<Vec<Vec<ArrayRef>>> {
    let reader = ParquetObjectReader::new(store, path.clone()).with_file_size(size);
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
    let reader = ParquetObjectReader::new(store, path.clone()).with_file_size(size);
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
    sorted_positions: &[u64],
    equality_deletes: &[LoadedEqualityDelete],
) -> Result<datafusion::arrow::array::BooleanArray> {
    let row_count = batch.num_rows();
    let mut keep_rows = vec![true; row_count];

    // Positional deletes: look up positions in this batch's row range.
    if !sorted_positions.is_empty() {
        let end_offset = row_offset + row_count as u64;
        let first_position = sorted_positions.partition_point(|&position| position < row_offset);
        let last_position = sorted_positions.partition_point(|&position| position < end_offset);
        for &position in &sorted_positions[first_position..last_position] {
            let row_index = (position - row_offset) as usize;
            if row_index < row_count {
                keep_rows[row_index] = false;
            }
        }
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
    use parquet::schema::parser::parse_message_type;

    use super::*;

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
                data_column_name: "first".to_string(),
                data_type: DataType::Int64,
            },
            EqualityKeyField {
                field_id: 2,
                data_column_name: "second".to_string(),
                data_type: DataType::Utf8,
            },
        ];

        let projection = EqualityDeleteProjection::try_new(&parquet_schema, &key_fields).unwrap();

        assert_eq!(
            projection.mask,
            ProjectionMask::roots(&parquet_schema, [1, 2])
        );
        assert_eq!(projection.key_column_indices, vec![1, 0]);
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
            data_column_name: "first".to_string(),
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
        let positions = vec![1u64, 3, 100];
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
        let positions = vec![9u64, 11, 14, 20];
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
            data_column_name: "id".to_string(),
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

        let mask = compute_delete_mask(&batch, 0, &[], &equality_deletes).unwrap();
        let values = (0..mask.len())
            .map(|index| mask.value(index))
            .collect::<Vec<_>>();
        // id 0,1,2,3,4 → keep 0,1,3; drop 2 and 4.
        assert_eq!(values, vec![true, true, false, true, false]);
    }

    #[test]
    fn mask_combines_positions_and_equality() {
        let batch = make_batch();
        let positions = vec![0u64]; // drops row 0

        let key_fields = vec![EqualityKeyField {
            field_id: 1,
            data_column_name: "id".to_string(),
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
        let mask = compute_delete_mask(&batch, 0, &[], &[]).unwrap();
        assert!((0..mask.len()).all(|i| mask.value(i)));
    }
}
