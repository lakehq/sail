use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result};
use futures::stream::TryStreamExt;
use object_store::path::Path as ObjectPath;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use url::Url;

use crate::io::StoreContext;
use crate::spec::delete_index::DeleteFileRef;
use crate::spec::Schema as IcebergSchema;

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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
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
        let positional = self.positional_deletes.clone();
        let equality = self.equality_deletes.clone();
        let table_url = self.table_url.clone();
        let iceberg_schema = self.iceberg_schema.clone();
        let schema_for_adapter = output_schema.clone();

        let stream = try_stream! {
            let table_url_parsed = Url::parse(&table_url)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let base_store = context
                .runtime_env()
                .object_store_registry
                .get_store(&table_url_parsed)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let store_ctx = StoreContext::new(base_store, &table_url_parsed)?;

            // Phase 1: load positional deletes (all files; filter by target path).
            let positions = load_positions(&store_ctx, &positional, &data_file_path).await?;

            // Phase 2: load equality-delete sets. Each entry is an independent set
            // because the `equality_ids` may differ per delete file, and each set
            // is applied disjunctively ("any match deletes").
            let eq_sets = load_equality_sets(&store_ctx, &equality, &iceberg_schema).await?;

            // Phase 3: stream the child and filter each batch.
            let mut row_offset: u64 = 0;
            let mut stream = child;
            while let Some(batch) = stream.try_next().await? {
                let n = batch.num_rows() as u64;
                let mask = compute_delete_mask(&batch, row_offset, &positions, &eq_sets)?;
                row_offset += n;
                let kept = filter_record_batch(&batch, &mask)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                if kept.num_rows() > 0 {
                    yield kept;
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
async fn load_positions(
    store_ctx: &StoreContext,
    delete_refs: &[DeleteFileRef],
    data_file_path: &str,
) -> Result<Vec<u64>> {
    if delete_refs.is_empty() {
        return Ok(Vec::new());
    }

    let mut all_positions: Vec<u64> = Vec::new();
    for r in delete_refs {
        let (store, path) = store_ctx.resolve(&r.data_file.file_path)?;
        let size = r.data_file.file_size_in_bytes;
        let batches = read_parquet_all(store.clone(), &path, size).await?;
        for batch in batches {
            let path_col = batch
                .column_by_name(POS_DELETE_FILE_PATH_COL)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "position-delete file {} missing '{}' column",
                        r.data_file.file_path, POS_DELETE_FILE_PATH_COL
                    ))
                })?
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "position-delete file {} '{}' column is not Utf8",
                        r.data_file.file_path, POS_DELETE_FILE_PATH_COL
                    ))
                })?
                .clone();
            let pos_col = batch
                .column_by_name(POS_DELETE_POS_COL)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "position-delete file {} missing '{}' column",
                        r.data_file.file_path, POS_DELETE_POS_COL
                    ))
                })?
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "position-delete file {} '{}' column is not Int64",
                        r.data_file.file_path, POS_DELETE_POS_COL
                    ))
                })?
                .clone();
            for i in 0..path_col.len() {
                if path_col.is_null(i) || pos_col.is_null(i) {
                    continue;
                }
                let matched = path_col.value(i) == data_file_path;
                if !matched {
                    // Per spec, position-delete rows reference a single data file each;
                    // most production writers group all rows for one data file into one
                    // delete file, but a delete file MAY reference multiple paths. Filter
                    // by exact string match.
                    continue;
                }
                let pos = pos_col.value(i);
                if pos >= 0 {
                    all_positions.push(pos as u64);
                }
            }
        }
    }

    all_positions.sort_unstable();
    all_positions.dedup();
    Ok(all_positions)
}

/// A fully-loaded equality-delete set for one delete file.
struct EqualityDeleteSet {
    /// Ordered (name, arrow type) tuples forming the equality key projection.
    key_spec: Vec<(String, DataType)>,
    /// Converter used to encode rows into sortable byte representations; NULLs
    /// compare equal to NULLs (IS NOT DISTINCT FROM semantics).
    converter: RowConverter,
    /// Encoded rows from the equality-delete file.
    rows: HashSet<OwnedRow>,
}

/// Resolve an Iceberg schema field name + Arrow data type for each `field_id`.
fn resolve_equality_key_spec(
    iceberg_schema: &IcebergSchema,
    equality_ids: &[i32],
) -> Result<Vec<(String, DataType)>> {
    let mut spec = Vec::with_capacity(equality_ids.len());
    for fid in equality_ids {
        let field = iceberg_schema.field_by_id(*fid).ok_or_else(|| {
            DataFusionError::Plan(format!("equality delete references unknown field id {fid}"))
        })?;
        let arrow_type = crate::datasource::type_converter::iceberg_type_to_arrow(
            &field.field_type,
        )
        .map_err(|e| {
            DataFusionError::External(Box::new(std::io::Error::other(format!(
                "failed to translate equality field '{}' to Arrow: {e}",
                field.name
            ))))
        })?;
        spec.push((field.name.clone(), arrow_type));
    }
    Ok(spec)
}

async fn load_equality_sets(
    store_ctx: &StoreContext,
    delete_refs: &[DeleteFileRef],
    iceberg_schema: &IcebergSchema,
) -> Result<Vec<EqualityDeleteSet>> {
    if delete_refs.is_empty() {
        return Ok(Vec::new());
    }

    let mut sets = Vec::with_capacity(delete_refs.len());
    for r in delete_refs {
        if r.data_file.equality_ids.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "equality delete file {} has empty equality_ids",
                r.data_file.file_path
            )));
        }
        let key_spec = resolve_equality_key_spec(iceberg_schema, &r.data_file.equality_ids)?;
        let sort_fields: Vec<SortField> = key_spec
            .iter()
            .map(|(_, dt)| SortField::new(dt.clone()))
            .collect();
        let converter = RowConverter::new(sort_fields)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let (store, path) = store_ctx.resolve(&r.data_file.file_path)?;
        let size = r.data_file.file_size_in_bytes;
        let batches = read_parquet_all(store.clone(), &path, size).await?;
        let mut rows_set: HashSet<OwnedRow> = HashSet::new();
        for batch in batches {
            let cols = project_columns(&batch, &key_spec).map_err(|e| {
                DataFusionError::Internal(format!(
                    "equality delete file {}: {e}",
                    r.data_file.file_path
                ))
            })?;
            let rows = converter
                .convert_columns(&cols)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            for i in 0..rows.num_rows() {
                rows_set.insert(rows.row(i).owned());
            }
        }
        sets.push(EqualityDeleteSet {
            key_spec,
            converter,
            rows: rows_set,
        });
    }

    Ok(sets)
}

fn project_columns(
    batch: &RecordBatch,
    key_spec: &[(String, DataType)],
) -> std::result::Result<Vec<datafusion::arrow::array::ArrayRef>, String> {
    let mut out = Vec::with_capacity(key_spec.len());
    for (name, expected_ty) in key_spec {
        let col = batch
            .column_by_name(name)
            .ok_or_else(|| format!("missing column '{name}'"))?;
        if col.data_type() != expected_ty {
            return Err(format!(
                "column '{name}' has type {:?}, expected {:?}",
                col.data_type(),
                expected_ty
            ));
        }
        out.push(col.clone());
    }
    Ok(out)
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
    eq_sets: &[EqualityDeleteSet],
) -> Result<datafusion::arrow::array::BooleanArray> {
    let n = batch.num_rows();
    let mut keep: Vec<bool> = vec![true; n];

    // Positional deletes: look up positions in [row_offset, row_offset + n).
    if !sorted_positions.is_empty() {
        let lo = row_offset;
        let hi = row_offset + n as u64;
        // Binary-search the range of positions that fall in [lo, hi).
        let start = sorted_positions.partition_point(|&p| p < lo);
        let end = sorted_positions.partition_point(|&p| p < hi);
        for &p in &sorted_positions[start..end] {
            let idx = (p - lo) as usize;
            if idx < n {
                keep[idx] = false;
            }
        }
    }

    // Equality deletes: convert data-batch rows once per eq set and probe the set.
    for eq in eq_sets {
        let cols = project_columns(batch, &eq.key_spec)
            .map_err(|e| DataFusionError::Internal(format!("equality-delete apply: {e}")))?;
        let rows = eq
            .converter
            .convert_columns(&cols)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let owned_rows: Vec<OwnedRow> = (0..n).map(|i| rows.row(i).owned()).collect();
        for (i, keep_slot) in keep.iter_mut().enumerate().take(n) {
            if !*keep_slot {
                continue;
            }
            if eq.rows.contains(&owned_rows[i]) {
                *keep_slot = false;
            }
        }
    }

    Ok(datafusion::arrow::array::BooleanArray::from(keep))
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]
    use std::sync::Arc;

    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::arrow::row::{RowConverter, SortField};

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
    fn mask_drops_positions_within_range() {
        let batch = make_batch();
        let positions = vec![1u64, 3, 100];
        let mask = compute_delete_mask(&batch, 0, &positions, &[]).unwrap();
        // Rows 1 and 3 dropped.
        let vals: Vec<bool> = (0..mask.len()).map(|i| mask.value(i)).collect();
        assert_eq!(vals, vec![true, false, true, false, true]);
    }

    #[test]
    fn mask_respects_row_offset_window() {
        let batch = make_batch(); // 5 rows
                                  // Upstream row offset 10 means this batch spans rows [10, 15).
        let positions = vec![9u64, 11, 14, 20];
        let mask = compute_delete_mask(&batch, 10, &positions, &[]).unwrap();
        // 9 < 10 → out; 11 → drop idx 1; 14 → drop idx 4; 20 → out.
        let vals: Vec<bool> = (0..mask.len()).map(|i| mask.value(i)).collect();
        assert_eq!(vals, vec![true, false, true, true, false]);
    }

    #[test]
    fn mask_applies_equality_sets() {
        let batch = make_batch();
        // Build an eq set keyed by ("id" Int64): delete id=2 and id=4.
        let key_spec = vec![("id".to_string(), DataType::Int64)];
        let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
        let delete_rows = converter
            .convert_columns(&[Arc::new(Int64Array::from(vec![2i64, 4])) as _])
            .unwrap();
        let mut set = HashSet::new();
        for i in 0..delete_rows.num_rows() {
            set.insert(delete_rows.row(i).owned());
        }
        let eq_sets = vec![EqualityDeleteSet {
            key_spec,
            converter,
            rows: set,
        }];

        let mask = compute_delete_mask(&batch, 0, &[], &eq_sets).unwrap();
        let vals: Vec<bool> = (0..mask.len()).map(|i| mask.value(i)).collect();
        // id 0,1,2,3,4 → keep 0,1,3; drop 2 and 4.
        assert_eq!(vals, vec![true, true, false, true, false]);
    }

    #[test]
    fn mask_combines_positions_and_equality() {
        let batch = make_batch();
        let positions = vec![0u64]; // drops row 0

        let key_spec = vec![("id".to_string(), DataType::Int64)];
        let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
        let delete_rows = converter
            .convert_columns(&[Arc::new(Int64Array::from(vec![4i64])) as _])
            .unwrap();
        let mut set = HashSet::new();
        set.insert(delete_rows.row(0).owned());
        let eq_sets = vec![EqualityDeleteSet {
            key_spec,
            converter,
            rows: set,
        }];

        let mask = compute_delete_mask(&batch, 0, &positions, &eq_sets).unwrap();
        let vals: Vec<bool> = (0..mask.len()).map(|i| mask.value(i)).collect();
        assert_eq!(vals, vec![false, true, true, true, false]);
    }

    #[test]
    fn mask_noop_when_no_deletes() {
        let batch = make_batch();
        let mask = compute_delete_mask(&batch, 0, &[], &[]).unwrap();
        assert!((0..mask.len()).all(|i| mask.value(i)));
    }
}
