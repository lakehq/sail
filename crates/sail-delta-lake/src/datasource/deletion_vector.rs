use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, Int64Array, RecordBatch};
use datafusion::arrow::buffer::BooleanBuffer;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::ParquetRowSelection;
use datafusion::datasource::physical_plan::{FileOpener, FileScanConfig, FileSource};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::physical_plan::apply_expression_roots;
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_datasource::morsel::{Morsel, MorselPlan, MorselPlanner, Morselizer};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{Column, DynamicFilterTracking};
use datafusion_physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use futures::TryStreamExt;
use futures::stream::BoxStream;
use moka::future::Cache;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use url::Url;

use crate::deletion_vector::{DeletionVectorBitmap, read_deletion_vector};
use crate::spec::DeletionVectorDescriptor;

const DV_CACHE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_SELECTION_RUNS: usize = 4096;

#[derive(Debug, Clone)]
pub(crate) struct DeltaFileDeletionVector {
    pub descriptor: DeletionVectorDescriptor,
    pub physical_rows: Option<usize>,
}

/// A scan-local cache. The table, snapshot and object-store authority are fixed by
/// the owning source; split readers share one load of each immutable DV segment.
type BitmapCache = Cache<(String, i32, i64), Arc<DeletionVectorBitmap>>;

#[derive(Clone)]
pub(crate) struct DeltaParquetSource {
    parquet: Arc<dyn FileSource>,
    projection: ProjectionExprs,
    row_index: usize,
    table_url: Url,
    bitmaps: BitmapCache,
}

impl DeltaParquetSource {
    pub fn new(parquet: Arc<dyn FileSource>, row_index: usize, table_url: Url) -> Self {
        let schema = parquet.table_schema().table_schema();
        let projection =
            ProjectionExprs::from_indices(&(0..schema.fields().len()).collect::<Vec<_>>(), schema);
        let bitmaps = Cache::builder()
            .max_capacity(DV_CACHE_BYTES)
            .weigher(
                |key: &(String, i32, i64), bitmap: &Arc<DeletionVectorBitmap>| {
                    bitmap
                        .inner()
                        .serialized_size()
                        .saturating_add(key.0.len())
                        .saturating_add(128)
                        .min(u32::MAX as usize) as u32
                },
            )
            .build();
        Self {
            parquet,
            projection,
            row_index,
            table_url,
            bitmaps,
        }
    }
}

impl FileSource for DeltaParquetSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        internal_err!("Delta Parquet scans require the morsel interface")
    }

    fn create_morselizer(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Box<dyn Morselizer>> {
        let mut reader_config = base_config.clone();
        // FileStream applies the fetch after DV filtering. A decoder-level fetch
        // would let deleted rows consume the quota on the position-filter path.
        reader_config.limit = None;
        let make_reader = |projection: &ProjectionExprs| -> Result<Arc<dyn Morselizer>> {
            let source = self
                .parquet
                .try_pushdown_projection(projection)?
                .ok_or_else(|| {
                    DataFusionError::Internal("Parquet projection was rejected".into())
                })?;
            Ok(Arc::from(source.create_morselizer(
                Arc::clone(&object_store),
                &reader_config,
                partition,
            )?))
        };
        let reader = make_reader(&self.projection)?;
        // Append a private physical row number only on the bitmap-filter path.
        // Projection after filtering also preserves zero-column batch row counts.
        let output_columns = self.projection.iter().count();
        let field = self.table_schema().table_schema().field(self.row_index);
        let position_projection =
            ProjectionExprs::new(self.projection.iter().cloned().chain([ProjectionExpr::new(
                Arc::new(Column::new(field.name(), self.row_index)),
                field.name(),
            )]));
        let position_reader = make_reader(&position_projection)?;
        let allow_selection = self.filter().is_none_or(|filter| {
            !DynamicFilterTracking::classify(&filter).contains_dynamic_filter()
        });
        Ok(Box::new(DeltaMorselizer {
            reader,
            position_reader,
            object_store,
            table_url: self.table_url.clone(),
            bitmaps: self.bitmaps.clone(),
            output_columns,
            allow_selection,
            metrics: DvMetrics::new(self.metrics(), partition),
        }))
    }

    fn table_schema(&self) -> &TableSchema {
        self.parquet.table_schema()
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.parquet = self.parquet.with_batch_size(batch_size);
        Arc::new(source)
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.parquet.filter()
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        source.projection = self.projection.try_merge(projection)?;
        Ok(Some(Arc::new(source)))
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.parquet.metrics()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(
            self.projection
                .iter()
                .map(|projection| &projection.expr)
                .chain(self.filter().iter()),
            f,
        )
    }

    fn file_type(&self) -> &str {
        "parquet"
    }

    fn fmt_extra(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        self.parquet.fmt_extra(t, f)?;
        write!(f, ", deletion_vectors=true")
    }
}

#[derive(Clone, Debug)]
struct DvMetrics {
    loads: Count,
    selections: Count,
    position_filters: Count,
    rows_filtered: Count,
}

impl DvMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            loads: MetricBuilder::new(metrics).counter("dv_bitmap_loads", partition),
            selections: MetricBuilder::new(metrics).counter("dv_row_selections", partition),
            position_filters: MetricBuilder::new(metrics).counter("dv_position_filters", partition),
            rows_filtered: MetricBuilder::new(metrics).counter("dv_rows_filtered", partition),
        }
    }
}

#[derive(Clone)]
struct DeltaMorselizer {
    reader: Arc<dyn Morselizer>,
    position_reader: Arc<dyn Morselizer>,
    object_store: Arc<dyn ObjectStore>,
    table_url: Url,
    bitmaps: BitmapCache,
    output_columns: usize,
    allow_selection: bool,
    metrics: DvMetrics,
}

impl fmt::Debug for DeltaMorselizer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("DeltaMorselizer").finish_non_exhaustive()
    }
}

impl Morselizer for DeltaMorselizer {
    fn plan_file(&self, file: PartitionedFile) -> Result<Box<dyn MorselPlanner>> {
        let dv = file.extensions.get_arc::<DeltaFileDeletionVector>();
        match dv {
            Some(dv) if dv.descriptor.cardinality != 0 => Ok(Box::new(LoadDeletionVector {
                source: self.clone(),
                file,
                dv,
            })),
            _ => self.reader.plan_file(file),
        }
    }
}

#[derive(Debug)]
struct LoadDeletionVector {
    source: DeltaMorselizer,
    file: PartitionedFile,
    dv: Arc<DeltaFileDeletionVector>,
}

impl MorselPlanner for LoadDeletionVector {
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
        Ok(Some(MorselPlan::new().with_pending_planner(async move {
            let Self {
                source,
                mut file,
                dv,
            } = *self;
            let descriptor = &dv.descriptor;
            let key = (
                descriptor.unique_id(),
                descriptor.size_in_bytes,
                descriptor.cardinality,
            );
            let bitmap = source
                .bitmaps
                .try_get_with(key, async {
                    source.metrics.loads.add(1);
                    read_deletion_vector(
                        source.object_store.as_ref(),
                        &source.table_url,
                        descriptor,
                    )
                    .await
                    .map(Arc::new)
                })
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            if source.allow_selection
                && let Some(rows) = dv.physical_rows
                && let Some(selection) = deletion_vector_selection(&bitmap, rows)
            {
                source.metrics.selections.add(1);
                file.extensions.insert(ParquetRowSelection::new(selection));
                return source.reader.plan_file(file);
            }
            source.metrics.position_filters.add(1);
            Ok(Box::new(FilterDeletedRowsPlanner {
                parquet: source.position_reader.plan_file(file)?,
                filter: PositionFilter {
                    bitmap,
                    output_columns: source.output_columns,
                    rows_filtered: source.metrics.rows_filtered,
                },
            }) as Box<dyn MorselPlanner>)
        })))
    }
}

/// Keep selection construction bounded when deletion positions are fragmented.
/// Unknown physical row counts use the reader's absolute row numbers instead.
fn deletion_vector_selection(bitmap: &DeletionVectorBitmap, rows: usize) -> Option<RowSelection> {
    if bitmap.inner().max().is_some_and(|row| row >= rows as u64) {
        return None;
    }
    if bitmap.len() == rows as u64 {
        return Some(RowSelection::from(vec![RowSelector::skip(rows)]));
    }
    let mut selectors = Vec::new();
    let mut position = 0;
    let mut deleted = bitmap.inner().iter().peekable();
    while let Some(start) = deleted.next() {
        let start = start as usize;
        if start > position {
            selectors.push(RowSelector::select(start - position));
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
        position = end;
    }
    if position < rows {
        selectors.push(RowSelector::select(rows - position));
    }
    Some(RowSelection::from(selectors))
}

#[derive(Clone, Debug)]
struct PositionFilter {
    bitmap: Arc<DeletionVectorBitmap>,
    output_columns: usize,
    rows_filtered: Count,
}

impl PositionFilter {
    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let positions = batch
            .column(self.output_columns)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Internal("Parquet row number is not Int64".into()))?;
        if positions.null_count() != 0 || positions.values().iter().any(|value| *value < 0) {
            return internal_err!("Parquet returned an invalid physical row number");
        }
        let keep = BooleanArray::new(
            positions
                .values()
                .iter()
                .map(|position| !self.bitmap.contains(*position as u64))
                .collect::<BooleanBuffer>(),
            None,
        );
        let batch = if keep.true_count() == batch.num_rows() {
            batch
        } else {
            self.rows_filtered.add(batch.num_rows() - keep.true_count());
            filter_record_batch(&batch, &keep)?
        };
        Ok(batch.project(&(0..self.output_columns).collect::<Vec<_>>())?)
    }
}

#[derive(Debug)]
struct FilterDeletedRowsPlanner {
    parquet: Box<dyn MorselPlanner>,
    filter: PositionFilter,
}

impl MorselPlanner for FilterDeletedRowsPlanner {
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
        let Self { parquet, filter } = *self;
        let Some(mut plan) = parquet.plan()? else {
            return Ok(None);
        };
        let morsels = plan
            .take_morsels()
            .into_iter()
            .map(|parquet| {
                Box::new(FilterDeletedRowsMorsel {
                    parquet,
                    filter: filter.clone(),
                }) as Box<dyn Morsel>
            })
            .collect();
        let planners = plan
            .take_ready_planners()
            .into_iter()
            .map(|parquet| {
                Box::new(Self {
                    parquet,
                    filter: filter.clone(),
                }) as Box<dyn MorselPlanner>
            })
            .collect();
        if let Some(pending) = plan.take_pending_planner() {
            plan.set_pending_planner(async move {
                Ok(Box::new(Self {
                    parquet: pending.await?,
                    filter,
                }) as Box<dyn MorselPlanner>)
            });
        }
        Ok(Some(plan.with_morsels(morsels).with_planners(planners)))
    }
}

#[derive(Debug)]
struct FilterDeletedRowsMorsel {
    parquet: Box<dyn Morsel>,
    filter: PositionFilter,
}

impl Morsel for FilterDeletedRowsMorsel {
    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
        let filter = self.filter;
        Box::pin(
            self.parquet
                .into_stream()
                .and_then(move |batch| futures::future::ready(filter.apply(batch))),
        )
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::config::TableParquetOptions;
    use datafusion::datasource::object_store::ObjectStoreUrl;
    use datafusion::datasource::physical_plan::{
        FileGroup, FileGroupPartitioner, FileScanConfigBuilder, ParquetSource,
    };
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
    use datafusion::prelude::SessionContext;
    use datafusion_physical_expr::expressions::{BinaryExpr, lit};
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use parquet::arrow::{ArrowWriter, RowNumber};
    use parquet::file::properties::{EnabledStatistics, WriterProperties};

    use super::*;
    use crate::deletion_vector::z85::z85_encode_padded;
    use crate::spec::StorageType;

    #[test]
    fn selections_bound_fragmentation_and_preserve_physical_positions() {
        let bitmap = DeletionVectorBitmap::from_row_indices([0, 1, 5, 6, 9]);
        let selection = deletion_vector_selection(&bitmap, 12);
        assert_eq!(
            selection,
            Some(RowSelection::from(vec![
                RowSelector::skip(2),
                RowSelector::select(3),
                RowSelector::skip(2),
                RowSelector::select(2),
                RowSelector::skip(1),
                RowSelector::select(2),
            ]))
        );
        let fragmented = DeletionVectorBitmap::from_row_indices((0..20_000).step_by(2));
        assert!(deletion_vector_selection(&fragmented, 20_000).is_none());
        assert!(deletion_vector_selection(&bitmap, 9).is_none());
    }

    async fn scan_with_dv(
        physical_rows: Option<usize>,
        projection: Vec<usize>,
        limit: Option<usize>,
    ) -> Result<(Vec<RecordBatch>, MetricsSet)> {
        let store = Arc::new(InMemory::new());
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..1024))],
        )?;
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(128))
            .set_data_page_row_count_limit(32)
            .set_write_batch_size(32)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut writer = ArrowWriter::try_new(Vec::new(), Arc::clone(&schema), Some(properties))?;
        writer.write(&batch)?;
        let bytes = writer.into_inner()?;
        let file_size = bytes.len() as u64;
        store
            .put(&Path::from("table/data.parquet"), bytes.into())
            .await?;

        let bitmap =
            DeletionVectorBitmap::from_row_indices([0, 511, 512, 513, 600, 700, 767, 1023]);
        let bytes = bitmap
            .serialize()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let descriptor = DeletionVectorDescriptor {
            storage_type: StorageType::Inline,
            path_or_inline_dv: z85_encode_padded(&bytes)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
            offset: None,
            size_in_bytes: bytes.len() as i32,
            cardinality: bitmap.len() as i64,
        };
        let table_schema = TableSchema::builder(schema)
            .with_virtual_columns(vec![Arc::new(
                Field::new("row_index", DataType::Int64, false).with_extension_type(RowNumber),
            )])
            .build();
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("id", 0)),
                Operator::GtEq,
                lit(520i64),
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("id", 0)),
                Operator::Lt,
                lit(750i64),
            )),
        ));
        let mut options = TableParquetOptions::default();
        options.global.pushdown_filters = true;
        let parquet = ParquetSource::new(table_schema)
            .with_table_parquet_options(options)
            .with_predicate(predicate);
        let source = Arc::new(DeltaParquetSource::new(
            Arc::new(parquet),
            1,
            Url::parse("memory://dv/table/").map_err(|e| DataFusionError::External(Box::new(e)))?,
        ));
        let metrics = source.metrics().clone();
        let file = PartitionedFile::new("table/data.parquet", file_size).with_extension(
            DeltaFileDeletionVector {
                descriptor,
                physical_rows,
            },
        );
        let groups = vec![FileGroup::new(vec![file])];
        let groups = FileGroupPartitioner::new()
            .with_target_partitions(if limit.is_some() { 1 } else { 4 })
            .with_repartition_file_min_size(0)
            .repartition_file_groups(&groups)
            .unwrap_or(groups);
        let store_url = ObjectStoreUrl::parse("memory://dv")?;
        let ctx = SessionContext::new();
        ctx.register_object_store(store_url.as_ref(), store);
        let config = FileScanConfigBuilder::new(store_url, source)
            .with_file_groups(groups)
            .with_projection_indices(Some(projection))?
            .with_limit(limit)
            .build();
        let batches = collect(DataSourceExec::from_data_source(config), ctx.task_ctx()).await?;
        Ok((batches, metrics.clone_inner()))
    }

    #[tokio::test]
    async fn parquet_pruning_splits_and_dv_preserve_original_row_numbers() -> Result<()> {
        for physical_rows in [Some(1024), None] {
            let (batches, metrics) = scan_with_dv(physical_rows, vec![1, 0], None).await?;
            let mut actual = Vec::new();
            for batch in batches {
                let positions = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("missing row index".into()))?;
                let ids = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("missing id".into()))?;
                assert_eq!(positions.values(), ids.values());
                actual.extend_from_slice(ids.values());
            }
            actual.sort_unstable();
            let expected = (520..750)
                .filter(|id| ![512, 513, 600, 700, 767].contains(id))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
            let metric = if physical_rows.is_some() {
                "dv_row_selections"
            } else {
                "dv_position_filters"
            };
            assert!(
                metrics
                    .sum_by_name(metric)
                    .is_some_and(|value| value.as_usize() > 0)
            );
            assert_eq!(
                metrics
                    .sum_by_name("dv_bitmap_loads")
                    .map(|value| value.as_usize()),
                Some(1)
            );
            assert!(
                matches!(metrics.sum_by_name("row_groups_pruned_statistics"), Some(MetricValue::PruningMetrics { pruning_metrics, .. }) if pruning_metrics.pruned() > 0)
            );
            assert!(
                metrics
                    .sum_by_name("pushdown_rows_pruned")
                    .is_some_and(|value| value.as_usize() > 0)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn zero_column_limit_counts_live_rows_on_both_dv_paths() -> Result<()> {
        for physical_rows in [Some(1024), None] {
            let (batches, _) = scan_with_dv(physical_rows, vec![], Some(1)).await?;
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            assert!(batches.iter().all(|batch| batch.num_columns() == 0));
        }
        Ok(())
    }
}
