use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use arrow_schema::extension::ExtensionType;
use async_stream::try_stream;
use datafusion::arrow::array::{Array, BooleanArray, Int64Array, RecordBatch};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileOpener, FileScanConfig, FileSource, ParquetSource,
};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::{DisplayFormatType, SortOrderPushdownResult};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, internal_err, plan_datafusion_err};
use datafusion_datasource::morsel::{Morsel, MorselPlan, MorselPlanner, Morselizer};
use futures::TryStreamExt;
use futures::stream::BoxStream;
use object_store::ObjectStore;
use parquet::arrow::RowNumber;
use roaring::RoaringTreemap;

use crate::datasource::file_pruning::{FilePruning, IcebergFilePruner};

/// Parquet scanning with Iceberg's original manifest facts available at execution.
#[derive(Clone)]
pub struct IcebergParquetSource {
    parquet: Arc<dyn FileSource>,
    files: Arc<BTreeMap<String, FilePruning>>,
    deleted_positions: Option<(Arc<RoaringTreemap>, Count)>,
    batch_size: Option<usize>,
}

impl IcebergParquetSource {
    pub(crate) fn new(parquet: Arc<dyn FileSource>, files: BTreeMap<String, FilePruning>) -> Self {
        Self {
            parquet,
            files: Arc::new(files),
            deleted_positions: None,
            batch_size: None,
        }
    }

    pub fn parquet(&self) -> Option<&ParquetSource> {
        self.parquet.downcast_ref::<ParquetSource>()
    }

    pub fn metadata_json(&self) -> Result<String> {
        serde_json::to_string(self.files.as_ref())
            .map_err(|error| plan_datafusion_err!("Iceberg file facts: {error}"))
    }

    pub fn try_from_metadata(parquet: Arc<dyn FileSource>, metadata: &str) -> Result<Self> {
        let files = serde_json::from_str(metadata)
            .map_err(|error| plan_datafusion_err!("Iceberg file facts: {error}"))?;
        Ok(Self::new(parquet, files))
    }

    pub(crate) fn file_pruner(
        &self,
        file: &PartitionedFile,
        predicate: Arc<dyn PhysicalExpr>,
        metric: Count,
    ) -> Option<IcebergFilePruner> {
        Some(IcebergFilePruner::new(
            predicate,
            self.table_schema().table_schema().clone(),
            self.files.get(file.object_meta.location.as_ref())?.clone(),
            metric,
        ))
    }

    fn replace_parquet(&self, parquet: Arc<dyn FileSource>) -> Arc<dyn FileSource> {
        Arc::new(Self {
            parquet,
            files: self.files.clone(),
            deleted_positions: self.deleted_positions.clone(),
            batch_size: self.batch_size,
        })
    }

    pub(crate) fn pruning_predicate(&self, predicate: Arc<dyn PhysicalExpr>) -> Result<Self> {
        let parquet = self
            .parquet()
            .ok_or_else(|| plan_datafusion_err!("Iceberg scan requires Parquet"))?;
        let mut source = self.clone();
        // Deleted rows must not be evaluated by a row predicate that could fail.
        source.parquet = Arc::new(
            parquet
                .with_predicate(predicate)
                .with_pushdown_filters(false),
        );
        Ok(source)
    }

    pub(crate) fn delete_positions(
        &self,
        positions: Arc<RoaringTreemap>,
        rows_read: Count,
    ) -> Self {
        let mut source = self.clone();
        source.deleted_positions = Some((positions, rows_read));
        source
    }
}

impl FileSource for IcebergParquetSource {
    fn create_file_opener(
        &self,
        _store: Arc<dyn ObjectStore>,
        _config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        internal_err!("Iceberg Parquet scans require the morsel interface")
    }

    fn create_morselizer(
        &self,
        store: Arc<dyn ObjectStore>,
        config: &FileScanConfig,
        partition: usize,
    ) -> Result<Box<dyn Morselizer>> {
        let (reader, deletes) = if let Some((positions, rows_read)) = &self.deleted_positions {
            let parquet = self
                .parquet()
                .ok_or_else(|| plan_datafusion_err!("Iceberg scan requires Parquet"))?;
            let table = self.table_schema();
            let mut virtual_columns = table.virtual_columns().to_vec();
            let row_index = table
                .table_schema()
                .fields()
                .iter()
                .position(|field| field.extension_type_name() == Some(RowNumber::NAME))
                .unwrap_or_else(|| {
                    virtual_columns.push(crate::row_level_metadata::parquet_row_position_field(
                        table.table_schema(),
                    ));
                    table.table_schema().fields().len()
                });
            let schema = TableSchema::builder(table.file_schema().clone())
                .with_table_partition_cols(table.table_partition_cols().clone())
                .with_virtual_columns(virtual_columns)
                .build();
            let projection = self.projection().cloned().unwrap_or_else(|| {
                ProjectionExprs::from_indices(
                    &(0..table.table_schema().fields().len()).collect::<Vec<_>>(),
                    table.table_schema(),
                )
            });
            let columns = projection.iter().count();
            let position = schema.table_schema().field(row_index);
            let projection =
                ProjectionExprs::new(projection.iter().cloned().chain([ProjectionExpr::new(
                    Arc::new(Column::new(position.name(), row_index)),
                    position.name(),
                )]));
            let mut positioned = ParquetSource::new(schema)
                .with_table_parquet_options(parquet.table_parquet_options().clone())
                .with_pushdown_filters(false);
            if let Some(factory) = parquet.parquet_file_reader_factory() {
                positioned = positioned.with_parquet_file_reader_factory(factory.clone());
            }
            if let Some(predicate) = self.filter() {
                positioned = positioned.with_predicate(predicate);
            }
            let positioned = positioned
                .try_pushdown_projection(&projection)?
                .ok_or_else(|| plan_datafusion_err!("Iceberg position projection was rejected"))?;
            let mut config = config.clone();
            config.limit = None;
            let positioned = positioned.with_batch_size(self.batch_size.ok_or_else(|| {
                plan_datafusion_err!("Iceberg reader batch size was not initialized")
            })?);
            let reader = positioned.create_morselizer(store, &config, partition)?;
            (
                reader,
                Some(DeletedRows {
                    positions: positions.clone(),
                    columns,
                    rows_read: rows_read.clone(),
                    metric: MetricBuilder::new(self.metrics())
                        .counter("position_delete_rows_filtered", partition),
                }),
            )
        } else {
            (
                self.parquet.create_morselizer(store, config, partition)?,
                None,
            )
        };
        Ok(Box::new(IcebergMorselizer {
            reader: Arc::from(reader),
            source: self.clone(),
            metric: MetricBuilder::new(self.metrics()).counter("iceberg_files_pruned", partition),
            deletes,
        }))
    }

    fn table_schema(&self) -> &TableSchema {
        self.parquet.table_schema()
    }
    fn with_batch_size(&self, size: usize) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.parquet = self.parquet.with_batch_size(size);
        source.batch_size = Some(size);
        Arc::new(source)
    }
    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.parquet.filter()
    }
    fn projection(&self) -> Option<&ProjectionExprs> {
        self.parquet.projection()
    }
    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.parquet.metrics()
    }
    fn file_type(&self) -> &str {
        "parquet"
    }
    fn reorder_files(&self, files: Vec<PartitionedFile>) -> Vec<PartitionedFile> {
        self.parquet.reorder_files(files)
    }
    fn fmt_extra(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.parquet.fmt_extra(t, f)
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.parquet.apply_expressions(f)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        Ok(self
            .parquet
            .try_pushdown_projection(projection)?
            .map(|parquet| self.replace_parquet(parquet)))
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let mut result = self.parquet.try_pushdown_filters(filters, config)?;
        result.updated_node = result
            .updated_node
            .map(|parquet| self.replace_parquet(parquet));
        Ok(result)
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
        properties: &EquivalenceProperties,
    ) -> Result<SortOrderPushdownResult<Arc<dyn FileSource>>> {
        Ok(self
            .parquet
            .try_pushdown_sort(order, properties)?
            .map(|parquet| self.replace_parquet(parquet)))
    }
}

struct IcebergMorselizer {
    reader: Arc<dyn Morselizer>,
    source: IcebergParquetSource,
    metric: Count,
    deletes: Option<DeletedRows>,
}

impl fmt::Debug for IcebergMorselizer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("IcebergMorselizer").finish_non_exhaustive()
    }
}

impl Morselizer for IcebergMorselizer {
    fn plan_file(&self, file: PartitionedFile) -> Result<Box<dyn MorselPlanner>> {
        let pruner = self.source.filter().and_then(|predicate| {
            self.source
                .file_pruner(&file, predicate, self.metric.clone())
        });
        let parquet = self.reader.plan_file(file)?;
        Ok(Box::new(PrunedPlanner {
            parquet,
            pruner,
            deletes: self.deletes.clone(),
        }))
    }
}

#[derive(Debug)]
struct PrunedPlanner {
    parquet: Box<dyn MorselPlanner>,
    pruner: Option<IcebergFilePruner>,
    deletes: Option<DeletedRows>,
}

impl MorselPlanner for PrunedPlanner {
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
        let Self {
            parquet,
            pruner,
            deletes,
        } = *self;
        if should_prune(&pruner)? {
            return Ok(None);
        }
        let Some(mut plan) = parquet.plan()? else {
            return Ok(None);
        };
        let morsels = plan
            .take_morsels()
            .into_iter()
            .map(|parquet| {
                Box::new(PrunedMorsel {
                    parquet,
                    pruner: pruner.clone(),
                    deletes: deletes.clone(),
                }) as Box<dyn Morsel>
            })
            .collect();
        let planners = plan
            .take_ready_planners()
            .into_iter()
            .map(|parquet| {
                Box::new(Self {
                    parquet,
                    pruner: pruner.clone(),
                    deletes: deletes.clone(),
                }) as Box<dyn MorselPlanner>
            })
            .collect();
        if let Some(pending) = plan.take_pending_planner() {
            plan.set_pending_planner(async move {
                Ok(Box::new(Self {
                    parquet: pending.await?,
                    pruner,
                    deletes,
                }) as Box<dyn MorselPlanner>)
            });
        }
        Ok(Some(plan.with_morsels(morsels).with_planners(planners)))
    }
}

#[derive(Debug)]
struct PrunedMorsel {
    parquet: Box<dyn Morsel>,
    pruner: Option<IcebergFilePruner>,
    deletes: Option<DeletedRows>,
}

impl Morsel for PrunedMorsel {
    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
        Box::pin(try_stream! {
            let mut stream = self.parquet.into_stream();
            while !should_prune(&self.pruner)? {
                let Some(batch) = stream.try_next().await? else { break; };
                let batch = match &self.deletes { Some(deletes) => deletes.apply(batch)?, None => batch };
                yield batch;
            }
        })
    }
}

fn should_prune(pruner: &Option<IcebergFilePruner>) -> Result<bool> {
    pruner
        .as_ref()
        .map(IcebergFilePruner::should_prune)
        .transpose()
        .map(|result| result.unwrap_or(false))
}

#[derive(Debug, Clone)]
struct DeletedRows {
    positions: Arc<RoaringTreemap>,
    columns: usize,
    metric: Count,
    rows_read: Count,
}

impl DeletedRows {
    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch> {
        self.rows_read.add(batch.num_rows());
        let positions = batch
            .column(self.columns)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| plan_datafusion_err!("Parquet row number is not Int64"))?;
        if positions.null_count() != 0 || positions.values().iter().any(|position| *position < 0) {
            return internal_err!("Invalid Parquet row number");
        }
        let keep = BooleanArray::from_iter(
            positions
                .values()
                .iter()
                .map(|position| Some(!self.positions.contains(*position as u64))),
        );
        self.metric.add(batch.num_rows() - keep.true_count());
        let batch = filter_record_batch(&batch, &keep)?;
        Ok(batch.project(&(0..self.columns).collect::<Vec<_>>())?)
    }
}
