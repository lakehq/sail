use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use futures::StreamExt;
use url::Url;

use crate::operations::vacuum::{
    collect_referenced_paths, delete_files, find_vacuum_candidates, resolve_retention_hours,
    retention_cutoff_ms, MIN_RETENTION_HOURS,
};
use crate::spec::TableFeature;
use crate::storage::{get_object_store_from_context, StorageConfig};
use crate::table::open_table_with_object_store_and_table_config;

fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("path", DataType::Utf8, false)]))
}

/// Physical execution node for Delta Lake `VACUUM`.
///
/// Listing, candidate selection, and optional deletion all happen inside the
/// single-partition stream produced by this node.
#[derive(Debug)]
pub struct DeltaVacuumExec {
    table_url: Url,
    retention_hours: Option<u64>,
    dry_run: bool,
    /// Skip the minimum-retention safety guard (requires explicit opt-in via table property).
    skip_retention_check: bool,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    cache: Arc<PlanProperties>,
}

impl DeltaVacuumExec {
    pub fn new(
        table_url: Url,
        retention_hours: Option<u64>,
        dry_run: bool,
        skip_retention_check: bool,
    ) -> Self {
        let schema = output_schema();
        let eq = EquivalenceProperties::new(schema.clone());
        let cache = Arc::new(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            table_url,
            retention_hours,
            dry_run,
            skip_retention_check,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }

    fn make_metrics(&self) -> VacuumMetrics {
        VacuumMetrics {
            num_files_to_delete: MetricBuilder::new(&self.metrics).global_counter("num_files_to_delete"),
            size_of_data_to_delete: MetricBuilder::new(&self.metrics).global_counter("size_of_data_to_delete"),
            num_deleted_files: MetricBuilder::new(&self.metrics).global_counter("num_deleted_files"),
        }
    }
}

struct VacuumMetrics {
    num_files_to_delete: Count,
    size_of_data_to_delete: Count,
    num_deleted_files: Count,
}

impl DisplayAs for DeltaVacuumExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DeltaVacuumExec: url={}, retention_hours={:?}, dry_run={}",
            self.table_url, self.retention_hours, self.dry_run
        )
    }
}

impl ExecutionPlan for DeltaVacuumExec {
    fn name(&self) -> &str {
        "DeltaVacuumExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![]
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let table_url = self.table_url.clone();
        let retention_hours_opt = self.retention_hours;
        let dry_run = self.dry_run;
        let skip_retention_check = self.skip_retention_check;
        let schema = self.schema.clone();
        let vacuumed = self.make_metrics();

        let stream = futures::stream::once(async move {
            let object_store = get_object_store_from_context(&context, &table_url)?;
            let storage_config = StorageConfig;
            let prefixed_store = storage_config
                .decorate_store(Arc::clone(&object_store), &table_url)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let table_config = crate::kernel::DeltaSnapshotConfig {
                require_files: true,
                ..Default::default()
            };
            let table = open_table_with_object_store_and_table_config(
                table_url.clone(),
                Arc::new(prefixed_store),
                StorageConfig,
                table_config,
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let snapshot = table
                .snapshot()
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .clone();
            let log_store = table.log_store();

            if snapshot
                .protocol()
                .has_writer_feature(&TableFeature::VacuumProtocolCheck)
            {
                return Err(DataFusionError::Plan(
                    "VACUUM: table requires the 'vacuumProtocolCheck' writer feature, \
                     which is not yet supported."
                        .to_string(),
                ));
            }

            let table_props = snapshot.table_properties();
            let effective_retention = resolve_retention_hours(
                retention_hours_opt,
                table_props.deleted_file_retention_duration(),
            );

            // NOTE: Spark surfaces this guard as a session-level config
            // (spark.databricks.delta.retentionDurationCheck.enabled).  Sail
            // exposes it as the table property
            // 'delta.vacuum.retentionDurationCheck.enabled' instead, which is
            // non-standard but avoids adding a new session-config surface.
            if !skip_retention_check && effective_retention < MIN_RETENTION_HOURS {
                return Err(DataFusionError::Plan(format!(
                    "VACUUM: retention of {effective_retention} hours is less than the \
                     minimum allowed threshold of {MIN_RETENTION_HOURS} hours. \
                     To override this safety check, set the table property \
                     'delta.vacuum.retentionDurationCheck.enabled' to 'false'."
                )));
            }

            let cutoff = retention_cutoff_ms(effective_retention);

            let referenced =
                collect_referenced_paths(snapshot.as_ref(), log_store.as_ref(), cutoff)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let raw_store = log_store.object_store(None);
            let candidates = find_vacuum_candidates(&raw_store, &referenced, cutoff)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            vacuumed
                .num_files_to_delete
                .add(candidates.len());
            vacuumed
                .size_of_data_to_delete
                .add(candidates.iter().map(|(_, s)| *s as usize).sum());

            let batch = if dry_run {
                let paths: Vec<&str> = candidates.iter().map(|(p, _)| p.as_ref()).collect();
                RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(paths))])
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
            } else {
                let paths_only: Vec<object_store::path::Path> =
                    candidates.into_iter().map(|(p, _)| p).collect();
                let deleted = delete_files(raw_store, paths_only)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                vacuumed.num_deleted_files.add(deleted);
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(StringArray::from(vec![table_url.as_str()]))],
                )
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
            };

            Ok::<RecordBatch, DataFusionError>(batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream.boxed(),
        )))
    }
}
