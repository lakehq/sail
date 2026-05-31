use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, LexRequirement, Partitioning,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::physical_planner::PhysicalPlanner;
use datafusion_common::{plan_err, DFSchemaRef, DataFusionError, Result};
use datafusion_expr::LogicalPlan;
use futures::TryStreamExt;
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::catalog::{CatalogPartitionField, TableKind};
use sail_common_datafusion::datasource::{
    create_sort_order, find_path_in_options, BucketBy, OptionLayer, PhysicalSinkMode, SinkInfo,
    SinkMode, TableFormatRegistry,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_logical_plan::file_write::FileWriteOptions;

pub async fn create_file_write_physical_plan(
    ctx: &SessionState,
    _planner: &dyn PhysicalPlanner,
    logical_input: &LogicalPlan,
    physical_input: Arc<dyn ExecutionPlan>,
    options: FileWriteOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let FileWriteOptions {
        format,
        mode,
        catalog_table,
        partition_by,
        sort_by,
        bucket_by,
        options,
    } = options;
    let mode = match mode {
        SinkMode::ErrorIfExists => PhysicalSinkMode::ErrorIfExists,
        SinkMode::IgnoreIfExists => PhysicalSinkMode::IgnoreIfExists,
        SinkMode::Append => PhysicalSinkMode::Append,
        SinkMode::Overwrite => PhysicalSinkMode::Overwrite,
        SinkMode::OverwriteIf { condition } => {
            let source = condition.source.clone();
            PhysicalSinkMode::OverwriteIf {
                condition: Some(condition),
                source,
            }
        }
        SinkMode::OverwritePartitions => PhysicalSinkMode::OverwritePartitions,
    };
    let sort_order = create_sort_order(ctx, sort_by, logical_input.schema())?;
    if let Some(catalog_table) = catalog_table.filter(|_| find_path_in_options(&options).is_none())
    {
        return Ok(Arc::new(DeferredCatalogTableWriteExec::new(
            ctx.clone(),
            physical_input,
            DeferredCatalogTableWriteInfo {
                format,
                mode,
                catalog_table,
                partition_by,
                bucket_by,
                sort_order,
                options,
                logical_schema: logical_input.schema().clone(),
            },
        )));
    }
    let info = SinkInfo {
        input: physical_input,
        mode,
        partition_by,
        bucket_by,
        sort_order,
        // TODO: detect duplicated keys in each set of options
        options,
        logical_schema: Some(logical_input.schema().clone()),
    };
    let registry = ctx.extension::<TableFormatRegistry>()?;
    registry.get(&format)?.create_writer(ctx, info).await
}

#[derive(Debug, Clone)]
struct DeferredCatalogTableWriteInfo {
    format: String,
    mode: PhysicalSinkMode,
    catalog_table: Vec<String>,
    partition_by: Vec<CatalogPartitionField>,
    bucket_by: Option<BucketBy>,
    sort_order: Option<LexRequirement>,
    options: Vec<OptionLayer>,
    logical_schema: DFSchemaRef,
}

#[derive(Debug)]
struct DeferredCatalogTableWriteExec {
    state: SessionState,
    input: Arc<dyn ExecutionPlan>,
    info: DeferredCatalogTableWriteInfo,
    properties: Arc<PlanProperties>,
}

impl DeferredCatalogTableWriteExec {
    fn new(
        state: SessionState,
        input: Arc<dyn ExecutionPlan>,
        info: DeferredCatalogTableWriteInfo,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::empty())),
            Partitioning::UnknownPartitioning(
                input.properties().output_partitioning().partition_count(),
            ),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            state,
            input,
            info,
            properties,
        }
    }
}

impl DisplayAs for DeferredCatalogTableWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}: table={}",
            self.name(),
            self.info.catalog_table.join(".")
        )
    }
}

impl ExecutionPlan for DeferredCatalogTableWriteExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        if self.info.format.eq_ignore_ascii_case("iceberg") {
            vec![Distribution::SinglePartition]
        } else {
            vec![Distribution::UnspecifiedDistribution]
        }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match (children.pop(), children.is_empty()) {
            (Some(child), true) => Ok(Arc::new(Self::new(
                self.state.clone(),
                child,
                self.info.clone(),
            ))),
            _ => plan_err!("{} should have exactly one child", self.name()),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let state = self.state.clone();
        let input = self.input.clone();
        let info = self.info.clone();
        let schema = self.schema();
        let output = futures::stream::once(async move {
            let manager = context.extension::<CatalogManager>()?;
            let status = manager
                .get_table(&info.catalog_table)
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            let TableKind::Table {
                location: Some(location),
                ..
            } = status.kind
            else {
                return Err(DataFusionError::Execution(format!(
                    "table does not have a location: {}",
                    info.catalog_table.join(".")
                )));
            };
            let mut options = info.options;
            options.push(OptionLayer::OptionList {
                items: vec![("path".to_string(), location)],
            });
            let sink = SinkInfo {
                input,
                mode: info.mode,
                partition_by: info.partition_by,
                bucket_by: info.bucket_by,
                sort_order: info.sort_order,
                options,
                logical_schema: Some(info.logical_schema),
            };
            let registry = context.extension::<TableFormatRegistry>()?;
            let plan = registry
                .get(&info.format)?
                .create_writer(&state, sink)
                .await?;
            plan.execute(partition, context)
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }
}
