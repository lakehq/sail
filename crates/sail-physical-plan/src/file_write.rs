use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion_common::Result;
use datafusion_expr::LogicalPlan;
use sail_common_datafusion::datasource::{
    create_sort_order, PhysicalSinkMode, SinkInfo, SinkMode, SinkTarget, TableFormatRegistry,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_logical_plan::file_write::{FileWriteOptions, FileWriteTarget};

pub async fn create_file_write_physical_plan(
    ctx: &SessionState,
    _planner: &dyn PhysicalPlanner,
    logical_input: &LogicalPlan,
    physical_input: Arc<dyn ExecutionPlan>,
    options: FileWriteOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let FileWriteOptions {
        target,
        mode,
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
    let format = match &target {
        FileWriteTarget::Table(table) => table.format().to_string(),
        FileWriteTarget::Path { format, .. } | FileWriteTarget::Sink { format, .. } => {
            format.clone()
        }
    };
    let sort_by = match &target {
        FileWriteTarget::Table(table) => table.sort_by().iter().cloned().map(Into::into).collect(),
        FileWriteTarget::Path { sort_by, .. } | FileWriteTarget::Sink { sort_by, .. } => {
            sort_by.clone()
        }
    };
    let sort_order = create_sort_order(ctx, sort_by, logical_input.schema())?;
    let target = match target {
        FileWriteTarget::Table(table) => SinkTarget::Table(table),
        FileWriteTarget::Path {
            path,
            partition_by,
            bucket_by,
            table_properties,
            ..
        } => SinkTarget::Path {
            path,
            partition_by,
            bucket_by,
            table_properties,
        },
        FileWriteTarget::Sink {
            partition_by,
            bucket_by,
            table_properties,
            ..
        } => SinkTarget::Sink {
            partition_by,
            bucket_by,
            table_properties,
        },
    };
    let info = SinkInfo {
        target,
        input: physical_input,
        mode,
        sort_order,
        // TODO: detect duplicated keys in each set of options
        options: options
            .into_iter()
            .map(|x| x.into_iter().collect())
            .collect(),
    };
    let registry = ctx.extension::<TableFormatRegistry>()?;
    registry.get(&format)?.create_writer(ctx, info).await
}
