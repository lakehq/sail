use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::datasource::TableProvider;
use datafusion::execution::SessionState;
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::{LogicalPlan, TableScan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::internal_err;
use sail_common_datafusion::datasource::{
    create_sort_order, PhysicalSinkInfo, PhysicalSinkMode, SinkMode,
};

use crate::logical::IcebergTableSource;
use crate::{IcebergTableFormat, IcebergWriteNode};

pub struct IcebergTablePhysicalPlanner;

#[async_trait]
impl ExtensionPlanner for IcebergTablePhysicalPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<IcebergWriteNode>() else {
            return Ok(None);
        };
        let [logical_input] = logical_inputs else {
            return internal_err!("IcebergWriteNode requires exactly one logical input");
        };
        let [physical_input] = physical_inputs else {
            return internal_err!("IcebergWriteNode requires exactly one physical input");
        };

        let sort_order = create_sort_order(session_state, node.sort_order().to_vec(), logical_input.schema())?;
        let mode = to_physical_sink_mode(node.mode());

        let info = PhysicalSinkInfo {
            input: physical_input.clone(),
            mode,
            partition_by: node.partition_by().to_vec(),
            bucket_by: node.bucket_by().cloned(),
            sort_order,
            options: node.options().to_vec(),
            logical_schema: Some(logical_input.schema().clone()),
        };

        let plan = IcebergTableFormat::create_physical_writer(session_state, info).await?;
        Ok(Some(plan))
    }

    async fn plan_table_scan(
        &self,
        _planner: &dyn PhysicalPlanner,
        scan: &TableScan,
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(source) = scan.source.as_any().downcast_ref::<IcebergTableSource>() else {
            return Ok(None);
        };
        let filters = unnormalize_cols(scan.filters.clone());
        let plan = source
            .provider()
            .scan(
                session_state,
                scan.projection.as_ref(),
                &filters,
                scan.fetch,
            )
            .await?;
        Ok(Some(plan))
    }
}

fn to_physical_sink_mode(mode: &SinkMode) -> PhysicalSinkMode {
    match mode.clone() {
        SinkMode::ErrorIfExists => PhysicalSinkMode::ErrorIfExists,
        SinkMode::IgnoreIfExists => PhysicalSinkMode::IgnoreIfExists,
        SinkMode::Append => PhysicalSinkMode::Append,
        SinkMode::Overwrite => PhysicalSinkMode::Overwrite,
        SinkMode::OverwriteIf { condition } => PhysicalSinkMode::OverwriteIf {
            source: condition.source.clone(),
            condition: Some(condition),
        },
        SinkMode::OverwritePartitions => PhysicalSinkMode::OverwritePartitions,
    }
}
