use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::datasource::TableProvider;
use datafusion::execution::SessionState;
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::{LogicalPlan, TableScan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use crate::logical::IcebergTableSource;
use crate::table_format::{IcebergWriteNode, plan_iceberg_write};

pub struct IcebergPhysicalPlanner;

#[async_trait]
impl ExtensionPlanner for IcebergPhysicalPlanner {
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
            return datafusion_common::internal_err!(
                "IcebergWriteNode requires exactly one logical input"
            );
        };
        let [physical_input] = physical_inputs else {
            return datafusion_common::internal_err!(
                "IcebergWriteNode requires exactly one physical input"
            );
        };
        plan_iceberg_write(session_state, logical_input, physical_input.clone(), node)
            .await
            .map(Some)
    }

    async fn plan_table_scan(
        &self,
        _planner: &dyn PhysicalPlanner,
        scan: &TableScan,
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(source) = scan.source.downcast_ref::<IcebergTableSource>() else {
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
