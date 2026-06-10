use std::sync::Arc;

use datafusion::common::{DFSchema, Result};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, TableScan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use crate::physical_plan::SystemTableExec;
use crate::table_source::SystemTableSource;

pub struct SystemTablePhysicalPlanner;

#[async_trait::async_trait]
impl ExtensionPlanner for SystemTablePhysicalPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        _node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    async fn plan_table_scan(
        &self,
        planner: &dyn PhysicalPlanner,
        scan: &TableScan,
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(source) = scan.source.as_any().downcast_ref::<SystemTableSource>() else {
            return Ok(None);
        };
        let table = source.table();
        let schema = Arc::new(DFSchema::try_from_qualified_schema(
            scan.table_name.clone(),
            &table.schema(),
        )?);
        let filters = scan
            .filters
            .iter()
            .map(|e| planner.create_physical_expr(e, &schema, session_state))
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(Arc::new(SystemTableExec::try_new(
            table,
            scan.projection.clone(),
            filters,
            scan.fetch,
        )?)))
    }
}
