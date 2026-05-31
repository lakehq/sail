use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::internal_err;
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};

use crate::formats::console::{ConsoleSinkExec, ConsoleWriteNode};
use crate::formats::noop::{NoopSinkExec, NoopWriteNode};
use crate::formats::python::{
    PythonDataSourceWriteCommitExec, PythonDataSourceWriteExec, PythonWriteNode,
};

/// Physical planner for data source write logical nodes produced by `TableFormat::create_writer`.
#[derive(Debug, Default)]
pub struct DataSourceWritePhysicalPlanner;

#[async_trait]
impl ExtensionPlanner for DataSourceWritePhysicalPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> datafusion_common::Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(_node) = node.as_any().downcast_ref::<ConsoleWriteNode>() {
            let [input] = physical_inputs else {
                return internal_err!("ConsoleWriteNode requires exactly one physical input");
            };
            return Ok(Some(Arc::new(ConsoleSinkExec::new(input.clone()))));
        }

        if let Some(_node) = node.as_any().downcast_ref::<NoopWriteNode>() {
            let [input] = physical_inputs else {
                return internal_err!("NoopWriteNode requires exactly one physical input");
            };
            return Ok(Some(Arc::new(NoopSinkExec::new(input.clone()))));
        }

        if let Some(node) = node.as_any().downcast_ref::<PythonWriteNode>() {
            let [input] = physical_inputs else {
                return internal_err!("PythonWriteNode requires exactly one physical input");
            };

            let expected_partitions = input.properties().partitioning.partition_count();
            let pickled_writer = node.pickled_writer().to_vec();
            let write_exec = Arc::new(PythonDataSourceWriteExec::new(
                input.clone(),
                pickled_writer.clone(),
                node.is_arrow(),
            ));
            let commit_exec = Arc::new(PythonDataSourceWriteCommitExec::new(
                write_exec,
                pickled_writer,
                expected_partitions,
            ));
            return Ok(Some(commit_exec));
        }

        Ok(None)
    }
}
