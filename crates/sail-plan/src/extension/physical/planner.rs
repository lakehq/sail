use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{internal_err, Result};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion_physical_expr::{create_physical_sort_exprs, LexOrdering};
use sail_streaming::logical_plan::sink::StreamSinkNode;
use sail_streaming::logical_plan::source::StreamSourceNode;
use sail_streaming::physical_plan::sink::StreamSinkExec;
use sail_streaming::physical_plan::source::StreamSourceExec;

use crate::extension::logical::{
    FileWriteNode, MapPartitionsNode, RangeNode, SchemaPivotNode, ShowStringNode,
    SortWithinPartitionsNode,
};
use crate::extension::physical::create_file_write_physical_plan;
use crate::extension::physical::map_partitions::MapPartitionsExec;
use crate::extension::physical::range::RangeExec;
use crate::extension::physical::schema_pivot::SchemaPivotExec;
use crate::extension::physical::show_string::ShowStringExec;

pub(crate) struct ExtensionPhysicalPlanner {}

#[async_trait]
impl ExtensionPlanner for ExtensionPhysicalPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let plan: Arc<dyn ExecutionPlan> =
            if let Some(node) = node.as_any().downcast_ref::<RangeNode>() {
                Arc::new(RangeExec::new(
                    node.range().clone(),
                    node.num_partitions(),
                    node.schema().inner().clone(),
                ))
            } else if let Some(node) = node.as_any().downcast_ref::<ShowStringNode>() {
                let [input] = physical_inputs else {
                    return internal_err!("ShowStringExec requires exactly one physical input");
                };
                Arc::new(ShowStringExec::new(
                    input.clone(),
                    node.names().to_vec(),
                    node.limit(),
                    node.format().clone(),
                    node.schema().inner().clone(),
                ))
            } else if let Some(node) = node.as_any().downcast_ref::<MapPartitionsNode>() {
                let [input] = physical_inputs else {
                    return internal_err!("MapPartitionsExec requires exactly one physical input");
                };
                Arc::new(MapPartitionsExec::new(
                    input.clone(),
                    node.udf().clone(),
                    node.schema().inner().clone(),
                ))
            } else if let Some(node) = node.as_any().downcast_ref::<SortWithinPartitionsNode>() {
                let [input] = physical_inputs else {
                    return internal_err!("SortExec requires exactly one physical input");
                };
                let expr = create_physical_sort_exprs(
                    node.sort_expr(),
                    node.schema(),
                    session_state.execution_props(),
                )?;
                let Some(ordering) = LexOrdering::new(expr) else {
                    return internal_err!("SortExec requires at least one sort expression");
                };
                let sort = SortExec::new(ordering, input.clone())
                    .with_fetch(node.fetch())
                    .with_preserve_partitioning(true);
                Arc::new(sort)
            } else if let Some(node) = node.as_any().downcast_ref::<SchemaPivotNode>() {
                let [input] = physical_inputs else {
                    return internal_err!("SchemaPivotExec requires exactly one physical input");
                };
                Arc::new(SchemaPivotExec::new(
                    input.clone(),
                    node.names().to_vec(),
                    node.schema().inner().clone(),
                ))
            } else if let Some(node) = node.as_any().downcast_ref::<FileWriteNode>() {
                let [logical_input] = logical_inputs else {
                    return internal_err!("FileWriteNode requires exactly one logical input");
                };
                let [physical_input] = physical_inputs else {
                    return internal_err!("FileWriteNode requires exactly one physical input");
                };
                create_file_write_physical_plan(
                    session_state,
                    planner,
                    logical_input,
                    physical_input.clone(),
                    node.options().clone(),
                )
                .await?
            } else if node.as_any().is::<StreamSourceNode>() {
                let [input] = physical_inputs else {
                    return internal_err!("StreamSourceExec requires exactly one physical input");
                };
                Arc::new(StreamSourceExec::new(input.clone()))
            } else if node.as_any().is::<StreamSinkNode>() {
                let [input] = physical_inputs else {
                    return internal_err!("StreamSinkExec requires exactly one physical input");
                };
                Arc::new(StreamSinkExec::try_new(
                    input.clone(),
                    node.schema().inner().clone(),
                )?)
            } else {
                return internal_err!("unsupported logical extension node: {:?}", node);
            };
        Ok(Some(plan))
    }
}
