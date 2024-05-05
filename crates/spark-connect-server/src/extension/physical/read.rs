use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::context::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, EmptyRecordBatchStream, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use tonic::async_trait;

use crate::extension::logical::{UnresolvedRelation, UnresolvedRelationNode};

#[derive(Debug)]
struct UnresolvedRelationExec {
    unresolved_relation: UnresolvedRelation,
    schema: SchemaRef,
    cache: PlanProperties,
}

impl DisplayAs for UnresolvedRelationExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "UnresolvedRelationExec")
    }
}

impl ExecutionPlan for UnresolvedRelationExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                "UnresolvedRelationExec should have no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema: SchemaRef = self.schema.clone();
        Ok(Box::pin(EmptyRecordBatchStream::new(schema)))
    }
}

pub(crate) struct UnresolvedRelationPlanner {}

#[async_trait]
impl ExtensionPlanner for UnresolvedRelationPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let node: &UnresolvedRelationNode = node
            .as_any()
            .downcast_ref::<UnresolvedRelationNode>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "UnresolvedRelationPlanner can only handle UnresolvedRelationNode".to_string(),
                )
            })?;
        let schema: SchemaRef = Arc::new(UserDefinedLogicalNode::schema(node).as_ref().into());
        let cache: PlanProperties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );
        Ok(Some(Arc::new(UnresolvedRelationExec {
            unresolved_relation: node.unresolved_relation().clone(),
            schema: schema,
            cache: cache,
        })))
    }
}
