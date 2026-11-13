use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::Result;
use sail_common_datafusion::utils::items::ItemTaker;

/// Evaluates each expression occurrence once per input batch. The private projector
/// uses DataFusion's CASE short circuit without exposing a fusible ProjectionExec.
#[derive(Debug)]
pub struct EvaluateOnceExec {
    projection: ProjectionExec,
    properties: Arc<PlanProperties>,
}

impl EvaluateOnceExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        expressions: Vec<ProjectionExpr>,
        schema: &Schema,
    ) -> Result<Self> {
        let projection = ProjectionExec::try_new_with_schema_metadata(expressions, input, schema)?;
        // Do not publish expression equivalences: downstream rewrites must use slots,
        // not substitute their producers (including distinct volatile occurrences).
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(projection.schema()),
            Partitioning::UnknownPartitioning(
                projection
                    .properties()
                    .output_partitioning()
                    .partition_count(),
            ),
            projection.properties().emission_type,
            projection.properties().boundedness,
        ));
        Ok(Self {
            projection,
            properties,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        self.projection.input()
    }
    pub fn expr(&self) -> &[ProjectionExpr] {
        self.projection.expr()
    }
    pub fn projection(&self) -> &ProjectionExec {
        &self.projection
    }
}

impl DisplayAs for EvaluateOnceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "EvaluateOnceExec: expr=[{}]",
            self.expr()
                .iter()
                .map(|projection| format!("{} as {}", projection.expr, projection.alias))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl ExecutionPlan for EvaluateOnceExec {
    fn name(&self) -> &'static str {
        "EvaluateOnceExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![self.input()]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new(
            children.one()?,
            self.expr().to_vec(),
            &self.schema(),
        )?))
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.projection.apply_expressions(f)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.projection.execute(partition, context)
    }
}
