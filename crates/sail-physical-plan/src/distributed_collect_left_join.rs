use std::any::Any;
use std::sync::Arc;

use datafusion::common::{JoinType, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::joins::utils::{build_join_schema, ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::JoinOn;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{internal_err, NullEquality};

#[derive(Debug, Clone)]
pub struct DistributedCollectLeftJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    filter: Option<JoinFilter>,
    join_type: JoinType,
    projection: Option<Vec<usize>>,
    null_equality: NullEquality,
    join_schema: Arc<datafusion::arrow::datatypes::Schema>,
    column_indices: Vec<ColumnIndex>,
    properties: PlanProperties,
}

impl DistributedCollectLeftJoinExec {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        projection: Option<Vec<usize>>,
        null_equality: NullEquality,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        let (join_schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, &join_type);
        let join_schema = Arc::new(join_schema);
        let properties = PlanProperties::new(
            EquivalenceProperties::new(join_schema.clone()),
            right.output_partitioning().clone(),
            right.pipeline_behavior(),
            right.boundedness(),
        );
        Ok(Self {
            left,
            right,
            on,
            filter,
            join_type,
            projection,
            null_equality,
            join_schema,
            column_indices,
            properties,
        })
    }

    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }

    pub fn on(&self) -> &JoinOn {
        &self.on
    }

    pub fn filter(&self) -> Option<&JoinFilter> {
        self.filter.as_ref()
    }

    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub fn projection(&self) -> Option<&[usize]> {
        self.projection.as_deref()
    }

    pub fn null_equality(&self) -> NullEquality {
        self.null_equality
    }

    pub fn join_schema(&self) -> Arc<datafusion::arrow::datatypes::Schema> {
        self.join_schema.clone()
    }

    pub fn column_indices(&self) -> &[ColumnIndex] {
        &self.column_indices
    }
}

impl DisplayAs for DistributedCollectLeftJoinExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "DistributedCollectLeftJoinExec")
    }
}

impl ExecutionPlan for DistributedCollectLeftJoinExec {
    fn name(&self) -> &str {
        "DistributedCollectLeftJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return internal_err!(
                "DistributedCollectLeftJoinExec expects two children, got {}",
                children.len()
            );
        }
        let right = children.pop().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "DistributedCollectLeftJoinExec expects two children, got 1".to_string(),
            )
        })?;
        let left = children.pop().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "DistributedCollectLeftJoinExec expects two children, got 0".to_string(),
            )
        })?;
        Ok(Arc::new(Self::try_new(
            left,
            right,
            self.on.clone(),
            self.filter.clone(),
            self.join_type,
            self.projection.clone(),
            self.null_equality,
        )?))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        internal_err!("DistributedCollectLeftJoinExec should be rewritten before execution")
    }
}
