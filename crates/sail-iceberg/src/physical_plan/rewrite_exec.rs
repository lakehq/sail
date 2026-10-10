use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, exec_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use futures::TryStreamExt;

/// Runs complete reader/writer groups sequentially within each worker task.
#[derive(Debug)]
pub struct IcebergRewriteExec {
    input: Arc<dyn ExecutionPlan>,
    assignments: Arc<Vec<Vec<usize>>>,
    properties: Arc<PlanProperties>,
}

impl IcebergRewriteExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, assignments: Vec<Vec<usize>>) -> Result<Self> {
        if assignments.is_empty() || assignments.iter().any(Vec::is_empty) {
            return plan_err!("Iceberg rewrite tasks require nonempty group assignments");
        }
        let groups = input.output_partitioning().partition_count();
        let mut assigned = vec![false; groups];
        for group in assignments.iter().flatten() {
            let Some(seen) = assigned.get_mut(*group) else {
                return plan_err!("Iceberg rewrite group {group} is out of range");
            };
            if std::mem::replace(seen, true) {
                return plan_err!("Iceberg rewrite group {group} is assigned more than once");
            }
        }
        if assigned.iter().any(|seen| !seen) {
            return plan_err!("Iceberg rewrite group assignment is incomplete");
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(assignments.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            assignments: Arc::new(assignments),
            properties,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn assignments(&self) -> &[Vec<usize>] {
        &self.assignments
    }
}

impl DisplayAs for IcebergRewriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "IcebergRewriteExec: concurrent_groups={}, groups={}",
            self.assignments.len(),
            self.input.output_partitioning().partition_count()
        )
    }
}

impl ExecutionPlan for IcebergRewriteExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input]: [Arc<dyn ExecutionPlan>; 1] = children.try_into().map_err(|_| {
            datafusion::common::plan_datafusion_err!("Iceberg rewrite runner requires one input")
        })?;
        Ok(Arc::new(Self::try_new(
            input,
            self.assignments.as_ref().clone(),
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.assignments.len() {
            return exec_err!("Invalid Iceberg rewrite task partition {partition}");
        }
        let assignments = self.assignments.clone();
        let input = self.input.clone();
        let stream = async_stream::try_stream! {
            for group in &assignments[partition] {
                let mut output = input.execute(*group, context.clone())?;
                while let Some(batch) = output.try_next().await? {
                    yield batch;
                }
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::sync::Mutex;

    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::Schema;

    use super::*;

    #[derive(Debug)]
    struct TrackedGroups {
        active: Arc<Mutex<Vec<usize>>>,
        started: Arc<Mutex<Vec<usize>>>,
        fail: Option<usize>,
        properties: Arc<PlanProperties>,
    }

    struct ActiveGroup(usize, Arc<Mutex<Vec<usize>>>);
    impl Drop for ActiveGroup {
        fn drop(&mut self) {
            self.1
                .lock()
                .expect("active groups")
                .retain(|group| *group != self.0);
        }
    }

    impl TrackedGroups {
        fn new(fail: Option<usize>) -> Arc<Self> {
            Arc::new(Self {
                active: Default::default(),
                started: Default::default(),
                fail,
                properties: Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(Arc::new(Schema::empty())),
                    Partitioning::UnknownPartitioning(4),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                )),
            })
        }
    }

    impl DisplayAs for TrackedGroups {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "TrackedGroups")
        }
    }

    impl ExecutionPlan for TrackedGroups {
        fn name(&self) -> &'static str {
            Self::static_name()
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }
        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            assert!(children.is_empty());
            Ok(self)
        }
        fn execute(
            &self,
            partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            self.started.lock().expect("started groups").push(partition);
            self.active.lock().expect("active groups").push(partition);
            let guard = ActiveGroup(partition, self.active.clone());
            let fail = self.fail == Some(partition);
            let schema = self.schema();
            let batch_schema = schema.clone();
            let stream = async_stream::try_stream! {
                let _guard = guard;
                yield RecordBatch::new_empty(batch_schema);
                if fail { Err(datafusion::common::exec_datafusion_err!("group failed"))?; }
            };
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
        }
    }

    #[tokio::test]
    async fn runs_one_group_per_task_and_drops_active_group_on_cancellation() -> Result<()> {
        let input = TrackedGroups::new(None);
        let runner = IcebergRewriteExec::try_new(input.clone(), vec![vec![0, 2], vec![1, 3]])?;
        let context = Arc::new(TaskContext::default());
        let mut first = runner.execute(0, context.clone())?;
        let mut second = runner.execute(1, context.clone())?;
        assert!(input.started.lock().expect("started").is_empty());
        assert!(first.try_next().await?.is_some());
        assert!(second.try_next().await?.is_some());
        assert_eq!(*input.active.lock().expect("active"), vec![0, 1]);
        assert!(first.try_next().await?.is_some());
        assert_eq!(*input.active.lock().expect("active"), vec![1, 2]);
        drop(first);
        assert_eq!(*input.active.lock().expect("active"), vec![1]);
        assert_eq!(second.try_collect::<Vec<_>>().await?.len(), 1);
        assert!(input.active.lock().expect("active").is_empty());
        assert_eq!(*input.started.lock().expect("started"), vec![0, 1, 2, 3]);
        assert!(runner.execute(2, context).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn failure_stops_later_groups_and_assignment_must_cover_input_once() -> Result<()> {
        let input = TrackedGroups::new(Some(0));
        for assignments in [
            vec![],
            vec![vec![]],
            vec![vec![0, 1, 2]],
            vec![vec![0, 1, 2, 4]],
            vec![vec![0, 1, 2, 3, 3]],
        ] {
            assert!(IcebergRewriteExec::try_new(input.clone(), assignments).is_err());
        }
        let runner = IcebergRewriteExec::try_new(input.clone(), vec![vec![0, 1, 2, 3]])?;
        assert!(
            runner
                .execute(0, Arc::new(TaskContext::default()))?
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
        assert_eq!(*input.started.lock().expect("started"), vec![0]);
        assert!(input.active.lock().expect("active").is_empty());
        Ok(())
    }
}
