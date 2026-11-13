#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, reset_plan_states};
use datafusion::physical_plan::scalar_subquery::{ScalarSubqueryExec, ScalarSubqueryLink};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{Result, exec_err, internal_err};
use futures::StreamExt;
use sail_common_datafusion::replay::{ReplayId, ReplayPartitionBuilder, ReplayRegistry};
use sail_common_datafusion::utils::items::ItemTaker;

/// Unbound marker consumed by the local binder or the distributed job planner.
/// It deliberately cannot execute its child: doing so would cross the cluster
/// stage boundary and evaluate the named relation once per reader.
#[derive(Debug, Clone)]
pub struct ReplayExec {
    id: ReplayId,
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl ReplayExec {
    pub fn try_new(id: ReplayId, input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        if input.properties().boundedness != Boundedness::Bounded {
            return exec_err!("replay requires a bounded producer");
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            input.output_partitioning().clone(),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            id,
            input,
            properties,
        })
    }

    pub fn id(&self) -> ReplayId {
        self.id
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for ReplayExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ReplayExec: {}", self.id)
    }
}

impl ExecutionPlan for ReplayExec {
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
    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children.one()?;
        if input.schema() != self.schema() {
            return internal_err!("replay schema changed: {}", self.id);
        }
        Ok(Arc::new(Self::try_new(self.id, input)?))
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        internal_err!(
            "unbound replay {}; the job runner must resolve its producer",
            self.id
        )
    }
}

#[derive(Debug, Clone)]
struct ReplayReaderExec {
    id: ReplayId,
    registry: Arc<ReplayRegistry>,
    properties: Arc<PlanProperties>,
}

impl DisplayAs for ReplayReaderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ReplayReaderExec: {}, epoch={:?}",
            self.id,
            self.registry.epoch()
        )
    }
}

impl ExecutionPlan for ReplayReaderExec {
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
    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        children.zero()?;
        Ok(self)
    }
    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.registry
            .open_reader(self.id, partition, context.runtime_env().as_ref())
            .inspect_err(|_| self.registry.retire())
    }
}

/// Owns both preparation cancellation and the handoff to the root stream. There
/// are no detached producer tasks; dropping this object cannot leave a writer
/// that publishes into a retired epoch.
enum Preparation {
    Replay(ReplayId, Arc<dyn ExecutionPlan>),
    Scalar(Arc<dyn ExecutionPlan>),
}

pub struct LocalReplayBinding {
    registry: Arc<ReplayRegistry>,
    plan: Arc<dyn ExecutionPlan>,
    producers: Vec<Preparation>,
    handed_off: bool,
}

impl LocalReplayBinding {
    pub fn try_new(
        plan: Arc<dyn ExecutionPlan>,
        registry: Arc<ReplayRegistry>,
        context: &TaskContext,
    ) -> Result<Self> {
        let mut binding = Self {
            registry,
            plan: plan.clone(),
            producers: vec![],
            handed_off: false,
        };
        let plan = binding.bind(plan, context, &mut HashSet::new())?;
        // Replacing children can preserve operator caches (e.g. HashJoin's
        // build future). Reset the entire consumer tree for this epoch too;
        // bound replay readers retain their registry and sealed attempts.
        binding.plan = reset_plan_states(plan)?;
        Ok(binding)
    }

    fn bind(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
        context: &TaskContext,
        visiting: &mut HashSet<ReplayId>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if plan.is::<ReplayReaderExec>() {
            return internal_err!("cannot bind an already execution-scoped replay reader");
        }
        // Deduplicate before descending: Arc addresses and plan hashes are not
        // identities. Dependencies are appended first, giving a topological order.
        if let Some(replay) = plan.downcast_ref::<ReplayExec>() {
            let id = replay.id();
            if visiting.contains(&id) {
                return internal_err!("cyclic replay dependency: {id}");
            }
            if self.registry.register(
                id,
                replay.schema(),
                replay.properties().output_partitioning().clone(),
                context.runtime_env().as_ref(),
            )? {
                visiting.insert(id);
                let producer = self.bind(replay.input().clone(), context, visiting)?;
                // Start a fresh execution, including stateful leaves. Nested
                // dependencies are already readers: their reset retains the
                // current epoch and never resets a sealed producer.
                let producer = reset_plan_states(producer)?;
                visiting.remove(&id);
                self.producers.push(Preparation::Replay(id, producer));
            }
            return Ok(Arc::new(ReplayReaderExec {
                id,
                registry: self.registry.clone(),
                properties: replay.properties.clone(),
            }));
        }
        if let Some(scalar) = plan.downcast_ref::<ScalarSubqueryExec>() {
            // ScalarSubqueryExec normally initializes its shared slots before its
            // main input runs. Eager replay preparation must respect that same
            // ordering, including replay dependencies inside scalar subqueries.
            let subqueries = scalar
                .subqueries()
                .iter()
                .map(|link| {
                    Ok(ScalarSubqueryLink {
                        plan: self.bind(link.plan.clone(), context, visiting)?,
                        index: link.index,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let initializer: Arc<dyn ExecutionPlan> = Arc::new(ScalarSubqueryExec::new(
                Arc::new(EmptyExec::new(scalar.input().schema())),
                subqueries,
                scalar.results().clone(),
            ));
            self.producers
                .push(Preparation::Scalar(reset_plan_states(initializer)?));
            // The initializer owns this scope; retaining a second coordinator in
            // the root would execute the subquery again after replay publication.
            return self.bind(scalar.input().clone(), context, visiting);
        }
        if plan.children().is_empty() {
            return Ok(plan);
        }
        let children = plan
            .children()
            .into_iter()
            .map(|child| self.bind(child.clone(), context, visiting))
            .collect::<Result<Vec<_>>>()?;
        plan.replace_children(
            children,
            datafusion::physical_plan::ReplaceChildrenOptions::new(
                datafusion::physical_plan::ChildrenPropertiesMode::Keep,
            ),
        )
    }

    pub async fn prepare(&self, context: Arc<TaskContext>) -> Result<()> {
        let result = self.prepare_inner(context).await;
        if result.is_err() {
            self.registry.retire();
        }
        result
    }

    async fn prepare_inner(&self, context: Arc<TaskContext>) -> Result<()> {
        // One producer at a time, one append at a time. Poll every partition:
        // draining them sequentially can deadlock a bounded RepartitionExec.
        // Every live writer and the partition index are pool-accounted.
        for preparation in &self.producers {
            let (id, producer) = match preparation {
                Preparation::Replay(id, producer) => (id, producer),
                Preparation::Scalar(initializer) => {
                    let mut stream = initializer.execute(0, context.clone())?;
                    loop {
                        let next = tokio::select! {
                            biased;
                            _ = self.registry.cancellation().cancelled() => return exec_err!("replay preparation cancelled"),
                            next = stream.next() => next,
                        };
                        match next {
                            Some(batch) => {
                                batch?;
                            }
                            None => break,
                        }
                    }
                    continue;
                }
            };
            self.registry.begin(*id)?;
            let count = producer.output_partitioning().partition_count();
            let mut builders = Vec::with_capacity(count);
            let mut streams = futures::stream::SelectAll::new();
            for partition in 0..count {
                builders.push(ReplayPartitionBuilder::try_new(
                    producer.schema(),
                    context.runtime_env().clone(),
                    partition,
                )?);
                streams.push(
                    producer
                        .execute(partition, context.clone())?
                        .map(move |batch| (partition, batch)),
                );
            }
            loop {
                let next = tokio::select! {
                    biased;
                    _ = self.registry.cancellation().cancelled() => return exec_err!("replay preparation cancelled"),
                    next = streams.next() => next,
                };
                match next {
                    Some((partition, batch)) => builders[partition].append(batch?)?,
                    None => break,
                }
            }
            let partitions = builders
                .into_iter()
                .map(|builder| builder.seal().map(Arc::new))
                .collect::<Result<Vec<_>>>()?;
            self.registry.publish(*id, partitions)?;
        }
        Ok(())
    }

    pub fn plan(&self) -> Arc<dyn ExecutionPlan> {
        self.plan.clone()
    }

    pub fn finish(mut self, stream: SendableRecordBatchStream) -> SendableRecordBatchStream {
        self.handed_off = true;
        self.registry.wrap_stream(stream, true)
    }
}

impl Drop for LocalReplayBinding {
    fn drop(&mut self) {
        if !self.handed_off {
            self.registry.retire();
        }
    }
}
