// SPDX-License-Identifier: Apache-2.0

mod planner;

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::internal_datafusion_err;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_expr::{Partitioning, PhysicalExpr, RangePartitioning};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PiecewiseMergeJoinExec,
};
use datafusion::physical_plan::scalar_subquery::ScalarSubqueryExec;
use datafusion::physical_plan::union::{InterleaveExec, UnionExec};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use sail_physical_plan::barrier::BarrierExec;

use crate::error::{ExecutionError, ExecutionResult};
use crate::plan::StageInputExec;
use crate::shuffle::ShuffleBackendKind;

#[derive(Debug, Clone)]
pub struct JobGraphOptions {
    pub shuffle_backend: ShuffleBackendKind,
}

/// A job graph represents a distributed execution plan for a job.
/// A job consists of multiple *stages*, where each stage has one or more
/// *partitions*. There are *tasks* which each corresponds to the execution of a single partition
/// of a stage and can have multiple *attempts*.
/// Each task produces output split into multiple *channels*.
#[derive(Debug, Clone)]
pub struct JobGraph {
    /// A list of stages sorted in topological order.
    /// For any stage, all its input stages are guaranteed to
    /// appear before it in the list.
    stages: Vec<Stage>,
    /// The output schema of the job.
    schema: SchemaRef,
    options: JobGraphOptions,
    /// The range shuffles of the job, in the order of their writer stages.
    range_shuffles: Vec<RangeShuffle>,
}

/// A range shuffle, whose channels are the partitions of Spark's `RangePartitioner` for
/// its split points. Their number is known once the split points are picked, which the
/// driver does in a stage of its own, as Spark does. The driver then reads the split
/// points and gives their number to the shuffle and to the stages that read it (see
/// [`JobGraph::resize_range_shuffle`]). Until then, the shuffle and the stages that read
/// it, directly or not, wait. Spark plans these stages after the split points too.
///
/// The stages that the split points read, directly or not, run before them. The other
/// stages read these stages only once they have run, so that no task region holds both
/// a stage that the split points need and a stage that waits for them.
#[derive(Debug, Clone)]
pub struct RangeShuffle {
    /// The stage that picks the split points, which runs on the driver.
    pub split_points: usize,
    /// The stage that writes the shuffle, with one channel per partition.
    pub writer: usize,
    /// For each stage, whether it runs before the split points are known. These are
    /// the split points and the stages that they read, directly or not.
    before: Vec<bool>,
    /// For each stage, whether it waits for the split points. These are the shuffle and
    /// the stages that read it, directly or not.
    after: Vec<bool>,
}

impl RangeShuffle {
    pub(crate) fn new(split_points: usize, writer: usize) -> Self {
        Self {
            split_points,
            writer,
            before: vec![],
            after: vec![],
        }
    }

    /// Finds the stages that run before and after the split points, once all stages are
    /// planned. The stages are in topological order.
    pub(crate) fn find_stages(&mut self, stages: &[Stage]) {
        let mut before = vec![false; stages.len()];
        let mut pending = vec![self.split_points];
        while let Some(stage) = pending.pop() {
            if !before[stage] {
                before[stage] = true;
                pending.extend(stages[stage].inputs.iter().map(|input| input.stage));
            }
        }
        let mut after = vec![false; stages.len()];
        for (s, stage) in stages.iter().enumerate() {
            after[s] = s == self.writer || stage.inputs.iter().any(|input| after[input.stage]);
        }
        self.before = before;
        self.after = after;
    }
}

impl JobGraph {
    pub fn stages(&self) -> &[Stage] {
        &self.stages
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(crate) fn shuffle_backend(&self) -> &ShuffleBackendKind {
        &self.options.shuffle_backend
    }

    pub fn range_shuffles(&self) -> &[RangeShuffle] {
        &self.range_shuffles
    }

    /// Whether the stage waits for the split points of the given range shuffle.
    pub fn waits_for_range_shuffle(&self, stage: usize, shuffle: usize) -> bool {
        self.range_shuffles[shuffle].after[stage]
    }

    /// Whether the consumer stage reads the producer stage only after the producer has
    /// run, since the producer runs before the split points of a range shuffle and the
    /// consumer waits for them. The stages are then not pipelined, and the producer keeps
    /// its output until the consumer reads it.
    pub fn reads_after_range_split_points(&self, producer: usize, consumer: usize) -> bool {
        self.range_shuffles
            .iter()
            .any(|shuffle| shuffle.before[producer] && !shuffle.before[consumer])
    }

    /// Gives the range shuffle its number of partitions, and changes the partitions of the
    /// stages that read the shuffle, directly or not, accordingly. Returns the writer and downstream stages whose
    /// partitioning or inputs changed. None of them has run, since they all wait for
    /// the split points of the shuffle.
    ///
    /// A stage reads as many partitions of the input as the input has channels for a
    /// shuffle or a broadcast, and as many as the input has partitions when it reads each
    /// partition or merges them. A rescaled input keeps its partitions, but no more than
    /// the input has, as a coalesce in Spark.
    pub fn resize_range_shuffle(
        &mut self,
        shuffle: usize,
        partitions: usize,
    ) -> ExecutionResult<Vec<usize>> {
        let writer = self.range_shuffles[shuffle].writer;
        let OutputDistribution::Channel { channels } = &mut self.stages[writer].distribution else {
            return Err(ExecutionError::InternalError(format!(
                "range shuffle stage {writer} does not write to channels"
            )));
        };
        if partitions == 0 || partitions > *channels {
            return Err(ExecutionError::InternalError(format!(
                "range shuffle stage {writer} with {channels} channels cannot have {partitions} partitions"
            )));
        }
        // The old and new number of channels or partitions of each stage that has changed.
        if *channels == partitions {
            return Ok(vec![]);
        }
        let changed_channels = HashMap::from([(writer, (*channels, partitions))]);
        *channels = partitions;
        let mut changed_partitions: HashMap<usize, (usize, usize)> = HashMap::new();
        let mut changed = vec![writer];
        for s in writer + 1..self.stages.len() {
            let stage = &mut self.stages[s];
            // The old and new number of partitions of each input of the stage that changes.
            let mut inputs = HashMap::new();
            for (i, input) in stage.inputs.iter_mut().enumerate() {
                let change = match &mut input.mode {
                    InputMode::Shuffle | InputMode::Broadcast => {
                        changed_channels.get(&input.stage).copied()
                    }
                    InputMode::Forward | InputMode::Merge => {
                        changed_partitions.get(&input.stage).copied()
                    }
                    InputMode::Rescale { partitions } => {
                        match changed_partitions.get(&input.stage) {
                            Some(&(_, new)) if new < *partitions => {
                                let old = *partitions;
                                *partitions = new;
                                Some((old, new))
                            }
                            _ => None,
                        }
                    }
                };
                if let Some(change) = change {
                    inputs.insert(i, change);
                }
            }
            if inputs.is_empty() {
                continue;
            }
            let old = stage.plan.output_partitioning().partition_count();
            stage.plan = Arc::clone(&stage.plan)
                .transform_up(|plan| {
                    let Some(input) = plan.downcast_ref::<StageInputExec<usize>>() else {
                        return Ok(Transformed::no(plan));
                    };
                    let Some(&(old, new)) = inputs.get(input.input()) else {
                        return Ok(Transformed::no(plan));
                    };
                    let properties = input.properties();
                    let partitioning = match properties.output_partitioning() {
                        Partitioning::UnknownPartitioning(n) if *n == old => {
                            Partitioning::UnknownPartitioning(new)
                        }
                        Partitioning::RoundRobinBatch(n) if *n == old => {
                            Partitioning::RoundRobinBatch(new)
                        }
                        other => {
                            return Err(internal_datafusion_err!(
                                "cannot read {new} partitions of a stage input with {other} \
                                 instead of {old} partitions"
                            ));
                        }
                    };
                    let properties = properties.as_ref().clone().with_partitioning(partitioning);
                    Ok(Transformed::yes(Arc::new(StageInputExec::new(
                        *input.input(),
                        Arc::new(properties),
                    ))))
                })?
                .data;
            let new = stage.plan.output_partitioning().partition_count();
            if new != old {
                changed_partitions.insert(s, (old, new));
            }
            changed.push(s);
        }
        Ok(changed)
    }

    /// Get the required number of output replicas for the given stage.
    pub fn replicas(&self, stage: usize) -> usize {
        let replicas = self
            .stages
            .iter()
            .flat_map(|x| {
                x.inputs
                    .iter()
                    .filter(|input| input.stage == stage)
                    .map(|input| match input.mode {
                        InputMode::Forward | InputMode::Shuffle | InputMode::Rescale { .. } => 1,
                        InputMode::Merge | InputMode::Broadcast => {
                            x.plan.output_partitioning().partition_count()
                        }
                    })
            })
            .sum::<usize>();
        // The driver reads the split points of a range shuffle for their number.
        let driver_reads = self
            .range_shuffles
            .iter()
            .filter(|shuffle| shuffle.split_points == stage)
            .count();
        // ensure one replica for final stages for the job output
        (replicas + driver_reads).max(1)
    }

    /// Whether the tasks of the given stage wait for the readers of their output
    /// when the readers fall behind.
    ///
    /// The tasks can only wait when each part of the output has a single reader that
    /// takes the data of all its producers in the order of arrival. This is not the
    /// case when the output is the result of the job, which is read at the pace of
    /// the client, when the output is read in full by several tasks or stages, or when
    /// the output is merged in order, so that the reader decides which producer it
    /// reads next, or when the reader runs after the stage, which happens for a stage
    /// that runs before the split points of a range shuffle that the reader waits for.
    /// Besides, the tasks of the reading stage must read all the inputs of the stage in a
    /// fixed order.
    pub fn bounded_output(&self, stage: usize) -> bool {
        let mut readers = self.stages.iter().enumerate().flat_map(|(r, reader)| {
            let inputs = reader.inputs.iter();
            inputs
                .filter(move |input| input.stage == stage)
                .map(move |input| (r, reader, input))
        });
        match (readers.next(), readers.next()) {
            (Some((r, _, _)), None) if self.reads_after_range_split_points(stage, r) => false,
            (Some((_, reader, input)), None) => match input.mode {
                InputMode::Forward | InputMode::Shuffle | InputMode::Rescale { .. } => {
                    reads_inputs_in_fixed_order(&reader.plan)
                }
                InputMode::Merge | InputMode::Broadcast => false,
            },
            _ => false,
        }
    }
}

/// Whether a task that runs the plan reads the inputs of the plan one after another
/// in an order that does not depend on the data, each one to its end, and gives up
/// an input that it does not read before it reads a later one.
///
/// This depends on the operators that have several inputs. It is known for the
/// operators listed here, and has to be shown for an operator before it is added.
/// A hash join reads its build side to the end, and drops it, before its probe
/// side. The joins that collect one side read that side first, and it is broadcast.
/// A union reads one input in each task, an interleave takes the data of all its
/// inputs as it comes, and a barrier runs its inputs to the end one by one.
/// Among others, a sort-merge join is not listed: it decides by the data which
/// input it reads next.
fn reads_inputs_in_fixed_order(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let children = plan.children();
    let known = plan.is::<HashJoinExec>()
        || plan.is::<NestedLoopJoinExec>()
        || plan.is::<CrossJoinExec>()
        || plan.is::<PiecewiseMergeJoinExec>()
        || plan.is::<UnionExec>()
        || plan.is::<InterleaveExec>()
        || plan.is::<ScalarSubqueryExec>()
        || plan.is::<BarrierExec>();
    (children.len() < 2 || known) && children.into_iter().all(reads_inputs_in_fixed_order)
}

impl fmt::Display for JobGraph {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (i, stage) in self.stages.iter().enumerate() {
            let displayable = DisplayableExecutionPlan::new(stage.plan.as_ref());
            writeln!(f, "=== stage {i} ===")?;
            writeln!(
                f,
                "inputs=[{}]",
                stage
                    .inputs
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
            if !stage.group.is_empty() {
                writeln!(f, "group={}", stage.group)?;
            }
            writeln!(f, "mode={}", stage.mode)?;
            writeln!(
                f,
                "partitions={}",
                stage.plan.output_partitioning().partition_count()
            )?;
            writeln!(f, "distribution={}", stage.distribution)?;
            writeln!(f, "placement={}", stage.placement)?;
            writeln!(f, "{}", displayable.indent(true))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Stage {
    pub inputs: Vec<StageInput>,
    pub plan: Arc<dyn ExecutionPlan>,
    /// The name of the "slot sharing group" for the stage.
    pub group: String,
    pub mode: OutputMode,
    pub distribution: OutputDistribution,
    pub placement: TaskPlacement,
}

/// Specifies whether a task must run on the driver or on any available worker node.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum TaskPlacement {
    Driver,
    Worker,
}

impl fmt::Display for TaskPlacement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TaskPlacement::Driver => write!(f, "Driver"),
            TaskPlacement::Worker => write!(f, "Worker"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StageInput {
    pub stage: usize,
    pub mode: InputMode,
}

impl From<&StageInput> for sail_system_store::types::StageInput {
    fn from(input: &StageInput) -> Self {
        Self {
            stage: input.stage as u64,
            mode: input.mode.to_string(),
        }
    }
}

impl fmt::Display for StageInput {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StageInput(stage={}, mode={})", self.stage, self.mode)
    }
}

/// Usually, when partition `p` is executed for the stage, all the child physical plan nodes
/// are executed recursively with the same partition `p`, but this is not always the case.
/// So we introduce input mode to describe the expectation how stage inputs (which are
/// leaf nodes of the physical plan) are executed and which partitions and channels are read
/// from the dependent stages.
#[derive(Debug, Clone, Copy)]
pub enum InputMode {
    /// For each partition in the current stage, execute the same partition to fetch the input
    /// which reads all channels from the corresponding partition in the input stage.
    Forward,
    /// For each partition in the current stage, execute all partitions to fetch the input
    /// which each reads all channels from the corresponding partition in the input stage.
    Merge,
    /// For each partition in the current stage, execute the same partition to fetch the input
    /// which reads data from the corresponding channel of all partitions in the input stage.
    Shuffle,
    /// For each partition in the current stage, execute a single partition to fetch the input
    /// which reads all channels from all partitions in the input stage.
    Broadcast,
    /// For each partition of this stage input, execute a contiguous subset of producer
    /// partitions and read all channels from each. The target partition count belongs to
    /// this input, which may differ from the containing stage (e.g. under a union).
    Rescale { partitions: usize },
}

impl fmt::Display for InputMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            InputMode::Forward => write!(f, "Forward"),
            InputMode::Merge => write!(f, "Merge"),
            InputMode::Shuffle => write!(f, "Shuffle"),
            InputMode::Broadcast => write!(f, "Broadcast"),
            InputMode::Rescale { partitions } => write!(f, "Rescale(partitions={partitions})"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum OutputMode {
    Pipelined,
    Blocking,
}

impl fmt::Display for OutputMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OutputMode::Pipelined => write!(f, "Pipelined"),
            OutputMode::Blocking => write!(f, "Blocking"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum OutputDistribution {
    Hash {
        keys: Vec<Arc<dyn PhysicalExpr>>,
        channels: usize,
    },
    /// Range distribution into `channels` channels. The partitioning can have fewer
    /// partitions than channels, and then the last channels receive no rows.
    Range {
        partitioning: RangePartitioning,
        channels: usize,
    },
    RoundRobinBatch {
        channels: usize,
    },
    /// Row-level round-robin distribution for explicit user repartition calls.
    /// Unlike `RoundRobinBatch`, this distributes individual rows across
    /// output partitions to ensure even data distribution.
    RoundRobinRow {
        channels: usize,
    },
    /// Each row names its channel in its last column, which is not written
    /// (see [`sail_physical_plan::range_split::RangeChannelExec`]).
    Channel {
        channels: usize,
    },
}

impl OutputDistribution {
    pub fn channels(&self) -> usize {
        match self {
            OutputDistribution::Hash { channels, .. } => *channels,
            OutputDistribution::Range { channels, .. } => *channels,
            OutputDistribution::RoundRobinBatch { channels } => *channels,
            OutputDistribution::RoundRobinRow { channels } => *channels,
            OutputDistribution::Channel { channels } => *channels,
        }
    }
}

impl fmt::Display for OutputDistribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutputDistribution::Hash { keys, channels } => {
                let keys = keys.iter().map(|k| k.to_string()).collect::<Vec<_>>();
                write!(f, "Hash(keys=[{}], channels={})", keys.join(", "), channels)
            }
            OutputDistribution::Range {
                partitioning,
                channels,
            } => write!(f, "{partitioning}, channels={channels}"),
            OutputDistribution::RoundRobinBatch { channels } => {
                write!(f, "RoundRobinBatch(channels={})", channels)
            }
            OutputDistribution::RoundRobinRow { channels } => {
                write!(f, "RoundRobinRow(channels={})", channels)
            }
            OutputDistribution::Channel { channels } => write!(f, "Channel(channels={channels})"),
        }
    }
}
