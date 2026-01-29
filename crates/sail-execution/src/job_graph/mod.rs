mod planner;

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, PhysicalExpr};

/// A job graph represents a distributed execution plan for a job.
/// A job consists of multiple *stages*, where each stage has one or more
/// *partitions*. There are *tasks* which each corresponds to the execution of a single partition
/// of a stage and can have multiple *attempts*.
/// Each task produces output split into multiple *channels*.
#[derive(Debug)]
pub struct JobGraph {
    /// A list of stages sorted in topological order.
    /// For any stage, all its input stages are guaranteed to
    /// appear before it in the list.
    stages: Vec<Stage>,
    /// The output schema of the job.
    schema: SchemaRef,
}

impl JobGraph {
    pub fn stages(&self) -> &[Stage] {
        &self.stages
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
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
                        InputMode::Forward | InputMode::Shuffle => 1,
                        InputMode::Merge | InputMode::Broadcast => {
                            x.plan.output_partitioning().partition_count()
                        }
                    })
            })
            .sum::<usize>();
        // ensure one replica for final stages for the job output
        replicas.max(1)
    }
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

#[derive(Debug)]
pub struct Stage {
    pub inputs: Vec<StageInput>,
    pub plan: Arc<dyn ExecutionPlan>,
    /// The name of the "slot sharing group" for the stage.
    pub group: String,
    pub mode: OutputMode,
    pub distribution: OutputDistribution,
    pub placement: TaskPlacement,
}

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
}

impl fmt::Display for InputMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            InputMode::Forward => write!(f, "Forward"),
            InputMode::Merge => write!(f, "Merge"),
            InputMode::Shuffle => write!(f, "Shuffle"),
            InputMode::Broadcast => write!(f, "Broadcast"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum OutputMode {
    Pipelined,
    #[expect(unused)]
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
    RoundRobin {
        channels: usize,
    },
}

impl OutputDistribution {
    pub fn channels(&self) -> usize {
        match self {
            OutputDistribution::Hash { channels, .. } => *channels,
            OutputDistribution::RoundRobin { channels } => *channels,
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
            OutputDistribution::RoundRobin { channels } => {
                write!(f, "RoundRobin(channels={})", channels)
            }
        }
    }
}
