use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, PhysicalExpr};

mod planner;

pub struct JobGraph {
    stages: Vec<Stage>,
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
        self.stages
            .iter()
            .flat_map(|x| {
                x.inputs
                    .iter()
                    .filter(|input| input.stage == stage)
                    .map(|input| match input.mode {
                        InputMode::Forward | InputMode::Shuffle => 1,
                        InputMode::Broadcast => x.plan.output_partitioning().partition_count(),
                    })
            })
            .sum()
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
            writeln!(f, "distribution={}", stage.distribution)?;
            writeln!(f, "placement={}", stage.placement)?;
            writeln!(f, "{}", displayable.indent(true))?;
        }
        Ok(())
    }
}

pub struct Stage {
    pub inputs: Vec<StageInput>,
    pub plan: Arc<dyn ExecutionPlan>,
    pub group: String,
    pub mode: OutputMode,
    pub distribution: OutputDistribution,
    pub placement: TaskPlacement,
}

#[derive(Clone, Copy)]
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

#[derive(Clone)]
pub struct StageInput {
    pub stage: usize,
    pub mode: InputMode,
}

impl fmt::Display for StageInput {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StageInput(stage={}, mode={})", self.stage, self.mode)
    }
}

#[derive(Clone, Copy)]
pub enum InputMode {
    /// For each partition in the current stage, read all channels from the
    /// same partition in the input stage.
    Forward,
    /// For each partition in the current stage, read data from the corresponding
    /// channel of all partitions in the input stage.
    Shuffle,
    /// For each partition in the current stage, read all channels from all
    /// partitions in the input stage.
    Broadcast,
}

impl fmt::Display for InputMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            InputMode::Forward => write!(f, "Forward"),
            InputMode::Shuffle => write!(f, "Shuffle"),
            InputMode::Broadcast => write!(f, "Broadcast"),
        }
    }
}

#[derive(Clone, Copy)]
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

#[derive(Clone)]
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
