use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlanProperties;
use sail_common_datafusion::error::CommonErrorCause;
use sail_system_store::types::TaskOutputStatistics;

use crate::driver::job_scheduler::topology::JobTopology;
use crate::driver::output::{JobOutputManager, JobOutputOutcome};
use crate::error::ExecutionResult;
use crate::job_graph::JobGraph;

/// Tracks graph/topology and runtime state for a single job.
pub struct JobDescriptor {
    pub context: Arc<TaskContext>,
    pub graph: JobGraph,
    pub topology: JobTopology,
    pub stages: Vec<StageDescriptor>,
    pub regions: Vec<TaskRegionDescriptor>,
    pub state: JobState,
}

pub enum JobState {
    Running { output: JobOutputManager },
    Draining,
    Succeeded,
    Failed,
    Canceled,
}

impl JobState {
    pub fn finish_output(&mut self, outcome: JobOutputOutcome) {
        match self {
            JobState::Running { .. } | JobState::Draining => {
                *self = match outcome {
                    JobOutputOutcome::Completed => JobState::Succeeded,
                    JobOutputOutcome::Failed => JobState::Failed,
                    JobOutputOutcome::Canceled => JobState::Canceled,
                };
            }
            JobState::Succeeded | JobState::Failed | JobState::Canceled => {}
        }
    }

    pub fn status(&self) -> &'static str {
        match self {
            JobState::Running { .. } => "RUNNING",
            JobState::Draining => "DRAINING",
            JobState::Succeeded => "SUCCEEDED",
            JobState::Failed => "FAILED",
            JobState::Canceled => "CANCELED",
        }
    }
}

impl JobDescriptor {
    pub fn try_new(
        graph: JobGraph,
        state: JobState,
        context: Arc<TaskContext>,
    ) -> ExecutionResult<Self> {
        let mut stages = vec![];
        for stage in graph.stages().iter() {
            let mut descriptor = StageDescriptor {
                tasks: vec![],
                state: StageState::Active,
            };
            for _ in 0..stage.plan.output_partitioning().partition_count() {
                descriptor.tasks.push(TaskDescriptor { attempts: vec![] });
            }
            stages.push(descriptor);
        }
        let topology = JobTopology::try_new(&graph)?;
        let regions = (0..topology.regions.len())
            .map(|_| TaskRegionDescriptor {
                state: TaskRegionState::Running,
            })
            .collect();
        Ok(Self {
            context,
            graph,
            topology,
            stages,
            regions,
            state,
        })
    }
}

#[derive(Debug)]
pub struct StageDescriptor {
    /// A list of tasks for each partition of the stage.
    pub tasks: Vec<TaskDescriptor>,
    pub state: StageState,
}

#[derive(Debug, Clone, Copy)]
pub enum StageState {
    /// The tasks in the stage are not yet completed,
    /// or the task streams are still being consumed.
    Active,
    /// The tasks in the stage will not be scheduled anymore,
    /// and the task streams are no longer being consumed.
    Inactive,
}

impl StageState {
    pub fn status(&self) -> &'static str {
        match self {
            StageState::Active => "ACTIVE",
            StageState::Inactive => "INACTIVE",
        }
    }
}

#[derive(Debug)]
pub struct TaskRegionDescriptor {
    pub state: TaskRegionState,
}

#[derive(Debug, Clone, Copy)]
pub enum TaskRegionState {
    Running,
    Failed,
    Succeeded,
}

#[derive(Debug)]
pub struct TaskDescriptor {
    pub attempts: Vec<TaskAttemptDescriptor>,
}

impl TaskDescriptor {
    /// Accept a single immutable output report from the current successful attempt.
    /// Missing, stale, malformed, failed and canceled reports remain unknown.
    pub fn record_output(
        &mut self,
        attempt: usize,
        channels: usize,
        statistics: &TaskOutputStatistics,
    ) -> bool {
        if self.attempts.len().checked_sub(1) != Some(attempt)
            || statistics.partitions.len() != channels
        {
            return false;
        }
        let current = &mut self.attempts[attempt];
        if !matches!(current.state, TaskState::Succeeded) || current.output_statistics.is_some() {
            return false;
        }
        current.output_statistics = Some(statistics.clone());
        true
    }
}

#[derive(Debug)]
pub struct TaskAttemptDescriptor {
    pub state: TaskState,
    pub messages: Vec<String>,
    pub cause: Option<CommonErrorCause>,
    /// Whether the task streams are fetched for the job output.
    /// This will always be false if the task does not belong to
    /// the final stages of the job.
    pub job_output_fetched: bool,
    pub output_statistics: Option<TaskOutputStatistics>,
}

#[derive(Debug, Clone, Copy)]
pub enum TaskState {
    /// The task attempt has been created, but is not assigned to any worker.
    /// A task attempt is only created when the task region is eligible for scheduling.
    Created,
    /// The task attempt is scheduled to a worker, but its status is unknown.
    Scheduled,
    /// The task attempt is running on a worker.
    Running,
    /// The task attempt has succeeded.
    Succeeded,
    /// The task attempt has failed.
    Failed,
    /// The task attempt has been canceled.
    Canceled,
}

impl TaskState {
    pub fn status(&self) -> &'static str {
        match self {
            TaskState::Created => "CREATED",
            TaskState::Scheduled => "SCHEDULED",
            TaskState::Running => "RUNNING",
            TaskState::Succeeded => "SUCCEEDED",
            TaskState::Failed => "FAILED",
            TaskState::Canceled => "CANCELED",
        }
    }

    pub fn consolidate(&self, next: Self) -> Self {
        match (self, next) {
            (TaskState::Created, x) => x,
            (TaskState::Scheduled, TaskState::Created) => *self,
            (TaskState::Scheduled, x) => x,
            (TaskState::Running, TaskState::Created | TaskState::Scheduled) => *self,
            (TaskState::Running, x) => x,
            (TaskState::Succeeded | TaskState::Failed | TaskState::Canceled, _) => *self,
        }
    }

    pub fn is_terminal(&self) -> bool {
        match self {
            TaskState::Succeeded | TaskState::Failed | TaskState::Canceled => true,
            TaskState::Created | TaskState::Scheduled | TaskState::Running => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use sail_system_store::types::TaskOutputPartitionStatistics;

    use super::*;

    fn attempt(state: TaskState) -> TaskAttemptDescriptor {
        TaskAttemptDescriptor {
            state,
            messages: vec![],
            cause: None,
            job_output_fetched: false,
            output_statistics: None,
        }
    }

    #[test]
    fn output_statistics_require_current_successful_attempt() {
        let stats = TaskOutputStatistics {
            complete: true,
            partitions: vec![TaskOutputPartitionStatistics::default(); 2],
        };
        for state in [
            TaskState::Created,
            TaskState::Scheduled,
            TaskState::Running,
            TaskState::Failed,
            TaskState::Canceled,
        ] {
            let mut task = TaskDescriptor {
                attempts: vec![attempt(state)],
            };
            assert!(!task.record_output(0, 2, &stats));
            assert!(task.attempts[0].output_statistics.is_none());
        }
        let mut task = TaskDescriptor {
            attempts: vec![attempt(TaskState::Succeeded), attempt(TaskState::Succeeded)],
        };
        assert!(!task.record_output(0, 2, &stats));
        assert!(!task.record_output(2, 2, &stats));
        assert!(!task.record_output(1, 3, &stats));
        assert!(task.record_output(1, 2, &stats));
        let mut duplicate = stats.clone();
        duplicate.partitions[0].num_rows = 123;
        assert!(!task.record_output(1, 2, &duplicate));
        assert_eq!(task.attempts[1].output_statistics.as_ref(), Some(&stats));
        assert!(task.attempts[0].output_statistics.is_none());
        // A retry has no inherited statistics. Its observed prefix may be recorded,
        // but must retain the incomplete marker rather than becoming exact empty.
        task.attempts.push(attempt(TaskState::Succeeded));
        assert!(!task.record_output(1, 2, &stats));
        let partial = TaskOutputStatistics {
            complete: false,
            ..stats
        };
        assert!(task.record_output(2, 2, &partial));
        assert_eq!(task.attempts[2].output_statistics.as_ref(), Some(&partial));
    }
}
