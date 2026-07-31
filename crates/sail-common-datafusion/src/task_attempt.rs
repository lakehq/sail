use crate::extension::SessionExtension;

/// Identity of the distributed task attempt executing a physical plan partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskAttemptContext {
    job_id: u64,
    stage: usize,
    partition: usize,
    attempt: usize,
}

impl TaskAttemptContext {
    pub fn new(job_id: u64, stage: usize, partition: usize, attempt: usize) -> Self {
        Self {
            job_id,
            stage,
            partition,
            attempt,
        }
    }

    pub fn job_id(&self) -> u64 {
        self.job_id
    }

    pub fn stage(&self) -> usize {
        self.stage
    }

    pub fn partition(&self) -> usize {
        self.partition
    }

    pub fn attempt(&self) -> usize {
        self.attempt
    }

    pub fn path_component(&self) -> String {
        format!(
            "job-{}-stage-{}-part-{}-attempt-{}",
            self.job_id, self.stage, self.partition, self.attempt
        )
    }
}

impl SessionExtension for TaskAttemptContext {
    fn name() -> &'static str {
        "TaskAttemptContext"
    }
}
