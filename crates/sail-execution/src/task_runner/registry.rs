use std::collections::HashSet;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskAttempt, TaskKey};

/// Admission history belongs to the worker session, not to shuffle stream lifetimes.
/// Actor serialization makes checking and recording a batch atomic with cancellation.
#[derive(Default)]
pub(super) struct TaskRegistry {
    admitted: HashSet<TaskKey>,
    canceled: HashSet<TaskKey>,
    closed_jobs: HashSet<JobId>,
}

impl TaskRegistry {
    /// Returns false for an empty batch or a replay of an already admitted batch.
    pub fn check_batch(
        &self,
        job_id: JobId,
        stage: usize,
        tasks: &[TaskAttempt],
    ) -> ExecutionResult<bool> {
        let invalid = |message: &str| ExecutionError::InvalidArgument(message.into());
        if tasks.is_empty() {
            return Ok(false);
        }
        let partitions = tasks
            .iter()
            .map(|task| task.partition)
            .collect::<HashSet<_>>();
        if partitions.len() != tasks.len() {
            return Err(invalid("task batch must contain distinct partitions"));
        }
        if self.is_job_closed(job_id)
            || tasks
                .iter()
                .any(|task| self.is_canceled(&task.task_key(job_id, stage)))
        {
            return Err(invalid("task batch has been canceled"));
        }
        let admitted = tasks
            .iter()
            .filter(|task| self.admitted.contains(&task.task_key(job_id, stage)))
            .count();
        if admitted == tasks.len() {
            return Ok(false);
        }
        if admitted > 0 {
            return Err(invalid("task batch overlaps admitted attempts"));
        }
        Ok(true)
    }

    pub fn record_batch(&mut self, job_id: JobId, stage: usize, tasks: &[TaskAttempt]) {
        self.admitted
            .extend(tasks.iter().map(|task| task.task_key(job_id, stage)));
    }

    pub fn cancel(&mut self, key: &TaskKey) {
        if !self.is_job_closed(key.job_id) {
            self.canceled.insert(key.clone());
        }
    }

    pub fn close_job(&mut self, job_id: JobId) {
        self.closed_jobs.insert(job_id);
        self.admitted.retain(|key| key.job_id != job_id);
        self.canceled.retain(|key| key.job_id != job_id);
    }

    pub fn is_job_closed(&self, job_id: JobId) -> bool {
        self.closed_jobs.contains(&job_id)
    }

    pub fn is_canceled(&self, key: &TaskKey) -> bool {
        self.canceled.contains(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(partition: usize, attempt: usize) -> TaskAttempt {
        TaskAttempt { partition, attempt }
    }

    #[test]
    fn stop_before_launch_rejects_whole_batch_but_allows_retry() -> ExecutionResult<()> {
        let job_id = JobId::from(1);
        let stage = 2;
        let mut registry = TaskRegistry::default();
        registry.cancel(&task(1, 0).task_key(job_id, stage));
        assert!(
            registry
                .check_batch(job_id, stage, &[task(0, 0), task(1, 0)])
                .is_err()
        );
        assert!(registry.check_batch(job_id, stage, &[task(0, 0)])?);
        assert!(registry.check_batch(job_id, stage, &[task(0, 1), task(1, 1)])?);
        assert!(registry.check_batch(job_id, stage + 1, &[task(1, 0)])?);
        Ok(())
    }

    #[test]
    fn replays_do_not_restart_admitted_or_canceled_attempts() -> ExecutionResult<()> {
        let job_id = JobId::from(1);
        let stage = 2;
        let mut registry = TaskRegistry::default();
        let tasks = [task(0, 0), task(1, 0)];
        assert!(!registry.check_batch(job_id, stage, &[])?);
        assert!(registry.check_batch(job_id, stage, &tasks)?);
        assert!(
            registry
                .check_batch(job_id, stage, &[task(0, 0), task(0, 1)])
                .is_err()
        );
        registry.record_batch(job_id, stage, &tasks);
        assert!(!registry.check_batch(job_id, stage, &tasks)?);
        assert!(registry.check_batch(job_id, stage + 1, &tasks)?);
        assert!(
            registry
                .check_batch(job_id, stage, &[task(1, 0), task(2, 0)])
                .is_err()
        );
        registry.cancel(&task(0, 0).task_key(job_id, stage));
        assert!(registry.check_batch(job_id, stage, &tasks).is_err());
        Ok(())
    }

    #[test]
    fn job_cleanup_compacts_history_without_allowing_late_launches() -> ExecutionResult<()> {
        let job_id = JobId::from(1);
        let stage = 2;
        let mut registry = TaskRegistry::default();
        registry.record_batch(job_id, stage, &[task(0, 0)]);
        registry.cancel(&task(1, 0).task_key(job_id, stage));
        registry.close_job(job_id);
        registry.cancel(&task(2, 0).task_key(job_id, stage));
        assert!(registry.admitted.is_empty());
        assert!(registry.canceled.is_empty());
        assert!(!registry.check_batch(job_id, stage, &[])?);
        assert!(registry.check_batch(job_id, stage, &[task(0, 0)]).is_err());
        assert!(registry.check_batch(job_id, stage, &[task(2, 1)]).is_err());
        assert!(registry.check_batch(JobId::from(2), stage, &[task(0, 0)])?);
        Ok(())
    }
}
