use std::collections::HashSet;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey};

/// Admission history belongs to the worker session, not to shuffle stream lifetimes.
/// Actor serialization makes checking and recording a batch atomic with cancellation.
#[derive(Default)]
pub(super) struct TaskRegistry {
    admitted: HashSet<TaskKey>,
    canceled: HashSet<TaskKey>,
    closed_jobs: HashSet<JobId>,
}

impl TaskRegistry {
    /// Returns false for a replay of an already admitted batch.
    pub fn check_batch(&self, keys: &[TaskKey]) -> ExecutionResult<bool> {
        let invalid = |message: &str| ExecutionError::InvalidArgument(message.into());
        let first = keys.first().ok_or_else(|| invalid("empty task batch"))?;
        let partitions = keys.iter().map(|key| key.partition).collect::<HashSet<_>>();
        if partitions.len() != keys.len()
            || keys
                .iter()
                .any(|key| key.job_id != first.job_id || key.stage != first.stage)
        {
            return Err(invalid(
                "task batch must contain distinct partitions of one stage",
            ));
        }
        if self.is_job_closed(first.job_id) || keys.iter().any(|key| self.is_canceled(key)) {
            return Err(invalid("task batch has been canceled"));
        }
        if keys.iter().all(|key| self.admitted.contains(key)) {
            return Ok(false);
        }
        if keys.iter().any(|key| self.admitted.contains(key)) {
            return Err(invalid("task batch overlaps admitted attempts"));
        }
        Ok(true)
    }

    pub fn record_batch(&mut self, keys: &[TaskKey]) {
        self.admitted.extend(keys.iter().cloned());
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

    fn key(partition: usize, attempt: usize) -> TaskKey {
        TaskKey {
            job_id: JobId::from(1),
            stage: 2,
            partition,
            attempt,
        }
    }

    #[test]
    fn stop_before_launch_rejects_whole_batch_but_allows_retry() -> ExecutionResult<()> {
        let mut registry = TaskRegistry::default();
        registry.cancel(&key(1, 0));
        assert!(registry.check_batch(&[key(0, 0), key(1, 0)]).is_err());
        assert!(registry.check_batch(&[key(0, 0)])?);
        assert!(registry.check_batch(&[key(0, 1), key(1, 1)])?);
        Ok(())
    }

    #[test]
    fn replays_do_not_restart_admitted_or_canceled_attempts() -> ExecutionResult<()> {
        let mut registry = TaskRegistry::default();
        let keys = [key(0, 0), key(1, 0)];
        assert!(registry.check_batch(&keys)?);
        registry.record_batch(&keys);
        assert!(!registry.check_batch(&keys)?);
        assert!(registry.check_batch(&[key(1, 0), key(2, 0)]).is_err());
        registry.cancel(&key(0, 0));
        assert!(registry.check_batch(&keys).is_err());
        Ok(())
    }

    #[test]
    fn job_cleanup_compacts_history_without_allowing_late_launches() -> ExecutionResult<()> {
        let mut registry = TaskRegistry::default();
        registry.record_batch(&[key(0, 0)]);
        registry.cancel(&key(1, 0));
        registry.close_job(JobId::from(1));
        registry.cancel(&key(2, 0));
        assert!(registry.admitted.is_empty());
        assert!(registry.canceled.is_empty());
        assert!(registry.check_batch(&[key(0, 0)]).is_err());
        assert!(registry.check_batch(&[key(2, 1)]).is_err());
        let other_job = TaskKey {
            job_id: JobId::from(2),
            ..key(0, 0)
        };
        assert!(registry.check_batch(&[other_job])?);
        Ok(())
    }
}
