use std::collections::{HashMap, HashSet};

use tokio::sync::oneshot;

use crate::id::{JobId, TaskKey};

mod core;
mod handler;
mod rpc;

use crate::driver::DriverClientSet;
use crate::rpc::ServerMonitor;
use crate::stream_manager::StreamManager;
use crate::task_runner::TaskRunner;
use crate::worker::WorkerOptions;
use crate::worker::peer_tracker::PeerTracker;

pub struct WorkerActor {
    options: WorkerOptions,
    server: ServerMonitor,
    driver_client_set: DriverClientSet,
    peer_tracker: PeerTracker,
    task_runner: TaskRunner,
    preparing_tasks: HashMap<u64, PreparingTaskResources>,
    current_preparations: HashMap<TaskKey, u64>,
    pending_job_cleanups: Vec<PendingJobCleanup>,
    pending_worker_stops: Vec<PendingWorkerStop>,
    stopping: bool,
    job_cleanup_generations: HashMap<JobId, u64>,
    next_job_cleanup_generation: u64,
    canceled_tasks: TaskCancellationTombstones,
    next_resource_preparation_id: u64,
    stream_manager: StreamManager,
    /// A monotonically increasing sequence number for ordered events.
    sequence: u64,
}

struct PreparingTaskResources {
    key: TaskKey,
    cancel: Option<oneshot::Sender<()>>,
}

struct PendingJobCleanup {
    job_id: JobId,
    preparation_ids: HashSet<u64>,
    generation: Option<u64>,
    result: oneshot::Sender<()>,
}

struct PendingWorkerStop {
    preparation_ids: HashSet<u64>,
    result: oneshot::Sender<()>,
}

impl PendingJobCleanup {
    fn finish_preparation(&mut self, preparation_id: u64) -> bool {
        self.preparation_ids.remove(&preparation_id);
        self.preparation_ids.is_empty()
    }
}

impl PendingWorkerStop {
    fn finish_preparation(&mut self, preparation_id: u64) -> bool {
        self.preparation_ids.remove(&preparation_id);
        self.preparation_ids.is_empty()
    }
}

fn should_run_materialized_task(is_current: bool, stopping: bool, canceled: bool) -> bool {
    is_current && !stopping && !canceled
}

#[derive(Default)]
struct TaskCancellationTombstones {
    tasks: HashSet<TaskKey>,
    stages: HashSet<(JobId, usize)>,
    jobs: HashSet<JobId>,
}

impl TaskCancellationTombstones {
    fn cancel(&mut self, key: TaskKey) {
        self.tasks.insert(key);
    }

    fn contains(&self, key: &TaskKey) -> bool {
        self.tasks.contains(key)
            || self.stages.contains(&(key.job_id, key.stage))
            || self.jobs.contains(&key.job_id)
    }

    fn clean_up_job(&mut self, job_id: JobId, stage: Option<usize>) {
        match stage {
            Some(stage) => {
                self.stages.insert((job_id, stage));
                self.tasks
                    .retain(|key| key.job_id != job_id || key.stage != stage);
            }
            None => {
                self.jobs.insert(job_id);
                self.stages.retain(|(candidate, _)| *candidate != job_id);
                self.tasks.retain(|key| key.job_id != job_id);
            }
        }
    }

    fn finish_job(&mut self, job_id: JobId) {
        // The driver only finishes a job after every earlier request has completed and this
        // worker has acknowledged that matching resource preparations have stopped.
        self.jobs.remove(&job_id);
        self.stages.retain(|(candidate, _)| *candidate != job_id);
        self.tasks.retain(|key| key.job_id != job_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task_key(job_id: u64, stage: usize, partition: usize) -> TaskKey {
        TaskKey {
            job_id: job_id.into(),
            stage,
            partition,
            attempt: 0,
        }
    }

    #[test]
    fn stop_before_run_remains_canceled_until_job_cleanup_barrier() {
        let key = task_key(1, 2, 3);
        let mut tombstones = TaskCancellationTombstones::default();

        tombstones.cancel(key.clone());
        assert!(tombstones.contains(&key));

        tombstones.clean_up_job(key.job_id, None);
        assert!(tombstones.contains(&key));
        assert!(tombstones.contains(&task_key(1, 8, 9)));

        tombstones.finish_job(key.job_id);
        assert!(!tombstones.contains(&key));
        assert!(!tombstones.contains(&task_key(1, 8, 9)));
    }

    #[test]
    fn stage_cleanup_cancels_the_stage_and_preserves_other_scopes() {
        let cleaned = task_key(1, 2, 3);
        let same_stage = task_key(1, 2, 4);
        let other_stage = task_key(1, 4, 3);
        let explicitly_canceled = task_key(5, 2, 3);
        let mut tombstones = TaskCancellationTombstones::default();
        tombstones.cancel(cleaned.clone());
        tombstones.cancel(explicitly_canceled.clone());

        tombstones.clean_up_job(cleaned.job_id, Some(cleaned.stage));

        assert!(tombstones.contains(&cleaned));
        assert!(tombstones.contains(&same_stage));
        assert!(!tombstones.contains(&other_stage));
        assert!(tombstones.contains(&explicitly_canceled));
    }

    #[test]
    fn cleanup_waiter_completes_after_every_preparation() {
        let (result, mut completed) = oneshot::channel();
        let mut cleanup = PendingJobCleanup {
            job_id: 1.into(),
            preparation_ids: HashSet::from([4, 7]),
            generation: Some(0),
            result,
        };

        assert!(!cleanup.finish_preparation(4));
        assert!(matches!(
            completed.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        assert!(cleanup.finish_preparation(7));
        assert!(cleanup.result.send(()).is_ok());
        assert_eq!(completed.try_recv(), Ok(()));
    }

    #[test]
    fn worker_stop_ack_waits_for_every_preparation() {
        let (result, mut completed) = oneshot::channel();
        let mut stop = PendingWorkerStop {
            preparation_ids: HashSet::from([2, 5]),
            result,
        };

        assert!(!stop.finish_preparation(2));
        assert!(matches!(
            completed.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(stop.finish_preparation(5));
        assert!(stop.result.send(()).is_ok());
        assert_eq!(completed.try_recv(), Ok(()));
    }

    #[test]
    fn materialization_completion_after_worker_stop_does_not_restart_task() {
        assert!(!should_run_materialized_task(true, true, false));
        assert!(should_run_materialized_task(true, false, false));
    }
}
