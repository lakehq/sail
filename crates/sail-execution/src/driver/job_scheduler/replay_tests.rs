use super::*;
use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::prelude::SessionContext;
use sail_common::actor::ActorSystem;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::replay::ReplayId;
use sail_physical_plan::replay::ReplayExec;

use crate::driver::job_scheduler::JobSchedulerOptions;
use crate::driver::{DriverComponents, DriverOptions};
use crate::id::{DriverId, WorkerId};
use crate::replay_integration_tests::test_event_reporter;
use crate::worker_manager::LocalWorkerManager;

struct Assignments(TaskAssignment);
impl TaskAssignmentGetter for Assignments {
    fn get(&self, _: &TaskKey) -> Option<&TaskAssignment> {
        Some(&self.0)
    }
}

fn scheduled(actions: &[JobAction]) -> Vec<TaskKey> {
    actions
        .iter()
        .flat_map(|action| match action {
            JobAction::ScheduleTaskRegion { region } => region
                .tasks
                .iter()
                .flat_map(|(_, set)| set.tasks().cloned())
                .collect(),
            _ => Vec::new(),
        })
        .collect()
}

#[tokio::test]
async fn replay_build_retry_consumer_retry_and_sealed_loss()
-> Result<(), Box<dyn std::error::Error>> {
    let mut config = AppConfig::load()?;
    config.cluster.task_max_attempts = 4;
    let handle = tokio::runtime::Handle::current();
    let runtime = RuntimeHandle::new(handle.clone(), handle);
    let options = DriverOptions::try_new(
        &config,
        runtime.clone(),
        "replay-scheduler".into(),
        DriverId::from(1),
        0,
    )?;
    let reporter = test_event_reporter();
    let mut scheduler = JobScheduler::new(JobSchedulerOptions::from(&options), reporter.clone());
    let mut system = ActorSystem::new();
    let driver = system.spawn::<DriverActor>((
        options,
        DriverComponents {
            worker_manager: Box::new(LocalWorkerManager::new(runtime, SessionContext::new())),
            event_reporter: reporter,
        },
    ));
    let mut ctx = ActorContext::new(&driver);
    let source =
        MemorySourceConfig::try_new_exec(&[vec![], vec![]], Arc::new(Schema::empty()), None)?;
    let plan = Arc::new(ReplayExec::try_new(
        ReplayId {
            scope: Default::default(),
            ordinal: 1,
        },
        source,
    )?);
    let (job_id, _output) =
        scheduler.accept_job(&mut ctx, plan, Arc::new(TaskContext::default()))?;
    let producer = scheduler.jobs[&job_id]
        .graph
        .stages()
        .iter()
        .position(|s| s.replay.is_some())
        .ok_or("no replay producer")?;
    let original = scheduled(&scheduler.refresh_job(job_id));
    assert_eq!(original.len(), 2);
    assert!(
        original
            .iter()
            .all(|k| k.stage == producer && k.attempt == 0)
    );
    let first = original[0].clone();
    let second = original[1].clone();
    scheduler.update_task(&first, TaskState::Succeeded, None, None);
    assert!(
        scheduled(&scheduler.refresh_job(job_id)).is_empty(),
        "all producer partitions must finish before any consumer starts"
    );
    assert!(
        scheduler.jobs[&job_id].stages[producer]
            .sealed_attempts
            .is_none()
    );
    scheduler.lose_replay_output(&first);
    let retries = scheduled(&scheduler.refresh_job(job_id));
    let replacement = retries
        .iter()
        .find(|k| k.partition == first.partition)
        .ok_or("missing build retry")?
        .clone();
    assert_eq!(replacement.attempt, 1);
    assert!(scheduler.jobs[&job_id].replay_failure.is_none());
    scheduler.update_task(&second, TaskState::Succeeded, None, None);
    scheduler.update_task(&replacement, TaskState::Succeeded, None, None);
    // If both producer partitions share a retry region, finish its second replacement too.
    for key in &retries {
        scheduler.update_task(key, TaskState::Succeeded, None, None);
    }
    let consumers = scheduled(&scheduler.refresh_job(job_id));
    assert!(!consumers.is_empty());
    assert!(consumers.iter().all(|k| k.stage != producer));
    let frozen = scheduler.jobs[&job_id].stages[producer]
        .sealed_attempts
        .clone()
        .ok_or("not sealed")?;
    assert_eq!(frozen[first.partition], 1);
    let consumer = consumers[0].clone();
    let assignments = Assignments(TaskAssignment::Worker {
        worker_id: WorkerId::from(7),
        slot: 0,
    });
    let input_keys =
        |scheduler: &JobScheduler, key: &TaskKey| -> ExecutionResult<Vec<Vec<TaskInputKey>>> {
            let job = &scheduler.jobs[&job_id];
            let input = job.graph.stages()[key.stage]
                .inputs
                .iter()
                .find(|i| i.stage == producer)
                .ok_or_else(|| ExecutionError::InternalError("missing replay input".into()))?;
            TaskInputBuilder::try_new(job, key, input, &assignments)?.build_task_input_keys()
        };
    let before = input_keys(&scheduler, &consumer)?;
    assert!(!before.iter().flatten().collect::<Vec<_>>().is_empty());
    for key in before.iter().flatten() {
        assert_eq!(key.attempt, frozen[key.partition]);
    }
    scheduler.update_task(
        &consumer,
        TaskState::Failed,
        None,
        Some(CommonErrorCause::Execution(
            "temporary consumer failure".into(),
        )),
    );
    let retries = scheduled(&scheduler.refresh_job(job_id));
    assert!(retries.iter().all(|k| k.stage != producer));
    let retry = retries
        .iter()
        .find(|k| k.stage == consumer.stage && k.partition == consumer.partition)
        .ok_or("missing consumer retry")?;
    assert_eq!(retry.attempt, 1);
    let tuples = |groups: Vec<Vec<TaskInputKey>>| {
        groups
            .into_iter()
            .map(|group| {
                group
                    .into_iter()
                    .map(|k| (k.partition, k.attempt, k.channel))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(tuples(input_keys(&scheduler, retry)?), tuples(before));
    assert_eq!(
        scheduler.jobs[&job_id].stages[producer]
            .sealed_attempts
            .as_ref(),
        Some(&frozen)
    );
    // A stale loss report for the pre-seal failed attempt cannot invalidate the replacement.
    scheduler.lose_replay_output(&first);
    assert!(scheduler.jobs[&job_id].replay_failure.is_none());
    scheduler.lose_replay_output(&replacement);
    let actions = scheduler.refresh_job(job_id);
    assert!(matches!(scheduler.jobs[&job_id].state, JobState::Failed));
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, JobAction::FailJobOutput { .. }))
    );
    assert!(scheduled(&actions).is_empty());
    assert!(
        scheduler.refresh_job(job_id).is_empty(),
        "sealed loss is terminal, not another producer attempt"
    );
    Ok(())
}
