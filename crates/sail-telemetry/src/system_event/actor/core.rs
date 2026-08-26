use sail_common::actor::{Actor, ActorAction, ActorContext};

use super::{SystemEventActor, SystemEventActorMessage};

#[tonic::async_trait]
impl Actor for SystemEventActor {
    type Message = SystemEventActorMessage;
    type Options = ();

    fn name() -> &'static str {
        "SystemEventActor"
    }

    fn new(_: Self::Options) -> Self {
        Self::default()
    }

    fn receive(&mut self, _: &mut ActorContext<Self>, message: Self::Message) -> ActorAction {
        match message {
            SystemEventActorMessage::Apply(event) => self.store.apply(event),
            SystemEventActorMessage::ReadJobs {
                session_id,
                job_id,
                fetch,
                result,
            } => {
                let _ = result.send(self.read_jobs(session_id, job_id, fetch));
            }
            SystemEventActorMessage::ReadStages {
                session_id,
                job_id,
                stage,
                fetch,
                result,
            } => {
                let _ = result.send(self.read_stages(session_id, job_id, stage, fetch));
            }
            SystemEventActorMessage::ReadTasks {
                session_id,
                job_id,
                stage,
                partition,
                attempt,
                fetch,
                result,
            } => {
                let _ = result
                    .send(self.read_tasks(session_id, job_id, stage, partition, attempt, fetch));
            }
            SystemEventActorMessage::ReadOptions { key, fetch, result } => {
                let _ = result.send(self.read_options(key, fetch));
            }
            SystemEventActorMessage::ReadSessions {
                session_id,
                fetch,
                result,
            } => {
                let _ = result.send(self.read_sessions(session_id, fetch));
            }
            SystemEventActorMessage::ReadWorkers {
                session_id,
                worker_id,
                fetch,
                result,
            } => {
                let _ = result.send(self.read_workers(session_id, worker_id, fetch));
            }
            SystemEventActorMessage::Shutdown => return ActorAction::Stop,
        }
        ActorAction::Continue
    }
}
