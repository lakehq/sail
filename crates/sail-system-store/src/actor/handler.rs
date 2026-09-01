use sail_common::actor::{ActorAction, ActorContext, UnboundedMailbox};

use super::{SystemStoreActor, SystemStoreMessage};
use crate::engine::StoreEngine;

impl<E> SystemStoreActor<E>
where
    E: StoreEngine,
{
    pub(super) async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self, UnboundedMailbox>,
        message: SystemStoreMessage,
    ) -> ActorAction {
        if self.failed.is_some() {
            match message {
                SystemStoreMessage::WriteEvent(_) => {}
                SystemStoreMessage::WriteMetrics { reply, .. }
                | SystemStoreMessage::Flush { reply } => {
                    let _ = reply.send(Err(self.failure()));
                }
                SystemStoreMessage::Read(query) => query.fail(self.failure()),
                SystemStoreMessage::Shutdown { reply } => {
                    let _ = reply.send(Ok(()));
                    return ActorAction::Stop;
                }
            }
            return ActorAction::Continue;
        }
        match message {
            SystemStoreMessage::WriteEvent(event) => {
                if let Err(error) = self.engine.write_event(event).await {
                    log::error!("failed to write system event: {error}");
                    self.fail(&error);
                }
            }
            SystemStoreMessage::WriteMetrics { samples, reply } => {
                let result = self.engine.write_metrics(samples).await;
                if let Err(error) = &result {
                    self.fail(error);
                }
                let _ = reply.send(result);
            }
            SystemStoreMessage::Read(query) => match self.engine.read(query).await {
                Ok(None) => {}
                Ok(Some(read)) => {
                    ctx.spawn(async move {
                        // Aborting this task cancels the reply. Tokio cannot interrupt a
                        // blocking scan that has already started, but it will release its snapshot
                        // when it completes.
                        if let Err(error) = tokio::task::spawn_blocking(read).await {
                            log::error!("system store read task failed: {error}");
                        }
                    });
                }
                Err(error) => {
                    self.fail(&error);
                    log::error!("failed to acquire system store read snapshot: {error}");
                }
            },
            SystemStoreMessage::Flush { reply } => {
                let result = self.engine.flush().await;
                if let Err(error) = &result {
                    self.fail(error);
                }
                let _ = reply.send(result);
            }
            SystemStoreMessage::Shutdown { reply } => {
                let _ = reply.send(Ok(()));
                return ActorAction::Stop;
            }
        }
        ActorAction::Continue
    }
}
