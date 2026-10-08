use sail_common::actor::{Actor, ActorAction, ActorContext, UnboundedMailbox};

use super::SystemStoreMessage;
use crate::SystemStoreError;
use crate::engine::StoreEngine;

pub(crate) struct SystemStoreActor<E> {
    pub(super) engine: E,
    pub(super) failed: Option<String>,
}

impl<E> SystemStoreActor<E>
where
    E: StoreEngine,
{
    pub(super) fn failure(&self) -> SystemStoreError {
        SystemStoreError::internal(format!(
            "system store is unavailable after a previous failure: {}",
            self.failed.as_deref().unwrap_or("unknown failure")
        ))
    }

    pub(super) fn fail(&mut self, error: &SystemStoreError) {
        if self.failed.is_none() {
            self.failed = Some(error.to_string());
            log::error!("system store entered failed state: {error}");
        }
    }
}

impl<E> Actor<UnboundedMailbox> for SystemStoreActor<E>
where
    E: StoreEngine,
{
    type Message = SystemStoreMessage;
    type Options = E;

    fn name() -> &'static str {
        "SystemStoreActor"
    }

    fn new(engine: Self::Options) -> Self {
        Self {
            engine,
            failed: None,
        }
    }

    async fn receive(
        &mut self,
        ctx: &mut ActorContext<Self, UnboundedMailbox>,
        message: Self::Message,
    ) -> ActorAction {
        self.handle(ctx, message).await
    }
}
