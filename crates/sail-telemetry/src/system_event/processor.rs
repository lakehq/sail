use std::time::Duration;

use opentelemetry::InstrumentationScope;
use opentelemetry::logs::AnyValue;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::{LogProcessor, SdkLogRecord};
use sail_common::actor::ActorHandle;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

use crate::system_event::{
    SYSTEM_EVENT_NAME, SystemEvent, SystemEventActor, SystemEventActorMessage,
};

/// Materializes system-event log records into [`SystemEventActor`] rows.
#[derive(Debug)]
pub struct SystemEventLogProcessor {
    actor: ActorHandle<SystemEventActor>,
    runtime: Handle,
    sender: UnboundedSender<SystemEventActorMessage>,
    forwarder: Option<JoinHandle<()>>,
}

impl SystemEventLogProcessor {
    pub fn new(actor: ActorHandle<SystemEventActor>, runtime: Handle) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut processor = Self {
            actor,
            runtime,
            sender,
            forwarder: None,
        };
        processor.start(receiver);
        processor
    }

    fn start(&mut self, mut receiver: UnboundedReceiver<SystemEventActorMessage>) {
        let actor = self.actor.clone();
        self.forwarder = Some(self.runtime.spawn(async move {
            while let Some(message) = receiver.recv().await {
                if actor.send(message).await.is_err() {
                    break;
                }
            }
        }));
    }
}

impl Drop for SystemEventLogProcessor {
    fn drop(&mut self) {
        if let Some(forwarder) = self.forwarder.take() {
            forwarder.abort();
        }
    }
}

impl LogProcessor for SystemEventLogProcessor {
    fn emit(&self, record: &mut SdkLogRecord, _: &InstrumentationScope) {
        if record.event_name() != Some(SYSTEM_EVENT_NAME) {
            return;
        }
        let Some(AnyValue::String(body)) = record.body() else {
            return;
        };
        let Ok(event) = serde_json::from_str::<SystemEvent>(body.as_ref()) else {
            return;
        };
        // Log processors run synchronously. An unbounded channel lets event reporting remain
        // non-blocking while the forwarder waits for the actor's bounded mailbox.
        let _ = self.sender.send(SystemEventActorMessage::Apply(event));
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
        // The shutdown message follows all buffered events, so the actor processes the
        // complete event stream before it stops.
        let _ = self.sender.send(SystemEventActorMessage::Shutdown);
        Ok(())
    }
}
