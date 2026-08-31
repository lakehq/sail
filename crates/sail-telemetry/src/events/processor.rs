use std::time::Duration;

use opentelemetry::InstrumentationScope;
use opentelemetry::logs::AnyValue;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::{LogProcessor, SdkLogRecord};
use sail_system_store::{SystemEvent, SystemStoreHandle};

use super::SYSTEM_EVENT_NAME;

/// Materializes system event log records into system store rows.
#[derive(Debug)]
pub struct SystemEventLogProcessor {
    store: SystemStoreHandle,
}

impl SystemEventLogProcessor {
    pub fn new(store: SystemStoreHandle) -> Self {
        Self { store }
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
        let _ = self.store.write_event(event);
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
        // Store shutdown is coordinated by telemetry after the logger provider is stopped.
        Ok(())
    }
}
