use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _};
use opentelemetry_sdk::logs::SdkLogger;

use crate::system_event::SystemEvent;

/// Emits serialized [`SystemEvent`] values through an OpenTelemetry SDK logger.
#[derive(Debug, Clone)]
pub struct SystemEventReporter {
    logger: SdkLogger,
}

impl SystemEventReporter {
    pub fn new(logger: SdkLogger) -> Self {
        Self { logger }
    }

    pub fn report(&self, event: SystemEvent) {
        let Ok(body) = serde_json::to_string(&event) else {
            return;
        };
        let mut record = self.logger.create_log_record();
        record.set_event_name("sail.system_event");
        record.set_body(AnyValue::String(body.into()));
        self.logger.emit(record);
    }
}
