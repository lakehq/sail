use datafusion::common::Result as DataFusionResult;
use opentelemetry::logs::{AnyValue, LogRecord, Logger};
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_sdk::logs::SdkLogger;
use sail_common::actor::ActorHandle;
use tokio::sync::oneshot;

use crate::SCOPE_NAME;
use crate::error::{TelemetryError, TelemetryResult};
use crate::system_event::{
    SYSTEM_EVENT_NAME, SystemEvent, SystemEventActor, SystemEventActorMessage,
};

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
        record.set_event_name(SYSTEM_EVENT_NAME);
        record.set_body(AnyValue::String(body.into()));
        self.logger.emit(record);
    }
}

/// Sends decoded OpenTelemetry metrics to the system event actor.
#[derive(Clone, Debug)]
pub struct SystemMetricReporter {
    actor: ActorHandle<SystemEventActor>,
}

impl SystemMetricReporter {
    pub fn new(actor: ActorHandle<SystemEventActor>) -> Self {
        Self { actor }
    }

    pub async fn report(&self, metrics: Vec<ResourceMetrics>) -> TelemetryResult<()> {
        let metrics = metrics
            .into_iter()
            .filter_map(|mut resource_metrics| {
                resource_metrics.scope_metrics.retain(|scope_metrics| {
                    scope_metrics
                        .scope
                        .as_ref()
                        .is_some_and(|scope| scope.name == SCOPE_NAME)
                });
                (!resource_metrics.scope_metrics.is_empty()).then_some(resource_metrics)
            })
            .collect();
        let (result, receiver) = oneshot::channel::<DataFusionResult<()>>();
        self.actor
            .send(SystemEventActorMessage::ApplyMetrics { metrics, result })
            .await
            .map_err(|e| TelemetryError::internal(format!("failed to report metrics: {e}")))?;
        receiver
            .await
            .map_err(|e| TelemetryError::internal(format!("failed to report metrics: {e}")))?
            .map_err(|e| TelemetryError::internal(format!("failed to store metrics: {e}")))
    }
}
