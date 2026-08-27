use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::metrics::Temporality;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use prost::Message;

use crate::system_event::SystemMetricReporter;

type MetricSendFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
type SendMetrics = Arc<dyn Fn(Vec<Vec<u8>>) -> MetricSendFuture + Send + Sync>;

#[derive(Clone)]
struct MetricSender {
    runtime: tokio::runtime::Handle,
    send: SendMetrics,
}

static METRIC_SENDER: OnceLock<RwLock<Option<MetricSender>>> = OnceLock::new();

pub fn set_metric_sender<F, Fut>(sender: F)
where
    F: Fn(Vec<Vec<u8>>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), String>> + Send + 'static,
{
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let sender = MetricSender {
        runtime,
        send: Arc::new(move |metrics| Box::pin(sender(metrics))),
    };
    if let Ok(mut current) = METRIC_SENDER.get_or_init(|| RwLock::new(None)).write() {
        *current = Some(sender);
    }
}

#[derive(Clone, Debug)]
pub enum SystemMetricExporterTarget {
    Local(SystemMetricReporter),
    Remote,
}

#[derive(Debug)]
pub struct SystemMetricExporter {
    target: SystemMetricExporterTarget,
    shutdown: AtomicBool,
}

impl SystemMetricExporter {
    pub fn new(target: SystemMetricExporterTarget) -> Self {
        Self {
            target,
            shutdown: AtomicBool::new(false),
        }
    }
}

impl PushMetricExporter for SystemMetricExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(OTelSdkError::AlreadyShutdown);
        }
        let request = ExportMetricsServiceRequest::from(metrics);
        match &self.target {
            SystemMetricExporterTarget::Local(reporter) => reporter
                .report(request.resource_metrics)
                .await
                .map_err(|e| OTelSdkError::InternalFailure(e.to_string())),
            SystemMetricExporterTarget::Remote => {
                let metrics = request
                    .resource_metrics
                    .into_iter()
                    .map(|metrics| metrics.encode_to_vec())
                    .collect();
                let sender = METRIC_SENDER
                    .get_or_init(|| RwLock::new(None))
                    .read()
                    .ok()
                    .and_then(|sender| sender.clone());
                // A worker can collect metrics briefly before its driver client is ready.
                // The next delta collection will contain newly active measurements.
                let Some(sender) = sender else {
                    return Ok(());
                };
                sender
                    .runtime
                    .spawn((sender.send)(metrics))
                    .await
                    .map_err(|error| OTelSdkError::InternalFailure(error.to_string()))?
                    .map_err(OTelSdkError::InternalFailure)
            }
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
        self.shutdown.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        Temporality::Delta
    }
}
