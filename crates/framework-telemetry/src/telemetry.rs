use std::time::Duration;

use opentelemetry::{global, trace::TracerProvider};
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    resource::{
        EnvResourceDetector, OsResourceDetector, ProcessResourceDetector, ResourceDetector,
        SdkProvidedResourceDetector, TelemetryResourceDetector,
    },
    runtime,
    trace::{
        config, BatchSpanProcessor as SDKBatchSpanProcessor, Tracer as SDKTracer,
        TracerProvider as SDKTracerProvider,
    },
};
use tracing_subscriber::{fmt, layer::SubscriberExt, Registry};

pub fn init_telemetry() -> Result<(), tracing::subscriber::SetGlobalDefaultError> {
    let tracer = init_tracer();
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = Registry::default()
        .with(fmt::layer()) // If we want JSON: fmt::layer().json().flatten_event(true)
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(telemetry);
    tracing::subscriber::set_global_default(subscriber)
}

pub fn init_tracer() -> SDKTracer {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let os_resource = OsResourceDetector.detect(Duration::from_secs(0));
    let process_resource = ProcessResourceDetector.detect(Duration::from_secs(0));
    let sdk_resource = SdkProvidedResourceDetector.detect(Duration::from_secs(0));
    let env_resource = EnvResourceDetector::new().detect(Duration::from_secs(0));
    let telemetry_resource = TelemetryResourceDetector.detect(Duration::from_secs(0));

    let exporter = opentelemetry_stdout::SpanExporter::default();
    let processor = SDKBatchSpanProcessor::builder(exporter, runtime::Tokio).build();
    let provider = SDKTracerProvider::builder()
        .with_span_processor(processor)
        .with_config(
            config().with_resource(
                os_resource
                    .merge(&process_resource)
                    .merge(&sdk_resource)
                    .merge(&env_resource)
                    .merge(&telemetry_resource),
            ),
        )
        .build();

    provider.tracer("Test") // TODO: should be service.name
}

// TODO: init_metrics
pub fn init_metrics() {}
