use std::time::Duration;

use opentelemetry::{global, trace::TraceError, trace::TracerProvider};
use opentelemetry_otlp::{self, WithExportConfig};
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    resource::{
        EnvResourceDetector, OsResourceDetector, ProcessResourceDetector, ResourceDetector,
        SdkProvidedResourceDetector, TelemetryResourceDetector,
    },
    runtime, trace as sdktrace,
};
use thiserror::Error;
use tracing::subscriber::SetGlobalDefaultError;
use tracing_subscriber::{fmt, layer::SubscriberExt, Registry};

#[derive(Debug, Error)]
pub enum TelemetryError {
    #[error("Trace error: {0}")]
    TraceError(#[from] TraceError),
    #[error("Set global default error: {0}")]
    SetGlobalDefaultError(#[from] SetGlobalDefaultError),
}

pub fn init_telemetry() -> Result<(), TelemetryError> {
    let tracer = init_tracer()?;
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = Registry::default()
        .with(fmt::layer()) // If we want JSON: fmt::layer().json().flatten_event(true)
        // .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(telemetry);
    tracing::subscriber::set_global_default(subscriber)?;

    Ok(())
}

pub fn init_tracer() -> Result<sdktrace::Tracer, TraceError> {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let os_resource = OsResourceDetector.detect(Duration::from_secs(0));
    let process_resource = ProcessResourceDetector.detect(Duration::from_secs(0));
    let sdk_resource = SdkProvidedResourceDetector.detect(Duration::from_secs(0));
    let env_resource = EnvResourceDetector::new().detect(Duration::from_secs(0));
    let telemetry_resource = TelemetryResourceDetector.detect(Duration::from_secs(0));

    // let exporter = opentelemetry_stdout::SpanExporter::default();
    // let processor = SDKBatchSpanProcessor::builder(exporter, runtime::Tokio).build();
    // let provider = SDKTracerProvider::builder()
    //     .with_span_processor(processor)
    //     .with_config(
    //         config().with_resource(
    //             os_resource
    //                 .merge(&process_resource)
    //                 .merge(&sdk_resource)
    //                 .merge(&env_resource)
    //                 .merge(&telemetry_resource),
    //         ),
    //     )
    //     .build();
    //
    // provider.tracer("Test") // TODO: should be service.name

    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint("http://0.0.0.0:4317"), // TODO: env var
        )
        .with_trace_config(
            sdktrace::config().with_resource(
                os_resource
                    .merge(&process_resource)
                    .merge(&sdk_resource)
                    .merge(&env_resource)
                    .merge(&telemetry_resource),
            ),
        )
        .install_batch(runtime::Tokio)
}

// TODO: init_metrics
pub fn init_metrics() {}
