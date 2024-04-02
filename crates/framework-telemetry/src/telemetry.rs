use std::env;
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
    #[error("Env error: {0}")]
    EnvError(#[from] env::VarError),
}

pub fn init_telemetry() -> Result<(), TelemetryError> {
    let tracer = init_tracer()?;
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = Registry::default()
        .with(telemetry)
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(fmt::layer());
    tracing::subscriber::set_global_default(subscriber)?;

    Ok(())
}

pub fn init_tracer() -> Result<sdktrace::Tracer, TelemetryError> {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let os_resource = OsResourceDetector.detect(Duration::from_secs(0));
    let process_resource = ProcessResourceDetector.detect(Duration::from_secs(0));
    let sdk_resource = SdkProvidedResourceDetector.detect(Duration::from_secs(0));
    let env_resource = EnvResourceDetector::new().detect(Duration::from_secs(0));
    let telemetry_resource = TelemetryResourceDetector.detect(Duration::from_secs(0));

    let use_collector = match env::var("LAKESAIL_OPENTELEMETRY_COLLECTOR") {
        Ok(val) => !val.is_empty(),
        Err(_) => false,
    };
    if use_collector {
        let host = env::var("LAKESAIL_OPENTELEMETRY_COLLECTOR_SERVICE_HOST")?;
        let port = env::var("LAKESAIL_OPENTELEMETRY_COLLECTOR_SERVICE_PORT_OTLP_GRPC")?;
        let url = format!("http://{}:{}", host, port);

        Ok(opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(url)
                    .with_protocol(opentelemetry_otlp::Protocol::Grpc)
                    .with_timeout(Duration::from_secs(3)),
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
            .install_batch(runtime::TokioCurrentThread)?)
    } else {
        let exporter = opentelemetry_stdout::SpanExporter::default();
        let processor = sdktrace::BatchSpanProcessor::builder(exporter, runtime::Tokio).build();
        let provider = sdktrace::TracerProvider::builder()
            .with_span_processor(processor)
            .with_config(
                sdktrace::config().with_resource(
                    os_resource
                        .merge(&process_resource)
                        .merge(&sdk_resource)
                        .merge(&env_resource)
                        .merge(&telemetry_resource),
                ),
            )
            .build();

        Ok(provider.tracer("Test")) // TODO: should be service.name
    }
}

// TODO: init_metrics
pub fn init_metrics() {}
