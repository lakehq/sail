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

// TODO: Make use_collector an env var
pub fn init_telemetry(use_collector: bool) -> Result<(), TelemetryError> {
    let tracer = init_tracer(use_collector)?;
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = Registry::default()
        .with(telemetry)
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(fmt::layer());
    tracing::subscriber::set_global_default(subscriber)?;

    Ok(())
}

// TODO: Make use_collector an env var
pub fn init_tracer(use_collector: bool) -> Result<sdktrace::Tracer, TelemetryError> {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let os_resource = OsResourceDetector.detect(Duration::from_secs(0));
    let process_resource = ProcessResourceDetector.detect(Duration::from_secs(0));
    let sdk_resource = SdkProvidedResourceDetector.detect(Duration::from_secs(0));
    let env_resource = EnvResourceDetector::new().detect(Duration::from_secs(0));
    let telemetry_resource = TelemetryResourceDetector.detect(Duration::from_secs(0));

    if use_collector {
        // let host = env::var("SIDECAR_FOR_LAKESAIL_COLLECTOR_SERVICE_HOST")?;
        // let host = "0.0.0.0";
        // let port = env::var("SIDECAR_FOR_LAKESAIL_COLLECTOR_SERVICE_PORT_OTLP_GRPC")?;
        // let url = format!("http://{}:{}", host, port);
        // let url = env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")?;
        // let host = env::var("LAKESAIL_PORT_4317_TCP_ADDR")?;
        // let port = env::var("LAKESAIL_SERVICE_PORT_OTLP")?;
        let url = "http://0.0.0.0:4317";

        Ok(
            opentelemetry_otlp::new_pipeline()
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
                            .merge(&telemetry_resource)
                    ),
                )
                .install_batch(runtime::TokioCurrentThread)?
        )
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
