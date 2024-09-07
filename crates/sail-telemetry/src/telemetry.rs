use std::env;

use opentelemetry::trace::{TraceError, TracerProvider};
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::{self, WithExportConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::{runtime, trace as sdktrace, Resource};
use opentelemetry_semantic_conventions::resource::{SERVICE_NAME, SERVICE_VERSION};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use thiserror::Error;
use tracing::subscriber::SetGlobalDefaultError;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Registry};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum TelemetryError {
    #[error("Trace error: {0}")]
    TraceError(#[from] TraceError),
    #[error("Set global default error: {0}")]
    SetGlobalDefaultError(#[from] SetGlobalDefaultError),
    #[error("Env error: {0}")]
    EnvError(#[from] env::VarError),
}

fn resource() -> Resource {
    Resource::from_schema_url(
        [
            KeyValue::new(SERVICE_NAME, env!("CARGO_PKG_NAME")),
            KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
        ],
        SCHEMA_URL,
    )
}

pub fn init_telemetry() -> Result<(), TelemetryError> {
    let tracer = init_tracer()?;
    Registry::default()
        .with(EnvFilter::from_default_env())
        .with(OpenTelemetryLayer::new(tracer))
        .with(fmt::layer())
        .init();
    Ok(())
}

pub fn init_tracer() -> Result<sdktrace::Tracer, TelemetryError> {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let use_collector = match env::var("LAKESAIL_OPENTELEMETRY_COLLECTOR") {
        Ok(val) => !val.is_empty(),
        Err(_) => false,
    };

    let mut exporter = opentelemetry_otlp::new_exporter().tonic();
    if use_collector {
        let host = env::var("LAKESAIL_OPENTELEMETRY_COLLECTOR_SERVICE_HOST")?;
        let port = env::var("LAKESAIL_OPENTELEMETRY_COLLECTOR_SERVICE_PORT_OTLP_GRPC")?;
        let url = format!("http://{}:{}", host, port);
        exporter = exporter.with_endpoint(url);
    }

    let tracer_provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .with_trace_config(
            sdktrace::Config::default()
                // If export trace to AWS X-Ray, you can use XrayIdGenerator
                .with_id_generator(sdktrace::RandomIdGenerator::default())
                .with_resource(resource()),
        )
        .with_batch_config(sdktrace::BatchConfig::default())
        .install_batch(runtime::TokioCurrentThread)?;

    global::set_tracer_provider(tracer_provider.clone());
    Ok(tracer_provider.tracer("lakesail"))
}

// TODO: init_metrics
pub fn init_metrics() {}
