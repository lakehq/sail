// use std::env;
// use std::time::Duration;
//
// use opentelemetry::global;
// use opentelemetry::trace::{TraceError, TracerProvider};
// use opentelemetry_otlp::{self, WithExportConfig};
// use opentelemetry_sdk::propagation::TraceContextPropagator;
// use opentelemetry_sdk::resource::{
//     EnvResourceDetector, ResourceDetector, SdkProvidedResourceDetector, TelemetryResourceDetector,
// };
// use opentelemetry_sdk::{runtime, trace as sdktrace};
// use opentelemetry_stdout::SpanExporter;
// use thiserror::Error;
// use tracing::subscriber::SetGlobalDefaultError;
// use tracing_subscriber::layer::SubscriberExt;
// use tracing_subscriber::{fmt, EnvFilter, Registry};

use fastrace::collector::{Config, ConsoleReporter};
use logforth::filter::EnvFilter;
use logforth::layout::TextLayout;
use logforth::{append, Dispatch, Logger};

// #[allow(clippy::enum_variant_names)]
// #[derive(Debug, Error)]
// pub enum TelemetryError {
//     #[error("Trace error: {0}")]
//     TraceError(#[from] TraceError),
//     #[error("Set global default error: {0}")]
//     SetGlobalDefaultError(#[from] SetGlobalDefaultError),
//     #[error("Env error: {0}")]
//     EnvError(#[from] env::VarError),
// }

pub fn init_telemetry() -> Result<(), log::SetLoggerError> {
    fastrace::set_reporter(ConsoleReporter, Config::default());
    Logger::new()
        .dispatch(
            Dispatch::new()
                .filter(EnvFilter::from_default_env_or("info"))
                .layout(TextLayout::default())
                .append(append::FastraceEvent)
                .append(append::Stdout),
        )
        .apply()?;
    Ok(())
}

// pub fn init_tracer() -> Result<sdktrace::Tracer, TelemetryError> {
//     global::set_text_map_propagator(TraceContextPropagator::new());
//
//     let sdk_resource = SdkProvidedResourceDetector.detect(Duration::from_secs(0));
//     let env_resource = EnvResourceDetector::new().detect(Duration::from_secs(0));
//     let telemetry_resource = TelemetryResourceDetector.detect(Duration::from_secs(0));
//
//     let trace_config = sdktrace::Config::default()
//         .with_resource(sdk_resource.merge(&env_resource).merge(&telemetry_resource));
//
//     let use_collector = match env::var("LAKESAIL_OPENTELEMETRY_COLLECTOR") {
//         Ok(val) => !val.is_empty(),
//         Err(_) => false,
//     };
//     let tracer_provider = if use_collector {
//         let host = env::var("LAKESAIL_OPENTELEMETRY_COLLECTOR_SERVICE_HOST")?;
//         let port = env::var("LAKESAIL_OPENTELEMETRY_COLLECTOR_SERVICE_PORT_OTLP_GRPC")?;
//         let url = format!("http://{host}:{port}");
//         opentelemetry_otlp::new_pipeline()
//             .tracing()
//             .with_exporter(
//                 opentelemetry_otlp::new_exporter()
//                     .tonic()
//                     .with_endpoint(url)
//                     .with_protocol(opentelemetry_otlp::Protocol::Grpc),
//             )
//             .with_batch_config(sdktrace::BatchConfig::default())
//             .with_trace_config(trace_config)
//             .install_batch(runtime::TokioCurrentThread)?
//     } else {
//         let processor =
//             sdktrace::BatchSpanProcessor::builder(SpanExporter::default(), runtime::Tokio).build();
//         sdktrace::TracerProvider::builder()
//             .with_span_processor(processor)
//             .with_config(trace_config)
//             .build()
//     };
//     global::set_tracer_provider(tracer_provider.clone());
//     Ok(tracer_provider.tracer("lakesail"))
// }
//
// // TODO: init_metrics
// pub fn init_metrics() {}
