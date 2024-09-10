use std::env;
use std::io::Write;
use std::time::Duration;

use fastrace::collector::{Config, Reporter, SpanRecord};
use fastrace_opentelemetry::OpenTelemetryReporter;
use fastrace::prelude::*;
// use log::error;
use crate::error::TelemetryResult;


use opentelemetry::global;
use opentelemetry::trace::{TraceError, TracerProvider};
use opentelemetry_otlp::{self, WithExportConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::resource::{
    EnvResourceDetector, ResourceDetector, SdkProvidedResourceDetector, TelemetryResourceDetector,
};
use opentelemetry_sdk::{runtime, trace as sdktrace};

pub fn init_telemetry() -> TelemetryResult<()> {
    let use_collector = match env::var("SAIL_OPENTELEMETRY_COLLECTOR") {
        Ok(val) => !val.is_empty(),
        Err(_) => false,
    };
    fastrace::set_reporter(
        DummyReporter,
        Config::default().report_before_root_finish(true),
    );
    init_tracer(use_collector)?;
    init_logger(use_collector)?;
    Ok(())
}

pub fn init_tracer(use_collector: bool) -> TelemetryResult<()> {
    if use_collector {
        let sdk_resource = SdkProvidedResourceDetector.detect(Duration::from_secs(0));
        let env_resource = EnvResourceDetector::new().detect(Duration::from_secs(0));
        let telemetry_resource = TelemetryResourceDetector.detect(Duration::from_secs(0));
        fastrace::set_reporter(
            DummyReporter,
            Config::default().report_before_root_finish(true),
        )
    } else {
        fastrace::set_reporter(
            DummyReporter,
            Config::default().report_before_root_finish(true),
        )
    };
    Ok(())
}

pub fn init_logger(use_collector: bool) -> TelemetryResult<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(move |buf, record| {
            if use_collector {
                Event::add_to_local_parent(record.level().as_str(), || {
                    [("message".into(), record.args().to_string().into())]
                });
            }
            let level = record.level();
            let target = record.module_path().unwrap();
            let style = buf.default_level_style(level);
            let timestamp = buf.timestamp();
            let args = record.args();
            if let Some(span_context) = SpanContext::current_local_parent() {
                let trace_id = span_context.trace_id.0;
                let span_id = span_context.span_id.0;
                writeln!(buf, "[{timestamp} {style}{level}{style:#} {target} trace: {trace_id} span: {span_id}] {args}")
            } else {
                writeln!(buf, "[{timestamp} {style}{level}{style:#} {target}] {args}")
            }
        })
        .init();
    Ok(())
}

pub struct DummyReporter;
impl Reporter for DummyReporter {
    fn report(&mut self, _spans: Vec<SpanRecord>) {
    }
}

// pub fn init_tracer() -> Result<sdktrace::Tracer, TelemetryError> {
//     global::set_text_map_propagator(TraceContextPropagator::new());
//
//
//     let trace_config = sdktrace::Config::default()
//         .with_resource(sdk_resource.merge(&env_resource).merge(&telemetry_resource));
//
//     let use_collector = match env::var("SAIL_OPENTELEMETRY_COLLECTOR") {
//         Ok(val) => !val.is_empty(),
//         Err(_) => false,
//     };
//     let tracer_provider = if use_collector {
//         let host = env::var("SAIL_OPENTELEMETRY_COLLECTOR_SERVICE_HOST")?;
//         let port = env::var("SAIL_OPENTELEMETRY_COLLECTOR_SERVICE_PORT_OTLP_GRPC")?;
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
