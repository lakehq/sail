use std::io::Write;

use fastrace::collector::{Config, Reporter, SpanRecord};
use fastrace::prelude::*;

use crate::error::TelemetryResult;
// use log::error;

pub fn init_telemetry() -> TelemetryResult<()> {
    fastrace::set_reporter(
        ConsoleReporter,
        Config::default().report_before_root_finish(true),
    );
    init_logger()?;
    Ok(())
}

pub fn init_logger() -> TelemetryResult<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(|buf, record| {
            Event::add_to_local_parent(record.level().as_str(), || {
                [("message".into(), record.args().to_string().into())]
            });
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
pub struct ConsoleReporter;
impl Reporter for ConsoleReporter {
    fn report(&mut self, _spans: Vec<SpanRecord>) {
        //     pub trace_id: TraceId,
        //     pub span_id: SpanId,
        //     pub parent_id: SpanId,
        //     pub begin_time_unix_ns: u64,
        //     pub duration_ns: u64,
        //     pub name: Cow<'static, str>,
        //     pub properties: Vec<(Cow<'static, str>, Cow<'static, str>)>,
        //     pub events: Vec<EventRecord>,
        // for span in spans {
        //     if !span.events.is_empty() {
        //         // let bytes =  format!("trace_id: {:?}, span_id: {:?}, parent_id: {:?}, begin_time_unix_ns: {}, duration_ns: {}, name: {}, properties: {:?}, events: {:?}\n", span.trace_id, span.span_id, span.parent_id, span.begin_time_unix_ns, span.duration_ns, span.name, span.properties, span.events).into_bytes();
        //         // std::io::stdout()
        //         //     .write_all(&bytes)
        //         //     .map_err(|e| error!("Failed to write to stdout: {e}"))
        //         //     .ok();
        //         eprintln!("trace_id: {:?}, span_id: {:?}, parent_id: {:?}, begin_time_unix_ns: {}, duration_ns: {}, name: {}, properties: {:?}, events: {:?}", span.trace_id, span.span_id, span.parent_id, span.begin_time_unix_ns, span.duration_ns, span.name, span.properties, span.events);
        //     }
        // }
    }
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
