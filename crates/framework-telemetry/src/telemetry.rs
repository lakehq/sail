use std::time::Duration;

use opentelemetry::global;
use opentelemetry::trace::TraceError;
use opentelemetry_otlp::{self, WithExportConfig};
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    resource::{
        EnvResourceDetector, OsResourceDetector, ProcessResourceDetector, ResourceDetector,
        SdkProvidedResourceDetector, TelemetryResourceDetector,
    },
    runtime, trace as sdktrace,
};

use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry};

pub fn init_telemetry() -> Result<(), tracing::subscriber::SetGlobalDefaultError> {
    // Methods to inject & extract text into injectors & extractors across process boundaries.
    global::set_text_map_propagator(TraceContextPropagator::new());

    let os_resource = OsResourceDetector.detect(Duration::from_secs(0));
    let process_resource = ProcessResourceDetector.detect(Duration::from_secs(0));
    let sdk_resource = SdkProvidedResourceDetector.detect(Duration::from_secs(0));
    let env_resource = EnvResourceDetector::new().detect(Duration::from_secs(0));
    let telemetry_resource = TelemetryResourceDetector.detect(Duration::from_secs(0));

    let subscriber = Registry::default().with(fmt::layer().json().flatten_event(true));
    // .with(EnvFilter::from_default_env());

    tracing::subscriber::set_global_default(subscriber)
}

//
// use opentelemetry::trace::TracerProvider as _;
// use opentelemetry_sdk::trace::TracerProvider;
// use opentelemetry_stdout as stdout;
// use tracing::{error, error_span, span};
// use tracing_subscriber::{
//     fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry
// };
//
// fn main() {
//     // Create a new OpenTelemetry trace pipeline that prints to stdout
//     let provider = TracerProvider::builder()
//         .with_simple_exporter(stdout::SpanExporter::default())
//         .build();
//
//     let tracer = provider.tracer("hello_example");
//
//     // Create a tracing layer with the configured tracer
//     let framework-telemetry = tracing_opentelemetry::layer()
//         .with_tracer(tracer);
//
//     let subscriber = Registry::default()
//         .with(framework-telemetry)
//         .with(EnvFilter::from_default_env())
//         .with(fmt::layer().json())
//         .try_init();
//
//     {
//         let root = error_span!("Meow Meow");
//         let _enter = root.enter();
//         error!("Meow Meow Meow I am Error Log in Trace"); // Works!!!!
//     }
//
//     error!("Meow Meow Meow I am Normal Error Log"); // Works!!!!
// }

// Important reading:
// https://github.com/tokio-rs/tracing?tab=readme-ov-file#in-libraries

// "Libraries should NOT call set_global_default()! That will cause conflicts when executables try to set them later."
// https://docs.rs/tracing/latest/tracing/subscriber/fn.set_global_default.html
