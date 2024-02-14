use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::TracerProvider;
use opentelemetry_stdout as stdout;
use tracing::{error, error_span, span};
use tracing_subscriber::{
    fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry
};

fn main() {
    // Create a new OpenTelemetry trace pipeline that prints to stdout
    let provider = TracerProvider::builder()
        .with_simple_exporter(stdout::SpanExporter::default())
        .build();

    let tracer = provider.tracer("hello_example");

    // Create a tracing layer with the configured tracer
    let telemetry = tracing_opentelemetry::layer()
        .with_tracer(tracer);

    let subscriber = Registry::default()
        .with(telemetry)
        .with(EnvFilter::from_default_env())
        .with(fmt::layer().json())
        .try_init();

    {
        let root = error_span!("Meow Meow");
        let _enter = root.enter();
        error!("Meow Meow Meow I am Error Log in Trace"); // Works!!!!
    }

    error!("Meow Meow Meow I am Normal Error Log"); // Works!!!!
}

// Important reading:
// https://github.com/tokio-rs/tracing?tab=readme-ov-file#in-libraries

// "Libraries should NOT call set_global_default()! That will cause conflicts when executables try to set them later."
// https://docs.rs/tracing/latest/tracing/subscriber/fn.set_global_default.html