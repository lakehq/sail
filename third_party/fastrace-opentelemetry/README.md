# fastrace-opentelemetry

[![Documentation](https://docs.rs/fastrace-opentelemetry/badge.svg)](https://docs.rs/fastrace-opentelemetry/)
[![Crates.io](https://img.shields.io/crates/v/fastrace-opentelemetry.svg)](https://crates.io/crates/fastrace-opentelemetry)
[![LICENSE](https://img.shields.io/github/license/fast/fastrace.svg)](https://github.com/fast/fastrace/blob/main/LICENSE)

[OpenTelemetry](https://github.com/open-telemetry/opentelemetry-rust) reporter for [`fastrace`](https://crates.io/crates/fastrace).

## Dependencies

```toml
[dependencies]
fastrace = "0.7"
fastrace-opentelemetry = "0.13"
```

## Setup OpenTelemetry Collector

Start OpenTelemetry Collector with Jaeger and Zipkin receivers:

```shell
docker compose -f dev/docker-compose.yaml up
```

Then, run the synchronous example:

```shell
cargo run --example synchronous
```

Jaeger UI is available on [http://127.0.0.1:16686/](http://127.0.0.1:16686/)

Zipkin UI is available on [http://127.0.0.1:9411/](http://127.0.0.1:9411/)

## Report to OpenTelemetry Collector

```rust, no_run
use std::borrow::Cow;
use fastrace::collector::Config;
use fastrace::prelude::*;
use fastrace_opentelemetry::OpenTelemetryReporter;
use opentelemetry_otlp::ExportConfig;
use opentelemetry_otlp::Protocol;
use opentelemetry_otlp::SpanExporter;
use opentelemetry_otlp::TonicConfig;
use opentelemetry_sdk::Resource;
use opentelemetry::KeyValue;
use opentelemetry::InstrumentationScope;
use opentelemetry_otlp::WithExportConfig;

// Initialize reporter
let reporter = OpenTelemetryReporter::new(
    SpanExporter::builder()
        .with_tonic()
        .with_endpoint("http://127.0.0.1:4317".to_string())
        .with_protocol(opentelemetry_otlp::Protocol::Grpc)
        .with_timeout(opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT)
        .build()
        .expect("initialize oltp exporter"),
    Cow::Owned(
        Resource::builder()
            .with_attributes([KeyValue::new("service.name", "asynchronous")])
            .build()
    ),
    InstrumentationScope::builder("example-crate").with_version(env!("CARGO_PKG_VERSION")).build(),
);
fastrace::set_reporter(reporter, Config::default());

{
    // Start tracing
    let root = Span::root("root", SpanContext::random());
}

fastrace::flush()
```
