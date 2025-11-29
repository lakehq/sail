use std::borrow::Cow;
use std::env;
use std::sync::Mutex;
use std::time::Duration;

use fastrace::collector::{Config, Reporter, SpanRecord};
use fastrace_opentelemetry::OpenTelemetryReporter;
use logforth::append::opentelemetry::OpentelemetryLogBuilder;
use logforth::append::Stderr;
use logforth::filter::env_filter::EnvFilterBuilder;
use opentelemetry::{global, InstrumentationScope};
use opentelemetry_otlp::{LogExporter, Protocol, WithExportConfig};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::Resource;
use sail_common::config::{OtlpProtocol, TelemetryConfig};

use crate::error::{TelemetryError, TelemetryResult};

enum TelemetryStatus {
    Uninitialized,
    Initialized(TelemetryState),
    Failed,
}

#[derive(Default)]
struct TelemetryState {
    meter_provider: Option<SdkMeterProvider>,
}

static TELEMETRY_STATUS: Mutex<TelemetryStatus> = Mutex::new(TelemetryStatus::Uninitialized);

pub fn init_telemetry(config: &TelemetryConfig) -> TelemetryResult<()> {
    let mut status = TELEMETRY_STATUS
        .lock()
        .map_err(|e| TelemetryError::internal(e.to_string()))?;

    match *status {
        TelemetryStatus::Uninitialized => {
            let mut state = TelemetryState::default();
            match init_traces(config, &mut state)
                .and_then(|_| init_metrics(config, &mut state))
                .and_then(|_| init_logs(config, &mut state))
            {
                Ok(()) => {
                    *status = TelemetryStatus::Initialized(state);
                    Ok(())
                }
                Err(e) => {
                    *status = TelemetryStatus::Failed;
                    Err(e)
                }
            }
        }
        TelemetryStatus::Initialized(_) => {
            Err(TelemetryError::internal("telemetry already initialized"))
        }
        TelemetryStatus::Failed => Err(TelemetryError::internal(
            "telemetry failed to initialize previously",
        )),
    }
}

fn init_traces(config: &TelemetryConfig, _: &mut TelemetryState) -> TelemetryResult<()> {
    if config.export_traces {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(config.otlp_endpoint.clone())
            .with_protocol(get_otlp_protocol(&config.otlp_protocol))
            .with_timeout(Duration::from_secs(config.otlp_timeout_secs))
            .build()?;
        let reporter = OpenTelemetryReporter::new(
            exporter,
            Cow::Owned(default_resource()),
            default_instrumentation_scope(),
        );
        let reporter_config = Config::default()
            .report_interval(Duration::from_millis(config.traces_export_millisecs));
        fastrace::set_reporter(reporter, reporter_config)
    } else {
        let reporter_config = Config::default().report_interval(Duration::MAX);
        // We use `NoOpReporter` instead of `ConsoleReporter` to avoid duplicated span output
        // to the console.
        fastrace::set_reporter(NoOpReporter, reporter_config)
    }
    Ok(())
}

fn init_metrics(config: &TelemetryConfig, state: &mut TelemetryState) -> TelemetryResult<()> {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(config.otlp_endpoint.clone())
        .with_protocol(get_otlp_protocol(&config.otlp_protocol))
        .with_timeout(Duration::from_secs(config.otlp_timeout_secs))
        .build()?;
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(default_resource())
        .build();
    global::set_meter_provider(provider.clone());
    state.meter_provider = Some(provider);
    Ok(())
}

fn init_logs(config: &TelemetryConfig, _: &mut TelemetryState) -> TelemetryResult<()> {
    let exporter = if config.export_logs {
        let exporter = LogExporter::builder()
            .with_tonic()
            .with_endpoint(config.otlp_endpoint.clone())
            .with_protocol(get_otlp_protocol(&config.otlp_protocol))
            .with_timeout(Duration::from_secs(config.otlp_timeout_secs))
            .build()?;
        Some(exporter)
    } else {
        None
    };
    logforth::core::builder()
        .dispatch(|d| {
            let d = d.filter(EnvFilterBuilder::from_default_env().build());
            let d = if let Some(exporter) = exporter {
                d.append(
                    OpentelemetryLogBuilder::new("sail", exporter)
                        .label("service.name", "sail")
                        .build(),
                )
            } else {
                d.append(Stderr::default())
            };
            if config.export_traces {
                d.append(logforth::append::FastraceEvent::default())
            } else {
                d
            }
        })
        .try_apply()
        .map_err(|_| TelemetryError::internal("the global logger is already set"))?;
    logforth::bridge::log::try_setup().map_err(|e| TelemetryError::internal(e.to_string()))?;
    Ok(())
}

pub fn shutdown_telemetry() {
    fastrace::flush();
    if let Ok(status) = TELEMETRY_STATUS.lock() {
        if let TelemetryStatus::Initialized(ref state) = *status {
            if let Some(meter_provider) = &state.meter_provider {
                let _ = meter_provider.shutdown();
            }
        }
    }
}

/// A fastrace reporter that does nothing.
pub struct NoOpReporter;

impl Reporter for NoOpReporter {
    fn report(&mut self, _spans: Vec<SpanRecord>) {}
}

fn get_otlp_protocol(protocol: &OtlpProtocol) -> Protocol {
    match protocol {
        OtlpProtocol::Grpc => Protocol::Grpc,
        OtlpProtocol::HttpBinary => Protocol::HttpBinary,
        OtlpProtocol::HttpJson => Protocol::HttpJson,
    }
}

fn default_resource() -> Resource {
    Resource::builder().with_service_name("sail").build()
}

fn default_instrumentation_scope() -> InstrumentationScope {
    InstrumentationScope::builder("sail")
        .with_version(env!("CARGO_PKG_VERSION"))
        .build()
}
