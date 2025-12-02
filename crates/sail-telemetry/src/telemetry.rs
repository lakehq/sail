use std::borrow::Cow;
use std::env;
use std::io::Write;
use std::sync::Mutex;
use std::time::Duration;

use fastrace::collector::{Config, Reporter, SpanRecord};
use fastrace_opentelemetry::OpenTelemetryReporter;
use log::Log;
use opentelemetry::{global, InstrumentationScope};
use opentelemetry_appender_log::OpenTelemetryLogBridge;
use opentelemetry_otlp::{LogExporter, Protocol, WithExportConfig};
use opentelemetry_sdk::logs::{BatchConfigBuilder, BatchLogProcessor, SdkLoggerProvider};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::Resource;
use sail_common::config::{OtlpProtocol, TelemetryConfig};

use crate::error::{TelemetryError, TelemetryResult};
use crate::logger::composite::CompositeLogger;
use crate::logger::span::SpanEventLogger;

enum TelemetryStatus {
    Uninitialized,
    Initialized(TelemetryState),
    Failed,
}

#[derive(Default)]
struct TelemetryState {
    meter_provider: Option<SdkMeterProvider>,
    logger_provider: Option<SdkLoggerProvider>,
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
            .report_interval(Duration::from_secs(config.traces_export_interval_secs));
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
    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(config.metrics_export_interval_secs))
        .build();
    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(default_resource())
        .build();
    global::set_meter_provider(provider.clone());
    state.meter_provider = Some(provider);
    Ok(())
}

fn init_logs(config: &TelemetryConfig, state: &mut TelemetryState) -> TelemetryResult<()> {
    let primary =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .format(move |buf, record| {
                let level = record.level();
                let target = record.target();
                let style = buf.default_level_style(level);
                let timestamp = buf.timestamp();
                let args = record.args();
                writeln!(buf, "[{timestamp} {style}{level}{style:#} {target}] {args}")
            })
            .build();
    let primary = Box::new(primary);
    let max_level = primary.filter();

    let mut secondary: Vec<Box<dyn Log>> = vec![];

    if config.export_logs {
        let exporter = LogExporter::builder()
            .with_tonic()
            .with_endpoint(config.otlp_endpoint.clone())
            .with_protocol(get_otlp_protocol(&config.otlp_protocol))
            .with_timeout(Duration::from_secs(config.otlp_timeout_secs))
            .build()?;
        let batch_config = BatchConfigBuilder::default()
            .with_scheduled_delay(Duration::from_secs(config.logs_export_interval_secs))
            .with_max_queue_size(
                usize::try_from(config.logs_export_max_queue_size)
                    .map_err(|_| TelemetryError::invalid("logs export max queue size"))?,
            )
            .with_max_export_batch_size(
                usize::try_from(config.logs_export_batch_size)
                    .map_err(|_| TelemetryError::invalid("logs export batch size"))?,
            )
            .build();
        let processor = BatchLogProcessor::builder(exporter)
            .with_batch_config(batch_config)
            .build();
        let provider = SdkLoggerProvider::builder()
            .with_log_processor(processor)
            .with_resource(default_resource())
            .build();
        secondary.push(Box::new(OpenTelemetryLogBridge::new(&provider)));
        state.logger_provider = Some(provider);
    }
    if config.export_traces {
        secondary.push(Box::new(SpanEventLogger));
    }

    log::set_boxed_logger(Box::new(CompositeLogger::new(primary, secondary)))
        .map_err(|e| TelemetryError::internal(e.to_string()))?;
    log::set_max_level(max_level);
    Ok(())
}

pub fn shutdown_telemetry() {
    fastrace::flush();
    if let Ok(status) = TELEMETRY_STATUS.lock() {
        if let TelemetryStatus::Initialized(ref state) = *status {
            if let Some(provider) = &state.meter_provider {
                let _ = provider.shutdown();
            }
            if let Some(provider) = &state.logger_provider {
                let _ = provider.shutdown();
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
