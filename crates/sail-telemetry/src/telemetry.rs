use std::borrow::Cow;
use std::env;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::common::runtime::set_join_set_tracer;
use fastrace::collector::{Config, Reporter, SpanRecord};
use fastrace_opentelemetry::OpenTelemetryReporter;
use log::{Log, debug};
use opentelemetry::InstrumentationScope;
use opentelemetry::logs::LoggerProvider;
use opentelemetry::metrics::{Meter, MeterProvider};
use opentelemetry_appender_log::OpenTelemetryLogBridge;
use opentelemetry_otlp::{LogExporter, Protocol, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::{BatchConfigBuilder, BatchLogProcessor, SdkLoggerProvider};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use sail_common::actor::ActorSystem;
use sail_common::config::{OtlpProtocol, SystemCatalogConfig, SystemCatalogStore, TelemetryConfig};
use sail_system_store::{SystemStoreHandle, SystemStoreReader};

use crate::error::{TelemetryError, TelemetryResult};
use crate::events::{SystemEventLogProcessor, SystemEventReporter};
use crate::execution::join_set::DefaultJoinSetTracer;
use crate::loggers::composite::CompositeLogger;
use crate::loggers::span::SpanEventLogger;
use crate::metrics::{
    MetricManager, MetricRegistry, SystemMetricExporter, SystemMetricExporterTarget,
    SystemMetricReporter,
};
use crate::{ResourceKind, ResourceOptions, SCOPE_NAME};

enum TelemetryStatus {
    Uninitialized,
    Initialized(Box<TelemetryState>),
    Failed,
    Finalized,
}

#[derive(Default)]
struct TelemetryState {
    meter_provider: Option<SdkMeterProvider>,
    meter: Option<Meter>,
    metrics: Option<MetricManager>,
    logger_provider: Option<SdkLoggerProvider>,
    runtime: Option<tokio::runtime::Handle>,
    actor_system: Option<ActorSystem>,
    system_store: Option<SystemStoreHandle>,
}

static TELEMETRY_STATUS: Mutex<TelemetryStatus> = Mutex::new(TelemetryStatus::Uninitialized);

pub fn init_telemetry(
    config: &TelemetryConfig,
    system_config: &SystemCatalogConfig,
    resource: ResourceOptions,
) -> TelemetryResult<()> {
    let mut status = TELEMETRY_STATUS
        .lock()
        .map_err(|e| TelemetryError::internal(e.to_string()))?;

    match *status {
        TelemetryStatus::Uninitialized => {
            let mut state = TelemetryState::default();
            match init_traces(config, &mut state, &resource)
                .and_then(|()| init_system_store(system_config, &mut state, &resource))
                .and_then(|()| init_logs(config, &mut state, &resource))
                .and_then(|()| init_metrics(config, &mut state, &resource))
                .and_then(|()| init_datafusion_telemetry())
            {
                Ok(()) => {
                    debug!("OpenTelemetry initialized");
                    *status = TelemetryStatus::Initialized(Box::new(state));
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
        TelemetryStatus::Finalized => Err(TelemetryError::internal(
            "telemetry has been finalized and cannot be re-initialized",
        )),
    }
}

fn init_system_store(
    config: &SystemCatalogConfig,
    state: &mut TelemetryState,
    resource: &ResourceOptions,
) -> TelemetryResult<()> {
    state.runtime = Some(tokio::runtime::Handle::try_current().map_err(|error| {
        TelemetryError::internal(format!("failed to get runtime handle: {error}"))
    })?);
    // The system catalog is owned by server processes. Workers report telemetry to the driver
    // and must not open the configured store path themselves.
    if resource.kind == ResourceKind::Worker {
        return Ok(());
    }
    let mut actor_system = ActorSystem::new();
    let handle = match &config.store {
        SystemCatalogStore::Memory => Ok(SystemStoreHandle::memory(&mut actor_system)),
        SystemCatalogStore::Disk { path } => SystemStoreHandle::fjall(&mut actor_system, path),
    }
    .map_err(|error| {
        TelemetryError::internal(format!("failed to initialize system store: {error}"))
    })?;
    state.system_store = Some(handle);
    state.actor_system = Some(actor_system);
    Ok(())
}

fn init_traces(
    config: &TelemetryConfig,
    _: &mut TelemetryState,
    resource: &ResourceOptions,
) -> TelemetryResult<()> {
    if config.export_traces
        && let Some(endpoint) = &config.exporter.otlp.endpoint
    {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint.clone())
            .with_protocol(get_otlp_protocol(&config.exporter.otlp.protocol))
            .with_timeout(Duration::from_secs(config.exporter.otlp.timeout_secs))
            .build()?;
        let reporter = OpenTelemetryReporter::new(
            exporter,
            Cow::Owned(get_resource(resource)),
            get_instrumentation_scope(),
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

fn init_metrics(
    config: &TelemetryConfig,
    state: &mut TelemetryState,
    resource: &ResourceOptions,
) -> TelemetryResult<()> {
    if config.export_metrics {
        let mut provider = SdkMeterProvider::builder().with_resource(get_resource(resource));
        if config.exporter.system.enabled {
            let target = if resource.kind == ResourceKind::Worker {
                SystemMetricExporterTarget::Remote
            } else {
                SystemMetricExporterTarget::Local(SystemMetricReporter::new(
                    state.system_store.clone().ok_or_else(|| {
                        TelemetryError::internal("system store telemetry is not initialized")
                    })?,
                ))
            };
            let system_reader = PeriodicReader::builder(SystemMetricExporter::new(target))
                .with_interval(Duration::from_secs(config.metrics_export_interval_secs))
                .build();
            provider = provider.with_reader(system_reader);
        }
        if let Some(endpoint) = &config.exporter.otlp.endpoint {
            let exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint.clone())
                .with_protocol(get_otlp_protocol(&config.exporter.otlp.protocol))
                .with_timeout(Duration::from_secs(config.exporter.otlp.timeout_secs))
                // Emit only active metrics. Since metrics may have attributes for sessions or jobs,
                // we do not want to emit metrics after the session or job has ended.
                .with_temporality(Temporality::Delta)
                .build()?;
            let reader = PeriodicReader::builder(exporter)
                .with_interval(Duration::from_secs(config.metrics_export_interval_secs))
                .build();
            provider = provider.with_reader(reader);
        }
        let provider = provider.build();
        let meter = provider.meter_with_scope(get_instrumentation_scope());
        state.meter_provider = Some(provider);
        state.metrics = Some(MetricManager {
            registry: Arc::new(MetricRegistry::new(&meter)),
            collection_interval: Duration::from_secs(config.metrics_collection_interval_secs),
        });
        state.meter = Some(meter);
    }
    Ok(())
}

fn init_logs(
    config: &TelemetryConfig,
    state: &mut TelemetryState,
    resource: &ResourceOptions,
) -> TelemetryResult<()> {
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
    let system_store = state.system_store.clone();
    let mut provider = SdkLoggerProvider::builder().with_resource(get_resource(resource));
    if let Some(store) = system_store {
        provider = provider.with_log_processor(SystemEventLogProcessor::new(store));
    }

    if config.export_logs
        && let Some(endpoint) = &config.exporter.otlp.endpoint
    {
        let exporter = LogExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint.clone())
            .with_protocol(get_otlp_protocol(&config.exporter.otlp.protocol))
            .with_timeout(Duration::from_secs(config.exporter.otlp.timeout_secs))
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
        provider = provider.with_log_processor(processor);
    }
    let provider = provider.build();
    if config.export_logs && config.exporter.otlp.endpoint.is_some() {
        secondary.push(Box::new(OpenTelemetryLogBridge::new(&provider)));
    }
    state.logger_provider = Some(provider);
    if config.export_traces && config.exporter.otlp.endpoint.is_some() {
        secondary.push(Box::new(SpanEventLogger));
    }

    log::set_boxed_logger(Box::new(CompositeLogger::new(primary, secondary)))
        .map_err(|e| TelemetryError::internal(e.to_string()))?;
    log::set_max_level(max_level);
    Ok(())
}

fn init_datafusion_telemetry() -> TelemetryResult<()> {
    set_join_set_tracer(&DefaultJoinSetTracer)
        .map_err(|e| TelemetryError::internal(e.to_string()))?;
    Ok(())
}

pub fn shutdown_telemetry() {
    debug!("Shutting down OpenTelemetry...");
    fastrace::flush();
    let state = TELEMETRY_STATUS.lock().ok().and_then(|mut status| {
        let previous = std::mem::replace(&mut *status, TelemetryStatus::Finalized);
        match previous {
            TelemetryStatus::Initialized(state) => Some(state),
            previous => {
                *status = previous;
                None
            }
        }
    });
    if let Some(state) = state {
        if let Some(provider) = state.meter_provider {
            let _ = provider.shutdown();
        }
        if let Some(provider) = state.logger_provider {
            let _ = provider.shutdown();
        }
        if let (Some(runtime), Some(store)) = (state.runtime.clone(), state.system_store) {
            runtime.block_on(async {
                let _ = store.flush().await;
                let _ = store.shutdown().await;
            });
        }
        if let (Some(runtime), Some(mut actor_system)) = (state.runtime, state.actor_system) {
            runtime.block_on(actor_system.join());
        }
    }
}

pub fn global_metrics() -> Option<MetricManager> {
    TELEMETRY_STATUS
        .lock()
        .ok()
        .and_then(|status| match &*status {
            TelemetryStatus::Initialized(state) => state.metrics.clone(),
            _ => None,
        })
}

pub fn global_system_store_reader() -> Option<SystemStoreReader> {
    TELEMETRY_STATUS
        .lock()
        .ok()
        .and_then(|status| match &*status {
            TelemetryStatus::Initialized(state) => {
                state.system_store.as_ref().map(SystemStoreHandle::reader)
            }
            _ => None,
        })
}

pub fn global_system_event_reporter() -> Option<SystemEventReporter> {
    TELEMETRY_STATUS
        .lock()
        .ok()
        .and_then(|status| match &*status {
            TelemetryStatus::Initialized(state) => state.logger_provider.as_ref().map(|provider| {
                SystemEventReporter::new(provider.logger_with_scope(get_instrumentation_scope()))
            }),
            _ => None,
        })
}

pub fn global_system_metric_reporter() -> Option<SystemMetricReporter> {
    TELEMETRY_STATUS
        .lock()
        .ok()
        .and_then(|status| match &*status {
            TelemetryStatus::Initialized(state) => {
                state.system_store.clone().map(SystemMetricReporter::new)
            }
            _ => None,
        })
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

fn get_resource(resource: &ResourceOptions) -> Resource {
    Resource::builder()
        .with_service_name(resource.kind.service_name())
        .build()
}

fn get_instrumentation_scope() -> InstrumentationScope {
    InstrumentationScope::builder(SCOPE_NAME)
        .with_version(env!("CARGO_PKG_VERSION"))
        .build()
}
