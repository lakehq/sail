//! OTLP receiver and actor-backed in-memory system telemetry store.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, RecordBatch, StringArray, StructArray, TimestampNanosecondArray,
    UInt32Array, UInt64Array,
};
use datafusion::arrow::compute::{and, concat_batches, filter_record_batch};
use datafusion::arrow::datatypes::{Field, SchemaRef};
use datafusion::common::{Result, internal_datafusion_err};
use datafusion::physical_expr::PhysicalExpr;
use opentelemetry_proto::tonic::collector::logs::v1 as collector_logs;
use opentelemetry_proto::tonic::collector::metrics::v1 as collector_metrics;
use opentelemetry_proto::tonic::collector::trace::v1 as collector_trace;
use opentelemetry_proto::tonic::common::v1 as common;
use opentelemetry_proto::tonic::metrics::v1 as metrics;
use parquet_variant_compute::VariantArrayBuilder;
use parquet_variant_json::JsonToVariant;
use sail_common_datafusion::system::catalog::SystemTable;
use sail_common_datafusion::variant::{VARIANT_METADATA_MARKER_KEY, VARIANT_METADATA_MARKER_VALUE};
use serde_json::{Map, Value, json};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};

const CHANNEL_CAPACITY: usize = 128;
const MAX_BATCHES_PER_TABLE: usize = 1_024;

#[derive(Clone)]
pub struct TelemetryStoreHandle {
    sender: mpsc::Sender<StoreMessage>,
}

impl Default for TelemetryStoreHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl TelemetryStoreHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
        tokio::spawn(TelemetryStoreActor::default().run(receiver));
        Self { sender }
    }

    pub async fn ingest(&self, table: SystemTable, batch: RecordBatch) -> Result<(), Status> {
        self.sender
            .send(StoreMessage::Ingest { table, batch })
            .await
            .map_err(|_| Status::unavailable("telemetry store is unavailable"))
    }

    pub async fn read(
        &self,
        table: SystemTable,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(StoreMessage::Read {
                table,
                filters,
                fetch,
                sender,
            })
            .await
            .map_err(|_| internal_datafusion_err!("telemetry store is unavailable"))?;
        receiver
            .await
            .map_err(|_| internal_datafusion_err!("telemetry store actor stopped"))?
    }
}

enum StoreMessage {
    Ingest {
        table: SystemTable,
        batch: RecordBatch,
    },
    Read {
        table: SystemTable,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        fetch: usize,
        sender: oneshot::Sender<Result<RecordBatch>>,
    },
}

#[derive(Default)]
struct TelemetryStoreActor {
    batches: HashMap<SystemTable, VecDeque<RecordBatch>>,
}

impl TelemetryStoreActor {
    async fn run(mut self, mut receiver: mpsc::Receiver<StoreMessage>) {
        while let Some(message) = receiver.recv().await {
            match message {
                StoreMessage::Ingest { table, batch } => {
                    let batches = self.batches.entry(table).or_default();
                    batches.push_back(batch);
                    if batches.len() > MAX_BATCHES_PER_TABLE {
                        let _ = batches.pop_front();
                    }
                }
                StoreMessage::Read {
                    table,
                    filters,
                    fetch,
                    sender,
                } => {
                    let result = self.read(table, &filters, fetch);
                    let _ = sender.send(result);
                }
            }
        }
    }

    fn read(
        &self,
        table: SystemTable,
        filters: &[Arc<dyn PhysicalExpr>],
        fetch: usize,
    ) -> Result<RecordBatch> {
        let schema = table.schema();
        let mut remaining = fetch;
        let mut output = vec![];
        for batch in self.batches.get(&table).into_iter().flatten() {
            if remaining == 0 {
                break;
            }
            let batch = apply_filters(batch, filters)?;
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = if batch.num_rows() > remaining {
                batch.slice(0, remaining)
            } else {
                batch
            };
            remaining -= batch.num_rows();
            output.push(batch);
        }
        if output.is_empty() {
            Ok(RecordBatch::new_empty(schema))
        } else {
            Ok(concat_batches(&schema, output.iter())?)
        }
    }
}

fn apply_filters(batch: &RecordBatch, filters: &[Arc<dyn PhysicalExpr>]) -> Result<RecordBatch> {
    let mut mask: Option<BooleanArray> = None;
    for filter in filters {
        let value = filter.evaluate(batch)?.into_array(batch.num_rows())?;
        let value = value
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| internal_datafusion_err!("telemetry filter did not return boolean"))?;
        mask = Some(match mask {
            Some(mask) => and(&mask, value)?,
            None => value.clone(),
        });
    }
    match mask {
        Some(mask) => Ok(filter_record_batch(batch, &mask)?),
        None => Ok(batch.clone()),
    }
}

#[derive(Clone)]
pub struct CollectorService {
    store: TelemetryStoreHandle,
}

impl CollectorService {
    pub fn new(store: TelemetryStoreHandle) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl collector_trace::trace_service_server::TraceService for CollectorService {
    async fn export(
        &self,
        request: Request<collector_trace::ExportTraceServiceRequest>,
    ) -> std::result::Result<Response<collector_trace::ExportTraceServiceResponse>, Status> {
        if let Some(batch) = traces_batch(request.into_inner())? {
            self.store.ingest(SystemTable::Traces, batch).await?;
        }
        Ok(Response::new(
            collector_trace::ExportTraceServiceResponse::default(),
        ))
    }
}

#[tonic::async_trait]
impl collector_logs::logs_service_server::LogsService for CollectorService {
    async fn export(
        &self,
        request: Request<collector_logs::ExportLogsServiceRequest>,
    ) -> std::result::Result<Response<collector_logs::ExportLogsServiceResponse>, Status> {
        if let Some(batch) = logs_batch(request.into_inner())? {
            self.store.ingest(SystemTable::Logs, batch).await?;
        }
        Ok(Response::new(
            collector_logs::ExportLogsServiceResponse::default(),
        ))
    }
}

#[tonic::async_trait]
impl collector_metrics::metrics_service_server::MetricsService for CollectorService {
    async fn export(
        &self,
        request: Request<collector_metrics::ExportMetricsServiceRequest>,
    ) -> std::result::Result<Response<collector_metrics::ExportMetricsServiceResponse>, Status>
    {
        for (table, batch) in metrics_batches(request.into_inner())? {
            self.store.ingest(table, batch).await?;
        }
        Ok(Response::new(
            collector_metrics::ExportMetricsServiceResponse::default(),
        ))
    }
}

pub async fn serve(
    listener: TcpListener,
    store: TelemetryStoreHandle,
    shutdown: impl std::future::Future<Output = ()>,
) -> std::result::Result<(), tonic::transport::Error> {
    let service = CollectorService::new(store);
    tonic::transport::Server::builder()
        .add_service(
            collector_trace::trace_service_server::TraceServiceServer::new(service.clone()),
        )
        .add_service(collector_logs::logs_service_server::LogsServiceServer::new(
            service.clone(),
        ))
        .add_service(collector_metrics::metrics_service_server::MetricsServiceServer::new(service))
        .serve_with_incoming_shutdown(TcpIncoming::from(listener), shutdown)
        .await
}

fn traces_batch(
    request: collector_trace::ExportTraceServiceRequest,
) -> std::result::Result<Option<RecordBatch>, Status> {
    let mut rows = vec![];
    for resource_spans in request.resource_spans {
        let resource_attributes = attributes_json(
            resource_spans
                .resource
                .as_ref()
                .map(|x| x.attributes.as_slice())
                .unwrap_or_default(),
        );
        let service_name = service_name(&resource_attributes);
        for scope_spans in resource_spans.scope_spans {
            let scope = scope_spans.scope.unwrap_or_default();
            for span in scope_spans.spans {
                rows.push(TraceRow {
                    timestamp: span.start_time_unix_nano,
                    trace_id: hex(&span.trace_id),
                    span_id: hex(&span.span_id),
                    parent_span_id: hex(&span.parent_span_id),
                    trace_state: span.trace_state,
                    span_name: span.name,
                    span_kind: span.kind.to_string(),
                    service_name: service_name.clone(),
                    resource_attributes: resource_attributes.clone(),
                    scope_name: scope.name.clone(),
                    scope_version: scope.version.clone(),
                    span_attributes: attributes_json(&span.attributes),
                    duration: span
                        .end_time_unix_nano
                        .saturating_sub(span.start_time_unix_nano),
                    status_code: span
                        .status
                        .as_ref()
                        .map(|x| x.code.to_string())
                        .unwrap_or_default(),
                    status_message: span.status.map(|x| x.message).unwrap_or_default(),
                    events: Value::String(format!("{:?}", span.events)),
                    links: Value::String(format!("{:?}", span.links)),
                });
            }
        }
    }
    (!rows.is_empty())
        .then(|| build_trace_batch(rows))
        .transpose()
}

fn logs_batch(
    request: collector_logs::ExportLogsServiceRequest,
) -> std::result::Result<Option<RecordBatch>, Status> {
    let mut rows = vec![];
    for resource_logs in request.resource_logs {
        let resource_attributes = attributes_json(
            resource_logs
                .resource
                .as_ref()
                .map(|x| x.attributes.as_slice())
                .unwrap_or_default(),
        );
        let service_name = service_name(&resource_attributes);
        for scope_logs in resource_logs.scope_logs {
            let scope = scope_logs.scope.unwrap_or_default();
            let scope_attributes = attributes_json(&scope.attributes);
            for log in scope_logs.log_records {
                rows.push(LogRow {
                    timestamp: if log.time_unix_nano == 0 {
                        log.observed_time_unix_nano
                    } else {
                        log.time_unix_nano
                    },
                    trace_id: hex(&log.trace_id),
                    span_id: hex(&log.span_id),
                    trace_flags: log.flags,
                    severity_text: log.severity_text,
                    severity_number: log.severity_number as u32,
                    service_name: service_name.clone(),
                    body: log.body.map(|x| any_value_json(&x)).unwrap_or(Value::Null),
                    resource_schema_url: resource_logs.schema_url.clone(),
                    resource_attributes: resource_attributes.clone(),
                    scope_schema_url: scope_logs.schema_url.clone(),
                    scope_name: scope.name.clone(),
                    scope_version: scope.version.clone(),
                    scope_attributes: scope_attributes.clone(),
                    log_attributes: attributes_json(&log.attributes),
                    event_name: log.event_name,
                });
            }
        }
    }
    (!rows.is_empty())
        .then(|| build_log_batch(rows))
        .transpose()
}

fn metrics_batches(
    request: collector_metrics::ExportMetricsServiceRequest,
) -> std::result::Result<Vec<(SystemTable, RecordBatch)>, Status> {
    let mut rows: HashMap<SystemTable, Vec<MetricRow>> = HashMap::new();
    for resource_metrics in request.resource_metrics {
        let resource_attributes = attributes_json(
            resource_metrics
                .resource
                .as_ref()
                .map(|x| x.attributes.as_slice())
                .unwrap_or_default(),
        );
        let service_name = service_name(&resource_attributes);
        for scope_metrics in resource_metrics.scope_metrics {
            let scope = scope_metrics.scope.unwrap_or_default();
            for metric in scope_metrics.metrics {
                let Some(data) = metric.data else { continue };
                let (table, points) = metric_rows(data);
                for point in points {
                    rows.entry(table).or_default().push(MetricRow {
                        resource_attributes: resource_attributes.clone(),
                        service_name: service_name.clone(),
                        scope_name: scope.name.clone(),
                        scope_version: scope.version.clone(),
                        metric_name: metric.name.clone(),
                        metric_description: metric.description.clone(),
                        metric_unit: metric.unit.clone(),
                        ..point
                    });
                }
            }
        }
    }
    rows.into_iter()
        .map(|(table, rows)| Ok((table, build_metric_batch(table.schema(), rows)?)))
        .collect()
}

fn metric_rows(data: metrics::metric::Data) -> (SystemTable, Vec<MetricRow>) {
    use metrics::metric::Data;
    match data {
        Data::Gauge(gauge) => (
            SystemTable::MetricsGauge,
            gauge
                .data_points
                .into_iter()
                .map(number_metric_row)
                .collect(),
        ),
        Data::Sum(sum) => (
            SystemTable::MetricsSum,
            sum.data_points.into_iter().map(number_metric_row).collect(),
        ),
        Data::Histogram(histogram) => (
            SystemTable::MetricsHistogram,
            histogram
                .data_points
                .into_iter()
                .map(debug_metric_row)
                .collect(),
        ),
        Data::ExponentialHistogram(histogram) => (
            SystemTable::MetricsExponentialHistogram,
            histogram
                .data_points
                .into_iter()
                .map(debug_metric_row)
                .collect(),
        ),
        Data::Summary(summary) => (
            SystemTable::MetricsSummary,
            summary
                .data_points
                .into_iter()
                .map(debug_metric_row)
                .collect(),
        ),
    }
}

fn number_metric_row(point: metrics::NumberDataPoint) -> MetricRow {
    let value = match point.value {
        Some(metrics::number_data_point::Value::AsDouble(value)) => json!(value),
        Some(metrics::number_data_point::Value::AsInt(value)) => json!(value),
        None => Value::Null,
    };
    MetricRow {
        attributes: attributes_json(&point.attributes),
        start_time: point.start_time_unix_nano,
        time: point.time_unix_nano,
        value,
        flags: point.flags,
        exemplars: Value::String(format!("{:?}", point.exemplars)),
        ..Default::default()
    }
}

fn debug_metric_row<T: std::fmt::Debug>(point: T) -> MetricRow {
    MetricRow {
        value: Value::String(format!("{point:?}")),
        ..Default::default()
    }
}

#[derive(Default)]
struct MetricRow {
    resource_attributes: Value,
    service_name: String,
    scope_name: String,
    scope_version: String,
    metric_name: String,
    metric_description: String,
    metric_unit: String,
    attributes: Value,
    start_time: u64,
    time: u64,
    value: Value,
    flags: u32,
    exemplars: Value,
}
struct TraceRow {
    timestamp: u64,
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    trace_state: String,
    span_name: String,
    span_kind: String,
    service_name: String,
    resource_attributes: Value,
    scope_name: String,
    scope_version: String,
    span_attributes: Value,
    duration: u64,
    status_code: String,
    status_message: String,
    events: Value,
    links: Value,
}
struct LogRow {
    timestamp: u64,
    trace_id: String,
    span_id: String,
    trace_flags: u32,
    severity_text: String,
    severity_number: u32,
    service_name: String,
    body: Value,
    resource_schema_url: String,
    resource_attributes: Value,
    scope_schema_url: String,
    scope_name: String,
    scope_version: String,
    scope_attributes: Value,
    log_attributes: Value,
    event_name: String,
}

fn build_trace_batch(rows: Vec<TraceRow>) -> std::result::Result<RecordBatch, Status> {
    let schema = SystemTable::Traces.schema();
    let columns: Vec<ArrayRef> = vec![
        timestamp_array(rows.iter().map(|x| x.timestamp)),
        string_array(rows.iter().map(|x| &x.trace_id)),
        string_array(rows.iter().map(|x| &x.span_id)),
        string_array(rows.iter().map(|x| &x.parent_span_id)),
        string_array(rows.iter().map(|x| &x.trace_state)),
        string_array(rows.iter().map(|x| &x.span_name)),
        string_array(rows.iter().map(|x| &x.span_kind)),
        string_array(rows.iter().map(|x| &x.service_name)),
        variant_array(rows.iter().map(|x| &x.resource_attributes))?,
        string_array(rows.iter().map(|x| &x.scope_name)),
        string_array(rows.iter().map(|x| &x.scope_version)),
        variant_array(rows.iter().map(|x| &x.span_attributes))?,
        u64_array(rows.iter().map(|x| x.duration)),
        string_array(rows.iter().map(|x| &x.status_code)),
        string_array(rows.iter().map(|x| &x.status_message)),
        variant_array(rows.iter().map(|x| &x.events))?,
        variant_array(rows.iter().map(|x| &x.links))?,
    ];
    RecordBatch::try_new(schema, columns).map_err(|e| Status::internal(e.to_string()))
}

fn build_log_batch(rows: Vec<LogRow>) -> std::result::Result<RecordBatch, Status> {
    let schema = SystemTable::Logs.schema();
    let columns: Vec<ArrayRef> = vec![
        timestamp_array(rows.iter().map(|x| x.timestamp)),
        string_array(rows.iter().map(|x| &x.trace_id)),
        string_array(rows.iter().map(|x| &x.span_id)),
        u32_array(rows.iter().map(|x| x.trace_flags)),
        string_array(rows.iter().map(|x| &x.severity_text)),
        u32_array(rows.iter().map(|x| x.severity_number)),
        string_array(rows.iter().map(|x| &x.service_name)),
        variant_array(rows.iter().map(|x| &x.body))?,
        string_array(rows.iter().map(|x| &x.resource_schema_url)),
        variant_array(rows.iter().map(|x| &x.resource_attributes))?,
        string_array(rows.iter().map(|x| &x.scope_schema_url)),
        string_array(rows.iter().map(|x| &x.scope_name)),
        string_array(rows.iter().map(|x| &x.scope_version)),
        variant_array(rows.iter().map(|x| &x.scope_attributes))?,
        variant_array(rows.iter().map(|x| &x.log_attributes))?,
        string_array(rows.iter().map(|x| &x.event_name)),
    ];
    RecordBatch::try_new(schema, columns).map_err(|e| Status::internal(e.to_string()))
}

fn build_metric_batch(
    schema: SchemaRef,
    rows: Vec<MetricRow>,
) -> std::result::Result<RecordBatch, Status> {
    let columns: Vec<ArrayRef> = vec![
        variant_array(rows.iter().map(|x| &x.resource_attributes))?,
        string_array(rows.iter().map(|x| &x.service_name)),
        string_array(rows.iter().map(|x| &x.scope_name)),
        string_array(rows.iter().map(|x| &x.scope_version)),
        string_array(rows.iter().map(|x| &x.metric_name)),
        string_array(rows.iter().map(|x| &x.metric_description)),
        string_array(rows.iter().map(|x| &x.metric_unit)),
        variant_array(rows.iter().map(|x| &x.attributes))?,
        timestamp_array(rows.iter().map(|x| x.start_time)),
        timestamp_array(rows.iter().map(|x| x.time)),
        variant_array(rows.iter().map(|x| &x.value))?,
        u32_array(rows.iter().map(|x| x.flags)),
        variant_array(rows.iter().map(|x| &x.exemplars))?,
    ];
    RecordBatch::try_new(schema, columns).map_err(|e| Status::internal(e.to_string()))
}

fn string_array<'a>(values: impl Iterator<Item = &'a String>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values.map(String::as_str)))
}
fn u64_array(values: impl Iterator<Item = u64>) -> ArrayRef {
    Arc::new(UInt64Array::from_iter_values(values))
}
fn u32_array(values: impl Iterator<Item = u32>) -> ArrayRef {
    Arc::new(UInt32Array::from_iter_values(values))
}
fn timestamp_array(values: impl Iterator<Item = u64>) -> ArrayRef {
    Arc::new(
        TimestampNanosecondArray::from_iter_values(
            values.map(|value| value.min(i64::MAX as u64) as i64),
        )
        .with_timezone("UTC"),
    )
}

fn variant_array<'a>(
    values: impl Iterator<Item = &'a Value>,
) -> std::result::Result<ArrayRef, Status> {
    let values = values.collect::<Vec<_>>();
    let mut builder = VariantArrayBuilder::new(values.len());
    for value in values {
        builder
            .append_json(value.to_string().as_str())
            .map_err(|e| Status::internal(e.to_string()))?;
    }
    let array: StructArray = builder.build().into();
    let fields = array
        .fields()
        .iter()
        .map(|field| {
            if field.name() == "metadata" {
                Field::new(field.name(), field.data_type().clone(), field.is_nullable())
                    .with_metadata(HashMap::from([(
                        VARIANT_METADATA_MARKER_KEY.to_string(),
                        VARIANT_METADATA_MARKER_VALUE.to_string(),
                    )]))
            } else {
                field.as_ref().clone()
            }
        })
        .collect();
    Ok(Arc::new(StructArray::new(
        fields,
        array.columns().to_vec(),
        array.nulls().cloned(),
    )))
}

fn attributes_json(attributes: &[common::KeyValue]) -> Value {
    Value::Object(
        attributes
            .iter()
            .map(|attribute| {
                (
                    attribute.key.clone(),
                    attribute
                        .value
                        .as_ref()
                        .map(any_value_json)
                        .unwrap_or(Value::Null),
                )
            })
            .collect::<Map<_, _>>(),
    )
}
fn any_value_json(value: &common::AnyValue) -> Value {
    use common::any_value::Value as V;
    match value.value.as_ref() {
        Some(V::StringValue(value)) => json!(value),
        Some(V::BoolValue(value)) => json!(value),
        Some(V::IntValue(value)) => json!(value),
        Some(V::DoubleValue(value)) => json!(value),
        Some(V::BytesValue(value)) => json!(hex(value)),
        Some(V::ArrayValue(values)) => {
            Value::Array(values.values.iter().map(any_value_json).collect())
        }
        Some(V::KvlistValue(values)) => attributes_json(&values.values),
        Some(V::StringValueStrindex(value)) => json!(value),
        None => Value::Null,
    }
}
fn service_name(attributes: &Value) -> String {
    attributes
        .get("service.name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{TraceRow, build_trace_batch};

    #[test]
    #[expect(clippy::expect_used)]
    fn trace_batch_uses_the_system_table_schema() {
        let batch = build_trace_batch(vec![TraceRow {
            timestamp: 1,
            trace_id: "trace".to_string(),
            span_id: "span".to_string(),
            parent_span_id: String::new(),
            trace_state: String::new(),
            span_name: "name".to_string(),
            span_kind: "internal".to_string(),
            service_name: "service".to_string(),
            resource_attributes: json!({"service.name": "service"}),
            scope_name: "scope".to_string(),
            scope_version: "1".to_string(),
            span_attributes: json!({"attribute": true}),
            duration: 1,
            status_code: "ok".to_string(),
            status_message: String::new(),
            events: json!([]),
            links: json!([]),
        }])
        .expect("trace batch");
        assert_eq!(batch.num_rows(), 1);
    }
}
