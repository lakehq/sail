use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{
    HistogramDataPoint, NumberDataPoint, ResourceMetrics, metric, number_data_point,
};
use sail_system_store::predicate::TimestampMicros;
use sail_system_store::types::{MetricHistogram, MetricNumber, MetricValue};
use sail_system_store::{MetricSample, SystemStoreHandle};

use crate::SCOPE_NAME;
use crate::error::{TelemetryError, TelemetryResult};

#[derive(Clone, Copy)]
enum NumericMetricKind {
    Count,
    Gauge,
}

/// Sends decoded OpenTelemetry metrics to the system store.
#[derive(Clone, Debug)]
pub struct SystemMetricReporter {
    store: SystemStoreHandle,
}

impl SystemMetricReporter {
    pub fn new(store: SystemStoreHandle) -> Self {
        Self { store }
    }

    pub async fn report(&self, metrics: Vec<ResourceMetrics>) -> TelemetryResult<()> {
        let metrics = metrics
            .into_iter()
            .filter_map(|mut resource_metrics| {
                resource_metrics.scope_metrics.retain(|scope_metrics| {
                    scope_metrics
                        .scope
                        .as_ref()
                        .is_some_and(|scope| scope.name == SCOPE_NAME)
                });
                (!resource_metrics.scope_metrics.is_empty()).then_some(resource_metrics)
            })
            .collect();
        let samples: Vec<MetricSample> = decode_metric_samples(metrics);
        self.store
            .write_metrics(samples)
            .await
            .map_err(|e| TelemetryError::internal(format!("failed to store metrics: {e}")))
    }
}

/// Decodes supported OTLP aggregations into backend-neutral system store samples.
fn decode_metric_samples(resource_metrics: Vec<ResourceMetrics>) -> Vec<MetricSample> {
    let mut samples = vec![];
    for resource_metrics in resource_metrics {
        for scope in resource_metrics.scope_metrics {
            for metric in scope.metrics {
                let name = metric.name;
                match metric.data {
                    Some(metric::Data::Gauge(gauge)) => {
                        for point in gauge.data_points {
                            append_number_sample(
                                &mut samples,
                                &name,
                                point,
                                NumericMetricKind::Gauge,
                            );
                        }
                    }
                    Some(metric::Data::Sum(sum)) => {
                        for point in sum.data_points {
                            append_number_sample(
                                &mut samples,
                                &name,
                                point,
                                NumericMetricKind::Count,
                            );
                        }
                    }
                    Some(metric::Data::Histogram(histogram)) => {
                        for point in histogram.data_points {
                            append_histogram_sample(&mut samples, &name, point);
                        }
                    }
                    // The public system table contract supports only count, gauge, and explicit
                    // histogram values. Other OTLP aggregations are intentionally ignored.
                    _ => {}
                }
            }
        }
    }
    samples
}

fn append_number_sample(
    samples: &mut Vec<MetricSample>,
    name: &str,
    point: NumberDataPoint,
    kind: NumericMetricKind,
) {
    let Some(timestamp) = timestamp_micros(point.time_unix_nano) else {
        return;
    };
    let Some(value) = point.value.map(|value| match value {
        number_data_point::Value::AsInt(value) => MetricNumber::Integer(value),
        number_data_point::Value::AsDouble(value) => MetricNumber::Float(value),
    }) else {
        return;
    };
    let value = match kind {
        NumericMetricKind::Count => MetricValue::Count(value),
        NumericMetricKind::Gauge => MetricValue::Gauge(value),
    };
    samples.push(MetricSample {
        name: name.to_string(),
        attributes: canonical_attributes(point.attributes),
        timestamp,
        start_timestamp: start_timestamp_micros(point.start_time_unix_nano),
        value,
    });
}

fn append_histogram_sample(samples: &mut Vec<MetricSample>, name: &str, point: HistogramDataPoint) {
    let Some(timestamp) = timestamp_micros(point.time_unix_nano) else {
        return;
    };
    samples.push(MetricSample {
        name: name.to_string(),
        attributes: canonical_attributes(point.attributes),
        timestamp,
        start_timestamp: start_timestamp_micros(point.start_time_unix_nano),
        value: MetricValue::Histogram(MetricHistogram {
            count: point.count,
            sum: point.sum,
            min: point.min,
            max: point.max,
            bucket_counts: point.bucket_counts,
            explicit_bounds: point.explicit_bounds,
        }),
    });
}

fn timestamp_micros(timestamp_nanos: u64) -> Option<TimestampMicros> {
    i64::try_from(timestamp_nanos / 1_000)
        .ok()
        .map(TimestampMicros)
}

fn start_timestamp_micros(timestamp_nanos: u64) -> Option<TimestampMicros> {
    (timestamp_nanos != 0)
        .then_some(timestamp_nanos)
        .and_then(timestamp_micros)
}

fn canonical_attributes(attributes: Vec<KeyValue>) -> BTreeMap<String, String> {
    attributes
        .into_iter()
        .filter_map(|attribute| {
            attribute
                .value
                .map(|value| (attribute.key, canonical_attribute_value(value)))
        })
        .collect()
}

fn canonical_attribute_value(value: AnyValue) -> String {
    match value.value {
        Some(any_value::Value::StringValue(value)) => value,
        Some(any_value::Value::BoolValue(value)) => value.to_string(),
        Some(any_value::Value::IntValue(value)) => value.to_string(),
        Some(any_value::Value::DoubleValue(value)) => value.to_string(),
        Some(any_value::Value::BytesValue(value)) => value
            .into_iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        Some(any_value::Value::ArrayValue(value)) => {
            serde_json::Value::Array(value.values.into_iter().map(attribute_json_value).collect())
                .to_string()
        }
        Some(any_value::Value::KvlistValue(value)) => {
            let values = canonical_attributes(value.values)
                .into_iter()
                .map(|(key, value)| (key, serde_json::Value::String(value)))
                .collect();
            serde_json::Value::Object(values).to_string()
        }
        Some(any_value::Value::StringValueStrindex(value)) => value.to_string(),
        None => String::new(),
    }
}

fn attribute_json_value(value: AnyValue) -> serde_json::Value {
    match value.value {
        Some(any_value::Value::StringValue(value)) => serde_json::Value::String(value),
        Some(any_value::Value::BoolValue(value)) => serde_json::Value::Bool(value),
        Some(any_value::Value::IntValue(value)) => value.into(),
        Some(any_value::Value::DoubleValue(value)) => serde_json::json!(value),
        Some(any_value::Value::BytesValue(value)) => serde_json::Value::String(
            value
                .into_iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        ),
        Some(any_value::Value::ArrayValue(value)) => {
            serde_json::Value::Array(value.values.into_iter().map(attribute_json_value).collect())
        }
        Some(any_value::Value::KvlistValue(value)) => {
            let values = value
                .values
                .into_iter()
                .filter_map(|item| {
                    item.value
                        .map(|value| (item.key, attribute_json_value(value)))
                })
                .collect();
            serde_json::Value::Object(values)
        }
        Some(any_value::Value::StringValueStrindex(value)) => value.into(),
        None => serde_json::Value::Null,
    }
}
use std::collections::BTreeMap;
