use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use datafusion::common::Result;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{
    HistogramDataPoint, NumberDataPoint, ResourceMetrics, metric, number_data_point,
};
use sail_common_datafusion::system::catalog::MetricRow;
use sail_common_datafusion::system::predicate::{
    MapValueFilter, TimestampMicros, ValueDomain, ValueFilter,
};
use sail_common_datafusion::system::types::{MetricHistogram, MetricNumber, MetricValue};

pub type MetricSeriesId = u64;
pub type MetricAttributes = BTreeMap<String, String>;

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
struct MetricSeriesKey {
    name: String,
    attributes: MetricAttributes,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum NumericMetricKind {
    Count,
    Gauge,
}

#[derive(Debug, Clone)]
pub enum MetricSeriesValues {
    Numeric {
        kind: NumericMetricKind,
        values: Vec<MetricNumber>,
    },
    Histogram(Vec<MetricHistogram>),
}

#[derive(Debug, Clone)]
pub struct MetricSeries {
    name: String,
    attributes: MetricAttributes,
    /// Sorted Unix timestamps in microseconds.
    timestamps: Vec<TimestampMicros>,
    values: MetricSeriesValues,
}

impl MetricSeries {
    fn new(
        name: String,
        attributes: MetricAttributes,
        timestamp: TimestampMicros,
        value: MetricValue,
    ) -> Self {
        let values = match value {
            MetricValue::Count(value) => MetricSeriesValues::Numeric {
                kind: NumericMetricKind::Count,
                values: vec![value],
            },
            MetricValue::Gauge(value) => MetricSeriesValues::Numeric {
                kind: NumericMetricKind::Gauge,
                values: vec![value],
            },
            MetricValue::Histogram(value) => MetricSeriesValues::Histogram(vec![value]),
        };
        Self {
            name,
            attributes,
            timestamps: vec![timestamp],
            values,
        }
    }

    fn insert(&mut self, timestamp: TimestampMicros, value: MetricValue) {
        let index = self.timestamps.partition_point(|value| *value <= timestamp);
        match (&mut self.values, value) {
            (
                MetricSeriesValues::Numeric {
                    kind: NumericMetricKind::Count,
                    values,
                },
                MetricValue::Count(value),
            )
            | (
                MetricSeriesValues::Numeric {
                    kind: NumericMetricKind::Gauge,
                    values,
                },
                MetricValue::Gauge(value),
            ) => values.insert(index, value),
            (MetricSeriesValues::Histogram(values), MetricValue::Histogram(value)) => {
                values.insert(index, value);
            }
            // OpenTelemetry metric identity includes the aggregation type. The system-table
            // series key is intentionally restricted to name and attributes, so a conflicting
            // aggregation type cannot be represented in the same series.
            _ => return,
        }
        self.timestamps.insert(index, timestamp);
    }

    fn value(&self, index: usize) -> MetricValue {
        match &self.values {
            MetricSeriesValues::Numeric { kind, values } => match kind {
                NumericMetricKind::Count => MetricValue::Count(values[index].clone()),
                NumericMetricKind::Gauge => MetricValue::Gauge(values[index].clone()),
            },
            MetricSeriesValues::Histogram(values) => MetricValue::Histogram(values[index].clone()),
        }
    }
}

#[derive(Default)]
pub struct MetricStore {
    next_series_id: MetricSeriesId,
    /// Canonical series identity to ID index.
    series_index: BTreeMap<MetricSeriesKey, MetricSeriesId>,
    /// Secondary B-tree indexes make name-only and individual attribute-value lookup efficient.
    name_index: BTreeMap<String, BTreeSet<MetricSeriesId>>,
    attribute_values_index: BTreeMap<String, BTreeMap<String, BTreeSet<MetricSeriesId>>>,
    series: BTreeMap<MetricSeriesId, MetricSeries>,
}

impl MetricStore {
    pub fn apply(&mut self, resource_metrics: Vec<ResourceMetrics>) {
        for resource_metrics in resource_metrics {
            let resource_attributes = resource_metrics
                .resource
                .map(|resource| canonical_attributes(resource.attributes))
                .unwrap_or_default();
            for scope in resource_metrics.scope_metrics {
                for metric in scope.metrics {
                    let name = metric.name;
                    match metric.data {
                        Some(metric::Data::Gauge(gauge)) => {
                            for point in gauge.data_points {
                                self.insert_number(
                                    &name,
                                    &resource_attributes,
                                    point,
                                    NumericMetricKind::Gauge,
                                );
                            }
                        }
                        Some(metric::Data::Sum(sum)) => {
                            for point in sum.data_points {
                                self.insert_number(
                                    &name,
                                    &resource_attributes,
                                    point,
                                    NumericMetricKind::Count,
                                );
                            }
                        }
                        Some(metric::Data::Histogram(histogram)) => {
                            for point in histogram.data_points {
                                self.insert_histogram(&name, &resource_attributes, point);
                            }
                        }
                        // The public table contract currently supports count, gauge, and
                        // explicit histogram values. Other OTLP aggregations are ignored.
                        _ => {}
                    }
                }
            }
        }
    }

    fn insert_number(
        &mut self,
        name: &str,
        resource_attributes: &MetricAttributes,
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
        self.insert(
            name.to_string(),
            merge_attributes(resource_attributes, point.attributes),
            timestamp,
            value,
        );
    }

    fn insert_histogram(
        &mut self,
        name: &str,
        resource_attributes: &MetricAttributes,
        point: HistogramDataPoint,
    ) {
        let Some(timestamp) = timestamp_micros(point.time_unix_nano) else {
            return;
        };
        let attributes = merge_attributes(resource_attributes, point.attributes);
        self.insert(
            name.to_string(),
            attributes,
            timestamp,
            MetricValue::Histogram(MetricHistogram {
                count: point.count,
                sum: point.sum,
                min: point.min,
                max: point.max,
                bucket_counts: point.bucket_counts,
                explicit_bounds: point.explicit_bounds,
            }),
        );
    }

    fn insert(
        &mut self,
        name: String,
        attributes: MetricAttributes,
        timestamp: TimestampMicros,
        value: MetricValue,
    ) {
        let key = MetricSeriesKey {
            name: name.clone(),
            attributes: attributes.clone(),
        };
        if let Some(series_id) = self.series_index.get(&key) {
            if let Some(series) = self.series.get_mut(series_id) {
                series.insert(timestamp, value);
            }
            return;
        }
        let series_id = self.next_series_id;
        self.next_series_id = self.next_series_id.saturating_add(1);
        self.series_index.insert(key, series_id);
        self.name_index
            .entry(name.clone())
            .or_default()
            .insert(series_id);
        for (key, value) in &attributes {
            self.attribute_values_index
                .entry(key.clone())
                .or_default()
                .entry(value.clone())
                .or_default()
                .insert(series_id);
        }
        self.series.insert(
            series_id,
            MetricSeries::new(name, attributes, timestamp, value),
        );
    }

    pub fn scan(
        &self,
        timestamp: ValueFilter<TimestampMicros>,
        name: ValueFilter<String>,
        attributes: Vec<MapValueFilter<String, String>>,
        fetch: usize,
    ) -> Result<Vec<MetricRow>> {
        if fetch == 0 {
            return Ok(vec![]);
        }
        let candidates = self.candidate_series(&name.domain, &attributes);
        let mut rows = Vec::new();
        for series_id in candidates {
            let Some(series) = self.series.get(&series_id) else {
                continue;
            };
            if !(name.predicate)(&series.name)? || !matches_attributes(&attributes, series)? {
                continue;
            }
            for index in timestamp_indices(&series.timestamps, &timestamp.domain) {
                let value = series.timestamps[index];
                if !(timestamp.predicate)(&value)? {
                    continue;
                }
                let Some(timestamp) = chrono::DateTime::from_timestamp_micros(value.0) else {
                    continue;
                };
                rows.push(MetricRow {
                    timestamp,
                    name: series.name.clone(),
                    attributes: series.attributes.clone(),
                    value: series.value(index),
                });
                if rows.len() >= fetch {
                    return Ok(rows);
                }
            }
        }
        Ok(rows)
    }

    fn candidate_series(
        &self,
        names: &ValueDomain<String>,
        attributes: &[MapValueFilter<String, String>],
    ) -> BTreeSet<MetricSeriesId> {
        let mut candidates = index_candidates(&self.name_index, names);
        for filter in attributes {
            let attribute_candidates = self
                .attribute_values_index
                .get(&filter.key)
                .map(|index| {
                    index_candidates(index, &filter.domain).unwrap_or_else(|| {
                        index.values().flat_map(BTreeSet::iter).copied().collect()
                    })
                })
                .unwrap_or_default();
            candidates = Some(match candidates {
                Some(candidates) => candidates
                    .intersection(&attribute_candidates)
                    .copied()
                    .collect(),
                None => attribute_candidates,
            });
        }
        candidates.unwrap_or_else(|| self.series.keys().copied().collect())
    }
}

fn matches_attributes(
    filters: &[MapValueFilter<String, String>],
    series: &MetricSeries,
) -> Result<bool> {
    for filter in filters {
        let Some(value) = series.attributes.get(&filter.key) else {
            return Ok(false);
        };
        if !(filter.predicate)(value)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn timestamp_micros(timestamp_nanos: u64) -> Option<TimestampMicros> {
    i64::try_from(timestamp_nanos / 1_000)
        .ok()
        .map(TimestampMicros)
}

fn merge_attributes(resource: &MetricAttributes, point: Vec<KeyValue>) -> MetricAttributes {
    let mut attributes = resource.clone();
    attributes.extend(canonical_attributes(point));
    attributes
}

fn canonical_attributes(attributes: Vec<KeyValue>) -> MetricAttributes {
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

fn index_candidates<K: Clone + Ord>(
    index: &BTreeMap<K, BTreeSet<MetricSeriesId>>,
    domain: &ValueDomain<K>,
) -> Option<BTreeSet<MetricSeriesId>> {
    if domain.is_all() {
        return None;
    }
    let mut result = BTreeSet::new();
    for range in domain.ranges() {
        for ids in index
            .range((range.lower.clone(), range.upper.clone()))
            .map(|(_, ids)| ids)
        {
            result.extend(ids);
        }
    }
    Some(result)
}

fn timestamp_indices(
    timestamps: &[TimestampMicros],
    domain: &ValueDomain<TimestampMicros>,
) -> BTreeSet<usize> {
    if domain.is_all() {
        return (0..timestamps.len()).collect();
    }
    let mut result = BTreeSet::new();
    for range in domain.ranges() {
        let start = match &range.lower {
            Bound::Included(value) => timestamps.partition_point(|timestamp| timestamp < value),
            Bound::Excluded(value) => timestamps.partition_point(|timestamp| timestamp <= value),
            Bound::Unbounded => 0,
        };
        let end = match &range.upper {
            Bound::Included(value) => timestamps.partition_point(|timestamp| timestamp <= value),
            Bound::Excluded(value) => timestamps.partition_point(|timestamp| timestamp < value),
            Bound::Unbounded => timestamps.len(),
        };
        result.extend(start..end);
    }
    result
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use sail_common_datafusion::system::predicate::{
        MapValueFilter, Predicates, ValueDomain, ValueFilter,
    };
    use sail_common_datafusion::system::types::{MetricNumber, MetricValue};

    use super::MetricStore;

    fn string_attribute(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_string())),
            }),
            key_strindex: 0,
        }
    }

    fn gauge_point(timestamp: u64, value: i64, job: &str) -> NumberDataPoint {
        NumberDataPoint {
            attributes: vec![string_attribute("job", job)],
            time_unix_nano: timestamp,
            value: Some(number_data_point::Value::AsInt(value)),
            ..Default::default()
        }
    }

    #[test]
    fn stores_series_with_canonical_attributes_and_sorted_timestamps()
    -> datafusion::common::Result<()> {
        let metrics = ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![string_attribute("service.name", "sail-worker")],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "test.gauge".to_string(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![
                            gauge_point(2_000, 2, "example"),
                            gauge_point(1_000, 1, "example"),
                            gauge_point(3_000, 3, "other"),
                        ],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut store = MetricStore::default();
        store.apply(vec![metrics]);

        let rows = store.scan(
            ValueFilter::all(Predicates::always_true()),
            ValueFilter::new(
                ValueDomain::point("test.gauge".to_string()),
                Predicates::always_true(),
            ),
            vec![MapValueFilter::new(
                "job".to_string(),
                ValueDomain::point("example".to_string()),
                Predicates::always_true(),
            )],
            usize::MAX,
        )?;

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].timestamp.timestamp_micros(), 1);
        assert_eq!(rows[1].timestamp.timestamp_micros(), 2);
        assert_eq!(rows[0].attributes["service.name"], "sail-worker");
        assert_eq!(rows[0].attributes["job"], "example");
        assert_eq!(rows[0].value, MetricValue::Gauge(MetricNumber::Integer(1)));
        assert_eq!(rows[1].value, MetricValue::Gauge(MetricNumber::Integer(2)));

        let rows = store.scan(
            ValueFilter::all(Predicates::always_true()),
            ValueFilter::all(Predicates::always_true()),
            vec![],
            0,
        )?;
        assert!(rows.is_empty());
        Ok(())
    }
}
