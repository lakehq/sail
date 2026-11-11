use std::mem::Discriminant;
use std::sync::Arc;

use datafusion::physical_plan::Metric;
use datafusion::physical_plan::metrics::{Label, MetricValue, MetricsSet};
use indexmap::IndexMap;

pub trait MetricsSetExt {
    /// Merges metrics that share the same name, partition, labels, and value type.
    fn aggregate_duplicates(&self) -> MetricsSet;
}

impl MetricsSetExt for MetricsSet {
    fn aggregate_duplicates(&self) -> MetricsSet {
        // The value's type is part of the key since `MetricValue::aggregate` will
        // panic if the types differ.
        type Key<'a> = (
            &'a str,                   // name
            Option<usize>,             // partition
            &'a [Label],               // labels
            Discriminant<MetricValue>, // value type
        );

        let mut groups: IndexMap<Key<'_>, Vec<Arc<Metric>>> = IndexMap::new();
        for metric in self.iter() {
            groups
                .entry((
                    metric.value().name(),
                    metric.partition(),
                    metric.labels(),
                    std::mem::discriminant(metric.value()),
                ))
                .or_default()
                .push(Arc::clone(metric));
        }

        let mut merged = MetricsSet::new();
        for group in groups.into_values() {
            let Some((first, rest)) = group.split_first() else {
                continue;
            };

            if rest.is_empty() {
                merged.push(Arc::clone(first));
                continue;
            }

            // `MetricValue` derives Clone, but the variant wrappers use `Arc<T>`,
            // so use `new_empty` to avoid getting a shared reference.
            let mut value = first.value().new_empty();
            for metric in &group {
                value.aggregate(metric.value());
            }

            let mut metric =
                Metric::new_with_labels(value, first.partition(), first.labels().to_vec())
                    .with_type(first.metric_type());
            if let Some(category) = first.metric_category() {
                metric = metric.with_category(category);
            }

            merged.push(Arc::new(metric));
        }

        merged
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::physical_plan::Metric;
    use datafusion::physical_plan::metrics::{Count, Gauge, Label, MetricValue, MetricsSet};

    use super::MetricsSetExt;

    fn count(n: usize) -> Count {
        let count = Count::new();
        count.add(n);
        count
    }

    fn output_rows(n: usize, partition: usize) -> Arc<Metric> {
        Arc::new(Metric::new(
            MetricValue::OutputRows(count(n)),
            Some(partition),
        ))
    }

    fn metrics_set(metrics: &[Arc<Metric>]) -> MetricsSet {
        let mut set = MetricsSet::new();
        for metric in metrics {
            set.push(Arc::clone(metric));
        }
        set
    }

    #[test]
    fn test_unique_metrics_pass_through() {
        let metrics = [output_rows(100, 0), output_rows(50, 1)];
        let merged = metrics_set(&metrics).aggregate_duplicates();
        let merged: Vec<_> = merged.iter().collect();
        assert_eq!(merged.len(), 2);
        assert!(Arc::ptr_eq(merged[0], &metrics[0]));
        assert!(Arc::ptr_eq(merged[1], &metrics[1]));
    }

    #[test]
    fn test_duplicate_metrics_are_aggregated() {
        let metrics = [output_rows(150, 0), output_rows(0, 0), output_rows(0, 0)];
        let merged = metrics_set(&metrics).aggregate_duplicates();
        let merged: Vec<_> = merged.iter().collect();
        assert_eq!(merged.len(), 1);
        assert!(matches!(merged[0].value(), MetricValue::OutputRows(_)));
        assert_eq!(merged[0].value().as_usize(), 150);
        assert_eq!(merged[0].partition(), Some(0));
    }

    #[test]
    fn test_merge_does_not_modify_original_metrics() {
        let metrics = [output_rows(100, 0), output_rows(50, 0)];
        let merged = metrics_set(&metrics).aggregate_duplicates();
        assert_eq!(merged.output_rows(), Some(150));
        assert_eq!(metrics[0].value().as_usize(), 100);
        assert_eq!(metrics[1].value().as_usize(), 50);
    }

    #[test]
    fn test_distinct_keys_are_kept_in_order() {
        let gauge = Gauge::new();
        gauge.set(7);
        let metrics = [
            output_rows(1, 0),
            output_rows(2, 1),
            Arc::new(
                Metric::new(MetricValue::OutputRows(count(3)), Some(0))
                    .with_label(Label::new("expr", "a")),
            ),
            Arc::new(Metric::new(
                MetricValue::Count {
                    name: "x".into(),
                    count: count(4),
                },
                Some(0),
            )),
            Arc::new(Metric::new(
                MetricValue::Gauge {
                    name: "x".into(),
                    gauge,
                },
                Some(0),
            )),
            output_rows(10, 0),
        ];
        let merged = metrics_set(&metrics).aggregate_duplicates();
        let values: Vec<_> = merged
            .iter()
            .map(|m| (m.value().name(), m.partition(), m.value().as_usize()))
            .collect();
        assert_eq!(
            values,
            vec![
                ("output_rows", Some(0), 11),
                ("output_rows", Some(1), 2),
                ("output_rows", Some(0), 3),
                ("x", Some(0), 4),
                ("x", Some(0), 7),
            ]
        );
    }
}
