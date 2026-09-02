//! Backend-neutral system store model types.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::catalog::{JobRow, OptionRow, SessionRow, StageRow, TaskRow, WorkerRow};
use crate::predicate::TimestampMicros;
use crate::types::MetricHistogram;

macro_rules! primary_key {
    ($name:ident { $($field:ident: $ty:ty),+ $(,)? }) => {
        #[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        pub struct $name { $(pub $field: $ty),+ }
    };
}

primary_key!(OptionPrimaryKey { key: String });
primary_key!(SessionPrimaryKey { session_id: String });
primary_key!(JobPrimaryKey {
    session_id: String,
    job_id: u64,
});
primary_key!(StagePrimaryKey {
    session_id: String,
    job_id: u64,
    stage: u64,
});
primary_key!(TaskPrimaryKey {
    session_id: String,
    job_id: u64,
    stage: u64,
    partition: u64,
    attempt: u64,
});
primary_key!(WorkerPrimaryKey {
    session_id: String,
    worker_id: u64,
});

/// Stable identifier for a metric series.
pub type MetricSeriesId = u64;

/// Canonical, ordered metric attributes.
pub type MetricAttributes = BTreeMap<String, String>;

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MetricSeriesKey {
    pub name: String,
    pub attributes: MetricAttributes,
    pub kind: MetricSeriesKind,
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MetricAttributeKey {
    pub key: String,
    pub value: String,
}

/// The logical kind of a metric series. Numeric storage is split by representation so scalar
/// series do not pay the layout cost of histograms.
#[derive(Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum MetricSeriesKind {
    IntegerCount,
    FloatCount,
    IntegerGauge,
    FloatGauge,
    Histogram,
}

/// One timestamped physical point. Values can be scalar or a small insertion-ordered batch of
/// duplicate samples for the same timestamp.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricPoint<T> {
    pub timestamp: TimestampMicros,
    pub value: T,
}

pub type MetricPointValues<T> = SmallVec<[T; 1]>;

/// Durable metadata for a metric series. A series is identified by its name, attributes, and
/// logical kind. Metric points live independently out of the metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSeriesMetadata {
    pub id: MetricSeriesId,
    pub name: String,
    pub attributes: MetricAttributes,
    pub kind: MetricSeriesKind,
}

/// Marker for a typed primary table in a store backend.
pub trait StoreTable {
    const NAME: &'static str;
    type Key: Clone + Ord + Send + Sync + 'static;
    type Value: Clone + Send + Sync + 'static;
}

/// Marker for a typed secondary index in a store backend.
pub trait StoreIndex {
    const NAME: &'static str;
    type Key: Clone + Ord + Send + Sync + 'static;
    type Value: Clone + Ord + Send + Sync + 'static;
}

/// Marker for a typed series in a store backend.
pub trait StoreSeries {
    const NAME: &'static str;
    type SeriesKey: Clone + Ord + Send + Sync + 'static;
    type PointKey: Clone + Ord + Send + Sync + 'static;
    type PointValue: Clone + Send + Sync + 'static;
}

macro_rules! table {
    ($name:ident, $store_name:literal, $key:ty, $value:ty) => {
        pub struct $name;

        impl StoreTable for $name {
            const NAME: &'static str = $store_name;
            type Key = $key;
            type Value = $value;
        }
    };
}

table!(OptionTable, "options", OptionPrimaryKey, OptionRow);
table!(SessionTable, "sessions", SessionPrimaryKey, SessionRow);
table!(JobTable, "jobs", JobPrimaryKey, JobRow);
table!(StageTable, "stages", StagePrimaryKey, StageRow);
table!(TaskTable, "tasks", TaskPrimaryKey, TaskRow);
table!(WorkerTable, "workers", WorkerPrimaryKey, WorkerRow);
table!(NextMetricSeriesIdTable, "metadata", (), MetricSeriesId);
table!(
    MetricSeriesTable,
    "metric_series",
    MetricSeriesId,
    MetricSeriesMetadata
);
table!(
    MetricSeriesIdentityTable,
    "metric_series_identities",
    MetricSeriesKey,
    MetricSeriesId
);

macro_rules! index {
    ($name:ident, $store_name:literal, $key:ty, $value:ty) => {
        pub struct $name;

        impl StoreIndex for $name {
            const NAME: &'static str = $store_name;
            type Key = $key;
            type Value = $value;
        }
    };
}

index!(MetricNameIndex, "metric_names", String, MetricSeriesId);
index!(
    MetricAttributeIndex,
    "metric_attributes",
    MetricAttributeKey,
    MetricSeriesId
);

macro_rules! series {
    ($name:ident, $store_name:literal, $series_key:ty, $point_key:ty, $point_value:ty) => {
        pub struct $name;

        impl StoreSeries for $name {
            const NAME: &'static str = $store_name;
            type SeriesKey = $series_key;
            type PointKey = $point_key;
            type PointValue = $point_value;
        }
    };
}

series!(
    MetricIntegerPointSeries,
    "metric_integer_points",
    MetricSeriesId,
    TimestampMicros,
    i64
);
series!(
    MetricFloatPointSeries,
    "metric_float_points",
    MetricSeriesId,
    TimestampMicros,
    f64
);
series!(
    MetricHistogramPointSeries,
    "metric_histogram_points",
    MetricSeriesId,
    TimestampMicros,
    MetricHistogram
);
