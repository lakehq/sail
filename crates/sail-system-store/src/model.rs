//! Backend-neutral system store model types.

use std::collections::BTreeMap;

use sail_common_datafusion::system::catalog::{
    JobRow, OptionRow, SessionRow, StageRow, TaskRow, WorkerRow,
};
use sail_common_datafusion::system::predicate::TimestampMicros;
use sail_common_datafusion::system::types::MetricValue;
use serde::{Deserialize, Serialize};

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
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MetricAttributeKey {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MetricPointKey {
    pub timestamp: TimestampMicros,
    pub ordinal: u64,
}

#[derive(Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MetricPointOrdinalKey {
    pub series: MetricSeriesId,
    pub timestamp: TimestampMicros,
}

/// Durable metadata for a metric series. A series is identified by its name and attributes,
/// regardless of its aggregation type. Metrics points live independently out of the metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSeriesMetadata {
    pub id: MetricSeriesId,
    pub name: String,
    pub attributes: MetricAttributes,
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
    MetricPointOrdinalTable,
    "metric_point_ordinals",
    MetricPointOrdinalKey,
    u64
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

index!(
    MetricSeriesIdentityIndex,
    "metric_series_identities",
    MetricSeriesKey,
    MetricSeriesId
);
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
    MetricPointSeries,
    "metric_points",
    MetricSeriesId,
    MetricPointKey,
    MetricValue
);
