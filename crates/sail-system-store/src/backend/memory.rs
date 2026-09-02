//! Actor-owned in-memory backend.

use std::borrow::{Borrow, BorrowMut};
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::ops::Bound;

use crate::access::{
    DirectStoreBackend, IndexReader, IndexWriter, SeriesReader, SeriesWriter, StoreReader,
    StoreWriter, TableReader, TableWriter,
};
use crate::catalog::{JobRow, OptionRow, SessionRow, StageRow, TaskRow, WorkerRow};
use crate::model::{
    JobPrimaryKey, JobTable, MetricAttributeIndex, MetricAttributeKey, MetricFloatPointSeries,
    MetricHistogramPointSeries, MetricIntegerPointSeries, MetricNameIndex, MetricPoint,
    MetricPointValue, MetricPointValues, MetricSeriesId, MetricSeriesIdentityTable,
    MetricSeriesKey, MetricSeriesMetadata, MetricSeriesTable, NextMetricSeriesIdTable,
    OptionPrimaryKey, OptionTable, SessionPrimaryKey, SessionTable, StagePrimaryKey, StageTable,
    TaskPrimaryKey, TaskTable, WorkerPrimaryKey, WorkerTable,
};

/// All in-memory backend state.
#[derive(Debug, Default)]
pub(crate) struct MemoryBackendState {
    options: BTreeMap<OptionPrimaryKey, OptionRow>,
    sessions: BTreeMap<SessionPrimaryKey, SessionRow>,
    jobs: BTreeMap<JobPrimaryKey, JobRow>,
    stages: BTreeMap<StagePrimaryKey, StageRow>,
    tasks: BTreeMap<TaskPrimaryKey, TaskRow>,
    workers: BTreeMap<WorkerPrimaryKey, WorkerRow>,
    next_metric_series_id: BTreeMap<(), MetricSeriesId>,
    metric_series: BTreeMap<MetricSeriesId, MetricSeriesMetadata>,
    metric_series_identities: BTreeMap<MetricSeriesKey, MetricSeriesId>,
    metric_names: BTreeMap<String, BTreeSet<MetricSeriesId>>,
    metric_attributes: BTreeMap<MetricAttributeKey, BTreeSet<MetricSeriesId>>,
    metric_integer_points:
        BTreeMap<MetricSeriesId, Vec<MetricPoint<MetricPointValues<MetricPointValue<i64>>>>>,
    metric_float_points:
        BTreeMap<MetricSeriesId, Vec<MetricPoint<MetricPointValues<MetricPointValue<f64>>>>>,
    metric_histogram_points: BTreeMap<
        MetricSeriesId,
        Vec<MetricPoint<MetricPointValues<MetricPointValue<crate::types::MetricHistogram>>>>,
    >,
}

/// Adapts borrowed in-memory state to the typed store contracts.
pub(crate) struct MemoryAccessor<S> {
    state: S,
}

pub(crate) type MemoryReader<'a> = MemoryAccessor<&'a MemoryBackendState>;
pub(crate) type MemoryWriter<'a> = MemoryAccessor<&'a mut MemoryBackendState>;

/// Actor-owned in-memory backend. Its accessors are deliberately infallible in practice.
#[derive(Default)]
pub(crate) struct MemoryBackend {
    state: MemoryBackendState,
}

impl DirectStoreBackend for MemoryBackend {
    type Reader<'a> = MemoryReader<'a>;
    type Writer<'a> = MemoryWriter<'a>;

    fn read(&self) -> Self::Reader<'_> {
        MemoryAccessor { state: &self.state }
    }

    fn write(&mut self) -> Self::Writer<'_> {
        MemoryAccessor {
            state: &mut self.state,
        }
    }
}

macro_rules! table {
    ($marker:ty, $key:ty, $value:ty, $field:ident) => {
        impl<S> TableReader<$marker, Infallible> for MemoryAccessor<S>
        where
            S: Borrow<MemoryBackendState>,
        {
            fn get(&self, key: &$key) -> Result<Option<$value>, Infallible> {
                Ok(self.state.borrow().$field.get(key).cloned())
            }

            fn scan(
                &self,
                lower: Bound<$key>,
                upper: Bound<$key>,
                visitor: &mut dyn FnMut($key, $value) -> bool,
            ) -> Result<(), Infallible> {
                for (key, value) in self.state.borrow().$field.range((lower, upper)) {
                    if !visitor(key.clone(), value.clone()) {
                        break;
                    }
                }
                Ok(())
            }
        }

        impl<S> TableWriter<$marker, Infallible> for MemoryAccessor<S>
        where
            S: Borrow<MemoryBackendState> + BorrowMut<MemoryBackendState>,
        {
            fn put(&mut self, key: $key, value: $value) -> Result<(), Infallible> {
                self.state.borrow_mut().$field.insert(key, value);
                Ok(())
            }
        }
    };
}

table!(OptionTable, OptionPrimaryKey, OptionRow, options);
table!(SessionTable, SessionPrimaryKey, SessionRow, sessions);
table!(JobTable, JobPrimaryKey, JobRow, jobs);
table!(StageTable, StagePrimaryKey, StageRow, stages);
table!(TaskTable, TaskPrimaryKey, TaskRow, tasks);
table!(WorkerTable, WorkerPrimaryKey, WorkerRow, workers);
table!(
    NextMetricSeriesIdTable,
    (),
    MetricSeriesId,
    next_metric_series_id
);
table!(
    MetricSeriesTable,
    MetricSeriesId,
    MetricSeriesMetadata,
    metric_series
);
table!(
    MetricSeriesIdentityTable,
    MetricSeriesKey,
    MetricSeriesId,
    metric_series_identities
);

macro_rules! index {
    ($marker:ty, $key:ty, $field:ident) => {
        impl<S> IndexReader<$marker, Infallible> for MemoryAccessor<S>
        where
            S: Borrow<MemoryBackendState>,
        {
            fn scan(
                &self,
                lower: Bound<$key>,
                upper: Bound<$key>,
                visitor: &mut dyn FnMut($key, MetricSeriesId) -> bool,
            ) -> Result<(), Infallible> {
                'entries: for (key, ids) in self.state.borrow().$field.range((lower, upper)) {
                    for id in ids {
                        if !visitor(key.clone(), *id) {
                            break 'entries;
                        }
                    }
                }
                Ok(())
            }
        }

        impl<S> IndexWriter<$marker, Infallible> for MemoryAccessor<S>
        where
            S: Borrow<MemoryBackendState> + BorrowMut<MemoryBackendState>,
        {
            fn put(&mut self, key: $key, value: MetricSeriesId) -> Result<(), Infallible> {
                self.state
                    .borrow_mut()
                    .$field
                    .entry(key)
                    .or_default()
                    .insert(value);
                Ok(())
            }
        }
    };
}

index!(MetricNameIndex, String, metric_names);
index!(MetricAttributeIndex, MetricAttributeKey, metric_attributes);

macro_rules! series {
    ($marker:ty, $value:ty, $field:ident) => {
        impl<S> SeriesReader<$marker, Infallible> for MemoryAccessor<S>
        where
            S: Borrow<MemoryBackendState>,
        {
            fn scan(
                &self,
                series: &MetricSeriesId,
                lower: Bound<crate::predicate::TimestampMicros>,
                upper: Bound<crate::predicate::TimestampMicros>,
                visitor: &mut dyn FnMut(crate::predicate::TimestampMicros, $value) -> bool,
            ) -> Result<(), Infallible> {
                if let Some(points) = self.state.borrow().$field.get(series) {
                    let start = match lower {
                        Bound::Included(timestamp) => {
                            points.partition_point(|point| point.timestamp < timestamp)
                        }
                        Bound::Excluded(timestamp) => {
                            points.partition_point(|point| point.timestamp <= timestamp)
                        }
                        Bound::Unbounded => 0,
                    };
                    for point in &points[start..] {
                        let within_upper_bound = match &upper {
                            Bound::Included(timestamp) => point.timestamp <= *timestamp,
                            Bound::Excluded(timestamp) => point.timestamp < *timestamp,
                            Bound::Unbounded => true,
                        };
                        if !within_upper_bound {
                            break;
                        }
                        for value in &point.value {
                            if !visitor(point.timestamp, value.clone()) {
                                return Ok(());
                            }
                        }
                    }
                }
                Ok(())
            }
        }

        impl<S> SeriesWriter<$marker, Infallible> for MemoryAccessor<S>
        where
            S: Borrow<MemoryBackendState> + BorrowMut<MemoryBackendState>,
        {
            fn put(
                &mut self,
                series: MetricSeriesId,
                timestamp: crate::predicate::TimestampMicros,
                value: $value,
            ) -> Result<(), Infallible> {
                let points = self.state.borrow_mut().$field.entry(series).or_default();
                match points.binary_search_by_key(&timestamp, |point| point.timestamp) {
                    Ok(index) => points[index].value.push(value),
                    Err(index) => points.insert(
                        index,
                        MetricPoint {
                            timestamp,
                            value: MetricPointValues::from_buf([value]),
                        },
                    ),
                }
                Ok(())
            }
        }
    };
}

series!(
    MetricIntegerPointSeries,
    MetricPointValue<i64>,
    metric_integer_points
);
series!(
    MetricFloatPointSeries,
    MetricPointValue<f64>,
    metric_float_points
);
series!(
    MetricHistogramPointSeries,
    MetricPointValue<crate::types::MetricHistogram>,
    metric_histogram_points
);

impl<S> StoreReader for MemoryAccessor<S>
where
    S: Borrow<MemoryBackendState>,
{
    type Error = Infallible;
}

impl<S> StoreWriter for MemoryAccessor<S> where
    S: Borrow<MemoryBackendState> + BorrowMut<MemoryBackendState>
{
}
