//! Actor-owned in-memory backend.

use std::borrow::{Borrow, BorrowMut};
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::ops::Bound;

use sail_common_datafusion::system::catalog::{
    JobRow, OptionRow, SessionRow, StageRow, TaskRow, WorkerRow,
};
use sail_common_datafusion::system::types::MetricValue;

use crate::access::{
    DirectStoreBackend, IndexReader, IndexWriter, SeriesReader, SeriesWriter, StoreReader,
    StoreWriter, TableReader, TableWriter,
};
use crate::model::{
    JobPrimaryKey, JobTable, MetricAttributeIndex, MetricAttributeKey, MetricNameIndex,
    MetricPointKey, MetricPointOrdinalKey, MetricPointOrdinalTable, MetricPointSeries,
    MetricSeriesId, MetricSeriesIdentityIndex, MetricSeriesKey, MetricSeriesMetadata,
    MetricSeriesTable, NextMetricSeriesIdTable, OptionPrimaryKey, OptionTable, SessionPrimaryKey,
    SessionTable, StagePrimaryKey, StageTable, TaskPrimaryKey, TaskTable, WorkerPrimaryKey,
    WorkerTable,
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
    metric_point_ordinals: BTreeMap<MetricPointOrdinalKey, u64>,
    metric_series_identities: BTreeMap<MetricSeriesKey, BTreeSet<MetricSeriesId>>,
    metric_names: BTreeMap<String, BTreeSet<MetricSeriesId>>,
    metric_attributes: BTreeMap<MetricAttributeKey, BTreeSet<MetricSeriesId>>,
    metric_points: BTreeMap<MetricSeriesId, BTreeMap<MetricPointKey, MetricValue>>,
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
    MetricPointOrdinalTable,
    MetricPointOrdinalKey,
    u64,
    metric_point_ordinals
);

macro_rules! index {
    ($marker:ty, $key:ty, $field:ident) => {
        impl<S> IndexReader<$marker, Infallible> for MemoryAccessor<S>
        where
            S: Borrow<MemoryBackendState>,
        {
            fn get(&self, key: &$key) -> Result<Vec<MetricSeriesId>, Infallible> {
                Ok(self
                    .state
                    .borrow()
                    .$field
                    .get(key)
                    .into_iter()
                    .flat_map(|ids| ids.iter().copied())
                    .collect())
            }

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

index!(
    MetricSeriesIdentityIndex,
    MetricSeriesKey,
    metric_series_identities
);
index!(MetricNameIndex, String, metric_names);
index!(MetricAttributeIndex, MetricAttributeKey, metric_attributes);

impl<S> SeriesReader<MetricPointSeries, Infallible> for MemoryAccessor<S>
where
    S: Borrow<MemoryBackendState>,
{
    fn scan(
        &self,
        series: &MetricSeriesId,
        lower: Bound<MetricPointKey>,
        upper: Bound<MetricPointKey>,
        visitor: &mut dyn FnMut(MetricPointKey, MetricValue) -> bool,
    ) -> Result<(), Infallible> {
        if let Some(points) = self.state.borrow().metric_points.get(series) {
            for (key, value) in points.range((lower, upper)) {
                if !visitor(*key, value.clone()) {
                    break;
                }
            }
        }
        Ok(())
    }
}

impl<S> SeriesWriter<MetricPointSeries, Infallible> for MemoryAccessor<S>
where
    S: Borrow<MemoryBackendState> + BorrowMut<MemoryBackendState>,
{
    fn put(
        &mut self,
        series: MetricSeriesId,
        point: MetricPointKey,
        value: MetricValue,
    ) -> Result<(), Infallible> {
        self.state
            .borrow_mut()
            .metric_points
            .entry(series)
            .or_default()
            .insert(point, value);
        Ok(())
    }
}

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
