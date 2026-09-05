//! Typed system store queries.

use std::collections::BTreeSet;
use std::ops::Bound;

use chrono::DateTime;
use tokio::sync::mpsc;

use super::candidate::{CandidateSet, ValueOrd, candidate_key_bound, candidate_set};
use crate::access::StoreReader;
use crate::catalog::{JobRow, MetricRow, OptionRow, SessionRow, StageRow, TaskRow, WorkerRow};
use crate::model::{
    JobPrimaryKey, JobTable, MetricAttributeIndex, MetricAttributeKey, MetricFloatPointSeries,
    MetricHistogramPointSeries, MetricIntegerPointSeries, MetricNameIndex, MetricPointValue,
    MetricSeriesId, MetricSeriesKind, MetricSeriesMetadata, MetricSeriesTable, OptionPrimaryKey,
    OptionTable, SessionPrimaryKey, SessionTable, StagePrimaryKey, StageTable, StoreSeries,
    TaskPrimaryKey, TaskTable, WorkerPrimaryKey, WorkerTable,
};
use crate::predicate::{MapValueFilter, TimestampMicros, ValueFilter};
use crate::types::{MetricNumber, MetricValue};
use crate::{SystemStoreError, SystemStoreResult};

/// Receives query rows without coupling the query implementation to its delivery mechanism.
pub(crate) trait RowCollector<T> {
    /// Returns `false` when the consumer has gone away and scanning should stop.
    fn push(&mut self, row: T) -> SystemStoreResult<bool>;
}

struct VecCollector<T> {
    rows: Vec<T>,
}

impl<T> Default for VecCollector<T> {
    fn default() -> Self {
        Self { rows: Vec::new() }
    }
}

impl<T> VecCollector<T> {
    fn into_rows(self) -> Vec<T> {
        self.rows
    }
}

impl<T> RowCollector<T> for VecCollector<T> {
    fn push(&mut self, row: T) -> SystemStoreResult<bool> {
        self.rows.push(row);
        Ok(true)
    }
}

struct ChannelCollector<T> {
    sender: mpsc::Sender<SystemStoreResult<Option<Vec<T>>>>,
    rows: Vec<T>,
    batch_size: usize,
}

impl<T> ChannelCollector<T> {
    fn new(sender: mpsc::Sender<SystemStoreResult<Option<Vec<T>>>>, batch_size: usize) -> Self {
        let batch_size = batch_size.max(1);
        Self {
            sender,
            rows: Vec::with_capacity(batch_size),
            batch_size,
        }
    }

    fn flush(&mut self) -> bool {
        if self.rows.is_empty() {
            return true;
        }
        let rows = std::mem::replace(&mut self.rows, Vec::with_capacity(self.batch_size));
        self.sender.blocking_send(Ok(Some(rows))).is_ok()
    }

    fn finish(&mut self) {
        if self.flush() {
            let _ = self.sender.blocking_send(Ok(None));
        }
    }

    fn fail(&self, error: SystemStoreError) {
        let _ = self.sender.blocking_send(Err(error));
    }
}

impl<T> RowCollector<T> for ChannelCollector<T> {
    fn push(&mut self, row: T) -> SystemStoreResult<bool> {
        self.rows.push(row);
        Ok(self.rows.len() < self.batch_size || self.flush())
    }
}

/// Materializes the query while borrowing the direct backend, then forwards row batches.
pub(crate) struct Eager;

/// Sends row batches as they are scanned from a transactional snapshot.
pub(crate) struct Deferred;

pub(crate) trait QueryDelivery {
    type Output;

    fn deliver<T>(
        self,
        sender: mpsc::Sender<SystemStoreResult<Option<Vec<T>>>>,
        batch_size: usize,
        execute: impl FnOnce(&mut dyn RowCollector<T>) -> SystemStoreResult<()>,
    ) -> Self::Output
    where
        T: Send + 'static;
}

impl QueryDelivery for Eager {
    type Output = Box<dyn FnOnce() + Send>;

    fn deliver<T>(
        self,
        sender: mpsc::Sender<SystemStoreResult<Option<Vec<T>>>>,
        batch_size: usize,
        execute: impl FnOnce(&mut dyn RowCollector<T>) -> SystemStoreResult<()>,
    ) -> Self::Output
    where
        T: Send + 'static,
    {
        let mut collector = VecCollector::default();
        let result = execute(&mut collector).map(|()| collector.into_rows());
        Box::new(move || send_rows(sender, result, batch_size))
    }
}

impl QueryDelivery for Deferred {
    type Output = ();

    fn deliver<T>(
        self,
        sender: mpsc::Sender<SystemStoreResult<Option<Vec<T>>>>,
        batch_size: usize,
        execute: impl FnOnce(&mut dyn RowCollector<T>) -> SystemStoreResult<()>,
    ) where
        T: Send + 'static,
    {
        let mut collector = ChannelCollector::new(sender, batch_size);
        match execute(&mut collector) {
            Ok(()) => {
                collector.finish();
            }
            Err(error) => collector.fail(error),
        }
    }
}

candidate_key_bound! {
    JobPrimaryKey => JobPrimaryKeyBound {
        session_id: String,
        job_id: u64,
    }
}

candidate_key_bound! {
    StagePrimaryKey => StagePrimaryKeyBound {
        session_id: String,
        job_id: u64,
        stage: u64,
    }
}

candidate_key_bound! {
    TaskPrimaryKey => TaskPrimaryKeyBound {
        session_id: String,
        job_id: u64,
        stage: u64,
        partition: u64,
        attempt: u64,
    }
}

candidate_key_bound! {
    OptionPrimaryKey => OptionPrimaryKeyBound {
        key: String,
    }
}

candidate_key_bound! {
    SessionPrimaryKey => SessionPrimaryKeyBound {
        session_id: String,
    }
}

candidate_key_bound! {
    WorkerPrimaryKey => WorkerPrimaryKeyBound {
        session_id: String,
        worker_id: u64,
    }
}

fn matches_metric_attributes(
    filters: &[MapValueFilter<String, String>],
    series: &MetricSeriesMetadata,
) -> SystemStoreResult<bool> {
    for filter in filters {
        // Attribute values are non-null strings, so this only supports predicates over present
        // map entries. In particular, `attributes['key'] IS NULL` cannot be pushed down here, since
        // a missing key has no attribute-index entry to evaluate. Such predicates remain residual
        // filters that are not seen by this function.
        let Some(value) = series.attributes.get(&filter.key) else {
            return Ok(false);
        };
        if !(filter.predicate)(value)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn metric_name_ids<R: StoreReader>(
    reader: &R,
    name: &ValueFilter<String>,
) -> SystemStoreResult<BTreeSet<MetricSeriesId>> {
    let mut ids = BTreeSet::new();
    for range in name.domain.ranges() {
        reader
            .index::<MetricNameIndex>()
            .scan(range.lower.clone(), range.upper.clone(), &mut |_, id| {
                ids.insert(id);
                true
            })
            .map_err(Into::into)?;
    }
    Ok(ids)
}

fn metric_attribute_ids<R: StoreReader>(
    reader: &R,
    filter: &MapValueFilter<String, String>,
) -> SystemStoreResult<BTreeSet<MetricSeriesId>> {
    let mut ids = BTreeSet::new();
    for range in filter.domain.ranges() {
        let lower = match &range.lower {
            Bound::Included(value) => Bound::Included(MetricAttributeKey {
                key: filter.key.clone(),
                value: value.clone(),
            }),
            Bound::Excluded(value) => Bound::Excluded(MetricAttributeKey {
                key: filter.key.clone(),
                value: value.clone(),
            }),
            Bound::Unbounded => Bound::Included(MetricAttributeKey {
                key: filter.key.clone(),
                value: String::new(),
            }),
        };
        let upper = match &range.upper {
            Bound::Included(value) => Bound::Included(MetricAttributeKey {
                key: filter.key.clone(),
                value: value.clone(),
            }),
            Bound::Excluded(value) => Bound::Excluded(MetricAttributeKey {
                key: filter.key.clone(),
                value: value.clone(),
            }),
            Bound::Unbounded => Bound::Unbounded,
        };
        let mut predicate_error = None;
        reader
            .index::<MetricAttributeIndex>()
            .scan(lower, upper, &mut |key, id| {
                if key.key != filter.key {
                    return false;
                }
                match (filter.predicate)(&key.value) {
                    Ok(true) => {
                        ids.insert(id);
                        true
                    }
                    Ok(false) => true,
                    Err(error) => {
                        predicate_error = Some(SystemStoreError::from(error));
                        false
                    }
                }
            })
            .map_err(Into::into)?;
        if let Some(error) = predicate_error {
            return Err(error);
        }
    }
    Ok(ids)
}

fn metric_series_ids<R: StoreReader>(
    reader: &R,
    name: &ValueFilter<String>,
    attributes: &[MapValueFilter<String, String>],
) -> SystemStoreResult<BTreeSet<MetricSeriesId>> {
    let mut ids = metric_name_ids(reader, name)?;
    for filter in attributes {
        let attribute_ids = metric_attribute_ids(reader, filter)?;
        ids.retain(|id| attribute_ids.contains(id));
    }
    Ok(ids)
}

/// A strongly typed system store read request.
///
/// Each variant owns a row-batch channel. `Ok(Some(rows))` delivers rows, `Ok(None)` marks
/// successful completion, and `Err(error)` fails the query. Closing the channel without one of
/// those terminal messages means the read was cancelled before it could report its result.
pub(crate) enum SystemStoreQuery {
    Jobs {
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        fetch: usize,
        batch_size: usize,
        sender: mpsc::Sender<SystemStoreResult<Option<Vec<JobRow>>>>,
    },
    Metrics {
        timestamp: ValueFilter<TimestampMicros>,
        name: ValueFilter<String>,
        attributes: Vec<MapValueFilter<String, String>>,
        fetch: usize,
        batch_size: usize,
        sender: mpsc::Sender<SystemStoreResult<Option<Vec<MetricRow>>>>,
    },
    Stages {
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        stage: ValueFilter<u64>,
        fetch: usize,
        batch_size: usize,
        sender: mpsc::Sender<SystemStoreResult<Option<Vec<StageRow>>>>,
    },
    Tasks {
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        stage: ValueFilter<u64>,
        partition: ValueFilter<u64>,
        attempt: ValueFilter<u64>,
        fetch: usize,
        batch_size: usize,
        sender: mpsc::Sender<SystemStoreResult<Option<Vec<TaskRow>>>>,
    },
    Options {
        key: ValueFilter<String>,
        fetch: usize,
        batch_size: usize,
        sender: mpsc::Sender<SystemStoreResult<Option<Vec<OptionRow>>>>,
    },
    Sessions {
        session_id: ValueFilter<String>,
        fetch: usize,
        batch_size: usize,
        sender: mpsc::Sender<SystemStoreResult<Option<Vec<SessionRow>>>>,
    },
    Workers {
        session_id: ValueFilter<String>,
        worker_id: ValueFilter<u64>,
        fetch: usize,
        batch_size: usize,
        sender: mpsc::Sender<SystemStoreResult<Option<Vec<WorkerRow>>>>,
    },
}

impl SystemStoreQuery {
    pub(crate) fn fail(self, error: SystemStoreError) {
        match self {
            Self::Jobs { sender, .. } => {
                let _ = sender.blocking_send(Err(error));
            }
            Self::Metrics { sender, .. } => {
                let _ = sender.blocking_send(Err(error));
            }
            Self::Stages { sender, .. } => {
                let _ = sender.blocking_send(Err(error));
            }
            Self::Tasks { sender, .. } => {
                let _ = sender.blocking_send(Err(error));
            }
            Self::Options { sender, .. } => {
                let _ = sender.blocking_send(Err(error));
            }
            Self::Sessions { sender, .. } => {
                let _ = sender.blocking_send(Err(error));
            }
            Self::Workers { sender, .. } => {
                let _ = sender.blocking_send(Err(error));
            }
        }
    }

    pub(crate) fn execute<R, D>(self, reader: &R, delivery: D) -> D::Output
    where
        R: StoreReader,
        D: QueryDelivery,
    {
        match self {
            Self::Jobs {
                session_id,
                job_id,
                fetch,
                batch_size,
                sender,
            } => delivery.deliver(sender, batch_size, |collector| {
                query_jobs(reader, session_id, job_id, fetch, collector)
            }),
            Self::Metrics {
                timestamp,
                name,
                attributes,
                fetch,
                batch_size,
                sender,
            } => delivery.deliver(sender, batch_size, |collector| {
                query_metrics(reader, timestamp, name, attributes, fetch, collector)
            }),
            Self::Stages {
                session_id,
                job_id,
                stage,
                fetch,
                batch_size,
                sender,
            } => delivery.deliver(sender, batch_size, |collector| {
                query_stages(reader, session_id, job_id, stage, fetch, collector)
            }),
            Self::Tasks {
                session_id,
                job_id,
                stage,
                partition,
                attempt,
                fetch,
                batch_size,
                sender,
            } => delivery.deliver(sender, batch_size, |collector| {
                query_tasks(
                    reader, session_id, job_id, stage, partition, attempt, fetch, collector,
                )
            }),
            Self::Options {
                key,
                fetch,
                batch_size,
                sender,
            } => delivery.deliver(sender, batch_size, |collector| {
                query_options(reader, key, fetch, collector)
            }),
            Self::Sessions {
                session_id,
                fetch,
                batch_size,
                sender,
            } => delivery.deliver(sender, batch_size, |collector| {
                query_sessions(reader, session_id, fetch, collector)
            }),
            Self::Workers {
                session_id,
                worker_id,
                fetch,
                batch_size,
                sender,
            } => delivery.deliver(sender, batch_size, |collector| {
                query_workers(reader, session_id, worker_id, fetch, collector)
            }),
        }
    }
}

fn send_rows<T>(
    sender: mpsc::Sender<SystemStoreResult<Option<Vec<T>>>>,
    result: SystemStoreResult<Vec<T>>,
    batch_size: usize,
) where
    T: Send + 'static,
{
    let batch_size = batch_size.max(1);
    match result {
        Ok(rows) => {
            let mut rows = rows.into_iter();
            loop {
                let batch = rows.by_ref().take(batch_size).collect::<Vec<_>>();
                if batch.is_empty() {
                    let _ = sender.blocking_send(Ok(None));
                    break;
                }
                if sender.blocking_send(Ok(Some(batch))).is_err() {
                    break;
                }
            }
        }
        Err(error) => {
            let _ = sender.blocking_send(Err(error));
        }
    }
}

fn scan_candidates<K, B, V, E, C>(
    candidates: CandidateSet<K, B>,
    fetch: usize,
    mut get: impl FnMut(&K) -> Result<Option<V>, E>,
    mut scan: impl FnMut(Bound<K>, Bound<K>, &mut dyn FnMut(K, V) -> bool) -> Result<(), E>,
    predicate: impl Fn(&V) -> SystemStoreResult<bool>,
    collector: &mut C,
) -> SystemStoreResult<()>
where
    K: Ord,
    B: ValueOrd<K>,
    E: Into<SystemStoreError>,
    C: RowCollector<V> + ?Sized,
{
    if fetch == 0 {
        return Ok(());
    }
    let mut count = 0;
    let mut add_if_matching = |row: V| -> SystemStoreResult<bool> {
        if predicate(&row)? {
            count += 1;
            if !collector.push(row)? || count == fetch {
                return Ok(true);
            }
        }
        Ok(false)
    };
    match candidates {
        CandidateSet::Points(points) => {
            for point in points {
                if let Some(row) = get(&point).map_err(Into::into)?
                    && add_if_matching(row)?
                {
                    break;
                }
            }
        }
        CandidateSet::Ranges(ranges) => {
            'ranges: for candidate in ranges {
                let mut predicate_error = None;
                let mut reached_fetch = false;
                scan(
                    Bound::Included(candidate.start),
                    Bound::Unbounded,
                    &mut |key, row| {
                        if candidate
                            .end
                            .as_ref()
                            .is_some_and(|end| end.cmp(&key).is_le())
                        {
                            return false;
                        }
                        match add_if_matching(row) {
                            Ok(done) => {
                                reached_fetch = done;
                                !done
                            }
                            Err(error) => {
                                predicate_error = Some(error);
                                false
                            }
                        }
                    },
                )
                .map_err(|error| -> SystemStoreError { error.into() })?;
                if let Some(error) = predicate_error {
                    return Err(error);
                }
                if reached_fetch {
                    break 'ranges;
                }
            }
        }
    }
    Ok(())
}

fn query_jobs<R, C>(
    reader: &R,
    session_id: ValueFilter<String>,
    job_id: ValueFilter<u64>,
    fetch: usize,
    collector: &mut C,
) -> SystemStoreResult<()>
where
    R: StoreReader,
    C: RowCollector<JobRow> + ?Sized,
{
    let candidates = candidate_set! {
        JobPrimaryKey => JobPrimaryKeyBound {
            session_id: String => &session_id.domain,
            job_id: u64 => &job_id.domain,
        }
    };
    scan_candidates(
        candidates,
        fetch,
        |key| reader.table::<JobTable>().get(key),
        |lower, upper, visitor| reader.table::<JobTable>().scan(lower, upper, visitor),
        |row| Ok((session_id.predicate)(&row.session_id)? && (job_id.predicate)(&row.job_id)?),
        collector,
    )
}

fn query_stages<R, C>(
    reader: &R,
    session_id: ValueFilter<String>,
    job_id: ValueFilter<u64>,
    stage: ValueFilter<u64>,
    fetch: usize,
    collector: &mut C,
) -> SystemStoreResult<()>
where
    R: StoreReader,
    C: RowCollector<StageRow> + ?Sized,
{
    let candidates = candidate_set! {
        StagePrimaryKey => StagePrimaryKeyBound {
            session_id: String => &session_id.domain,
            job_id: u64 => &job_id.domain,
            stage: u64 => &stage.domain,
        }
    };
    scan_candidates(
        candidates,
        fetch,
        |key| reader.table::<StageTable>().get(key),
        |lower, upper, visitor| reader.table::<StageTable>().scan(lower, upper, visitor),
        |row| {
            Ok((session_id.predicate)(&row.session_id)?
                && (job_id.predicate)(&row.job_id)?
                && (stage.predicate)(&row.stage)?)
        },
        collector,
    )
}

fn query_tasks<R, C>(
    reader: &R,
    session_id: ValueFilter<String>,
    job_id: ValueFilter<u64>,
    stage: ValueFilter<u64>,
    partition: ValueFilter<u64>,
    attempt: ValueFilter<u64>,
    fetch: usize,
    collector: &mut C,
) -> SystemStoreResult<()>
where
    R: StoreReader,
    C: RowCollector<TaskRow> + ?Sized,
{
    let candidates = candidate_set! {
        TaskPrimaryKey => TaskPrimaryKeyBound {
            session_id: String => &session_id.domain,
            job_id: u64 => &job_id.domain,
            stage: u64 => &stage.domain,
            partition: u64 => &partition.domain,
            attempt: u64 => &attempt.domain,
        }
    };
    scan_candidates(
        candidates,
        fetch,
        |key| reader.table::<TaskTable>().get(key),
        |lower, upper, visitor| reader.table::<TaskTable>().scan(lower, upper, visitor),
        |row| {
            Ok((session_id.predicate)(&row.session_id)?
                && (job_id.predicate)(&row.job_id)?
                && (stage.predicate)(&row.stage)?
                && (partition.predicate)(&row.partition)?
                && (attempt.predicate)(&row.attempt)?)
        },
        collector,
    )
}

fn query_options<R, C>(
    reader: &R,
    key: ValueFilter<String>,
    fetch: usize,
    collector: &mut C,
) -> SystemStoreResult<()>
where
    R: StoreReader,
    C: RowCollector<OptionRow> + ?Sized,
{
    let candidates = candidate_set! {
        OptionPrimaryKey => OptionPrimaryKeyBound {
            key: String => &key.domain,
        }
    };
    scan_candidates(
        candidates,
        fetch,
        |key| reader.table::<OptionTable>().get(key),
        |lower, upper, visitor| reader.table::<OptionTable>().scan(lower, upper, visitor),
        |row| Ok((key.predicate)(&row.key)?),
        collector,
    )
}

fn query_sessions<R, C>(
    reader: &R,
    session_id: ValueFilter<String>,
    fetch: usize,
    collector: &mut C,
) -> SystemStoreResult<()>
where
    R: StoreReader,
    C: RowCollector<SessionRow> + ?Sized,
{
    let candidates = candidate_set! {
        SessionPrimaryKey => SessionPrimaryKeyBound {
            session_id: String => &session_id.domain,
        }
    };
    scan_candidates(
        candidates,
        fetch,
        |key| reader.table::<SessionTable>().get(key),
        |lower, upper, visitor| reader.table::<SessionTable>().scan(lower, upper, visitor),
        |row| Ok((session_id.predicate)(&row.session_id)?),
        collector,
    )
}

fn query_workers<R, C>(
    reader: &R,
    session_id: ValueFilter<String>,
    worker_id: ValueFilter<u64>,
    fetch: usize,
    collector: &mut C,
) -> SystemStoreResult<()>
where
    R: StoreReader,
    C: RowCollector<WorkerRow> + ?Sized,
{
    let candidates = candidate_set! {
        WorkerPrimaryKey => WorkerPrimaryKeyBound {
            session_id: String => &session_id.domain,
            worker_id: u64 => &worker_id.domain,
        }
    };
    scan_candidates(
        candidates,
        fetch,
        |key| reader.table::<WorkerTable>().get(key),
        |lower, upper, visitor| reader.table::<WorkerTable>().scan(lower, upper, visitor),
        |row| Ok((session_id.predicate)(&row.session_id)? && (worker_id.predicate)(&row.worker_id)?),
        collector,
    )
}

struct MetricScanState {
    fetch: usize,
    count: usize,
}

fn scan_metric_points<R, S, T, C>(
    reader: &R,
    series: &MetricSeriesMetadata,
    timestamp: &ValueFilter<TimestampMicros>,
    lower: Bound<TimestampMicros>,
    upper: Bound<TimestampMicros>,
    scan_state: &mut MetricScanState,
    collector: &mut C,
    to_metric_value: impl Fn(T) -> MetricValue,
) -> SystemStoreResult<bool>
where
    R: StoreReader + crate::access::SeriesReader<S, R::Error>,
    S: StoreSeries<
            SeriesKey = MetricSeriesId,
            PointKey = TimestampMicros,
            PointValue = MetricPointValue<T>,
        >,
    C: RowCollector<MetricRow> + ?Sized,
{
    let mut predicate_error = None;
    let mut stop = false;
    reader
        .series::<S>()
        .scan(&series.id, lower, upper, &mut |point_timestamp,
                                              point: MetricPointValue<
            T,
        >| {
            match (timestamp.predicate)(&point_timestamp) {
                Ok(true) => {
                    if let Some(timestamp) = DateTime::from_timestamp_micros(point_timestamp.0) {
                        scan_state.count += 1;
                        let row = MetricRow {
                            timestamp,
                            start_timestamp: point
                                .start_timestamp
                                .and_then(|timestamp| DateTime::from_timestamp_micros(timestamp.0)),
                            name: series.name.clone(),
                            attributes: series.attributes.clone(),
                            value: to_metric_value(point.value),
                        };
                        match collector.push(row) {
                            Ok(continue_scan) => {
                                if !continue_scan || scan_state.count == scan_state.fetch {
                                    stop = true;
                                    return false;
                                }
                            }
                            Err(error) => {
                                predicate_error = Some(error);
                                return false;
                            }
                        }
                    }
                    true
                }
                Ok(false) => true,
                Err(error) => {
                    predicate_error = Some(SystemStoreError::from(error));
                    false
                }
            }
        })
        .map_err(|error| -> SystemStoreError { error.into() })?;
    if let Some(error) = predicate_error {
        return Err(error);
    }
    Ok(stop)
}

fn query_metrics<R, C>(
    reader: &R,
    timestamp: ValueFilter<TimestampMicros>,
    name: ValueFilter<String>,
    attributes: Vec<MapValueFilter<String, String>>,
    fetch: usize,
    collector: &mut C,
) -> SystemStoreResult<()>
where
    R: StoreReader,
    C: RowCollector<MetricRow> + ?Sized,
{
    if fetch == 0 {
        return Ok(());
    }
    let mut scan_state = MetricScanState { fetch, count: 0 };
    for id in metric_series_ids(reader, &name, &attributes)? {
        let Some(series) = reader
            .table::<MetricSeriesTable>()
            .get(&id)
            .map_err(Into::into)?
        else {
            continue;
        };
        if !(name.predicate)(&series.name)? || !matches_metric_attributes(&attributes, &series)? {
            continue;
        }
        for range in timestamp.domain.ranges() {
            let complete = match series.kind {
                MetricSeriesKind::IntegerCount => {
                    scan_metric_points::<_, MetricIntegerPointSeries, _, _>(
                        reader,
                        &series,
                        &timestamp,
                        range.lower,
                        range.upper,
                        &mut scan_state,
                        collector,
                        |value| MetricValue::Count(MetricNumber::Integer(value)),
                    )?
                }
                MetricSeriesKind::FloatCount => {
                    scan_metric_points::<_, MetricFloatPointSeries, _, _>(
                        reader,
                        &series,
                        &timestamp,
                        range.lower,
                        range.upper,
                        &mut scan_state,
                        collector,
                        |value| MetricValue::Count(MetricNumber::Float(value)),
                    )?
                }
                MetricSeriesKind::IntegerGauge => {
                    scan_metric_points::<_, MetricIntegerPointSeries, _, _>(
                        reader,
                        &series,
                        &timestamp,
                        range.lower,
                        range.upper,
                        &mut scan_state,
                        collector,
                        |value| MetricValue::Gauge(MetricNumber::Integer(value)),
                    )?
                }
                MetricSeriesKind::FloatGauge => {
                    scan_metric_points::<_, MetricFloatPointSeries, _, _>(
                        reader,
                        &series,
                        &timestamp,
                        range.lower,
                        range.upper,
                        &mut scan_state,
                        collector,
                        |value| MetricValue::Gauge(MetricNumber::Float(value)),
                    )?
                }
                MetricSeriesKind::Histogram => {
                    scan_metric_points::<_, MetricHistogramPointSeries, _, _>(
                        reader,
                        &series,
                        &timestamp,
                        range.lower,
                        range.upper,
                        &mut scan_state,
                        collector,
                        MetricValue::Histogram,
                    )?
                }
            };
            if complete {
                return Ok(());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::Arc;

    use chrono::Utc;

    use super::{ValueFilter, VecCollector, query_jobs, query_metrics};
    use crate::access::DirectStoreBackend;
    use crate::backend::MemoryBackend;
    use crate::engine::{MetricSample, write_event, write_metrics};
    use crate::predicate::{MapValueFilter, Predicates, TimestampMicros, ValueDomain};
    use crate::types::{MetricNumber, MetricValue};
    use crate::{SystemEvent, SystemStoreResult};

    #[test]
    fn job_query_uses_composite_key_candidates() -> SystemStoreResult<()> {
        let mut backend = MemoryBackend::default();
        let created_at = Utc::now();
        {
            let mut writer = backend.write();
            for (session_id, job_id) in [("other", 2), ("target", 1), ("target", 2)] {
                write_event(
                    &mut writer,
                    SystemEvent::JobCreated {
                        session_id: session_id.to_string(),
                        job_id,
                        status: "RUNNING".to_string(),
                        created_at,
                    },
                )?;
            }
        }

        let reader = backend.read();
        let mut collector = VecCollector::default();
        query_jobs(
            &reader,
            ValueFilter::new(
                ValueDomain::point("target".to_string()),
                Predicates::always_true(),
            ),
            ValueFilter::new(
                ValueDomain::range(Bound::Excluded(1), Bound::Included(2)),
                Arc::new(|job_id| Ok(*job_id > 1 && *job_id <= 2)),
            ),
            usize::MAX,
            &mut collector,
        )?;
        let rows = collector.into_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].session_id, "target");
        assert_eq!(rows[0].job_id, 2);
        Ok(())
    }

    #[test]
    fn metric_query_uses_name_and_attribute_indexes() -> SystemStoreResult<()> {
        let mut backend = MemoryBackend::default();
        {
            let mut writer = backend.write();
            write_metrics(
                &mut writer,
                vec![
                    MetricSample {
                        name: "target".to_string(),
                        attributes: [("host".to_string(), "one".to_string())].into(),
                        timestamp: TimestampMicros(1),
                        start_timestamp: None,
                        value: MetricValue::Gauge(MetricNumber::Integer(1)),
                    },
                    MetricSample {
                        name: "target".to_string(),
                        attributes: [("host".to_string(), "two".to_string())].into(),
                        timestamp: TimestampMicros(2),
                        start_timestamp: Some(TimestampMicros(1)),
                        value: MetricValue::Gauge(MetricNumber::Integer(2)),
                    },
                    MetricSample {
                        name: "other".to_string(),
                        attributes: [("host".to_string(), "one".to_string())].into(),
                        timestamp: TimestampMicros(3),
                        start_timestamp: None,
                        value: MetricValue::Gauge(MetricNumber::Integer(3)),
                    },
                ],
            )?;
        }

        let mut collector = VecCollector::default();
        query_metrics(
            &backend.read(),
            ValueFilter::all(Predicates::always_true()),
            ValueFilter::new(
                ValueDomain::point("target".to_string()),
                Predicates::always_true(),
            ),
            vec![MapValueFilter::new(
                "host".to_string(),
                ValueDomain::point("two".to_string()),
                Predicates::always_true(),
            )],
            usize::MAX,
            &mut collector,
        )?;
        let rows = collector.into_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "target");
        assert_eq!(rows[0].attributes["host"], "two");
        assert_eq!(
            rows[0].start_timestamp,
            chrono::DateTime::from_timestamp_micros(1)
        );
        Ok(())
    }
}
