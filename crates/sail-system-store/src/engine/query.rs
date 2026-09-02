//! Typed system store queries.

use std::collections::BTreeSet;
use std::ops::Bound;

use chrono::DateTime;
use tokio::sync::oneshot;

use super::candidate::{CandidateSet, ValueOrd, candidate_key_bound, candidate_set};
use crate::access::StoreReader;
use crate::catalog::{JobRow, MetricRow, OptionRow, SessionRow, StageRow, TaskRow, WorkerRow};
use crate::model::{
    JobPrimaryKey, JobTable, MetricAttributeIndex, MetricAttributeKey, MetricFloatPointSeries,
    MetricHistogramPointSeries, MetricIntegerPointSeries, MetricNameIndex, MetricSeriesId,
    MetricSeriesKind, MetricSeriesMetadata, MetricSeriesTable, OptionPrimaryKey, OptionTable,
    SessionPrimaryKey, SessionTable, StagePrimaryKey, StageTable, StoreSeries, TaskPrimaryKey,
    TaskTable, WorkerPrimaryKey, WorkerTable,
};
use crate::predicate::{MapValueFilter, TimestampMicros, ValueFilter};
use crate::{SystemStoreError, SystemStoreResult};

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

/// A strongly typed system store read request. Each variant owns the reply channel for its row type.
pub(crate) enum SystemStoreQuery {
    Jobs {
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        fetch: usize,
        reply: oneshot::Sender<SystemStoreResult<Vec<JobRow>>>,
    },
    Metrics {
        timestamp: ValueFilter<TimestampMicros>,
        name: ValueFilter<String>,
        attributes: Vec<MapValueFilter<String, String>>,
        fetch: usize,
        reply: oneshot::Sender<SystemStoreResult<Vec<MetricRow>>>,
    },
    Stages {
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        stage: ValueFilter<u64>,
        fetch: usize,
        reply: oneshot::Sender<SystemStoreResult<Vec<StageRow>>>,
    },
    Tasks {
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        stage: ValueFilter<u64>,
        partition: ValueFilter<u64>,
        attempt: ValueFilter<u64>,
        fetch: usize,
        reply: oneshot::Sender<SystemStoreResult<Vec<TaskRow>>>,
    },
    Options {
        key: ValueFilter<String>,
        fetch: usize,
        reply: oneshot::Sender<SystemStoreResult<Vec<OptionRow>>>,
    },
    Sessions {
        session_id: ValueFilter<String>,
        fetch: usize,
        reply: oneshot::Sender<SystemStoreResult<Vec<SessionRow>>>,
    },
    Workers {
        session_id: ValueFilter<String>,
        worker_id: ValueFilter<u64>,
        fetch: usize,
        reply: oneshot::Sender<SystemStoreResult<Vec<WorkerRow>>>,
    },
}

impl SystemStoreQuery {
    pub(crate) fn fail(self, error: SystemStoreError) {
        match self {
            Self::Jobs { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::Metrics { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::Stages { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::Tasks { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::Options { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::Sessions { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::Workers { reply, .. } => {
                let _ = reply.send(Err(error));
            }
        }
    }

    pub(crate) fn execute<R>(self, reader: &R)
    where
        R: StoreReader,
    {
        match self {
            Self::Jobs {
                session_id,
                job_id,
                fetch,
                reply,
            } => {
                let _ = reply.send(query_jobs(reader, session_id, job_id, fetch));
            }
            Self::Metrics {
                timestamp,
                name,
                attributes,
                fetch,
                reply,
            } => {
                let _ = reply.send(query_metrics(reader, timestamp, name, attributes, fetch));
            }
            Self::Stages {
                session_id,
                job_id,
                stage,
                fetch,
                reply,
            } => {
                let _ = reply.send(query_stages(reader, session_id, job_id, stage, fetch));
            }
            Self::Tasks {
                session_id,
                job_id,
                stage,
                partition,
                attempt,
                fetch,
                reply,
            } => {
                let _ = reply.send(query_tasks(
                    reader, session_id, job_id, stage, partition, attempt, fetch,
                ));
            }
            Self::Options { key, fetch, reply } => {
                let _ = reply.send(query_options(reader, key, fetch));
            }
            Self::Sessions {
                session_id,
                fetch,
                reply,
            } => {
                let _ = reply.send(query_sessions(reader, session_id, fetch));
            }
            Self::Workers {
                session_id,
                worker_id,
                fetch,
                reply,
            } => {
                let _ = reply.send(query_workers(reader, session_id, worker_id, fetch));
            }
        }
    }
}

fn scan_candidates<K, B, V, E>(
    candidates: CandidateSet<K, B>,
    fetch: usize,
    mut get: impl FnMut(&K) -> Result<Option<V>, E>,
    mut scan: impl FnMut(Bound<K>, Bound<K>, &mut dyn FnMut(K, V) -> bool) -> Result<(), E>,
    predicate: impl Fn(&V) -> SystemStoreResult<bool>,
) -> SystemStoreResult<Vec<V>>
where
    K: Ord,
    B: ValueOrd<K>,
    E: Into<SystemStoreError>,
{
    if fetch == 0 {
        return Ok(vec![]);
    }
    let mut rows = Vec::new();
    let mut add_if_matching = |row: V| -> SystemStoreResult<bool> {
        if predicate(&row)? {
            rows.push(row);
            if rows.len() == fetch {
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
    Ok(rows)
}

fn query_jobs<R: StoreReader>(
    reader: &R,
    session_id: ValueFilter<String>,
    job_id: ValueFilter<u64>,
    fetch: usize,
) -> SystemStoreResult<Vec<JobRow>> {
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
    )
}

fn query_stages<R: StoreReader>(
    reader: &R,
    session_id: ValueFilter<String>,
    job_id: ValueFilter<u64>,
    stage: ValueFilter<u64>,
    fetch: usize,
) -> SystemStoreResult<Vec<StageRow>> {
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
    )
}

fn query_tasks<R: StoreReader>(
    reader: &R,
    session_id: ValueFilter<String>,
    job_id: ValueFilter<u64>,
    stage: ValueFilter<u64>,
    partition: ValueFilter<u64>,
    attempt: ValueFilter<u64>,
    fetch: usize,
) -> SystemStoreResult<Vec<TaskRow>> {
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
    )
}

fn query_options<R: StoreReader>(
    reader: &R,
    key: ValueFilter<String>,
    fetch: usize,
) -> SystemStoreResult<Vec<OptionRow>> {
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
    )
}

fn query_sessions<R: StoreReader>(
    reader: &R,
    session_id: ValueFilter<String>,
    fetch: usize,
) -> SystemStoreResult<Vec<SessionRow>> {
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
    )
}

fn query_workers<R: StoreReader>(
    reader: &R,
    session_id: ValueFilter<String>,
    worker_id: ValueFilter<u64>,
    fetch: usize,
) -> SystemStoreResult<Vec<WorkerRow>> {
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
    )
}

fn scan_metric_points<R, S, T>(
    reader: &R,
    series: &MetricSeriesMetadata,
    timestamp: &ValueFilter<TimestampMicros>,
    lower: Bound<TimestampMicros>,
    upper: Bound<TimestampMicros>,
    fetch: usize,
    out: &mut Vec<MetricRow>,
    to_metric_value: impl Fn(T) -> crate::types::MetricValue,
) -> SystemStoreResult<bool>
where
    R: StoreReader + crate::access::SeriesReader<S, R::Error>,
    S: StoreSeries<SeriesKey = MetricSeriesId, PointKey = TimestampMicros, PointValue = T>,
{
    let mut predicate_error = None;
    reader
        .series::<S>()
        .scan(
            &series.id,
            lower,
            upper,
            &mut |point_timestamp, value| match (timestamp.predicate)(&point_timestamp) {
                Ok(true) => {
                    if let Some(timestamp) = DateTime::from_timestamp_micros(point_timestamp.0) {
                        out.push(MetricRow {
                            timestamp,
                            name: series.name.clone(),
                            attributes: series.attributes.clone(),
                            value: to_metric_value(value),
                        });
                        if out.len() == fetch {
                            return false;
                        }
                    }
                    true
                }
                Ok(false) => true,
                Err(error) => {
                    predicate_error = Some(SystemStoreError::from(error));
                    false
                }
            },
        )
        .map_err(|error| -> SystemStoreError { error.into() })?;
    if let Some(error) = predicate_error {
        return Err(error);
    }
    Ok(out.len() == fetch)
}

fn query_metrics<R: StoreReader>(
    reader: &R,
    timestamp: ValueFilter<TimestampMicros>,
    name: ValueFilter<String>,
    attributes: Vec<MapValueFilter<String, String>>,
    fetch: usize,
) -> SystemStoreResult<Vec<MetricRow>> {
    if fetch == 0 {
        return Ok(vec![]);
    }
    let mut out = Vec::new();
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
                    scan_metric_points::<_, MetricIntegerPointSeries, _>(
                        reader,
                        &series,
                        &timestamp,
                        range.lower,
                        range.upper,
                        fetch,
                        &mut out,
                        |value| {
                            crate::types::MetricValue::Count(crate::types::MetricNumber::Integer(
                                value,
                            ))
                        },
                    )?
                }
                MetricSeriesKind::FloatCount => scan_metric_points::<_, MetricFloatPointSeries, _>(
                    reader,
                    &series,
                    &timestamp,
                    range.lower,
                    range.upper,
                    fetch,
                    &mut out,
                    |value| {
                        crate::types::MetricValue::Count(crate::types::MetricNumber::Float(value))
                    },
                )?,
                MetricSeriesKind::IntegerGauge => {
                    scan_metric_points::<_, MetricIntegerPointSeries, _>(
                        reader,
                        &series,
                        &timestamp,
                        range.lower,
                        range.upper,
                        fetch,
                        &mut out,
                        |value| {
                            crate::types::MetricValue::Gauge(crate::types::MetricNumber::Integer(
                                value,
                            ))
                        },
                    )?
                }
                MetricSeriesKind::FloatGauge => scan_metric_points::<_, MetricFloatPointSeries, _>(
                    reader,
                    &series,
                    &timestamp,
                    range.lower,
                    range.upper,
                    fetch,
                    &mut out,
                    |value| {
                        crate::types::MetricValue::Gauge(crate::types::MetricNumber::Float(value))
                    },
                )?,
                MetricSeriesKind::Histogram => {
                    scan_metric_points::<_, MetricHistogramPointSeries, _>(
                        reader,
                        &series,
                        &timestamp,
                        range.lower,
                        range.upper,
                        fetch,
                        &mut out,
                        crate::types::MetricValue::Histogram,
                    )?
                }
            };
            if complete {
                return Ok(out);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;
    use std::sync::Arc;

    use chrono::Utc;

    use super::{ValueFilter, query_jobs, query_metrics};
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
        let rows = query_jobs(
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
        )?;

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
                        value: MetricValue::Gauge(MetricNumber::Integer(1)),
                    },
                    MetricSample {
                        name: "target".to_string(),
                        attributes: [("host".to_string(), "two".to_string())].into(),
                        timestamp: TimestampMicros(2),
                        value: MetricValue::Gauge(MetricNumber::Integer(2)),
                    },
                    MetricSample {
                        name: "other".to_string(),
                        attributes: [("host".to_string(), "one".to_string())].into(),
                        timestamp: TimestampMicros(3),
                        value: MetricValue::Gauge(MetricNumber::Integer(3)),
                    },
                ],
            )?;
        }

        let rows = query_metrics(
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
        )?;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "target");
        assert_eq!(rows[0].attributes["host"], "two");
        Ok(())
    }
}
