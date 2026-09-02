//! System store mutations.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::SystemEvent;
use crate::access::{StoreReader, StoreWriter};
use crate::catalog::{JobRow, OptionRow, SessionRow, StageRow, TaskRow, WorkerRow};
use crate::event::{
    is_job_stopped, is_session_deleted, is_stage_stopped, is_task_stopped, is_worker_stopped,
};
use crate::model::{
    JobPrimaryKey, JobTable, MetricAttributeIndex, MetricAttributeKey, MetricAttributes,
    MetricFloatPointSeries, MetricHistogramPointSeries, MetricIntegerPointSeries, MetricNameIndex,
    MetricSeriesIdentityTable, MetricSeriesKey, MetricSeriesKind, MetricSeriesMetadata,
    MetricSeriesTable, NextMetricSeriesIdTable, OptionPrimaryKey, OptionTable, SessionPrimaryKey,
    SessionTable, StagePrimaryKey, StageTable, TaskPrimaryKey, TaskTable, WorkerPrimaryKey,
    WorkerTable,
};
use crate::predicate::TimestampMicros;
use crate::types::{MetricNumber, MetricValue, StageInput};

type WriteResult<W> = Result<(), <W as StoreReader>::Error>;

/// A decoded and canonicalized metric point supplied by telemetry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSample {
    pub name: String,
    pub attributes: MetricAttributes,
    pub timestamp: TimestampMicros,
    pub value: MetricValue,
}

fn metric_series_kind(value: &MetricValue) -> MetricSeriesKind {
    match value {
        MetricValue::Count(MetricNumber::Integer(_)) => MetricSeriesKind::IntegerCount,
        MetricValue::Count(MetricNumber::Float(_)) => MetricSeriesKind::FloatCount,
        MetricValue::Gauge(MetricNumber::Integer(_)) => MetricSeriesKind::IntegerGauge,
        MetricValue::Gauge(MetricNumber::Float(_)) => MetricSeriesKind::FloatGauge,
        MetricValue::Histogram(_) => MetricSeriesKind::Histogram,
    }
}

/// Writes an event using only the backend-neutral typed writer surface.
pub fn write_event<W>(writer: &mut W, event: SystemEvent) -> WriteResult<W>
where
    W: StoreWriter,
{
    match event {
        SystemEvent::OptionCreated { key, value } => write_option_created_event(writer, key, value),
        SystemEvent::OptionUpdated { key, value } => write_option_updated_event(writer, key, value),
        SystemEvent::SessionCreated {
            session_id,
            user_id,
            status,
            created_at,
        } => write_session_created_event(writer, session_id, user_id, status, created_at),
        SystemEvent::SessionUpdated {
            session_id,
            status,
            updated_at,
        } => write_session_updated_event(writer, session_id, status, updated_at),
        SystemEvent::JobCreated {
            session_id,
            job_id,
            status,
            created_at,
        } => write_job_created_event(writer, session_id, job_id, status, created_at),
        SystemEvent::JobUpdated {
            session_id,
            job_id,
            status,
            updated_at,
        } => write_job_updated_event(writer, session_id, job_id, status, updated_at),
        SystemEvent::StageCreated {
            session_id,
            job_id,
            stage,
            partitions,
            inputs,
            group,
            mode,
            distribution,
            placement,
            status,
            created_at,
        } => write_stage_created_event(
            writer,
            session_id,
            job_id,
            stage,
            partitions,
            inputs,
            group,
            mode,
            distribution,
            placement,
            status,
            created_at,
        ),
        SystemEvent::StageUpdated {
            session_id,
            job_id,
            stage,
            status,
            updated_at,
        } => write_stage_updated_event(writer, session_id, job_id, stage, status, updated_at),
        SystemEvent::TaskCreated {
            session_id,
            job_id,
            stage,
            partition,
            attempt,
            status,
            created_at,
        } => write_task_created_event(
            writer, session_id, job_id, stage, partition, attempt, status, created_at,
        ),
        SystemEvent::TaskUpdated {
            session_id,
            job_id,
            stage,
            partition,
            attempt,
            status,
            updated_at,
        } => write_task_updated_event(
            writer, session_id, job_id, stage, partition, attempt, status, updated_at,
        ),
        SystemEvent::WorkerCreated {
            session_id,
            worker_id,
            host,
            port,
            status,
            created_at,
        } => write_worker_created_event(
            writer, session_id, worker_id, host, port, status, created_at,
        ),
        SystemEvent::WorkerUpdated {
            session_id,
            worker_id,
            host,
            port,
            status,
            updated_at,
        } => write_worker_updated_event(
            writer, session_id, worker_id, host, port, status, updated_at,
        ),
    }
}

fn write_option_created_event<W: StoreWriter>(
    writer: &mut W,
    key: String,
    value: String,
) -> WriteResult<W> {
    writer.table_mut::<OptionTable>().put(
        OptionPrimaryKey { key: key.clone() },
        OptionRow { key, value },
    )
}

fn write_option_updated_event<W: StoreWriter>(
    writer: &mut W,
    key: String,
    value: String,
) -> WriteResult<W> {
    write_option_created_event(writer, key, value)
}

fn write_session_created_event<W: StoreWriter>(
    writer: &mut W,
    session_id: String,
    user_id: String,
    status: String,
    created_at: DateTime<Utc>,
) -> WriteResult<W> {
    writer
        .table_mut::<SessionTable>()
        .insert_if_absent(
            SessionPrimaryKey {
                session_id: session_id.clone(),
            },
            SessionRow {
                session_id,
                user_id,
                status,
                created_at,
                deleted_at: None,
            },
        )
        .map(|_| ())
}

fn write_session_updated_event<W: StoreWriter>(
    writer: &mut W,
    session_id: String,
    status: String,
    updated_at: DateTime<Utc>,
) -> WriteResult<W> {
    writer
        .table_mut::<SessionTable>()
        .update_if_present(&SessionPrimaryKey { session_id }, |row| {
            row.status = status;
            if row.deleted_at.is_none() && is_session_deleted(&row.status) {
                row.deleted_at = Some(updated_at);
            }
        })
        .map(|_| ())
}

fn write_job_created_event<W: StoreWriter>(
    writer: &mut W,
    session_id: String,
    job_id: u64,
    status: String,
    created_at: DateTime<Utc>,
) -> WriteResult<W> {
    writer
        .table_mut::<JobTable>()
        .insert_if_absent(
            JobPrimaryKey {
                session_id: session_id.clone(),
                job_id,
            },
            JobRow {
                session_id,
                job_id,
                status,
                created_at,
                stopped_at: None,
            },
        )
        .map(|_| ())
}

fn write_job_updated_event<W: StoreWriter>(
    writer: &mut W,
    session_id: String,
    job_id: u64,
    status: String,
    updated_at: DateTime<Utc>,
) -> WriteResult<W> {
    writer
        .table_mut::<JobTable>()
        .update_if_present(&JobPrimaryKey { session_id, job_id }, |row| {
            row.status = status;
            if row.stopped_at.is_none() && is_job_stopped(&row.status) {
                row.stopped_at = Some(updated_at);
            }
        })
        .map(|_| ())
}

#[expect(
    clippy::too_many_arguments,
    reason = "the durable stage-created event intentionally mirrors its wire representation"
)]
fn write_stage_created_event<W: StoreWriter>(
    writer: &mut W,
    session_id: String,
    job_id: u64,
    stage: u64,
    partitions: u64,
    inputs: Vec<StageInput>,
    group: String,
    mode: String,
    distribution: String,
    placement: String,
    status: String,
    created_at: DateTime<Utc>,
) -> WriteResult<W> {
    writer
        .table_mut::<StageTable>()
        .insert_if_absent(
            StagePrimaryKey {
                session_id: session_id.clone(),
                job_id,
                stage,
            },
            StageRow {
                session_id,
                job_id,
                stage,
                partitions,
                inputs,
                group,
                mode,
                distribution,
                placement,
                status,
                created_at,
                stopped_at: None,
            },
        )
        .map(|_| ())
}

fn write_stage_updated_event<W: StoreWriter>(
    writer: &mut W,
    session_id: String,
    job_id: u64,
    stage: u64,
    status: String,
    updated_at: DateTime<Utc>,
) -> WriteResult<W> {
    writer
        .table_mut::<StageTable>()
        .update_if_present(
            &StagePrimaryKey {
                session_id,
                job_id,
                stage,
            },
            |row| {
                row.status = status;
                if row.stopped_at.is_none() && is_stage_stopped(&row.status) {
                    row.stopped_at = Some(updated_at);
                }
            },
        )
        .map(|_| ())
}

fn write_task_created_event<W: StoreWriter>(
    writer: &mut W,
    session_id: String,
    job_id: u64,
    stage: u64,
    partition: u64,
    attempt: u64,
    status: String,
    created_at: DateTime<Utc>,
) -> WriteResult<W> {
    writer
        .table_mut::<TaskTable>()
        .insert_if_absent(
            TaskPrimaryKey {
                session_id: session_id.clone(),
                job_id,
                stage,
                partition,
                attempt,
            },
            TaskRow {
                session_id,
                job_id,
                stage,
                partition,
                attempt,
                status,
                created_at,
                stopped_at: None,
            },
        )
        .map(|_| ())
}

fn write_task_updated_event<W: StoreWriter>(
    writer: &mut W,
    session_id: String,
    job_id: u64,
    stage: u64,
    partition: u64,
    attempt: u64,
    status: String,
    updated_at: DateTime<Utc>,
) -> WriteResult<W> {
    writer
        .table_mut::<TaskTable>()
        .update_if_present(
            &TaskPrimaryKey {
                session_id,
                job_id,
                stage,
                partition,
                attempt,
            },
            |row| {
                row.status = status;
                if row.stopped_at.is_none() && is_task_stopped(&row.status) {
                    row.stopped_at = Some(updated_at);
                }
            },
        )
        .map(|_| ())
}

fn write_worker_created_event<W: StoreWriter>(
    writer: &mut W,
    session_id: String,
    worker_id: u64,
    host: Option<String>,
    port: Option<u16>,
    status: String,
    created_at: DateTime<Utc>,
) -> WriteResult<W> {
    writer
        .table_mut::<WorkerTable>()
        .insert_if_absent(
            WorkerPrimaryKey {
                session_id: session_id.clone(),
                worker_id,
            },
            WorkerRow {
                session_id,
                worker_id,
                host,
                port,
                status,
                created_at,
                stopped_at: None,
            },
        )
        .map(|_| ())
}

fn write_worker_updated_event<W: StoreWriter>(
    writer: &mut W,
    session_id: String,
    worker_id: u64,
    host: Option<String>,
    port: Option<u16>,
    status: String,
    updated_at: DateTime<Utc>,
) -> WriteResult<W> {
    writer
        .table_mut::<WorkerTable>()
        .update_if_present(
            &WorkerPrimaryKey {
                session_id,
                worker_id,
            },
            |row| {
                row.host = host;
                row.port = port;
                row.status = status;
                if row.stopped_at.is_none() && is_worker_stopped(&row.status) {
                    row.stopped_at = Some(updated_at);
                }
            },
        )
        .map(|_| ())
}

/// Writes telemetry samples and maintains the metric identity and filter indexes.
pub fn write_metrics<W>(writer: &mut W, samples: Vec<MetricSample>) -> WriteResult<W>
where
    W: StoreWriter,
{
    for sample in samples {
        let kind = metric_series_kind(&sample.value);
        let identity = MetricSeriesKey {
            name: sample.name.clone(),
            attributes: sample.attributes.clone(),
            kind,
        };
        let id = match writer.table::<MetricSeriesIdentityTable>().get(&identity)? {
            Some(id) => id,
            None => {
                let next_key = ();
                let id = writer
                    .table::<NextMetricSeriesIdTable>()
                    .get(&next_key)?
                    .unwrap_or_default();
                writer
                    .table_mut::<NextMetricSeriesIdTable>()
                    .put(next_key, id.saturating_add(1))?;
                writer.table_mut::<MetricSeriesTable>().put(
                    id,
                    MetricSeriesMetadata {
                        id,
                        name: sample.name.clone(),
                        attributes: sample.attributes.clone(),
                        kind,
                    },
                )?;
                writer
                    .table_mut::<MetricSeriesIdentityTable>()
                    .put(identity, id)?;
                writer
                    .index_mut::<MetricNameIndex>()
                    .put(sample.name.clone(), id)?;
                for (key, value) in &sample.attributes {
                    writer.index_mut::<MetricAttributeIndex>().put(
                        MetricAttributeKey {
                            key: key.clone(),
                            value: value.clone(),
                        },
                        id,
                    )?;
                }
                id
            }
        };
        match sample.value {
            MetricValue::Count(MetricNumber::Integer(value))
            | MetricValue::Gauge(MetricNumber::Integer(value)) => writer
                .series_mut::<MetricIntegerPointSeries>()
                .put(id, sample.timestamp, value)?,
            MetricValue::Count(MetricNumber::Float(value))
            | MetricValue::Gauge(MetricNumber::Float(value)) => writer
                .series_mut::<MetricFloatPointSeries>()
                .put(id, sample.timestamp, value)?,
            MetricValue::Histogram(value) => writer
                .series_mut::<MetricHistogramPointSeries>()
                .put(id, sample.timestamp, value)?,
        }
    }
    Ok(())
}
