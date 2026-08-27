use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use sail_common_datafusion::array::serde::ArrowSerializer;
use sail_common_datafusion::system::catalog::SystemTable;
use sail_common_datafusion::system::predicate::ValueFilter;
use sail_common_datafusion::system::reader::read_ordered_map;
use sail_common_datafusion::{candidate_key_bound, candidate_set};
use serde::{Deserialize, Serialize};

use super::SystemEventActor;
use crate::system_event::{
    JobPrimaryKey, OptionPrimaryKey, SessionPrimaryKey, StagePrimaryKey, TaskPrimaryKey,
    WorkerPrimaryKey,
};

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

impl SystemEventActor {
    pub(super) fn read_jobs(
        &self,
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let candidates = candidate_set! {
            JobPrimaryKey => JobPrimaryKeyBound {
                session_id: String => &session_id.domain,
                job_id: u64 => &job_id.domain,
            }
        };
        let rows = read_ordered_map(
            &self.store.jobs,
            candidates,
            |row| Ok((session_id.predicate)(&row.session_id)? && (job_id.predicate)(&row.job_id)?),
            fetch,
        )?;
        Self::build_batch(SystemTable::Jobs, rows)
    }

    pub(super) fn read_stages(
        &self,
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        stage: ValueFilter<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let candidates = candidate_set! {
            StagePrimaryKey => StagePrimaryKeyBound {
                session_id: String => &session_id.domain,
                job_id: u64 => &job_id.domain,
                stage: u64 => &stage.domain,
            }
        };
        let rows = read_ordered_map(
            &self.store.stages,
            candidates,
            |row| {
                Ok((session_id.predicate)(&row.session_id)?
                    && (job_id.predicate)(&row.job_id)?
                    && (stage.predicate)(&row.stage)?)
            },
            fetch,
        )?;
        Self::build_batch(SystemTable::Stages, rows)
    }

    pub(super) fn read_tasks(
        &self,
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        stage: ValueFilter<u64>,
        partition: ValueFilter<u64>,
        attempt: ValueFilter<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let candidates = candidate_set! {
            TaskPrimaryKey => TaskPrimaryKeyBound {
                session_id: String => &session_id.domain,
                job_id: u64 => &job_id.domain,
                stage: u64 => &stage.domain,
                partition: u64 => &partition.domain,
                attempt: u64 => &attempt.domain,
            }
        };
        let rows = read_ordered_map(
            &self.store.tasks,
            candidates,
            |row| {
                Ok((session_id.predicate)(&row.session_id)?
                    && (job_id.predicate)(&row.job_id)?
                    && (stage.predicate)(&row.stage)?
                    && (partition.predicate)(&row.partition)?
                    && (attempt.predicate)(&row.attempt)?)
            },
            fetch,
        )?;
        Self::build_batch(SystemTable::Tasks, rows)
    }

    pub(super) fn read_options(
        &self,
        key: ValueFilter<String>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let candidates = candidate_set! {
            OptionPrimaryKey => OptionPrimaryKeyBound {
                key: String => &key.domain,
            }
        };
        let rows = read_ordered_map(
            &self.store.options,
            candidates,
            |row| (key.predicate)(&row.key),
            fetch,
        )?;
        Self::build_batch(SystemTable::Options, rows)
    }

    pub(super) fn read_sessions(
        &self,
        session_id: ValueFilter<String>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let candidates = candidate_set! {
            SessionPrimaryKey => SessionPrimaryKeyBound {
                session_id: String => &session_id.domain,
            }
        };
        let rows = read_ordered_map(
            &self.store.sessions,
            candidates,
            |row| (session_id.predicate)(&row.session_id),
            fetch,
        )?;
        Self::build_batch(SystemTable::Sessions, rows)
    }

    pub(super) fn read_workers(
        &self,
        session_id: ValueFilter<String>,
        worker_id: ValueFilter<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let candidates = candidate_set! {
            WorkerPrimaryKey => WorkerPrimaryKeyBound {
                session_id: String => &session_id.domain,
                worker_id: u64 => &worker_id.domain,
            }
        };
        let rows = read_ordered_map(
            &self.store.workers,
            candidates,
            |row| {
                Ok((session_id.predicate)(&row.session_id)?
                    && (worker_id.predicate)(&row.worker_id)?)
            },
            fetch,
        )?;
        Self::build_batch(SystemTable::Workers, rows)
    }

    fn build_batch<T>(table: SystemTable, rows: Vec<T>) -> Result<RecordBatch>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        ArrowSerializer::build_record_batch_with_schema(&rows, table.schema())
    }
}
