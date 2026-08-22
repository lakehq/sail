use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use sail_common_datafusion::array::serde::ArrowSerializer;
use sail_common_datafusion::system::catalog::SystemTable;
use sail_common_datafusion::system::predicate::Predicate;
use serde::{Deserialize, Serialize};

use super::SystemEventActor;

impl SystemEventActor {
    pub(super) fn read_jobs(
        &self,
        session_id: Predicate<String>,
        job_id: Predicate<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let mut rows = Vec::new();
        for row in self.store.jobs.values() {
            if rows.len() >= fetch {
                break;
            }
            if session_id(&row.session_id)? && job_id(&row.job_id)? {
                rows.push(row.clone());
            }
        }
        Self::build_batch(SystemTable::Jobs, rows)
    }

    pub(super) fn read_stages(
        &self,
        session_id: Predicate<String>,
        job_id: Predicate<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let mut rows = Vec::new();
        for row in self.store.stages.values() {
            if rows.len() >= fetch {
                break;
            }
            if session_id(&row.session_id)? && job_id(&row.job_id)? {
                rows.push(row.clone());
            }
        }
        Self::build_batch(SystemTable::Stages, rows)
    }

    pub(super) fn read_tasks(
        &self,
        session_id: Predicate<String>,
        job_id: Predicate<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let mut rows = Vec::new();
        for row in self.store.tasks.values() {
            if rows.len() >= fetch {
                break;
            }
            if session_id(&row.session_id)? && job_id(&row.job_id)? {
                rows.push(row.clone());
            }
        }
        Self::build_batch(SystemTable::Tasks, rows)
    }

    pub(super) fn read_options(&self, key: Predicate<String>, fetch: usize) -> Result<RecordBatch> {
        let mut rows = Vec::new();
        for row in self.store.options.values() {
            if rows.len() >= fetch {
                break;
            }
            if key(&row.key)? {
                rows.push(row.clone());
            }
        }
        Self::build_batch(SystemTable::Options, rows)
    }

    pub(super) fn read_sessions(
        &self,
        session_id: Predicate<String>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let mut rows = Vec::new();
        for row in self.store.sessions.values() {
            if rows.len() >= fetch {
                break;
            }
            if session_id(&row.session_id)? {
                rows.push(row.clone());
            }
        }
        Self::build_batch(SystemTable::Sessions, rows)
    }

    pub(super) fn read_workers(
        &self,
        session_id: Predicate<String>,
        worker_id: Predicate<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let mut rows = Vec::new();
        for row in self.store.workers.values() {
            if rows.len() >= fetch {
                break;
            }
            if session_id(&row.session_id)? && worker_id(&row.worker_id)? {
                rows.push(row.clone());
            }
        }
        Self::build_batch(SystemTable::Workers, rows)
    }

    fn build_batch<T>(table: SystemTable, rows: Vec<T>) -> Result<RecordBatch>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        ArrowSerializer::build_record_batch_with_schema(&rows, table.schema())
    }
}
