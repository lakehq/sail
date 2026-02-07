use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{internal_datafusion_err, Result};
use datafusion::physical_expr::PhysicalExpr;
use sail_common_datafusion::array::serde::ArrowSerializer;
use sail_common_datafusion::extension::SessionExtension;
use sail_common_datafusion::system::catalog::SystemTable;
use sail_common_datafusion::system::observable::{SessionManagerObserver, StateObservable};
use sail_common_datafusion::system::predicate::Predicates;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::predicate::PredicateExtractor;

pub struct SystemTableService {
    system_manager: Box<dyn StateObservable<SessionManagerObserver>>,
}

impl SystemTableService {
    pub fn new(system_manager: Box<dyn StateObservable<SessionManagerObserver>>) -> Self {
        Self { system_manager }
    }

    pub async fn read(
        &self,
        table: SystemTable,
        projection: Option<Vec<usize>>,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        fetch: Option<usize>,
    ) -> Result<RecordBatch> {
        let schema = table.schema();
        let fetch = fetch.unwrap_or(usize::MAX);
        let mut filters = PredicateExtractor::new(filters);
        let batch = match table {
            SystemTable::Jobs => {
                let session_id = filters
                    .extract("session_id")?
                    .unwrap_or_else(Predicates::always_true);
                let job_id = filters
                    .extract("job_id")?
                    .unwrap_or_else(Predicates::always_true);
                filters.finalize()?;
                self.observe_system_manager(
                    |tx| SessionManagerObserver::Jobs {
                        session_id,
                        job_id,
                        fetch,
                        result: tx,
                    },
                    schema,
                )
                .await?
            }
            SystemTable::Stages => {
                let session_id = filters
                    .extract("session_id")?
                    .unwrap_or_else(Predicates::always_true);
                let job_id = filters
                    .extract("job_id")?
                    .unwrap_or_else(Predicates::always_true);
                filters.finalize()?;
                self.observe_system_manager(
                    |tx| SessionManagerObserver::Stages {
                        session_id,
                        job_id,
                        fetch,
                        result: tx,
                    },
                    schema,
                )
                .await?
            }
            SystemTable::Tasks => {
                let session_id = filters
                    .extract("session_id")?
                    .unwrap_or_else(Predicates::always_true);
                let job_id = filters
                    .extract("job_id")?
                    .unwrap_or_else(Predicates::always_true);
                filters.finalize()?;
                self.observe_system_manager(
                    |tx| SessionManagerObserver::Tasks {
                        session_id,
                        job_id,
                        fetch,
                        result: tx,
                    },
                    schema,
                )
                .await?
            }
            SystemTable::Sessions => {
                let session_id = filters
                    .extract("session_id")?
                    .unwrap_or_else(Predicates::always_true);
                filters.finalize()?;
                self.observe_system_manager(
                    |tx| SessionManagerObserver::Sessions {
                        session_id,
                        fetch,
                        result: tx,
                    },
                    schema,
                )
                .await?
            }
            SystemTable::Workers => {
                let session_id = filters
                    .extract("session_id")?
                    .unwrap_or_else(Predicates::always_true);
                let worker_id = filters
                    .extract("worker_id")?
                    .unwrap_or_else(Predicates::always_true);
                filters.finalize()?;
                self.observe_system_manager(
                    |tx| SessionManagerObserver::Workers {
                        session_id,
                        worker_id,
                        fetch,
                        result: tx,
                    },
                    schema,
                )
                .await?
            }
        };
        if let Some(projection) = projection {
            Ok(batch.project(&projection)?)
        } else {
            Ok(batch)
        }
    }

    async fn observe_system_manager<T, F>(
        &self,
        observer: F,
        schema: SchemaRef,
    ) -> Result<RecordBatch>
    where
        T: Serialize + for<'de> Deserialize<'de>,
        F: FnOnce(oneshot::Sender<Result<Vec<T>>>) -> SessionManagerObserver,
    {
        let (tx, rx) = oneshot::channel();
        let observer = observer(tx);
        self.system_manager.observe(observer).await;
        let items = rx
            .await
            .map_err(|_| internal_datafusion_err!("failed to observe system manager"))??;
        ArrowSerializer::build_record_batch_with_schema(&items, schema)
    }
}

impl SessionExtension for SystemTableService {
    fn name() -> &'static str {
        "SystemTableService"
    }
}
