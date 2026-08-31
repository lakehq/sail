//! Execution adapters for system store backends.

use std::future::Future;

mod candidate;
mod mutation;
mod query;

pub use mutation::MetricSample;
pub(crate) use mutation::{write_event, write_metrics};
pub(crate) use query::SystemStoreQuery;

use crate::access::{Commit, DirectStoreBackend, TransactionalStoreBackend};
use crate::{SystemEvent, SystemStoreError, SystemStoreResult};

/// Actor-facing store execution adapter.
pub(crate) trait StoreEngine: Send + 'static {
    fn write_event(
        &mut self,
        event: SystemEvent,
    ) -> impl Future<Output = SystemStoreResult<()>> + Send;

    fn write_metrics(
        &mut self,
        samples: Vec<MetricSample>,
    ) -> impl Future<Output = SystemStoreResult<()>> + Send;

    fn read(
        &mut self,
        query: SystemStoreQuery,
    ) -> impl Future<Output = SystemStoreResult<Option<Box<dyn FnOnce() + Send>>>> + Send;

    fn flush(&mut self) -> impl Future<Output = SystemStoreResult<()>> + Send;
}

pub(crate) struct DirectStoreEngine<B> {
    pub(crate) backend: B,
}

impl<B> StoreEngine for DirectStoreEngine<B>
where
    B: DirectStoreBackend,
{
    async fn write_event(&mut self, event: SystemEvent) -> SystemStoreResult<()> {
        write_event(&mut self.backend.write(), event).map_err(SystemStoreError::from)
    }

    async fn write_metrics(&mut self, samples: Vec<MetricSample>) -> SystemStoreResult<()> {
        write_metrics(&mut self.backend.write(), samples).map_err(SystemStoreError::from)
    }

    async fn read(
        &mut self,
        query: SystemStoreQuery,
    ) -> SystemStoreResult<Option<Box<dyn FnOnce() + Send>>> {
        // The direct storage engine executes the query immediately
        // and does not return a deferred read closure to be executed later.
        // This is because the direct storage engine does not support snapshots or transactions,
        // so the read operation must finish before subsequent writes.
        query.execute(&self.backend.read());
        Ok(None)
    }

    async fn flush(&mut self) -> SystemStoreResult<()> {
        Ok(())
    }
}

pub(crate) struct TransactionalStoreEngine<B> {
    pub(crate) backend: B,
}

impl<B> StoreEngine for TransactionalStoreEngine<B>
where
    B: TransactionalStoreBackend,
    SystemStoreError: From<B::Error>,
{
    async fn write_event(&mut self, event: SystemEvent) -> SystemStoreResult<()> {
        let store = self.backend.clone();
        tokio::task::spawn_blocking(move || -> SystemStoreResult<()> {
            let mut transaction = store.transaction().map_err(SystemStoreError::from)?;
            write_event(&mut transaction, event).map_err(SystemStoreError::from)?;
            transaction.commit().map_err(SystemStoreError::from)?;
            Ok(())
        })
        .await
        .map_err(|error| SystemStoreError::internal(format!("system store task failed: {error}")))?
    }

    async fn write_metrics(&mut self, samples: Vec<MetricSample>) -> SystemStoreResult<()> {
        let store = self.backend.clone();
        tokio::task::spawn_blocking(move || -> SystemStoreResult<()> {
            let mut transaction = store.transaction().map_err(SystemStoreError::from)?;
            write_metrics(&mut transaction, samples).map_err(SystemStoreError::from)?;
            transaction.commit().map_err(SystemStoreError::from)?;
            Ok(())
        })
        .await
        .map_err(|error| SystemStoreError::internal(format!("system store task failed: {error}")))?
    }

    async fn read(
        &mut self,
        query: SystemStoreQuery,
    ) -> SystemStoreResult<Option<Box<dyn FnOnce() + Send>>> {
        // The transactional storage engine acquires a snapshot of the store and returns
        // a closure that executes the query against that snapshot.
        // This allows multiple concurrent reads without blocking writes.
        let snapshot = tokio::task::spawn_blocking({
            let store = self.backend.clone();
            move || store.snapshot().map_err(SystemStoreError::from)
        })
        .await
        .map_err(|error| {
            SystemStoreError::internal(format!("system store task failed: {error}"))
        })??;
        Ok(Some(Box::new(move || query.execute(&snapshot))))
    }

    async fn flush(&mut self) -> SystemStoreResult<()> {
        let store = self.backend.clone();
        tokio::task::spawn_blocking(move || store.flush().map_err(SystemStoreError::from))
            .await
            .map_err(|error| {
                SystemStoreError::internal(format!("system store task failed: {error}"))
            })?
    }
}
