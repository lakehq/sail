// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Local log store abstraction backed by `object_store`.

// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/logstore/mod.rs>
// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/logstore/default_logstore.rs>
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::execution::context::TaskContext;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use log::{debug, error};
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use serde_json::Deserializer as JsonDeserializer;
use url::Url;
use uuid::Uuid;

use crate::delta_log::latest_version_from_listing;
use crate::spec::{
    commit_path, Action, DeltaError as DeltaTableError, DeltaError, DeltaResult, TransactionError,
};

mod config;

pub use config::StorageConfig;

pub type ObjectStoreRef = Arc<dyn ObjectStore>;
pub type LogStoreRef = Arc<dyn LogStore>;

/// Retrieve an object store for the provided table URL from the given TaskContext.
pub fn get_object_store_from_context(
    context: &Arc<TaskContext>,
    table_url: &Url,
) -> DataFusionResult<ObjectStoreRef> {
    context
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

/// Holder for temporary commit paths or prepared bytes.
#[derive(Clone)]
pub enum CommitOrBytes {
    TmpCommit(Path),
    LogBytes(Bytes),
}

/// Configuration parameters for a log store.
#[derive(Debug, Clone)]
pub struct LogStoreConfig {
    /// URL corresponding to the storage location.
    pub location: Url,
    /// Options used for configuring backend storage.
    pub options: StorageConfig,
}

impl LogStoreConfig {
    pub fn decorate_store(
        &self,
        store: Arc<dyn ObjectStore>,
        table_root: Option<&Url>,
    ) -> DeltaResult<Arc<dyn ObjectStore>> {
        let table_url = table_root.unwrap_or(&self.location);
        self.options.decorate_store(store, table_url)
    }
}

/// Return the default log store implementation for the provided configuration.
pub fn default_logstore(
    prefixed_store: ObjectStoreRef,
    root_store: ObjectStoreRef,
    location: &Url,
    options: &StorageConfig,
) -> LogStoreRef {
    Arc::new(DefaultLogStore::new(
        prefixed_store,
        root_store,
        LogStoreConfig {
            location: location.clone(),
            options: options.clone(),
        },
    ))
}

/// Reads a commit and gets list of actions.
pub fn get_actions(version: i64, commit_log_bytes: &Bytes) -> Result<Vec<Action>, DeltaTableError> {
    debug!("parsing commit with version {version}...");
    JsonDeserializer::from_slice(commit_log_bytes)
        .into_iter::<Action>()
        .map(|result| {
            result.map_err(|e| {
                let line = format!("Error at line {}, column {}", e.line(), e.column());
                DeltaTableError::generic(format!(
                    "Invalid JSON in log record, version={version}, {line}"
                ))
            })
        })
        .collect()
}

#[async_trait]
pub trait LogStore: Send + Sync {
    /// Return the name of this LogStore implementation.
    fn name(&self) -> String;

    /// Trigger sync operation on log store.
    async fn refresh(&self) -> DeltaResult<()> {
        Ok(())
    }

    /// Read data for commit entry with the given version.
    async fn read_commit_entry(&self, version: i64) -> DeltaResult<Option<Bytes>>;

    /// Write list of actions as delta commit entry for given version.
    async fn write_commit_entry(
        &self,
        version: i64,
        commit_or_bytes: CommitOrBytes,
        operation_id: Uuid,
    ) -> Result<(), TransactionError>;

    /// Abort the commit entry for the given version.
    async fn abort_commit_entry(
        &self,
        version: i64,
        commit_or_bytes: CommitOrBytes,
        operation_id: Uuid,
    ) -> Result<(), TransactionError>;

    /// Find latest version currently stored in the delta log.
    async fn get_latest_version(&self, start_version: i64) -> DeltaResult<i64>;

    /// Get object store, can pass operation_id for stores linked to an operation.
    fn object_store(&self, operation_id: Option<Uuid>) -> Arc<dyn ObjectStore>;

    /// Get the root object store (without table prefix).
    fn root_object_store(&self, operation_id: Option<Uuid>) -> Arc<dyn ObjectStore>;

    /// Get configuration representing configured log store.
    fn config(&self) -> &LogStoreConfig;

    /// Get fully qualified uri for table root.
    fn root_uri(&self) -> String {
        to_uri(&self.config().location, &Path::from(""))
    }
}

#[derive(Debug, Clone)]
struct DefaultLogStore {
    prefixed_store: ObjectStoreRef,
    root_store: ObjectStoreRef,
    config: LogStoreConfig,
}

impl DefaultLogStore {
    fn new(
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        config: LogStoreConfig,
    ) -> Self {
        Self {
            prefixed_store,
            root_store,
            config,
        }
    }
}

#[async_trait]
impl LogStore for DefaultLogStore {
    fn name(&self) -> String {
        "DefaultLogStore".into()
    }

    async fn read_commit_entry(&self, version: i64) -> DeltaResult<Option<Bytes>> {
        read_commit_entry(self.object_store(None).as_ref(), version).await
    }

    async fn write_commit_entry(
        &self,
        version: i64,
        commit_or_bytes: CommitOrBytes,
        _: Uuid,
    ) -> Result<(), TransactionError> {
        match commit_or_bytes {
            CommitOrBytes::LogBytes(log_bytes) => self
                .object_store(None)
                .put_opts(
                    &commit_path(version),
                    log_bytes.into(),
                    put_options().clone(),
                )
                .await
                .map(|_| ())
                .map_err(|err| match err {
                    ObjectStoreError::AlreadyExists { .. } => {
                        TransactionError::VersionAlreadyExists(version)
                    }
                    _ => TransactionError::from(err),
                }),
            CommitOrBytes::TmpCommit(_) => {
                unreachable!("DefaultLogStore should not receive temporary commits")
            }
        }
    }

    async fn abort_commit_entry(
        &self,
        _: i64,
        commit_or_bytes: CommitOrBytes,
        _: Uuid,
    ) -> Result<(), TransactionError> {
        match commit_or_bytes {
            CommitOrBytes::LogBytes(_) => Ok(()),
            CommitOrBytes::TmpCommit(_) => {
                unreachable!("DefaultLogStore should not receive temporary commits")
            }
        }
    }

    async fn get_latest_version(&self, start_version: i64) -> DeltaResult<i64> {
        let start = start_version.max(0);
        let latest = latest_version_from_listing(self.object_store(None)).await?;
        match latest {
            Some(version) if version >= start => Ok(version),
            Some(_) | None => Err(DeltaError::MissingVersion),
        }
    }

    fn object_store(&self, _: Option<Uuid>) -> Arc<dyn ObjectStore> {
        self.prefixed_store.clone()
    }

    fn root_object_store(&self, _: Option<Uuid>) -> Arc<dyn ObjectStore> {
        self.root_store.clone()
    }

    fn config(&self) -> &LogStoreConfig {
        &self.config
    }
}

fn put_options() -> &'static PutOptions {
    static PUT_OPTS: LazyLock<PutOptions> = LazyLock::new(|| PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    });
    &PUT_OPTS
}

async fn read_commit_entry(storage: &dyn ObjectStore, version: i64) -> DeltaResult<Option<Bytes>> {
    let commit_uri = commit_path(version);
    match storage.get(&commit_uri).await {
        Ok(res) => {
            let bytes = res.bytes().await?;
            debug!(
                "commit entry read successfully (size={} bytes)",
                bytes.len()
            );
            Ok(Some(bytes))
        }
        Err(ObjectStoreError::NotFound { .. }) => {
            debug!("commit entry not found");
            Ok(None)
        }
        Err(err) => {
            error!(
                target: "sail-delta-lake",
                "failed to read commit entry (version={version}): {err}"
            );
            Err(err.into())
        }
    }
}

fn to_uri(root: &Url, location: &Path) -> String {
    if location.as_ref().is_empty() || location.as_ref() == "/" {
        root.as_ref().to_string()
    } else if root.as_ref().ends_with('/') {
        format!("{}{}", root.as_ref(), location.as_ref())
    } else {
        format!("{}/{}", root.as_ref(), location.as_ref())
    }
}
