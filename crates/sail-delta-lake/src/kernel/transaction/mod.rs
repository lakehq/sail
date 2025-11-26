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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/1f0b4d0965a85400c1effc6e9b4c7ebbb6795978/crates/core/src/kernel/transaction/mod.rs>

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use datafusion::catalog::Session;
use delta_kernel::table_features::TableFeature;
use delta_kernel::table_properties::TableProperties;
use futures::future::BoxFuture;
use log::*;
use object_store::path::Path;
use object_store::Error as ObjectStoreError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::kernel::checkpoints::cleanup_expired_logs_for;
use crate::kernel::models::{Action, Metadata, Protocol, Transaction};
use crate::kernel::snapshot::EagerSnapshot;
use crate::kernel::transaction::conflict_checker::{TransactionInfo, WinningCommitSummary};
use crate::kernel::{DeltaOperation, DeltaResult, TablePropertiesExt};
use crate::storage::{CommitOrBytes, LogStoreRef, ObjectStoreRef};
use crate::table::DeltaTableState;

mod conflict_checker;
mod protocol;

use conflict_checker::ConflictChecker;
pub use protocol::INSTANCE as PROTOCOL;

const DELTA_LOG_FOLDER: &str = "_delta_log";
pub(crate) const DEFAULT_RETRIES: usize = 15;

#[derive(Default, Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitMetrics {
    pub num_retries: u64,
}

#[derive(Default, Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostCommitMetrics {
    pub new_checkpoint_created: bool,
    pub num_log_files_cleaned_up: u64,
}

#[derive(Default, Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metrics {
    pub num_retries: u64,
    pub new_checkpoint_created: bool,
    pub num_log_files_cleaned_up: u64,
}

#[derive(Error, Debug)]
pub enum TransactionError {
    #[error("Tried committing existing table version: {0}")]
    VersionAlreadyExists(i64),

    #[error("Error serializing commit log to json: {json_err}")]
    SerializeLogJson { json_err: serde_json::error::Error },

    #[error("Log storage error: {source}")]
    ObjectStore {
        #[from]
        source: ObjectStoreError,
    },

    #[error("Failed to commit transaction: {0}")]
    CommitConflict(#[from] conflict_checker::CommitConflictError),

    #[error("Failed to commit transaction: {0}")]
    MaxCommitAttempts(i32),

    #[error(
        "The transaction includes Remove action with data change but Delta table is append-only"
    )]
    DeltaTableAppendOnly,

    #[error("Unsupported table features required: {0:?}")]
    UnsupportedTableFeatures(Vec<TableFeature>),

    #[error("Table features must be specified, please specify: {0:?}")]
    TableFeaturesRequired(TableFeature),

    #[error("Transaction failed: {msg}")]
    LogStoreError {
        msg: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

#[derive(Debug)]
pub struct CommitData {
    pub actions: Vec<Action>,
    pub operation: DeltaOperation,
    _app_metadata: HashMap<String, Value>,
    _app_transactions: Vec<Transaction>,
}

impl CommitData {
    pub fn new(
        mut actions: Vec<Action>,
        operation: DeltaOperation,
        mut app_metadata: HashMap<String, Value>,
        app_transactions: Vec<Transaction>,
    ) -> Self {
        if !actions.iter().any(|a| matches!(a, Action::CommitInfo(..))) {
            let mut commit_info = operation.get_commit_info();
            commit_info.timestamp = Some(Utc::now().timestamp_millis());
            app_metadata.insert(
                "clientVersion".to_string(),
                Value::String(format!("sail-delta-lake.{}", env!("CARGO_PKG_VERSION"))),
            );
            app_metadata.extend(commit_info.info.clone());
            commit_info.info = app_metadata.clone();
            actions.push(Action::CommitInfo(commit_info));
        }

        for txn in &app_transactions {
            actions.push(Action::Txn(txn.clone()));
        }

        Self {
            actions,
            operation,
            _app_metadata: app_metadata,
            _app_transactions: app_transactions,
        }
    }

    pub fn get_bytes(&self) -> Result<Bytes, TransactionError> {
        let mut jsons = Vec::<String>::new();
        for action in &self.actions {
            let json = serde_json::to_string(action)
                .map_err(|e| TransactionError::SerializeLogJson { json_err: e })?;
            jsons.push(json);
        }
        Ok(Bytes::from(jsons.join("\n")))
    }
}

#[async_trait]
pub trait CustomExecuteHandler: Send + Sync {
    async fn before_post_commit_hook(
        &self,
        log_store: &LogStoreRef,
        file_operation: bool,
        operation_id: Uuid,
    ) -> DeltaResult<()>;

    async fn after_post_commit_hook(
        &self,
        log_store: &LogStoreRef,
        file_operation: bool,
        operation_id: Uuid,
    ) -> DeltaResult<()>;
}

/// Reference to some structure that contains mandatory attributes for performing a commit.
pub trait TableReference: Send + Sync {
    /// Well known table configuration
    fn config(&self) -> &TableProperties;

    /// Get the table protocol of the snapshot
    fn protocol(&self) -> &Protocol;

    /// Get the table metadata of the snapshot
    #[allow(dead_code)]
    fn metadata(&self) -> &Metadata;

    /// Try to cast this table reference to a `EagerSnapshot`
    fn eager_snapshot(&self) -> &EagerSnapshot;
}

impl TableReference for EagerSnapshot {
    fn protocol(&self) -> &Protocol {
        EagerSnapshot::protocol(self)
    }

    fn metadata(&self) -> &Metadata {
        EagerSnapshot::metadata(self)
    }

    fn config(&self) -> &TableProperties {
        self.table_properties()
    }

    fn eager_snapshot(&self) -> &EagerSnapshot {
        self
    }
}

impl TableReference for DeltaTableState {
    fn config(&self) -> &TableProperties {
        self.table_properties()
    }

    fn protocol(&self) -> &Protocol {
        EagerSnapshot::protocol(self)
    }

    fn metadata(&self) -> &Metadata {
        EagerSnapshot::metadata(self)
    }

    fn eager_snapshot(&self) -> &EagerSnapshot {
        self
    }
}

#[derive(Clone, Debug, Copy)]
/// Properties for post commit hook.
pub struct PostCommitHookProperties {
    create_checkpoint: bool,
    /// Override the EnableExpiredLogCleanUp setting, if None config setting is used
    cleanup_expired_logs: Option<bool>,
}

#[derive(Clone, Debug)]
/// End user facing interface to be used by operations on the table.
/// Enable controlling commit behaviour and modifying metadata that is written during a commit.
pub struct CommitProperties {
    pub(crate) app_metadata: HashMap<String, Value>,
    pub(crate) app_transaction: Vec<Transaction>,
    max_retries: usize,
    create_checkpoint: bool,
    cleanup_expired_logs: Option<bool>,
}

impl Default for CommitProperties {
    fn default() -> Self {
        Self {
            app_metadata: Default::default(),
            app_transaction: Vec::new(),
            max_retries: DEFAULT_RETRIES,
            create_checkpoint: true,
            cleanup_expired_logs: None,
        }
    }
}

// impl CommitProperties {
//     /// Specify metadata the be committed
//     pub fn with_metadata(
//         mut self,
//         metadata: impl IntoIterator<Item = (String, serde_json::Value)>,
//     ) -> Self {
//         self.app_metadata = HashMap::from_iter(metadata);
//         self
//     }

//     /// Specify maximum number of times to retry the transaction before failing to commit
//     pub fn with_max_retries(mut self, max_retries: usize) -> Self {
//         self.max_retries = max_retries;
//         self
//     }

//     /// Specify if it should create a checkpoint when the commit interval condition is met
//     pub fn with_create_checkpoint(mut self, create_checkpoint: bool) -> Self {
//         self.create_checkpoint = create_checkpoint;
//         self
//     }

//     /// Add an additional application transaction to the commit
//     pub fn with_application_transaction(mut self, txn: Transaction) -> Self {
//         self.app_transaction.push(txn);
//         self
//     }

//     /// Override application transactions for the commit
//     pub fn with_application_transactions(mut self, txn: Vec<Transaction>) -> Self {
//         self.app_transaction = txn;
//         self
//     }

//     /// Specify if it should clean up the logs when the logRetentionDuration interval is met
//     pub fn with_cleanup_expired_logs(mut self, cleanup_expired_logs: Option<bool>) -> Self {
//         self.cleanup_expired_logs = cleanup_expired_logs;
//         self
//     }
// }

impl From<CommitProperties> for CommitBuilder {
    fn from(value: CommitProperties) -> Self {
        CommitBuilder {
            max_retries: value.max_retries,
            app_metadata: value.app_metadata,
            post_commit_hook: Some(PostCommitHookProperties {
                create_checkpoint: value.create_checkpoint,
                cleanup_expired_logs: value.cleanup_expired_logs,
            }),
            app_transaction: value.app_transaction,
            ..Default::default()
        }
    }
}

/// Prepare data to be committed to the Delta log and control how the commit is performed
pub struct CommitBuilder {
    actions: Vec<Action>,
    app_metadata: HashMap<String, Value>,
    app_transaction: Vec<Transaction>,
    max_retries: usize,
    post_commit_hook: Option<PostCommitHookProperties>,
    post_commit_hook_handler: Option<Arc<dyn CustomExecuteHandler>>,
    operation_id: Uuid,
}

impl Default for CommitBuilder {
    fn default() -> Self {
        CommitBuilder {
            actions: Vec::new(),
            app_metadata: HashMap::new(),
            app_transaction: Vec::new(),
            max_retries: DEFAULT_RETRIES,
            post_commit_hook: None,
            post_commit_hook_handler: None,
            operation_id: Uuid::new_v4(),
        }
    }
}

impl<'a> CommitBuilder {
    /// Actions to be included in the commit
    pub fn with_actions(mut self, actions: Vec<Action>) -> Self {
        self.actions = actions;
        self
    }

    // /// Metadata for the operation performed like metrics, user, and notebook
    // pub fn with_app_metadata(mut self, app_metadata: HashMap<String, Value>) -> Self {
    //     self.app_metadata = app_metadata;
    //     self
    // }

    // /// Maximum number of times to retry the transaction before failing to commit
    // pub fn with_max_retries(mut self, max_retries: usize) -> Self {
    //     self.max_retries = max_retries;
    //     self
    // }

    // /// Specify all the post commit hook properties
    // pub fn with_post_commit_hook(mut self, post_commit_hook: PostCommitHookProperties) -> Self {
    //     self.post_commit_hook = Some(post_commit_hook);
    //     self
    // }

    // /// Propagate operation id to log store
    // pub fn with_operation_id(mut self, operation_id: Uuid) -> Self {
    //     self.operation_id = operation_id;
    //     self
    // }

    // /// Set a custom execute handler, for pre and post execution
    // pub fn with_post_commit_hook_handler(
    //     mut self,
    //     handler: Option<Arc<dyn CustomExecuteHandler>>,
    // ) -> Self {
    //     self.post_commit_hook_handler = handler;
    //     self
    // }

    /// Prepare a Commit operation using the configured builder
    pub fn build(
        self,
        table_data: Option<&'a dyn TableReference>,
        log_store: LogStoreRef,
        operation: DeltaOperation,
        session: &'a dyn Session,
    ) -> PreCommit<'a> {
        let data = CommitData::new(
            self.actions,
            operation,
            self.app_metadata,
            self.app_transaction,
        );
        PreCommit {
            log_store,
            table_data,
            max_retries: self.max_retries,
            data,
            post_commit_hook: self.post_commit_hook,
            post_commit_hook_handler: self.post_commit_hook_handler,
            operation_id: self.operation_id,
            session,
        }
    }
}

/// Represents a commit that has not yet started but all details are finalized
pub struct PreCommit<'a> {
    log_store: LogStoreRef,
    table_data: Option<&'a dyn TableReference>,
    data: CommitData,
    max_retries: usize,
    post_commit_hook: Option<PostCommitHookProperties>,
    post_commit_hook_handler: Option<Arc<dyn CustomExecuteHandler>>,
    operation_id: Uuid,
    session: &'a dyn Session,
}

impl<'a> std::future::IntoFuture for PreCommit<'a> {
    type Output = DeltaResult<FinalizedCommit>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.into_prepared_commit_future().await?.await?.await })
    }
}

impl<'a> PreCommit<'a> {
    /// Prepare the commit but do not finalize it
    pub fn into_prepared_commit_future(self) -> BoxFuture<'a, DeltaResult<PreparedCommit<'a>>> {
        let this = self;

        // Write delta log entry as temporary file to storage. For the actual commit,
        // the temporary file is moved (atomic rename) to the delta log folder within `commit` function.
        async fn write_tmp_commit(
            log_entry: Bytes,
            store: ObjectStoreRef,
        ) -> DeltaResult<CommitOrBytes> {
            let token = uuid::Uuid::new_v4().to_string();
            let path = Path::from_iter([DELTA_LOG_FOLDER, &format!("_commit_{token}.json.tmp")]);
            store.put(&path, log_entry.into()).await?;
            Ok(CommitOrBytes::TmpCommit(path))
        }

        Box::pin(async move {
            let local_actions: Vec<_> = this.data.actions.to_vec();
            if let Some(table_reference) = this.table_data {
                PROTOCOL.can_commit(table_reference, &local_actions, &this.data.operation)?;
            }
            let log_entry = this.data.get_bytes()?;

            // With the DefaultLogStore & LakeFSLogstore, we just pass the bytes around, since we use conditionalPuts
            // Other stores will use tmp_commits
            let commit_or_bytes = if ["LakeFSLogStore", "DefaultLogStore"]
                .contains(&this.log_store.name().as_str())
            {
                CommitOrBytes::LogBytes(log_entry)
            } else {
                write_tmp_commit(
                    log_entry,
                    this.log_store.object_store(Some(this.operation_id)),
                )
                .await?
            };

            Ok(PreparedCommit {
                commit_or_bytes,
                log_store: this.log_store,
                table_data: this.table_data,
                max_retries: this.max_retries,
                data: this.data,
                post_commit: this.post_commit_hook,
                post_commit_hook_handler: this.post_commit_hook_handler,
                operation_id: this.operation_id,
                session: this.session,
            })
        })
    }
}

/// Represents a inflight commit
pub struct PreparedCommit<'a> {
    commit_or_bytes: CommitOrBytes,
    log_store: LogStoreRef,
    data: CommitData,
    table_data: Option<&'a dyn TableReference>,
    max_retries: usize,
    post_commit: Option<PostCommitHookProperties>,
    post_commit_hook_handler: Option<Arc<dyn CustomExecuteHandler>>,
    operation_id: Uuid,
    session: &'a dyn Session,
}

// impl PreparedCommit<'_> {
//     /// The temporary commit file created
//     pub fn commit_or_bytes(&self) -> &CommitOrBytes {
//         &self.commit_or_bytes
//     }
// }

impl<'a> std::future::IntoFuture for PreparedCommit<'a> {
    type Output = DeltaResult<PostCommit>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let commit_or_bytes = this.commit_or_bytes;
            let local_actions: Vec<_> = this.data.actions.to_vec();

            if this.table_data.is_none() {
                this.log_store
                    .write_commit_entry(0, commit_or_bytes.clone(), this.operation_id)
                    .await?;
                return Ok(PostCommit {
                    version: 0,
                    data: this.data,
                    create_checkpoint: false,
                    cleanup_expired_logs: None,
                    log_store: this.log_store,
                    table_data: None,
                    custom_execute_handler: this.post_commit_hook_handler,
                    metrics: CommitMetrics { num_retries: 0 },
                });
            }

            // unwrap() is safe here due to the above check
            #[allow(clippy::unwrap_used)]
            let mut read_snapshot = this.table_data.unwrap().eager_snapshot().clone();

            let mut attempt_number = 1;
            let total_retries = this.max_retries + 1;
            while attempt_number <= total_retries {
                let latest_version = this
                    .log_store
                    .get_latest_version(read_snapshot.version())
                    .await?;

                if latest_version > read_snapshot.version() {
                    // If max_retries are set to 0, do not try to use the conflict checker to resolve the conflict
                    // and throw immediately
                    if this.max_retries == 0 {
                        return Err(
                            TransactionError::MaxCommitAttempts(this.max_retries as i32).into()
                        );
                    }
                    warn!(
                        "Attempting to write a transaction {} but the underlying table has been updated to {latest_version} (log_store={})",
                        read_snapshot.version() + 1,
                        this.log_store.name()
                    );
                    let mut steps = latest_version - read_snapshot.version();

                    // Need to check for conflicts with each version between the read_snapshot and
                    // the latest!
                    while steps != 0 {
                        let summary = WinningCommitSummary::try_new(
                            this.log_store.as_ref(),
                            latest_version - steps,
                            (latest_version - steps) + 1,
                        )
                        .await?;
                        let transaction_info = TransactionInfo::try_new(
                            read_snapshot.log_data(),
                            this.data.operation.read_predicate(),
                            &local_actions,
                            this.data.operation.read_whole_table(),
                            Some(this.session),
                        )?;
                        let conflict_checker = ConflictChecker::new(
                            transaction_info,
                            summary,
                            Some(&this.data.operation),
                        );

                        match conflict_checker.check_conflicts() {
                            Ok(_) => {}
                            Err(err) => {
                                return Err(TransactionError::CommitConflict(err).into());
                            }
                        }
                        steps -= 1;
                    }
                    // Update snapshot to latest version after successful conflict check
                    read_snapshot
                        .update(this.log_store.as_ref(), Some(latest_version as u64))
                        .await?;
                }
                let version: i64 = latest_version + 1;

                match this
                    .log_store
                    .write_commit_entry(version, commit_or_bytes.clone(), this.operation_id)
                    .await
                {
                    Ok(()) => {
                        return Ok(PostCommit {
                            version,
                            data: this.data,
                            create_checkpoint: this
                                .post_commit
                                .map(|v| v.create_checkpoint)
                                .unwrap_or_default(),
                            cleanup_expired_logs: this
                                .post_commit
                                .map(|v| v.cleanup_expired_logs)
                                .unwrap_or_default(),
                            log_store: this.log_store,
                            table_data: Some(Box::new(read_snapshot)),
                            custom_execute_handler: this.post_commit_hook_handler,
                            metrics: CommitMetrics {
                                num_retries: attempt_number as u64 - 1,
                            },
                        });
                    }
                    Err(TransactionError::VersionAlreadyExists(version)) => {
                        error!("The transaction {version} already exists, will retry!");
                        // If the version already exists, loop through again and re-check
                        // conflicts
                        attempt_number += 1;
                    }
                    Err(err) => {
                        this.log_store
                            .abort_commit_entry(version, commit_or_bytes, this.operation_id)
                            .await?;
                        return Err(err.into());
                    }
                }
            }

            Err(TransactionError::MaxCommitAttempts(this.max_retries as i32).into())
        })
    }
}

/// Represents items for the post commit hook
pub struct PostCommit {
    /// The winning version number of the commit
    pub version: i64,
    /// The data that was committed to the log store
    #[allow(unused)]
    pub data: CommitData,
    create_checkpoint: bool,
    cleanup_expired_logs: Option<bool>,
    log_store: LogStoreRef,
    table_data: Option<Box<dyn TableReference>>,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
    metrics: CommitMetrics,
}

impl PostCommit {
    /// Runs the post commit activities
    async fn run_post_commit_hook(&self) -> DeltaResult<(DeltaTableState, PostCommitMetrics)> {
        if let Some(table) = &self.table_data {
            let post_commit_operation_id = Uuid::new_v4();
            let mut snapshot = table.eager_snapshot().clone();
            if self.version != snapshot.version() {
                snapshot
                    .update(self.log_store.as_ref(), Some(self.version as u64))
                    .await?;
            }

            let mut state = DeltaTableState { snapshot };

            let cleanup_logs = if let Some(cleanup_logs) = self.cleanup_expired_logs {
                cleanup_logs
            } else {
                state.table_properties().enable_expired_log_cleanup()
            };

            // Run arbitrary before_post_commit_hook code
            if let Some(custom_execute_handler) = &self.custom_execute_handler {
                custom_execute_handler
                    .before_post_commit_hook(
                        &self.log_store,
                        cleanup_logs || self.create_checkpoint,
                        post_commit_operation_id,
                    )
                    .await?
            }

            let mut new_checkpoint_created = false;
            if self.create_checkpoint {
                // Execute create checkpoint hook
                new_checkpoint_created = self
                    .create_checkpoint(
                        &state,
                        &self.log_store,
                        self.version,
                        post_commit_operation_id,
                    )
                    .await?;
            }

            let mut num_log_files_cleaned_up: u64 = 0;
            if cleanup_logs {
                // Execute clean up logs hook
                num_log_files_cleaned_up = cleanup_expired_logs_for(
                    self.version,
                    self.log_store.as_ref(),
                    Utc::now().timestamp_millis()
                        - state
                            .table_properties()
                            .log_retention_duration()
                            .as_millis() as i64,
                    Some(post_commit_operation_id),
                )
                .await? as u64;
                if num_log_files_cleaned_up > 0 {
                    state = DeltaTableState::try_new(
                        self.log_store.as_ref(),
                        state.load_config().clone(),
                        Some(self.version),
                    )
                    .await?;
                }
            }

            // Run arbitrary after_post_commit_hook code
            if let Some(custom_execute_handler) = &self.custom_execute_handler {
                custom_execute_handler
                    .after_post_commit_hook(
                        &self.log_store,
                        cleanup_logs || self.create_checkpoint,
                        post_commit_operation_id,
                    )
                    .await?
            }
            Ok((
                state,
                PostCommitMetrics {
                    new_checkpoint_created,
                    num_log_files_cleaned_up,
                },
            ))
        } else {
            let state = DeltaTableState::try_new(
                self.log_store.as_ref(),
                Default::default(),
                Some(self.version),
            )
            .await?;
            Ok((
                state,
                PostCommitMetrics {
                    new_checkpoint_created: false,
                    num_log_files_cleaned_up: 0,
                },
            ))
        }
    }
    async fn create_checkpoint(
        &self,
        table_state: &DeltaTableState,
        _log_store: &LogStoreRef,
        version: i64,
        _operation_id: Uuid,
    ) -> DeltaResult<bool> {
        if !table_state.load_config().require_files {
            warn!("Checkpoint creation in post_commit_hook has been skipped due to table being initialized without files.");
            return Ok(false);
        }

        let checkpoint_interval = table_state.config().checkpoint_interval().get() as i64;
        if ((version + 1) % checkpoint_interval) == 0 {
            // create_checkpoint_for(version as u64, log_store.as_ref(), Some(operation_id)).await?;
            // FIXME: check point
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

/// A commit that successfully completed
#[allow(unused)]
pub struct FinalizedCommit {
    /// The new table state after a commit
    pub snapshot: DeltaTableState,

    /// Version of the finalized commit
    pub version: i64,

    /// Metrics associated with the commit operation
    pub metrics: Metrics,
}
#[allow(unused)]
impl FinalizedCommit {
    /// The new table state after a commit
    pub fn snapshot(&self) -> DeltaTableState {
        self.snapshot.clone()
    }
    /// Version of the finalized commit
    pub fn version(&self) -> i64 {
        self.version
    }
}

impl std::future::IntoFuture for PostCommit {
    type Output = DeltaResult<FinalizedCommit>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            match this.run_post_commit_hook().await {
                Ok((snapshot, post_commit_metrics)) => Ok(FinalizedCommit {
                    snapshot,
                    version: this.version,
                    metrics: Metrics {
                        num_retries: this.metrics.num_retries,
                        new_checkpoint_created: post_commit_metrics.new_checkpoint_created,
                        num_log_files_cleaned_up: post_commit_metrics.num_log_files_cleaned_up,
                    },
                }),
                Err(err) => Err(err),
            }
        })
    }
}
