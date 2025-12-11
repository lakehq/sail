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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/1f0b4d0965a85400c1effc6e9b4c7ebbb6795978/crates/core/src/kernel/transaction/conflict_checker.rs>

//! Helper module to check if a transaction can be committed in case of conflicting commits.

use std::collections::HashSet;

use datafusion::catalog::Session;
use datafusion::logical_expr::Expr;
use delta_kernel::table_properties::IsolationLevel;
use delta_kernel::Error as KernelError;
use thiserror::Error;

use crate::datasource::parse_log_data_predicate;
use crate::kernel::models::{Action, Add, CommitInfo, Metadata, Protocol, Remove, Transaction};
use crate::kernel::snapshot::LogDataHandler;
use crate::kernel::{DeltaOperation, DeltaResult, TablePropertiesExt};
use crate::storage::{get_actions, LogStore};

/// Exceptions raised during commit conflict resolution.
#[derive(Error, Debug)]
pub enum CommitConflictError {
    #[error("Commit failed: a concurrent transactions added new data.\nHelp: This transaction's query must be rerun to include the new data. Also, if you don't care to require this check to pass in the future, the isolation level can be set to Snapshot Isolation.")]
    ConcurrentAppend,

    #[error("Commit failed: a concurrent transaction deleted data this operation read.\nHelp: This transaction's query must be rerun to exclude the removed data. Also, if you don't care to require this check to pass in the future, the isolation level can be set to Snapshot Isolation.")]
    ConcurrentDeleteRead,

    #[error("Commit failed: a concurrent transaction deleted the same data your transaction deletes.\nHelp: you should retry this write operation. If it was based on data contained in the table, you should rerun the query generating the data.")]
    ConcurrentDeleteDelete,

    #[error("Metadata changed since last commit.")]
    MetadataChanged,

    #[error("Concurrent transaction failed.")]
    ConcurrentTransaction,

    #[error("Protocol changed since last commit: {0}")]
    ProtocolChanged(String),

    #[error("Delta-rs does not support writer version {0}")]
    UnsupportedWriterVersion(i32),

    #[error("Delta-rs does not support reader version {0}")]
    UnsupportedReaderVersion(i32),

    #[error("Snapshot is corrupted: {source}")]
    CorruptedState {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[error("Error evaluating predicate: {source}")]
    Predicate {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[error("No metadata found, please make sure table is loaded.")]
    NoMetadata,
}

/// A struct representing different attributes of current transaction needed for conflict detection.
#[allow(unused)]
pub(crate) struct TransactionInfo<'a> {
    txn_id: String,
    /// partition predicates by which files have been queried by the transaction
    read_predicates: Option<Expr>,
    /// appIds that have been seen by the transaction
    read_app_ids: HashSet<String>,
    /// delta log actions that the transaction wants to commit
    actions: &'a [Action],
    /// read [`DeltaTableState`] used for the transaction
    read_snapshot: LogDataHandler<'a>,
    /// Whether the transaction tainted the whole table
    read_whole_table: bool,
}

impl<'a> TransactionInfo<'a> {
    pub fn try_new(
        read_snapshot: LogDataHandler<'a>,
        read_predicates: Option<String>,
        actions: &'a [Action],
        read_whole_table: bool,
        session: Option<&dyn Session>,
    ) -> DeltaResult<Self> {
        let read_predicates = match (read_predicates, session) {
            (Some(pred), Some(s)) => Some(parse_log_data_predicate(&read_snapshot, pred, s)?),
            _ => None,
        };

        let mut read_app_ids = HashSet::<String>::new();
        for action in actions.iter() {
            if let Action::Txn(Transaction { app_id, .. }) = action {
                read_app_ids.insert(app_id.clone());
            }
        }

        Ok(Self::new(
            read_snapshot,
            read_predicates,
            actions,
            read_whole_table,
        ))
    }

    pub fn new(
        read_snapshot: LogDataHandler<'a>,
        read_predicates: Option<Expr>,
        actions: &'a [Action],
        read_whole_table: bool,
    ) -> Self {
        let mut read_app_ids = HashSet::<String>::new();
        for action in actions.iter() {
            if let Action::Txn(Transaction { app_id, .. }) = action {
                read_app_ids.insert(app_id.clone());
            }
        }
        Self {
            txn_id: "".into(),
            read_predicates,
            read_app_ids,
            actions,
            read_snapshot,
            read_whole_table,
        }
    }

    /// Whether the transaction changed the tables metadatas
    pub fn metadata_changed(&self) -> bool {
        self.actions
            .iter()
            .any(|a| matches!(a, Action::Metadata(_)))
    }

    // TODO: properly handle predicates in the PhysicalPlan
    // pub fn read_files(&self) -> Result<impl Iterator<Item = Add> + '_, CommitConflictError> {
    //     use crate::datasource::files_matching_predicate;

    //     if let Some(predicate) = &self.read_predicates {
    //         Ok(Either::Left(
    //             files_matching_predicate(self.read_snapshot.clone(), &[predicate.clone()])
    //                 .map_err(|err| CommitConflictError::Predicate {
    //                     source: Box::new(err),
    //                 })?,
    //         ))
    //     } else {
    //         Ok(Either::Right(
    //             self.read_snapshot.iter().map(|f| f.add_action()),
    //         ))
    //     }
    // }

    /// Files read by the transaction
    pub fn read_files(&self) -> Result<impl Iterator<Item = Add> + '_, CommitConflictError> {
        Ok(self.read_snapshot.iter().map(|f| f.add_action()))
    }

    /// Whether the whole table was read during the transaction
    pub fn read_whole_table(&self) -> bool {
        self.read_whole_table
    }

    pub fn protocol_action(&self) -> Option<&Protocol> {
        self.actions.iter().find_map(|a| match a {
            Action::Protocol(p) => Some(p),
            _ => None,
        })
    }

    pub fn metadata_action(&self) -> Option<&Metadata> {
        self.actions.iter().find_map(|a| match a {
            Action::Metadata(m) => Some(m),
            _ => None,
        })
    }
}

/// Summary of the Winning commit against which we want to check the conflict
#[derive(Debug)]
pub(crate) struct WinningCommitSummary {
    pub actions: Vec<Action>,
    pub commit_info: Option<CommitInfo>,
}

impl WinningCommitSummary {
    pub async fn try_new(
        log_store: &dyn LogStore,
        read_version: i64,
        winning_commit_version: i64,
    ) -> DeltaResult<Self> {
        // NOTE using assert, since a wrong version would right now mean a bug in our code.
        assert_eq!(winning_commit_version, read_version + 1);

        let commit_log_bytes = log_store.read_commit_entry(winning_commit_version).await?;
        match commit_log_bytes {
            Some(bytes) => {
                let actions = get_actions(winning_commit_version, &bytes)?
                    .into_iter()
                    .collect::<Vec<_>>();
                let commit_info = actions
                    .iter()
                    .find(|action| matches!(action, Action::CommitInfo(_)))
                    .map(|action| match action {
                        Action::CommitInfo(info) => info.clone(),
                        _ => unreachable!(),
                    });

                Ok(Self {
                    actions,
                    commit_info,
                })
            }
            None => Err(KernelError::MissingVersion.into()),
        }
    }

    pub fn metadata_updates(&self) -> Vec<Metadata> {
        self.actions
            .iter()
            .cloned()
            .filter_map(|action| match action {
                Action::Metadata(metadata) => Some(metadata),
                _ => None,
            })
            .collect()
    }

    pub fn app_level_transactions(&self) -> HashSet<String> {
        self.actions
            .iter()
            .cloned()
            .filter_map(|action| match action {
                Action::Txn(txn) => Some(txn.app_id),
                _ => None,
            })
            .collect()
    }

    pub fn protocol(&self) -> Vec<Protocol> {
        self.actions
            .iter()
            .cloned()
            .filter_map(|action| match action {
                Action::Protocol(protocol) => Some(protocol),
                _ => None,
            })
            .collect()
    }

    pub fn removed_files(&self) -> Vec<Remove> {
        self.actions
            .iter()
            .cloned()
            .filter_map(|action| match action {
                Action::Remove(remove) => Some(remove),
                _ => None,
            })
            .collect()
    }

    pub fn added_files(&self) -> Vec<Add> {
        self.actions
            .iter()
            .cloned()
            .filter_map(|action| match action {
                Action::Add(add) => Some(add),
                _ => None,
            })
            .collect()
    }

    pub fn blind_append_added_files(&self) -> Vec<Add> {
        if self.is_blind_append().unwrap_or(false) {
            self.added_files()
        } else {
            vec![]
        }
    }

    pub fn changed_data_added_files(&self) -> Vec<Add> {
        if self.is_blind_append().unwrap_or(false) {
            vec![]
        } else {
            self.added_files()
        }
    }

    pub fn is_blind_append(&self) -> Option<bool> {
        self.commit_info
            .as_ref()
            .map(|opt| opt.is_blind_append.unwrap_or(false))
    }
}

/// Checks if a failed commit may be committed after a conflicting winning commit
pub(crate) struct ConflictChecker<'a> {
    /// transaction information for current transaction at start of check
    txn_info: TransactionInfo<'a>,
    /// Summary of the transaction, that has been committed ahead of the current transaction
    winning_commit_summary: WinningCommitSummary,
    /// Isolation level for the current transaction
    isolation_level: IsolationLevel,
}

impl<'a> ConflictChecker<'a> {
    pub fn new(
        transaction_info: TransactionInfo<'a>,
        winning_commit_summary: WinningCommitSummary,
        operation: Option<&DeltaOperation>,
    ) -> ConflictChecker<'a> {
        let isolation_level = operation
            .and_then(|op| {
                if can_downgrade_to_snapshot_isolation(
                    &winning_commit_summary.actions,
                    op,
                    &transaction_info
                        .read_snapshot
                        .table_properties()
                        .isolation_level(),
                ) {
                    Some(IsolationLevel::SnapshotIsolation)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| {
                transaction_info
                    .read_snapshot
                    .table_properties()
                    .isolation_level()
            });

        Self {
            txn_info: transaction_info,
            winning_commit_summary,
            isolation_level,
        }
    }

    /// This function checks conflict of the `initial_current_transaction_info` against the
    /// `winning_commit_version` and returns an updated [`TransactionInfo`] that represents
    /// the transaction as if it had started while reading the `winning_commit_version`.
    pub fn check_conflicts(&self) -> Result<(), CommitConflictError> {
        self.check_protocol_compatibility()?;
        self.check_no_metadata_updates()?;
        self.check_for_added_files_that_should_have_been_read_by_current_txn()?;
        self.check_for_deleted_files_against_current_txn_read_files()?;
        self.check_for_deleted_files_against_current_txn_deleted_files()?;
        self.check_for_updated_application_transaction_ids_that_current_txn_depends_on()?;
        Ok(())
    }

    /// Asserts that the client is up to date with the protocol and is allowed
    /// to read and write against the protocol set by the committed transaction.
    fn check_protocol_compatibility(&self) -> Result<(), CommitConflictError> {
        for p in self.winning_commit_summary.protocol() {
            let (win_read, curr_read) = (
                p.min_reader_version(),
                self.txn_info.read_snapshot.protocol().min_reader_version(),
            );
            let (win_write, curr_write) = (
                p.min_writer_version(),
                self.txn_info.read_snapshot.protocol().min_writer_version(),
            );
            if curr_read < win_read || win_write < curr_write {
                return Err(CommitConflictError::ProtocolChanged(
                    format!("required read/write {win_read}/{win_write}, current read/write {curr_read}/{curr_write}"),
                ));
            };
        }
        if !self.winning_commit_summary.protocol().is_empty() {
            if let Some(txn_protocol) = self.txn_info.protocol_action() {
                let wins = self.winning_commit_summary.protocol();
                if wins.iter().any(|p| p != txn_protocol) {
                    return Err(CommitConflictError::ProtocolChanged(
                        "protocol changed".into(),
                    ));
                }
            } else {
                return Err(CommitConflictError::ProtocolChanged(
                    "protocol changed".into(),
                ));
            }
        };
        Ok(())
    }

    /// Check if the committed transaction has changed metadata.
    fn check_no_metadata_updates(&self) -> Result<(), CommitConflictError> {
        // Fail if the metadata is different than what the txn read.
        if !self.winning_commit_summary.metadata_updates().is_empty() {
            if let Some(txn_metadata) = self.txn_info.metadata_action() {
                let wins = self.winning_commit_summary.metadata_updates();
                if wins.iter().any(|m| m != txn_metadata) {
                    return Err(CommitConflictError::MetadataChanged);
                }
            } else {
                return Err(CommitConflictError::MetadataChanged);
            }
        }
        Ok(())
    }

    /// Check if the new files added by the already committed transactions
    /// should have been read by the current transaction.
    fn check_for_added_files_that_should_have_been_read_by_current_txn(
        &self,
    ) -> Result<(), CommitConflictError> {
        // Skip check, if the operation can be downgraded to snapshot isolation
        if matches!(self.isolation_level, IsolationLevel::SnapshotIsolation) {
            return Ok(());
        }

        // Fail if new files have been added that the txn should have read.
        let added_files_to_check = match self.isolation_level {
            IsolationLevel::WriteSerializable if !self.txn_info.metadata_changed() => {
                // don't conflict with blind appends
                self.winning_commit_summary.changed_data_added_files()
            }
            IsolationLevel::Serializable | IsolationLevel::WriteSerializable => {
                let mut files = self.winning_commit_summary.changed_data_added_files();
                files.extend(self.winning_commit_summary.blind_append_added_files());
                files
            }
            IsolationLevel::SnapshotIsolation => vec![],
        };

        // TODO: properly handle predicate
        // let added_files_matching_predicates = if let (Some(predicate), false) = (
        //     &self.txn_info.read_predicates,
        //     self.txn_info.read_whole_table(),
        // ) {
        //     let arrow_schema = self.txn_info.read_snapshot.arrow_schema().map_err(|err| {
        //         CommitConflictError::CorruptedState {
        //             source: Box::new(err),
        //         }
        //     })?;
        //     let partition_columns = &self.txn_info.read_snapshot.metadata().partition_columns();
        //     AddContainer::new(&added_files_to_check, partition_columns, arrow_schema)
        //         .predicate_matches(predicate.clone())
        //         .map_err(|err| CommitConflictError::Predicate {
        //             source: Box::new(err),
        //         })?
        //         .cloned()
        //         .collect::<Vec<_>>()
        // } else if self.txn_info.read_whole_table() {
        //     added_files_to_check
        // } else {
        //     vec![]
        // };

        let added_files_matching_predicates = if self.txn_info.read_whole_table() {
            added_files_to_check
        } else {
            vec![]
        };

        if !added_files_matching_predicates.is_empty() {
            Err(CommitConflictError::ConcurrentAppend)
        } else {
            Ok(())
        }
    }

    /// Check if [Remove] actions added by already committed transactions
    /// conflicts with files read by the current transaction.
    fn check_for_deleted_files_against_current_txn_read_files(
        &self,
    ) -> Result<(), CommitConflictError> {
        // Fail if files have been deleted that the txn read.
        let read_file_path: HashSet<String> = self
            .txn_info
            .read_files()?
            .map(|f| f.path.clone())
            .collect();
        let deleted_read_overlap = self
            .winning_commit_summary
            .removed_files()
            .iter()
            .find(|&f| read_file_path.contains(&f.path))
            .cloned();
        if deleted_read_overlap.is_some()
            || (!self.winning_commit_summary.removed_files().is_empty()
                && self.txn_info.read_whole_table())
        {
            Err(CommitConflictError::ConcurrentDeleteRead)
        } else {
            Ok(())
        }
    }

    /// Check if [Remove] actions added by already committed transactions conflicts
    /// with [Remove] actions this transaction is trying to add.
    fn check_for_deleted_files_against_current_txn_deleted_files(
        &self,
    ) -> Result<(), CommitConflictError> {
        // Fail if a file is deleted twice.
        let txn_deleted_files: HashSet<String> = self
            .txn_info
            .actions
            .iter()
            .cloned()
            .filter_map(|action| match action {
                Action::Remove(remove) => Some(remove.path),
                _ => None,
            })
            .collect();
        let winning_deleted_files: HashSet<String> = self
            .winning_commit_summary
            .removed_files()
            .iter()
            .cloned()
            .map(|r| r.path)
            .collect();
        let intersection: HashSet<&String> = txn_deleted_files
            .intersection(&winning_deleted_files)
            .collect();

        if !intersection.is_empty() {
            Err(CommitConflictError::ConcurrentDeleteDelete)
        } else {
            Ok(())
        }
    }

    /// Checks if the winning transaction corresponds to some AppId on which
    /// current transaction also depends.
    fn check_for_updated_application_transaction_ids_that_current_txn_depends_on(
        &self,
    ) -> Result<(), CommitConflictError> {
        // Fail if the appIds seen by the current transaction has been updated by the winning
        // transaction i.e. the winning transaction have [Txn] corresponding to
        // some appId on which current transaction depends on. Example - This can happen when
        // multiple instances of the same streaming query are running at the same time.
        let winning_txns = self.winning_commit_summary.app_level_transactions();
        let txn_overlap: HashSet<&String> = winning_txns
            .intersection(&self.txn_info.read_app_ids)
            .collect();
        if !txn_overlap.is_empty() {
            Err(CommitConflictError::ConcurrentTransaction)
        } else {
            Ok(())
        }
    }
}

// implementation and comments adopted from
// https://github.com/delta-io/delta/blob/1c18c1d972e37d314711b3a485e6fb7c98fce96d/core/src/main/scala/org/apache/spark/sql/delta/OptimisticTransaction.scala#L1268
//
// For no-data-change transactions such as OPTIMIZE/Auto Compaction/ZorderBY, we can
// change the isolation level to SnapshotIsolation. SnapshotIsolation allows reduced conflict
// detection by skipping the
// [ConflictChecker::check_for_added_files_that_should_have_been_read_by_current_txn] check i.e.
// don't worry about concurrent appends.
//
// We can also use SnapshotIsolation for empty transactions. e.g. consider a commit:
// t0 - Initial state of table
// t1 - Q1, Q2 starts
// t2 - Q1 commits
// t3 - Q2 is empty and wants to commit.
// In this scenario, we can always allow Q2 to commit without worrying about new files
// generated by Q1.
//
// The final order which satisfies both Serializability and WriteSerializability is: Q2, Q1
// Note that Metadata only update transactions shouldn't be considered empty. If Q2 above has
// a Metadata update (say schema change/identity column high watermark update), then Q2 can't
// be moved above Q1 in the final SERIALIZABLE order. This is because if Q2 is moved above Q1,
// then Q1 should see the updates from Q2 - which actually didn't happen.
pub(super) fn can_downgrade_to_snapshot_isolation<'a>(
    actions: impl IntoIterator<Item = &'a Action>,
    operation: &DeltaOperation,
    isolation_level: &IsolationLevel,
) -> bool {
    let mut data_changed = false;
    let mut has_non_file_actions = false;
    for action in actions {
        match action {
            Action::Add(act) if act.data_change => data_changed = true,
            Action::Remove(rem) if rem.data_change => data_changed = true,
            _ => has_non_file_actions = true,
        }
    }

    if has_non_file_actions {
        // if Non-file-actions are present (e.g. METADATA etc.), then don't downgrade the isolation level.
        return false;
    }

    match isolation_level {
        IsolationLevel::Serializable => !data_changed,
        IsolationLevel::WriteSerializable => !data_changed && !operation.changes_data(),
        IsolationLevel::SnapshotIsolation => false, // this case should never happen, since spanpshot isolation cannot be configured on table
    }
}
