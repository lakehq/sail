// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion_common::{DataFusionError, Result};
use futures::StreamExt;
use futures::future::BoxFuture;
use log::warn;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStoreExt, PutMode, PutOptions};
use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::lakehouse::{
    DeltaRatifiedCommitRequest, DeltaRatifiedCommitResponse, LakehouseCommitOutcome,
    LakehouseCommitRequest,
};
use sail_catalog::manager::CatalogManager;
use sail_common::spec::{SAIL_SPARK_UDT_METADATA_KEY, SPARK_METADATA_JSON_KEY};
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::error::CommitStateUnknownError;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use url::Url;
use uuid::Uuid;

use crate::checkpoint::create_checkpoint_for;
use crate::delta_log::LogStoreRef;
use crate::snapshot::{
    DeltaSnapshotConfig, catalog_managed_commit_path, catalog_managed_commit_version,
};
use crate::spec::{
    CommitAction, DataType as DeltaDataType, DeltaError, DeltaOperation, DeltaResult,
    DomainMetadata, Metadata, MetadataValue, PrimitiveType, Protocol, StructField, TableProperties,
    commit_path,
};
use crate::table::{DeltaSnapshot, load_catalog_managed_commits_for_snapshot};
use crate::transaction::{
    CatalogManagedStagedCommit, CommitBuilder, CommitData, CommitProperties, DEFAULT_RETRIES,
    check_catalog_managed_commit_conflicts, should_create_checkpoint,
};

#[derive(Debug, Clone)]
pub(crate) struct DeltaCatalogManagedTable {
    pub(crate) table_id: String,
    pub(crate) table_uri: String,
}

#[derive(Debug)]
struct UnityCommitColumnType {
    type_text: String,
    type_json: serde_json::Value,
    type_name: &'static str,
    type_precision: Option<i32>,
    type_scale: Option<i32>,
}

// TODO: Extend catalog-managed Delta coordination to cover inline ratified
// commits, provider protocol profiles, latest-ratified read bounds, and published
// commit consistency checks.
pub(crate) struct DeltaCatalogCommitCoordinator<'a, C: SessionExtensionAccessor + ?Sized> {
    context: &'a C,
    catalog_table: &'a [String],
}

impl<'a, C: SessionExtensionAccessor + ?Sized> DeltaCatalogCommitCoordinator<'a, C> {
    pub(crate) fn new(context: &'a C, catalog_table: &'a [String]) -> Self {
        Self {
            context,
            catalog_table,
        }
    }

    pub(crate) async fn get_ratified_commits(
        &self,
        lakehouse_table: &LakehouseExecutionContext,
        table_uri: impl Into<String>,
        start_version: i64,
        end_version: Option<i64>,
    ) -> Result<DeltaRatifiedCommitResponse> {
        self.request_ratified_commits(
            lakehouse_table,
            table_uri,
            start_version,
            end_version,
            false,
        )
        .await
    }

    /// Asks the catalog whether a table that is not known to be catalog-managed is one.
    /// The response of a catalog-managed table has its ratified commits as well.
    pub(crate) async fn discover_ratified_commits(
        &self,
        lakehouse_table: &LakehouseExecutionContext,
        table_uri: impl Into<String>,
        start_version: i64,
        end_version: Option<i64>,
    ) -> Result<DeltaRatifiedCommitResponse> {
        self.request_ratified_commits(lakehouse_table, table_uri, start_version, end_version, true)
            .await
    }

    async fn request_ratified_commits(
        &self,
        lakehouse_table: &LakehouseExecutionContext,
        table_uri: impl Into<String>,
        start_version: i64,
        end_version: Option<i64>,
        discover: bool,
    ) -> Result<DeltaRatifiedCommitResponse> {
        let manager = self.context.extension::<CatalogManager>()?;
        manager
            .get_delta_ratified_commits(
                self.catalog_table,
                DeltaRatifiedCommitRequest {
                    context: lakehouse_table.clone(),
                    table_uri: table_uri.into(),
                    start_version,
                    end_version,
                    discover,
                },
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    /// Commits the staged commit to the catalog, and publishes it when every version before
    /// it is published. The `base` is the snapshot of the table that the commit is based on.
    /// The staged commit file is deleted when the catalog rejects the commit, and kept when
    /// the outcome is not known.
    pub(crate) async fn commit_staged(
        &self,
        lakehouse_table: &LakehouseExecutionContext,
        table: &DeltaCatalogManagedTable,
        log_store: &LogStoreRef,
        base: Option<&DeltaSnapshot>,
        staged: &CatalogManagedStagedCommit,
        actions: &[CommitAction],
        latest_backfilled_version: Option<i64>,
    ) -> Result<()> {
        match self
            .try_commit_staged(
                lakehouse_table,
                table,
                log_store,
                base,
                staged,
                actions,
                latest_backfilled_version,
            )
            .await?
        {
            StagedCommitOutcome::Committed(latest_backfilled_version) => {
                if publish_ratified_commit(log_store, staged, latest_backfilled_version).await {
                    checkpoint_published_commit(
                        log_store,
                        staged.version,
                        post_commit_checkpoint_interval(base, actions),
                    )
                    .await;
                }
                Ok(())
            }
            StagedCommitOutcome::Conflict(message) => Err(commit_conflict_error(&message)),
        }
    }

    /// Commits the staged commit to the catalog, and returns whether the catalog ratified it
    /// or another writer won. The staged commit file is deleted when it is not ratified.
    ///
    /// A request whose outcome is not known (a server error, a timeout, or a network error)
    /// is sent again unchanged, since the catalog recognizes the staged commit that it has
    /// ratified. When the outcome is still not known, the commits of the table tell whether
    /// the staged commit is ratified. The staged commit file is kept and the error is a
    /// commit state unknown error when they cannot tell.
    async fn try_commit_staged(
        &self,
        lakehouse_table: &LakehouseExecutionContext,
        table: &DeltaCatalogManagedTable,
        log_store: &LogStoreRef,
        base: Option<&DeltaSnapshot>,
        staged: &CatalogManagedStagedCommit,
        actions: &[CommitAction],
        mut latest_backfilled_version: Option<i64>,
    ) -> Result<StagedCommitOutcome> {
        let manager = self.context.extension::<CatalogManager>()?;
        let mut update = match delta_catalog_managed_commit_update(
            table,
            base.map(|base| (base.metadata(), base.protocol())),
            staged,
            actions,
            latest_backfilled_version,
        ) {
            Ok(update) => update,
            Err(error) => {
                delete_staged_commit(log_store, staged).await;
                return Err(error);
            }
        };
        let mut resends = 0;
        let mut outcome_unknown = false;
        let mut published_ratified_commits = false;
        loop {
            let outcome = manager
                .commit_lakehouse_table(
                    self.catalog_table,
                    LakehouseCommitRequest {
                        context: lakehouse_table.clone(),
                        format: "delta".to_string(),
                        requirements: vec![],
                        updates: vec![update.clone()],
                        payload: None,
                    },
                )
                .await;
            let Some(error) = commit_outcome_error(outcome) else {
                return Ok(StagedCommitOutcome::Committed(latest_backfilled_version));
            };
            let unknown = matches!(error, CatalogError::CommitStateUnknown(_));
            outcome_unknown |= unknown;
            if (unknown || matches!(error, CatalogError::RateLimited(_)))
                && resends < MAX_COMMIT_RESENDS
            {
                tokio::time::sleep(COMMIT_RESEND_BACKOFF * (1 << resends)).await;
                resends += 1;
                continue;
            }
            if outcome_unknown {
                // An earlier request may have been ratified whatever the catalog answers now.
                match self
                    .staged_commit_state(lakehouse_table, table, log_store, staged)
                    .await
                {
                    Ok(StagedCommitState::Ratified) => {
                        return Ok(StagedCommitOutcome::Committed(latest_backfilled_version));
                    }
                    Ok(StagedCommitState::Lost) => {
                        delete_staged_commit(log_store, staged).await;
                        return Ok(StagedCommitOutcome::Conflict(format!(
                            "version {} of the table is the commit of another writer",
                            staged.version
                        )));
                    }
                    Ok(StagedCommitState::Pending) if !unknown => {}
                    Ok(StagedCommitState::Pending) => {
                        return Err(commit_state_unknown_error(staged, error));
                    }
                    Err(e) => {
                        warn!(
                            "Failed to find the staged Delta commit `{}` in the commits of the table: {e}",
                            staged.staged_path
                        );
                        return Err(commit_state_unknown_error(staged, error));
                    }
                }
            }
            if !commit_is_rejected(&error) {
                return Err(DataFusionError::External(Box::new(error)));
            }
            if matches!(error, CatalogError::ResourceExhausted(_)) && !published_ratified_commits {
                // The catalog accepts the commit after the ratified commits of the table are
                // published and reported, which any writer of the table may do.
                published_ratified_commits = true;
                match self
                    .publish_ratified_commits(lakehouse_table, table, log_store, staged.version)
                    .await
                {
                    Ok(Some(version)) if Some(version) > latest_backfilled_version => {
                        latest_backfilled_version = Some(version);
                        update["latest_backfilled_version"] = serde_json::json!(version);
                        continue;
                    }
                    Ok(_) => {}
                    Err(e) => warn!(
                        "Failed to publish the ratified Delta commits of `{}`: {e}",
                        table.table_uri
                    ),
                }
            }
            // A request that the catalog did not answer may still be ratified, unless the
            // catalog answers the same request with a conflict.
            if !outcome_unknown || matches!(error, CatalogError::Conflict(_)) {
                delete_staged_commit(log_store, staged).await;
            }
            return match error {
                CatalogError::Conflict(message) => Ok(StagedCommitOutcome::Conflict(message)),
                error => Err(DataFusionError::External(Box::new(error))),
            };
        }
    }

    /// Tells whether the staged commit is the commit of its version in the table.
    async fn staged_commit_state(
        &self,
        lakehouse_table: &LakehouseExecutionContext,
        table: &DeltaCatalogManagedTable,
        log_store: &LogStoreRef,
        staged: &CatalogManagedStagedCommit,
    ) -> Result<StagedCommitState> {
        let response = self
            .get_ratified_commits(
                lakehouse_table,
                table.table_uri.clone(),
                staged.version,
                None,
            )
            .await?;
        if response.latest_table_version < staged.version {
            return Ok(StagedCommitState::Pending);
        }
        // The catalog does not return a commit that is published and reported.
        let path = response
            .commits
            .iter()
            .find(|commit| {
                commit.version == staged.version
                    && catalog_managed_commit_version(&commit.file_name) == Some(staged.version)
            })
            .map(|commit| catalog_managed_commit_path(&commit.file_name))
            .unwrap_or_else(|| commit_path(staged.version));
        if path == staged.staged_path {
            return Ok(StagedCommitState::Ratified);
        }
        // The file name of a published commit does not tell whose commit it is.
        let store = log_store.object_store(None);
        let read = async |path: &Path| {
            let result = store.get(path).await;
            let result = result.map_err(|e| DataFusionError::External(Box::new(e)))?;
            let bytes = result.bytes().await;
            bytes.map_err(|e| DataFusionError::External(Box::new(e)))
        };
        if read(&path).await? == read(&staged.staged_path).await? {
            Ok(StagedCommitState::Ratified)
        } else {
            Ok(StagedCommitState::Lost)
        }
    }

    /// Publishes the ratified commits of the table before `version` that are not published,
    /// in the order of their versions. Returns the latest version up to which every commit
    /// is published.
    async fn publish_ratified_commits(
        &self,
        lakehouse_table: &LakehouseExecutionContext,
        table: &DeltaCatalogManagedTable,
        log_store: &LogStoreRef,
        version: i64,
    ) -> Result<Option<i64>> {
        let response = self
            .get_ratified_commits(
                lakehouse_table,
                table.table_uri.clone(),
                1,
                Some(version - 1),
            )
            .await?;
        Ok(publish_ratified_commits_before(log_store, &response, version).await)
    }

    /// Stages the actions as the next commit of the table and commits it to the catalog.
    /// Returns the staged commit that the catalog ratified, which is published when every
    /// version before it is published.
    ///
    /// When another writer wins the version, a blind append is checked against the commits
    /// of the other writers and staged again on top of them. Any other commit fails, since
    /// what it read from the table is not known here.
    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn commit_actions(
        &self,
        lakehouse_table: &LakehouseExecutionContext,
        table: &DeltaCatalogManagedTable,
        table_url: &Url,
        log_store: &LogStoreRef,
        reference: Option<Arc<DeltaSnapshot>>,
        actions: &[CommitAction],
        operation: &DeltaOperation,
        properties: CommitProperties,
    ) -> Result<CatalogManagedStagedCommit> {
        let blind_append = CommitData::is_blind_append(actions, operation);
        let mut reference = reference;
        let mut conflict: Option<(i64, String)> = None;
        let mut retries = 0;
        loop {
            let response = self
                .get_ratified_commits(lakehouse_table, table.table_uri.clone(), 1, None)
                .await?;
            let latest_version = response.latest_table_version;
            if let Some((version, message)) = &conflict
                && latest_version < *version
            {
                // The catalog refused the version although no other writer has it.
                return Err(commit_conflict_error(message));
            }
            if blind_append {
                reference = self
                    .rebase_reference(
                        lakehouse_table,
                        table_url,
                        log_store,
                        reference,
                        latest_version,
                        actions,
                        operation,
                    )
                    .await?;
            }
            let staging = CommitBuilder::from(properties.clone())
                .with_actions(actions.to_vec())
                .build(reference.clone(), log_store.clone(), operation.clone())
                .into_staged_commit_future_with_catalog_latest_version(latest_version);
            let (mut staged, latest_backfilled_version) =
                stage_and_publish_ratified_commits(log_store, &response, staging).await?;
            match self
                .try_commit_staged(
                    lakehouse_table,
                    table,
                    log_store,
                    reference.as_deref(),
                    &staged,
                    actions,
                    latest_backfilled_version,
                )
                .await?
            {
                StagedCommitOutcome::Committed(latest_backfilled_version) => {
                    staged.metrics.num_retries = retries;
                    if publish_ratified_commit(log_store, &staged, latest_backfilled_version).await
                    {
                        staged.metrics.new_checkpoint_created = checkpoint_published_commit(
                            log_store,
                            staged.version,
                            post_commit_checkpoint_interval(reference.as_deref(), actions),
                        )
                        .await;
                    }
                    return Ok(staged);
                }
                StagedCommitOutcome::Conflict(message) => {
                    if !blind_append || retries >= DEFAULT_RETRIES as u64 {
                        return Err(commit_conflict_error(&message));
                    }
                    retries += 1;
                    conflict = Some((staged.version, message));
                }
            }
        }
    }

    /// Returns the snapshot of the table at the latest version of the catalog for a commit
    /// that was prepared from the `reference` snapshot. The commit must not conflict with
    /// the commits after the `reference` snapshot.
    async fn rebase_reference(
        &self,
        lakehouse_table: &LakehouseExecutionContext,
        table_url: &Url,
        log_store: &LogStoreRef,
        reference: Option<Arc<DeltaSnapshot>>,
        latest_version: i64,
        actions: &[CommitAction],
        operation: &DeltaOperation,
    ) -> Result<Option<Arc<DeltaSnapshot>>> {
        let Some(snapshot) = reference else {
            return Ok(None);
        };
        if latest_version < 0 || snapshot.version() == latest_version {
            return Ok(Some(snapshot));
        }
        if snapshot.version() > latest_version {
            return Err(DataFusionError::Internal(format!(
                "catalog-managed Delta commit snapshot version {} is newer than catalog latest ratified version {latest_version}",
                snapshot.version()
            )));
        }

        let catalog_managed_commits = load_catalog_managed_commits_for_snapshot(
            self.context,
            lakehouse_table,
            table_url,
            log_store.clone(),
            Some(latest_version),
        )
        .await?
        .commits;
        check_catalog_managed_commit_conflicts(
            log_store,
            &snapshot,
            catalog_managed_commits.as_ref(),
            latest_version,
            actions,
            operation,
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let snapshot = DeltaSnapshot::try_new(
            log_store.as_ref(),
            DeltaSnapshotConfig {
                require_files: false,
                catalog_managed_commits,
                ..Default::default()
            },
            Some(latest_version),
            None,
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Some(Arc::new(snapshot)))
    }
}

/// The number of times that a commit request is sent again when its outcome is not known or
/// the catalog throttles it.
const MAX_COMMIT_RESENDS: u32 = 3;

/// The time before a commit request is sent again for the first time. The time doubles for
/// each of the following requests.
const COMMIT_RESEND_BACKOFF: Duration = Duration::from_millis(200);

/// What the catalog decided about a staged commit.
enum StagedCommitOutcome {
    /// The staged commit is ratified, with the latest backfilled version that the commit
    /// request reported.
    Committed(Option<i64>),
    /// The staged commit is not ratified since the table has changed.
    Conflict(String),
}

/// What the commits of the table tell about a staged commit.
enum StagedCommitState {
    /// The staged commit is the commit of its version.
    Ratified,
    /// The commit of another writer is the commit of the version.
    Lost,
    /// The table does not have the version yet.
    Pending,
}

/// Returns the error of a commit that the catalog has not answered with success.
fn commit_outcome_error(outcome: CatalogResult<LakehouseCommitOutcome>) -> Option<CatalogError> {
    match outcome {
        Ok(LakehouseCommitOutcome::Committed { .. } | LakehouseCommitOutcome::Noop { .. }) => None,
        Ok(LakehouseCommitOutcome::RetryableConflict { message }) => {
            Some(CatalogError::Conflict(message))
        }
        Ok(LakehouseCommitOutcome::StateUnknown { message }) => {
            Some(CatalogError::CommitStateUnknown(message.into()))
        }
        Ok(LakehouseCommitOutcome::Rejected { message }) => {
            Some(CatalogError::CommitRejected(message))
        }
        Err(error) => Some(error),
    }
}

fn commit_conflict_error(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("Delta catalog commit conflict: {message}"))
}

fn commit_state_unknown_error(
    staged: &CatalogManagedStagedCommit,
    error: CatalogError,
) -> DataFusionError {
    DataFusionError::External(Box::new(CatalogError::CommitStateUnknown(
        CommitStateUnknownError::with_source(
            format!(
                "the catalog did not tell whether version {} of the Delta table is the staged commit `{}`; \
                the staged commit is kept, and the history of the table shows whether the write is committed",
                staged.version, staged.staged_path
            ),
            error,
        ),
    )))
}

/// Publishes the ratified commits before `version` that the catalog returned, in the order of
/// their versions, and returns the latest version up to which every commit is published,
/// which is the version that a commit request may report as backfilled.
///
/// The catalog returns the ratified commits that are not reported as published, so every
/// version before the first of them, or up to the latest version of the table when there is
/// none, is published. Any other version counts only when its published commit exists.
/// Commits are published in the order of their versions, so the publishing stops at the
/// first commit that cannot be published. A commit whose file name has another version is
/// not published, since it is not the commit of its version.
async fn publish_ratified_commits_before(
    log_store: &LogStoreRef,
    response: &DeltaRatifiedCommitResponse,
    version: i64,
) -> Option<i64> {
    let mut published = response
        .commits
        .iter()
        .map(|commit| commit.version)
        .min()
        .map_or(response.latest_table_version, |first| first - 1)
        .min(version - 1)
        .max(-1);
    let ratified = response
        .commits
        .iter()
        .filter(|commit| commit.version < version)
        .filter(|commit| {
            let matches = catalog_managed_commit_version(&commit.file_name) == Some(commit.version);
            if !matches {
                warn!(
                    "The catalog returned the Delta commit `{}` for version {}",
                    commit.file_name, commit.version
                );
            }
            matches
        })
        .map(|commit| (commit.version, commit.file_name.as_str()))
        .collect::<BTreeMap<_, _>>();
    // The published commits are looked up at the same time, and only the commits that are
    // missing are published, in order.
    let store = log_store.object_store(None);
    let mut lookups = futures::stream::iter((published + 1)..version)
        .map(|next| {
            let store = store.clone();
            async move { (next, store.head(&commit_path(next)).await) }
        })
        .buffered(PUBLISHED_COMMIT_LOOKUP_CONCURRENCY);
    while let Some((next, lookup)) = lookups.next().await {
        let path = ratified
            .get(&next)
            .map(|file_name| catalog_managed_commit_path(file_name));
        let result = match (lookup, path) {
            (Ok(_), _) => Ok(()),
            (Err(_), None) => break,
            (Err(ObjectStoreError::NotFound { .. }), Some(path)) if path != commit_path(next) => {
                publish_commit(log_store, &path, next)
                    .await
                    .map_err(|error| (path, error))
            }
            (Err(error), Some(path)) => Err((path, DataFusionError::External(Box::new(error)))),
        };
        if let Err((path, error)) = result {
            warn!("Failed to publish the ratified Delta commit `{path}`: {error}");
            break;
        }
        published = next;
    }
    (published >= 0).then_some(published)
}

/// Stages a commit on top of the latest version of the catalog, and publishes the ratified
/// commits before it at the same time. Returns the staged commit and the latest version up
/// to which every commit is published.
pub(crate) async fn stage_and_publish_ratified_commits(
    log_store: &LogStoreRef,
    response: &DeltaRatifiedCommitResponse,
    staging: BoxFuture<'static, DeltaResult<CatalogManagedStagedCommit>>,
) -> Result<(CatalogManagedStagedCommit, Option<i64>)> {
    let to_error = |e: DeltaError| DataFusionError::External(Box::new(e));
    if response.latest_table_version < 0 {
        // The version of the commit is not known before it is staged.
        let staged = staging.await.map_err(to_error)?;
        let published = publish_ratified_commits_before(log_store, response, staged.version).await;
        return Ok((staged, published));
    }
    // The staged commit has the version after the latest version of the catalog.
    let version = response.latest_table_version + 1;
    let (staged, published) = futures::join!(
        staging,
        publish_ratified_commits_before(log_store, response, version)
    );
    Ok((staged.map_err(to_error)?, published))
}

/// The number of published commits that are looked up at the same time before a commit.
const PUBLISHED_COMMIT_LOOKUP_CONCURRENCY: usize = 16;

/// Publishes a commit that the catalog ratified once every version before it is published,
/// since the versions of a table are published in order. A failure to publish does not fail
/// the commit, which any writer of the table may publish later. Returns whether the commit
/// and every version before it are published.
async fn publish_ratified_commit(
    log_store: &LogStoreRef,
    staged: &CatalogManagedStagedCommit,
    latest_backfilled_version: Option<i64>,
) -> bool {
    if latest_backfilled_version != Some(staged.version - 1) {
        warn!(
            "Catalog-managed Delta commit version {} is not published since an earlier version is not published",
            staged.version
        );
        return false;
    }
    if let Err(error) = publish_commit(log_store, &staged.staged_path, staged.version).await {
        warn!(
            "Failed to publish catalog-managed Delta commit version {} after catalog ratification: {error}",
            staged.version
        );
        return false;
    }
    true
}

/// Returns the checkpoint interval of the table after a commit of the actions on top of the
/// `base` snapshot.
fn post_commit_checkpoint_interval(base: Option<&DeltaSnapshot>, actions: &[CommitAction]) -> i64 {
    let interval = actions
        .iter()
        .find_map(|action| match action {
            CommitAction::Metadata(metadata) => {
                Some(TableProperties::from(metadata.configuration().iter()).checkpoint_interval())
            }
            _ => None,
        })
        .or_else(|| base.map(|base| base.table_properties().checkpoint_interval()))
        .unwrap_or_else(|| TableProperties::default().checkpoint_interval());
    i64::try_from(interval.get()).unwrap_or(i64::MAX)
}

/// Writes a checkpoint for a published commit when its version is at the checkpoint interval
/// of the table. A catalog-managed table may only be checkpointed at a version whose commit
/// and the commits before it are published in `_delta_log`. A failure to write the checkpoint
/// does not fail the commit, since the log can always be replayed without it. Returns whether
/// the checkpoint is written.
async fn checkpoint_published_commit(log_store: &LogStoreRef, version: i64, interval: i64) -> bool {
    if !should_create_checkpoint(version, interval) {
        return false;
    }
    match create_checkpoint_for(version, log_store.as_ref(), Uuid::new_v4()).await {
        Ok(()) => true,
        Err(error) => {
            warn!(
                "Failed to create checkpoint for catalog-managed Delta version {version}: {error}"
            );
            false
        }
    }
}

/// Copies a ratified commit to its place in the published Delta log. The commit that is
/// published already is not written again.
async fn publish_commit(log_store: &LogStoreRef, path: &Path, version: i64) -> Result<()> {
    let published = commit_path(version);
    if *path == published {
        return Ok(());
    }
    let store = log_store.object_store(None);
    let bytes = store
        .get(path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .bytes()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let options = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    match store.put_opts(&published, bytes.into(), options).await {
        Ok(_) | Err(ObjectStoreError::AlreadyExists { .. }) => Ok(()),
        Err(err) => Err(DataFusionError::External(Box::new(err))),
    }
}

/// Whether the catalog has answered that the commit is not ratified. A failure without such
/// an answer (a server error, a timeout, or a network error) may follow a ratified commit.
fn commit_is_rejected(error: &CatalogError) -> bool {
    matches!(
        error,
        CatalogError::InvalidArgument(_)
            | CatalogError::NotFound(_, _)
            | CatalogError::AlreadyExists(_, _)
            | CatalogError::Conflict(_)
            | CatalogError::NotSupported(_)
            | CatalogError::Unauthorized(_)
            | CatalogError::Forbidden(_)
            | CatalogError::AuthExpired(_)
            | CatalogError::RateLimited(_)
            | CatalogError::ResourceExhausted(_)
            | CatalogError::ReadOnly(_)
            | CatalogError::UnsupportedCapability(_)
            | CatalogError::StaleMetadata(_)
            | CatalogError::CommitRejected(_)
    )
}

/// Deletes a staged commit that is not ratified, so that it is never read as a commit of the
/// table. A failure to delete does not change the outcome of the commit.
async fn delete_staged_commit(log_store: &LogStoreRef, staged: &CatalogManagedStagedCommit) {
    if let Err(error) = log_store
        .object_store(None)
        .delete(&staged.staged_path)
        .await
    {
        warn!(
            "Failed to delete the rejected staged Delta commit `{}`: {error}",
            staged.staged_path
        );
    }
}

fn delta_catalog_managed_commit_update(
    table: &DeltaCatalogManagedTable,
    base: Option<(&Metadata, &Protocol)>,
    staged: &CatalogManagedStagedCommit,
    actions: &[CommitAction],
    latest_backfilled_version: Option<i64>,
) -> Result<serde_json::Value> {
    let mut update = serde_json::json!({
        "table_id": table.table_id,
        "table_uri": table.table_uri,
        "commit_info": {
            "version": staged.version,
            "timestamp": staged.in_commit_timestamp,
            "file_name": staged.file_name,
            "file_size": staged.file_size,
            "file_modification_timestamp": staged.file_modification_timestamp,
        }
    });
    if let Some(latest_backfilled_version) = latest_backfilled_version {
        update["latest_backfilled_version"] = serde_json::json!(latest_backfilled_version);
    }
    match unity_commit_metadata(actions) {
        Ok(Some(metadata)) => update["metadata"] = metadata,
        Ok(None) => {}
        // The metadata of the legacy commit API is not used by a catalog with the Delta
        // REST API, so the catalog reports the error when it needs the metadata.
        Err(error) => update["metadata_error"] = serde_json::json!(error.to_string()),
    }
    if let Some(changes) = delta_log_changes(base, actions, &staged.domain_metadata)? {
        update["delta_log"] = changes;
    }
    Ok(update)
}

/// Returns the metadata action and the protocol action of the commit that differ from
/// the metadata and the protocol of the base snapshot, in the form of the Delta log.
/// The metadata action comes with the metadata of the base snapshot, so that the catalog
/// can tell what the commit changes. The domain metadata actions of the commit are
/// returned as they are.
fn delta_log_changes(
    base: Option<(&Metadata, &Protocol)>,
    actions: &[CommitAction],
    domain_metadata: &[DomainMetadata],
) -> Result<Option<serde_json::Value>> {
    let json = |value: serde_json::Result<serde_json::Value>| {
        value.map_err(|e| DataFusionError::External(Box::new(e)))
    };
    let mut changes = serde_json::Map::new();
    let metadata = actions.iter().rev().find_map(|action| match action {
        CommitAction::Metadata(metadata) => Some(metadata),
        _ => None,
    });
    if let Some(metadata) = metadata.filter(|x| base.map(|(metadata, _)| metadata) != Some(x)) {
        changes.insert(
            "metaData".to_string(),
            json(serde_json::to_value(metadata))?,
        );
        if let Some((base, _)) = base {
            changes.insert(
                "baseMetaData".to_string(),
                json(serde_json::to_value(base))?,
            );
        }
    }
    let protocol = actions.iter().rev().find_map(|action| match action {
        CommitAction::Protocol(protocol) => Some(protocol),
        _ => None,
    });
    if let Some(protocol) = protocol.filter(|x| base.map(|(_, protocol)| protocol) != Some(x)) {
        changes.insert(
            "protocol".to_string(),
            json(serde_json::to_value(protocol))?,
        );
    }
    if !domain_metadata.is_empty() {
        changes.insert(
            "domainMetadata".to_string(),
            json(serde_json::to_value(domain_metadata))?,
        );
    }
    Ok((!changes.is_empty()).then_some(serde_json::Value::Object(changes)))
}

fn unity_commit_metadata(actions: &[CommitAction]) -> Result<Option<serde_json::Value>> {
    let Some(metadata) = actions.iter().rev().find_map(|action| match action {
        CommitAction::Metadata(metadata) => Some(metadata),
        _ => None,
    }) else {
        return Ok(None);
    };

    let schema = metadata
        .parse_schema()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let columns = schema
        .fields()
        .enumerate()
        .map(|(position, field)| unity_column_info(metadata, field, position))
        .collect::<Result<Vec<_>>>()?;

    let mut out = serde_json::Map::new();
    if let Some(description) = metadata.description() {
        out.insert(
            "description".to_string(),
            serde_json::Value::String(description.to_string()),
        );
    }
    out.insert(
        "schema".to_string(),
        serde_json::json!({
            "columns": columns,
        }),
    );
    if !metadata.configuration().is_empty() {
        out.insert(
            "properties".to_string(),
            serde_json::json!({
                "properties": metadata.configuration(),
            }),
        );
    }

    Ok(Some(serde_json::Value::Object(out)))
}

fn unity_column_info(
    metadata: &Metadata,
    field: &StructField,
    position: usize,
) -> Result<serde_json::Value> {
    let column_type = unity_column_type(field.data_type())?;
    let mut column = serde_json::json!({
        "name": field.name(),
        "nullable": field.is_nullable(),
        "position": i32::try_from(position).unwrap_or(i32::MAX),
        "type_text": column_type.type_text,
        "type_json": unity_struct_field_type_json(field)?.to_string(),
        "type_name": column_type.type_name,
    });

    if let Some(precision) = column_type.type_precision {
        column["type_precision"] = serde_json::json!(precision);
    }
    if let Some(scale) = column_type.type_scale {
        column["type_scale"] = serde_json::json!(scale);
    }
    if let Some(partition_index) = metadata
        .partition_columns()
        .iter()
        .position(|column| column.eq_ignore_ascii_case(field.name()))
    {
        column["partition_index"] =
            serde_json::json!(i32::try_from(partition_index).unwrap_or(i32::MAX));
    }
    if let Some(MetadataValue::String(comment)) = field.metadata().get("comment") {
        column["comment"] = serde_json::Value::String(comment.clone());
    }

    Ok(column)
}

fn unity_struct_field_type_json(field: &StructField) -> Result<serde_json::Value> {
    Ok(serde_json::json!({
        "name": field.name(),
        "type": spark_delta_data_type_json(field.data_type())?,
        "nullable": field.is_nullable(),
        "metadata": unity_field_metadata(field)?,
    }))
}

fn spark_delta_data_type_json(data_type: &DeltaDataType) -> Result<serde_json::Value> {
    match data_type {
        DeltaDataType::Primitive(primitive) => Ok(spark_delta_primitive_json(primitive)),
        DeltaDataType::Array(array) => Ok(serde_json::json!({
            "type": "array",
            "elementType": spark_delta_data_type_json(array.element_type())?,
            "containsNull": array.contains_null(),
        })),
        DeltaDataType::Struct(struct_type) => {
            let fields = struct_type
                .fields()
                .map(unity_struct_field_type_json)
                .collect::<Result<Vec<_>>>()?;
            Ok(serde_json::json!({
                "type": "struct",
                "fields": fields,
            }))
        }
        DeltaDataType::Map(map) => Ok(serde_json::json!({
            "type": "map",
            "keyType": spark_delta_data_type_json(map.key_type())?,
            "valueType": spark_delta_data_type_json(map.value_type())?,
            "valueContainsNull": map.value_contains_null(),
        })),
        DeltaDataType::Variant(_) => Err(DataFusionError::NotImplemented(
            "Unity Catalog metadata payloads for Delta variant columns".to_string(),
        )),
    }
}

fn spark_delta_primitive_json(primitive: &PrimitiveType) -> serde_json::Value {
    let type_name = match primitive {
        PrimitiveType::String => "string".to_string(),
        PrimitiveType::Long => "long".to_string(),
        PrimitiveType::Integer => "integer".to_string(),
        PrimitiveType::Short => "short".to_string(),
        PrimitiveType::Byte => "byte".to_string(),
        PrimitiveType::Float => "float".to_string(),
        PrimitiveType::Double => "double".to_string(),
        PrimitiveType::Boolean => "boolean".to_string(),
        PrimitiveType::Binary => "binary".to_string(),
        PrimitiveType::Date => "date".to_string(),
        PrimitiveType::Timestamp => "timestamp".to_string(),
        PrimitiveType::TimestampNtz => "timestamp_ntz".to_string(),
        PrimitiveType::Decimal(decimal) => {
            format!("decimal({},{})", decimal.precision(), decimal.scale())
        }
    };
    serde_json::Value::String(type_name)
}

fn unity_column_type(data_type: &DeltaDataType) -> Result<UnityCommitColumnType> {
    match data_type {
        DeltaDataType::Primitive(primitive) => Ok(unity_primitive_column_type(primitive)),
        DeltaDataType::Array(array) => {
            let element_type = unity_column_type(array.element_type())?;
            Ok(UnityCommitColumnType {
                type_text: format!("array<{}>", element_type.type_text),
                type_json: serde_json::json!({
                    "type": {
                        "type": "array",
                        "elementType": element_type.type_json,
                        "containsNull": array.contains_null(),
                    },
                }),
                type_name: "ARRAY",
                type_precision: None,
                type_scale: None,
            })
        }
        DeltaDataType::Struct(struct_type) => {
            let mut type_text_parts = Vec::new();
            let mut fields = Vec::new();
            for field in struct_type.fields() {
                let field_type = unity_column_type(field.data_type())?;
                type_text_parts.push(format!("{}:{}", field.name(), field_type.type_text));
                fields.push(serde_json::json!({
                    "name": field.name(),
                    "type": field_type.type_json,
                    "nullable": field.is_nullable(),
                    "metadata": unity_field_metadata(field)?,
                }));
            }
            Ok(UnityCommitColumnType {
                type_text: format!("struct<{}>", type_text_parts.join(",")),
                type_json: serde_json::json!({
                    "type": {
                        "type": "struct",
                        "fields": fields,
                    },
                }),
                type_name: "STRUCT",
                type_precision: None,
                type_scale: None,
            })
        }
        DeltaDataType::Map(map) => {
            let key_type = unity_column_type(map.key_type())?;
            let value_type = unity_column_type(map.value_type())?;
            Ok(UnityCommitColumnType {
                type_text: format!("map<{},{}>", key_type.type_text, value_type.type_text),
                type_json: serde_json::json!({
                    "type": {
                        "type": "map",
                        "keyType": key_type.type_json,
                        "valueType": value_type.type_json,
                        "valueContainsNull": map.value_contains_null(),
                    },
                }),
                type_name: "MAP",
                type_precision: None,
                type_scale: None,
            })
        }
        DeltaDataType::Variant(_) => Err(DataFusionError::NotImplemented(
            "Unity Catalog metadata payloads for Delta variant columns".to_string(),
        )),
    }
}

fn unity_primitive_column_type(primitive: &PrimitiveType) -> UnityCommitColumnType {
    let (type_text, type_name) = match primitive {
        PrimitiveType::String => ("string".to_string(), "STRING"),
        PrimitiveType::Long => ("long".to_string(), "LONG"),
        PrimitiveType::Integer => ("int".to_string(), "INT"),
        PrimitiveType::Short => ("short".to_string(), "SHORT"),
        PrimitiveType::Byte => ("byte".to_string(), "BYTE"),
        PrimitiveType::Float => ("float".to_string(), "FLOAT"),
        PrimitiveType::Double => ("double".to_string(), "DOUBLE"),
        PrimitiveType::Boolean => ("boolean".to_string(), "BOOLEAN"),
        PrimitiveType::Binary => ("binary".to_string(), "BINARY"),
        PrimitiveType::Date => ("date".to_string(), "DATE"),
        PrimitiveType::Timestamp => ("timestamp".to_string(), "TIMESTAMP"),
        PrimitiveType::TimestampNtz => ("timestamp_ntz".to_string(), "TIMESTAMP_NTZ"),
        PrimitiveType::Decimal(decimal) => {
            return UnityCommitColumnType {
                type_text: format!("decimal({},{})", decimal.precision(), decimal.scale()),
                type_json: serde_json::json!({
                    "type": {
                        "type": "decimal",
                        "precision": decimal.precision(),
                        "scale": decimal.scale(),
                    },
                }),
                type_name: "DECIMAL",
                type_precision: Some(i32::from(decimal.precision())),
                type_scale: Some(i32::from(decimal.scale())),
            };
        }
    };

    UnityCommitColumnType {
        type_json: serde_json::Value::String(type_text.clone()),
        type_text,
        type_name,
        type_precision: None,
        type_scale: None,
    }
}

fn unity_field_metadata(field: &StructField) -> Result<serde_json::Value> {
    let mut out = if let Some(value) = field.metadata().get(SPARK_METADATA_JSON_KEY) {
        match spark_metadata_value_json(value)? {
            serde_json::Value::Object(object) => object,
            other => {
                return Err(DataFusionError::External(Box::new(
                    CatalogError::InvalidArgument(format!(
                        "{SPARK_METADATA_JSON_KEY} must contain a JSON object, got {other:?}",
                    )),
                )));
            }
        }
    } else {
        serde_json::Map::new()
    };

    for (key, value) in field.metadata() {
        if key == SPARK_METADATA_JSON_KEY || key == SAIL_SPARK_UDT_METADATA_KEY {
            continue;
        }
        out.entry(key.clone())
            .or_insert(metadata_value_json(value)?);
    }

    Ok(serde_json::Value::Object(out))
}

fn spark_metadata_value_json(value: &MetadataValue) -> Result<serde_json::Value> {
    match value {
        MetadataValue::String(value) => serde_json::from_str(value).map_err(|e| {
            DataFusionError::External(Box::new(CatalogError::InvalidArgument(format!(
                "{SPARK_METADATA_JSON_KEY} contains invalid JSON: {e}",
            ))))
        }),
        other => metadata_value_json(other),
    }
}

fn metadata_value_json(value: &MetadataValue) -> Result<serde_json::Value> {
    match value {
        MetadataValue::Number(value) => Ok(serde_json::Value::Number((*value).into())),
        MetadataValue::String(value) => Ok(serde_json::Value::String(value.clone())),
        MetadataValue::Boolean(value) => Ok(serde_json::Value::Bool(*value)),
        MetadataValue::Other(value) => Ok(value.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::StructType;

    #[test]
    fn delta_log_changes_are_relative_to_the_base_snapshot() -> Result<()> {
        let external = |e| DataFusionError::External(Box::new(e));
        let schema = |columns: &[&str]| {
            StructType::try_new(
                columns
                    .iter()
                    .map(|column| StructField::nullable(*column, DeltaDataType::LONG)),
            )
        };
        let base_metadata = Metadata::try_new(
            None,
            None,
            schema(&["id"]).map_err(external)?,
            vec![],
            0,
            [("owner".to_string(), "a".to_string())].into(),
        )
        .map_err(external)?;
        let base_protocol = Protocol::new(3, 7, Some(vec![]), Some(vec![]));
        let base = Some((&base_metadata, &base_protocol));

        // The actions of the base snapshot written again are not changes.
        let actions = vec![
            CommitAction::Protocol(base_protocol.clone()),
            CommitAction::Metadata(base_metadata.clone()),
        ];
        assert_eq!(delta_log_changes(base, &actions, &[])?, None);
        assert_eq!(delta_log_changes(base, &[], &[])?, None);

        let metadata = base_metadata
            .clone()
            .with_schema(&schema(&["id", "value"]).map_err(external)?)
            .map_err(external)?;
        let protocol = Protocol::new(3, 7, Some(vec![]), None);
        let actions = vec![
            CommitAction::Protocol(protocol.clone()),
            CommitAction::Metadata(metadata.clone()),
        ];
        assert_eq!(
            delta_log_changes(base, &actions, &[])?,
            Some(serde_json::json!({
                "metaData": metadata,
                "baseMetaData": base_metadata,
                "protocol": protocol,
            }))
        );
        assert_eq!(
            delta_log_changes(None, &actions, &[])?,
            Some(serde_json::json!({
                "metaData": metadata,
                "protocol": protocol,
            }))
        );

        // The domain metadata actions of the commit are changes of their own.
        let domain = DomainMetadata {
            domain: "delta.rowTracking".to_string(),
            configuration: r#"{"rowIdHighWaterMark":41}"#.to_string(),
            removed: false,
        };
        assert_eq!(
            delta_log_changes(base, &[], std::slice::from_ref(&domain))?,
            Some(serde_json::json!({
                "domainMetadata": [{
                    "domain": "delta.rowTracking",
                    "configuration": r#"{"rowIdHighWaterMark":41}"#,
                    "removed": false,
                }],
            }))
        );
        Ok(())
    }

    #[test]
    fn commit_update_defers_legacy_metadata_errors() -> Result<()> {
        let external = |e| DataFusionError::External(Box::new(e));
        let metadata = |data_type: DeltaDataType| {
            Metadata::try_new(
                None,
                None,
                StructType::try_new([StructField::nullable("value", data_type)])?,
                vec![],
                0,
                Default::default(),
            )
        };
        let table = DeltaCatalogManagedTable {
            table_id: "id".to_string(),
            table_uri: "memory:///t".to_string(),
        };
        let staged = CatalogManagedStagedCommit {
            version: 1,
            file_name: "00000000000000000001.uuid.json".to_string(),
            file_size: 1,
            file_modification_timestamp: 1,
            in_commit_timestamp: 1,
            staged_path: object_store::path::Path::from("t/_delta_log/_staged_commits/1.json"),
            domain_metadata: vec![],
            metrics: Default::default(),
        };
        let base_metadata = metadata(DeltaDataType::LONG).map_err(external)?;
        let base_protocol = Protocol::new(3, 7, Some(vec![]), Some(vec![]));
        let base = Some((&base_metadata, &base_protocol));

        let variant = metadata(DeltaDataType::unshredded_variant()).map_err(external)?;
        let actions = vec![CommitAction::Metadata(variant.clone())];
        let update = delta_catalog_managed_commit_update(&table, base, &staged, &actions, None)?;
        assert_eq!(update.get("metadata"), None);
        assert!(
            update["metadata_error"]
                .as_str()
                .is_some_and(|error| error.contains("variant columns")),
            "{update}"
        );
        assert_eq!(update["delta_log"]["metaData"], serde_json::json!(variant));

        let actions = vec![CommitAction::Metadata(base_metadata.clone())];
        let update = delta_catalog_managed_commit_update(&table, base, &staged, &actions, None)?;
        assert!(update["metadata"].is_object(), "{update}");
        assert_eq!(update.get("metadata_error"), None);
        Ok(())
    }

    #[test]
    fn definite_commit_rejection_preserves_its_typed_outcome() -> Result<()> {
        let error = commit_outcome_error(Ok(LakehouseCommitOutcome::Rejected {
            message: "policy denied".to_string(),
        }));
        assert!(
            matches!(&error, Some(CatalogError::CommitRejected(message)) if message == "policy denied"),
            "{error:?}"
        );
        assert!(error.as_ref().is_some_and(commit_is_rejected));
        Ok(())
    }

    /// A case of publishing the ratified commits before a commit.
    struct PublishCase {
        name: &'static str,
        /// The published commits and the staged commits (`<version>.<id>`) in the log.
        files: &'static [&'static str],
        /// The published commits whose lookup fails.
        failing: &'static [i64],
        latest_table_version: i64,
        /// The versions and the file names that the catalog returns.
        commits: &'static [(i64, &'static str)],
        version: i64,
        expected: Option<i64>,
        /// The published commits after publishing.
        published: &'static [i64],
    }

    fn log_file(name: &str) -> Path {
        match name.split_once('.') {
            Some((version, id)) => Path::from(format!(
                "_delta_log/_staged_commits/{:020}.{id}.json",
                version.parse::<i64>().unwrap_or_default()
            )),
            None => commit_path(name.parse().unwrap_or_default()),
        }
    }

    #[tokio::test]
    async fn ratified_commits_are_published_in_order() -> Result<()> {
        use crate::delta_log::{StorageConfig, default_logstore};
        use crate::test_store::ObservedStore;

        let cases = [
            PublishCase {
                name: "versions up to the latest version are published without a lookup",
                files: &[],
                failing: &[],
                latest_table_version: 1,
                commits: &[],
                version: 2,
                expected: Some(1),
                published: &[],
            },
            PublishCase {
                name: "version 0 is looked up when the catalog has no version",
                files: &["0"],
                failing: &[],
                latest_table_version: -1,
                commits: &[],
                version: 1,
                expected: Some(0),
                published: &[0],
            },
            PublishCase {
                name: "a missing version 0 is not published",
                files: &[],
                failing: &[],
                latest_table_version: -1,
                commits: &[],
                version: 1,
                expected: None,
                published: &[],
            },
            PublishCase {
                name: "publishing stops at a version that is neither returned nor published",
                files: &["0", "1", "2", "3.a", "5.b"],
                failing: &[],
                latest_table_version: 5,
                commits: &[(3, "a"), (5, "b")],
                version: 6,
                expected: Some(3),
                published: &[0, 1, 2, 3],
            },
            PublishCase {
                name: "publishing stops at a failed lookup",
                files: &["0", "1.a", "2.b"],
                failing: &[1],
                latest_table_version: 2,
                commits: &[(1, "a"), (2, "b")],
                version: 3,
                expected: Some(0),
                published: &[0],
            },
            PublishCase {
                name: "a published commit is not written again",
                files: &["0", "1", "1.a"],
                failing: &[],
                latest_table_version: 1,
                commits: &[(1, "a")],
                version: 2,
                expected: Some(1),
                published: &[0, 1],
            },
            PublishCase {
                name: "a commit whose file name has another version is not published",
                files: &["0", "1.a"],
                failing: &[],
                latest_table_version: 2,
                commits: &[(2, "a")],
                version: 3,
                expected: Some(1),
                published: &[0],
            },
        ];
        for case in cases {
            let store = Arc::new(ObservedStore::default());
            for file in case.files {
                store
                    .put(&log_file(file), format!("{file}\n").into())
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            }
            for version in case.failing {
                store.fail_lookups(commit_path(*version));
            }
            let object_store = store.clone() as Arc<dyn object_store::ObjectStore>;
            let url =
                Url::parse("memory:///").map_err(|e| DataFusionError::External(Box::new(e)))?;
            let log_store =
                default_logstore(object_store.clone(), object_store, &url, &StorageConfig);
            let response = DeltaRatifiedCommitResponse {
                latest_table_version: case.latest_table_version,
                commits: case
                    .commits
                    .iter()
                    .map(
                        |(version, id)| sail_catalog::lakehouse::DeltaRatifiedCommit {
                            // The file name of a misplaced commit has the version before it.
                            version: *version,
                            timestamp: 1,
                            file_name: if case.name.contains("another version") {
                                format!("{:020}.{id}.json", version - 1)
                            } else {
                                format!("{version:020}.{id}.json")
                            },
                            file_size: 1,
                            file_modification_timestamp: 1,
                        },
                    )
                    .collect(),
                ..Default::default()
            };
            let actual = publish_ratified_commits_before(&log_store, &response, case.version).await;
            assert_eq!(actual, case.expected, "{}", case.name);
            let mut published = vec![];
            for version in 0..case.version {
                if !case.failing.contains(&version)
                    && store.head(&commit_path(version)).await.is_ok()
                {
                    published.push(version);
                }
            }
            assert_eq!(published, case.published, "{}", case.name);
            if case.name.contains("not written again") {
                let bytes = store
                    .get(&commit_path(1))
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .bytes()
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                assert_eq!(bytes.as_ref(), b"1\n", "{}", case.name);
            }
        }
        Ok(())
    }
}
