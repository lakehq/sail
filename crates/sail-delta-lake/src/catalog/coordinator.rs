// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use bytes::Bytes;
use datafusion_common::{DataFusionError, Result};
use futures::StreamExt;
use futures::future::BoxFuture;
use log::warn;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStoreExt, PutMode, PutOptions};
use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::lakehouse::{
    DeltaCommitOutcome, DeltaRatifiedCommitRequest, DeltaRatifiedCommitResponse,
};
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::catalog::delta::{
    DeltaCatalogCommit, DeltaCatalogCommitInfo, DeltaCommitActions, DeltaDomainMetadataInfo,
};
use sail_common_datafusion::error::CommitStateUnknownError;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use uuid::Uuid;

use crate::checkpoint::create_checkpoint_for;
use crate::delta_log::{LogStoreRef, ObjectStoreRef};
use crate::lake_source::{delta_metadata_info, delta_protocol_info};
use crate::snapshot::{
    DeltaSnapshotConfig, catalog_managed_commit_path, catalog_managed_commit_version,
};
use crate::spec::{
    CommitAction, DeltaError, DeltaOperation, DeltaResult, DomainMetadata, Metadata, Protocol,
    TableProperties, commit_path,
};
use crate::table::{DeltaSnapshot, load_catalog_managed_commits};
use crate::transaction::{
    CatalogManagedStagedCommit, CommitBuilder, CommitData, CommitProperties, DEFAULT_RETRIES,
    check_catalog_managed_commit_conflicts, should_create_checkpoint,
};

#[derive(Debug, Clone)]
pub(crate) struct DeltaCatalogManagedTable {
    pub(crate) table_id: String,
    pub(crate) table_uri: String,
}

impl DeltaCatalogManagedTable {
    /// Returns the table that the catalog resolved, or `None` for a table that is resolved
    /// without its ID or its location, such as a table that is not created yet.
    pub(crate) fn from_context(lakehouse_table: &LakehouseExecutionContext) -> Option<Self> {
        let identity = &lakehouse_table.table_identity;
        Some(Self {
            table_id: identity.table_id.clone()?,
            table_uri: identity.table_uri.clone()?,
        })
    }
}

// TODO: Extend catalog-managed Delta coordination to cover inline ratified
// commits and provider protocol profiles.
pub(crate) struct DeltaCatalogCommitCoordinator<'a, C: SessionExtensionAccessor + ?Sized> {
    context: &'a C,
    lakehouse_table: &'a LakehouseExecutionContext,
}

impl<'a, C: SessionExtensionAccessor + ?Sized> DeltaCatalogCommitCoordinator<'a, C> {
    pub(crate) fn new(context: &'a C, lakehouse_table: &'a LakehouseExecutionContext) -> Self {
        Self {
            context,
            lakehouse_table,
        }
    }

    /// Requests the ratified commits of the table from the catalog.
    pub(crate) async fn get_ratified_commits(
        &self,
        start_version: i64,
        end_version: Option<i64>,
    ) -> Result<DeltaRatifiedCommitResponse> {
        let manager = self.context.extension::<CatalogManager>()?;
        manager
            .get_delta_ratified_commits(
                self.lakehouse_table.catalog_table(),
                DeltaRatifiedCommitRequest {
                    context: self.lakehouse_table.clone(),
                    start_version,
                    end_version,
                },
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
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
        table: &DeltaCatalogManagedTable,
        log_store: &LogStoreRef,
        base: Option<&DeltaSnapshot>,
        staged: &CatalogManagedStagedCommit,
        actions: &[CommitAction],
        latest_backfilled_version: Option<i64>,
    ) -> Result<StagedCommitOutcome> {
        let manager = self.context.extension::<CatalogManager>()?;
        let mut commit = delta_catalog_managed_commit(
            table,
            base.map(|base| (base.metadata(), base.protocol())),
            staged,
            actions,
            latest_backfilled_version,
        );
        let mut resends = 0;
        let mut outcome_unknown = false;
        let mut published_ratified_commits = false;
        let mut publish_failure = None;
        loop {
            let answer = match manager
                .commit_delta_table(self.lakehouse_table.catalog_table(), commit.clone())
                .await
            {
                Ok(DeltaCommitOutcome::Committed) => {
                    return Ok(StagedCommitOutcome::Committed(
                        commit.latest_backfilled_version,
                    ));
                }
                answer => answer,
            };
            let unknown = matches!(answer, Ok(DeltaCommitOutcome::Unknown(_)));
            outcome_unknown |= unknown;
            if (unknown || matches!(answer, Ok(DeltaCommitOutcome::Throttled(_))))
                && resends < MAX_COMMIT_RESENDS
            {
                tokio::time::sleep(COMMIT_RESEND_BACKOFF * (1 << resends)).await;
                resends += 1;
                continue;
            }
            // The catalog refuses the staged commit itself when the version of the commit is
            // taken, or when the catalog is at its limit of commits that are not published,
            // which it checks after it looks for the staged commit among the commits that it
            // has ratified.
            let conflict = matches!(answer, Ok(DeltaCommitOutcome::Conflict(_)));
            let backfill_required = matches!(answer, Ok(DeltaCommitOutcome::BackfillRequired(_)));
            if outcome_unknown {
                // An earlier request may have been ratified whatever the catalog answers now.
                match self.staged_commit_state(log_store, staged).await {
                    Ok(StagedCommitState::Ratified) => {
                        return Ok(StagedCommitOutcome::Committed(
                            commit.latest_backfilled_version,
                        ));
                    }
                    Ok(StagedCommitState::Lost) => {
                        delete_staged_commit(log_store, staged).await;
                        return Ok(StagedCommitOutcome::Conflict(format!(
                            "version {} of the table is the commit of another writer",
                            staged.version
                        )));
                    }
                    // The catalog has not ratified the staged commit so far, but it may still
                    // ratify the request that it did not answer. Only an answer about the
                    // staged commit itself is followed. Any other answer (e.g. a request that
                    // is throttled or not authorized) tells nothing about the earlier request.
                    Ok(StagedCommitState::Pending) if conflict || backfill_required => {}
                    Ok(StagedCommitState::Pending) => {
                        return Err(commit_state_unknown_error(staged, refusal(answer)));
                    }
                    Err(e) => {
                        warn!(
                            "Failed to find the staged Delta commit `{}` in the commits of the table: {e}",
                            staged.staged_path
                        );
                        return Err(commit_state_unknown_error(staged, refusal(answer)));
                    }
                }
            }
            if backfill_required && !published_ratified_commits {
                // The catalog accepts the commit after the ratified commits of the table are
                // published and reported, which any writer of the table may do.
                published_ratified_commits = true;
                match self
                    .publish_ratified_commits(log_store, staged.version)
                    .await
                {
                    Ok(published) => {
                        publish_failure = published.failure;
                        // The catalog counts only the backfill that a request reports, so the
                        // request is sent again with the backfill even after an unknown outcome.
                        // The catalog checks for a replay of the staged commit before its limit,
                        // so the refusal shows that no earlier request was ratified.
                        if published.version > commit.latest_backfilled_version {
                            commit.latest_backfilled_version = published.version;
                            continue;
                        }
                    }
                    Err(e) => warn!(
                        "Failed to publish the ratified Delta commits of `{}`: {e}",
                        table.table_uri
                    ),
                }
            }
            // A request that the catalog did not answer may still be ratified, unless the
            // catalog answers the same request with a conflict.
            if !outcome_unknown || conflict {
                delete_staged_commit(log_store, staged).await;
            }
            return match (answer, publish_failure) {
                (Ok(DeltaCommitOutcome::Conflict(message)), _) => {
                    Ok(StagedCommitOutcome::Conflict(message))
                }
                // The catalog is at its limit now, which does not tell whether it ratifies
                // the request that it did not answer.
                (answer, _) if outcome_unknown => {
                    Err(commit_state_unknown_error(staged, refusal(answer)))
                }
                // The commit that blocks the backfill must be repaired before the catalog
                // accepts another commit.
                (Ok(DeltaCommitOutcome::BackfillRequired(message)), Some(failure)) => {
                    Err(DataFusionError::External(Box::new(
                        CatalogError::ResourceExhausted(format!("{message}; {failure}")),
                    )))
                }
                (answer, _) => Err(DataFusionError::External(Box::new(refusal(answer)))),
            };
        }
    }

    /// Tells whether the staged commit is the commit of its version in the table.
    async fn staged_commit_state(
        &self,
        log_store: &LogStoreRef,
        staged: &CatalogManagedStagedCommit,
    ) -> Result<StagedCommitState> {
        let response = self.get_ratified_commits(staged.version, None).await?;
        staged_commit_state_in(log_store, &response, staged).await
    }

    /// Publishes the ratified commits of the table before `version` that are not published,
    /// in the order of their versions.
    async fn publish_ratified_commits(
        &self,
        log_store: &LogStoreRef,
        version: i64,
    ) -> Result<PublishedCommits> {
        let response = self.get_ratified_commits(1, Some(version - 1)).await?;
        Ok(publish_ratified_commits_before(log_store, &response, version).await)
    }

    /// Stages the actions as the next commit of the table and commits it to the catalog.
    /// Returns the staged commit that the catalog ratified, which is published when every
    /// version before it is published. This is the only way that a commit goes to the
    /// catalog.
    ///
    /// The first commit is staged on top of the ratified commits that the catalog returned
    /// when it resolved the table, when the `reference` snapshot is at their latest version,
    /// so that the catalog is not asked again. The catalog answers with a conflict when the
    /// table has changed since then.
    ///
    /// When another writer wins the version, a blind append is checked against the commits
    /// of the other writers and staged again on top of them. Any other commit fails, since
    /// what it read from the table is not known here.
    pub(crate) async fn commit_actions(
        &self,
        table: &DeltaCatalogManagedTable,
        log_store: &LogStoreRef,
        reference: Option<Arc<DeltaSnapshot>>,
        actions: &[CommitAction],
        operation: &DeltaOperation,
        properties: CommitProperties,
    ) -> Result<CatalogManagedStagedCommit> {
        let blind_append = CommitData::is_blind_append(actions, operation);
        let mut resolved = self
            .lakehouse_table
            .delta
            .as_ref()
            .and_then(|state| state.commits.as_ref())
            .filter(|commits| {
                reference
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.version() == commits.latest_table_version)
            })
            .cloned();
        let mut reference = reference;
        let mut conflict: Option<(i64, String)> = None;
        let mut retries = 0;
        loop {
            let response = match resolved.take() {
                Some(response) => response,
                None => self.get_ratified_commits(1, None).await?,
            };
            let latest_version = response.latest_table_version;
            if let Some((version, message)) = &conflict
                && latest_version < *version
            {
                // The catalog refused the version although no other writer has it.
                return Err(commit_conflict_error(message));
            }
            if blind_append {
                reference = self
                    .rebase_reference(log_store, reference, &response, actions, operation)
                    .await?;
            }
            // A commit that is not a blind append is not staged on top of the commits of
            // other writers, since what it read from the table is not known here.
            if let Some(snapshot) = &reference
                && latest_version > snapshot.version()
            {
                return Err(commit_conflict_error(&format!(
                    "another writer committed version {latest_version} of the table after \
                     version {} that the write was prepared from",
                    snapshot.version()
                )));
            }
            let staging = CommitBuilder::from(properties.clone())
                .with_actions(actions.to_vec())
                .build(reference.clone(), log_store.clone(), operation.clone())
                .into_staged_commit_future_with_catalog_latest_version(latest_version);
            let (mut staged, latest_backfilled_version) =
                stage_and_publish_ratified_commits(log_store, &response, staging).await?;
            match self
                .try_commit_staged(
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
    /// the commits after the `reference` snapshot. The `response` has the ratified commits
    /// of the table from version 1.
    async fn rebase_reference(
        &self,
        log_store: &LogStoreRef,
        reference: Option<Arc<DeltaSnapshot>>,
        response: &DeltaRatifiedCommitResponse,
        actions: &[CommitAction],
        operation: &DeltaOperation,
    ) -> Result<Option<Arc<DeltaSnapshot>>> {
        let latest_version = response.latest_table_version;
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

        let catalog_managed_commits = Some(
            load_catalog_managed_commits(
                self.context,
                self.lakehouse_table,
                Some(latest_version),
                Some(response.clone()),
            )
            .await?,
        );
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
#[derive(Debug, PartialEq)]
enum StagedCommitState {
    /// The staged commit is the commit of its version.
    Ratified,
    /// The commit of another writer is the commit of the version.
    Lost,
    /// The table does not have the version yet.
    Pending,
}

/// Tells whether the staged commit is the commit of its version in the commits of the table.
async fn staged_commit_state_in(
    log_store: &LogStoreRef,
    response: &DeltaRatifiedCommitResponse,
    staged: &CatalogManagedStagedCommit,
) -> Result<StagedCommitState> {
    if response.latest_table_version < staged.version {
        return Ok(StagedCommitState::Pending);
    }
    let path = ratified_commit_path(response, staged.version);
    if path == staged.staged_path {
        return Ok(StagedCommitState::Ratified);
    }
    // The file name of a published commit does not tell whose commit it is.
    let store = log_store.object_store(None);
    let published = read_object(&store, &path).await?;
    if is_ratified_commit(&published, &read_object(&store, &staged.staged_path).await?) {
        Ok(StagedCommitState::Ratified)
    } else {
        Ok(StagedCommitState::Lost)
    }
}

/// The published commits of a table before a version.
#[derive(Debug, PartialEq)]
struct PublishedCommits {
    /// The latest version up to which every commit is published, which is the version that a
    /// commit request may report as backfilled.
    version: Option<i64>,
    /// Why the commit of the version after it is not published, when it failed to publish.
    failure: Option<String>,
}

/// Returns the error of a commit that the catalog did not ratify.
fn refusal(answer: CatalogResult<DeltaCommitOutcome>) -> CatalogError {
    match answer {
        Ok(DeltaCommitOutcome::Committed) => {
            CatalogError::Internal("the catalog ratified the Delta commit".to_string())
        }
        Ok(DeltaCommitOutcome::Conflict(message)) => CatalogError::Conflict(message),
        Ok(DeltaCommitOutcome::BackfillRequired(message)) => {
            CatalogError::ResourceExhausted(message)
        }
        Ok(DeltaCommitOutcome::Throttled(message)) => CatalogError::RateLimited(message),
        Ok(DeltaCommitOutcome::Unknown(message)) => {
            CatalogError::CommitStateUnknown(message.into())
        }
        Err(error) => error,
    }
}

pub(crate) fn commit_conflict_error(message: &str) -> DataFusionError {
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

/// Returns the path of the commit of the version that the catalog returned, or the path of
/// the published commit of the version when the catalog returned none, since the catalog does
/// not return a commit that is published and reported. A commit whose file name has another
/// version is not the commit of the version.
fn ratified_commit_path(response: &DeltaRatifiedCommitResponse, version: i64) -> Path {
    response
        .commits
        .iter()
        .find(|commit| {
            commit.version == version
                && catalog_managed_commit_version(&commit.file_name) == Some(version)
        })
        .map(|commit| catalog_managed_commit_path(&commit.file_name))
        .unwrap_or_else(|| commit_path(version))
}

async fn read_object(store: &ObjectStoreRef, path: &Path) -> Result<Bytes> {
    store
        .get(path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .bytes()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

/// Checks that the published commit of the version is the ratified commit at the path. A
/// published commit of another commit was not written by publishing the ratified commit, so
/// the ratified commit is not published.
fn check_published_commit(
    published: &Bytes,
    ratified: &Bytes,
    path: &Path,
    version: i64,
) -> Result<()> {
    if is_ratified_commit(published, ratified) {
        Ok(())
    } else {
        Err(DataFusionError::Execution(format!(
            "the published Delta commit `{}` of version {version} is not the ratified commit `{path}`",
            commit_path(version)
        )))
    }
}

/// Whether the published commit is the ratified commit. A writer may publish a ratified commit
/// by writing its lines again, for example with a line terminator after every line, as Delta
/// Spark does, so the commits are compared by their non-empty lines without line terminators,
/// and by the actions on those lines otherwise. This is more lenient than Delta Spark, which
/// also requires the files to have the same length and does not compare the actions.
fn is_ratified_commit(published: &[u8], ratified: &[u8]) -> bool {
    if published == ratified {
        return true;
    }
    let (Ok(published), Ok(ratified)) = (
        std::str::from_utf8(published),
        std::str::from_utf8(ratified),
    ) else {
        return false;
    };
    let (published, ratified) = (commit_lines(published), commit_lines(ratified));
    if published == ratified {
        return true;
    }
    let actions = |lines: &[&str]| {
        lines
            .iter()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .collect::<Option<Vec<_>>>()
    };
    matches!(
        (actions(&published), actions(&ratified)),
        (Some(published), Some(ratified)) if published == ratified
    )
}

/// Returns the non-empty lines of a commit file without their line terminators.
fn commit_lines(commit: &str) -> Vec<&str> {
    commit
        .split('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line))
        .filter(|line| !line.is_empty())
        .collect()
}

/// The ratified commits that this process has published or found published, with the entity
/// tag of their published commits. A ratified commit is not compared with its published commit
/// again while the published commit has the same entity tag.
static PUBLISHED_RATIFIED_COMMITS: LazyLock<moka::sync::Cache<String, String>> =
    LazyLock::new(|| moka::sync::Cache::new(PUBLISHED_RATIFIED_COMMIT_CAPACITY));

/// The number of published ratified commits that are remembered.
const PUBLISHED_RATIFIED_COMMIT_CAPACITY: u64 = 4096;

fn published_ratified_commit_key(log_store: &LogStoreRef, path: &Path) -> String {
    format!("{}/{path}", log_store.root_uri().trim_end_matches('/'))
}

fn is_known_published_ratified_commit(
    log_store: &LogStoreRef,
    path: &Path,
    e_tag: Option<&str>,
) -> bool {
    e_tag.is_some_and(|e_tag| {
        PUBLISHED_RATIFIED_COMMITS
            .get(&published_ratified_commit_key(log_store, path))
            .is_some_and(|known| known == e_tag)
    })
}

fn remember_published_ratified_commit(log_store: &LogStoreRef, path: &Path, e_tag: Option<String>) {
    if let Some(e_tag) = e_tag {
        PUBLISHED_RATIFIED_COMMITS.insert(published_ratified_commit_key(log_store, path), e_tag);
    }
}

/// Publishes the ratified commits before `version` that the catalog returned, in the order of
/// their versions, and returns the latest version up to which every commit is published,
/// which is the version that a commit request may report as backfilled, and why the commit
/// after it failed to publish.
///
/// The catalog returns the ratified commits that are not reported as published, so every
/// version before the first of them, or up to the latest version of the table when there is
/// none, is published. Any other version counts only when its published commit exists, and
/// for a version that the catalog returned, only when the published commit is the ratified
/// commit. Commits are published in the order of their versions, so the
/// publishing stops at the first commit that cannot be published. A commit whose file name
/// has another version is not published, since it is not the commit of its version.
async fn publish_ratified_commits_before(
    log_store: &LogStoreRef,
    response: &DeltaRatifiedCommitResponse,
    version: i64,
) -> PublishedCommits {
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
            (Ok(meta), Some(path)) if path != commit_path(next) => {
                verify_published_commit(log_store, &path, next, meta.e_tag)
                    .await
                    .map_err(|error| (path, error))
            }
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
            let failure = format!(
                "failed to publish the ratified Delta commit `{path}` of version {next}: {error}"
            );
            warn!("{failure}");
            return PublishedCommits {
                version: (published >= 0).then_some(published),
                failure: Some(failure),
            };
        }
        published = next;
    }
    PublishedCommits {
        version: (published >= 0).then_some(published),
        failure: None,
    }
}

/// Stages a commit on top of the latest version of the catalog, and publishes the ratified
/// commits before it at the same time. Returns the staged commit and the latest version up
/// to which every commit is published.
async fn stage_and_publish_ratified_commits(
    log_store: &LogStoreRef,
    response: &DeltaRatifiedCommitResponse,
    staging: BoxFuture<'static, DeltaResult<CatalogManagedStagedCommit>>,
) -> Result<(CatalogManagedStagedCommit, Option<i64>)> {
    let to_error = |e: DeltaError| DataFusionError::External(Box::new(e));
    if response.latest_table_version < 0 {
        // The version of the commit is not known before it is staged.
        let staged = staging.await.map_err(to_error)?;
        let published = publish_ratified_commits_before(log_store, response, staged.version).await;
        return Ok((staged, published.version));
    }
    // The staged commit has the version after the latest version of the catalog.
    let version = response.latest_table_version + 1;
    let (staged, published) = futures::join!(
        staging,
        publish_ratified_commits_before(log_store, response, version)
    );
    Ok((staged.map_err(to_error)?, published.version))
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

/// Checks that the published commit of the version is the ratified commit at the path. A
/// ratified commit that is deleted after it is published cannot be compared, and its published
/// commit is taken as it is. The entity tag is the one of the published commit.
async fn verify_published_commit(
    log_store: &LogStoreRef,
    path: &Path,
    version: i64,
    e_tag: Option<String>,
) -> Result<()> {
    if is_known_published_ratified_commit(log_store, path, e_tag.as_deref()) {
        return Ok(());
    }
    let store = log_store.object_store(None);
    let ratified = match store.get(path).await {
        Ok(result) => result
            .bytes()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
        Err(ObjectStoreError::NotFound { .. }) => return Ok(()),
        Err(e) => return Err(DataFusionError::External(Box::new(e))),
    };
    let published = read_object(&store, &commit_path(version)).await?;
    check_published_commit(&published, &ratified, path, version)?;
    remember_published_ratified_commit(log_store, path, e_tag);
    Ok(())
}

/// Copies a ratified commit to its place in the published Delta log. A commit that is
/// published already is not written again, and must be the ratified commit.
async fn publish_commit(log_store: &LogStoreRef, path: &Path, version: i64) -> Result<()> {
    let published = commit_path(version);
    if *path == published {
        return Ok(());
    }
    let store = log_store.object_store(None);
    let bytes = read_object(&store, path).await?;
    let options = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    match store
        .put_opts(&published, bytes.clone().into(), options)
        .await
    {
        Ok(result) => {
            remember_published_ratified_commit(log_store, path, result.e_tag);
            Ok(())
        }
        Err(ObjectStoreError::AlreadyExists { .. }) => {
            let existing = read_object(&store, &published).await?;
            check_published_commit(&existing, &bytes, path, version)
        }
        Err(err) => Err(DataFusionError::External(Box::new(err))),
    }
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

fn delta_catalog_managed_commit(
    table: &DeltaCatalogManagedTable,
    base: Option<(&Metadata, &Protocol)>,
    staged: &CatalogManagedStagedCommit,
    actions: &[CommitAction],
    latest_backfilled_version: Option<i64>,
) -> DeltaCatalogCommit {
    DeltaCatalogCommit {
        table_id: table.table_id.clone(),
        table_uri: table.table_uri.clone(),
        commit_info: DeltaCatalogCommitInfo {
            version: staged.version,
            timestamp: staged.in_commit_timestamp,
            file_name: staged.file_name.clone(),
            file_size: staged.file_size,
            file_modification_timestamp: staged.file_modification_timestamp,
        },
        latest_backfilled_version,
        delta_log: delta_commit_actions(base, actions, &staged.domain_metadata),
    }
}

/// Returns the metadata action, the protocol action, and the domain metadata actions of
/// the commit. The metadata action and the protocol action come with the metadata and the
/// protocol of the base snapshot.
fn delta_commit_actions(
    base: Option<(&Metadata, &Protocol)>,
    actions: &[CommitAction],
    domain_metadata: &[DomainMetadata],
) -> DeltaCommitActions {
    let metadata = actions.iter().rev().find_map(|action| match action {
        CommitAction::Metadata(metadata) => Some(metadata),
        _ => None,
    });
    let protocol = actions.iter().rev().find_map(|action| match action {
        CommitAction::Protocol(protocol) => Some(protocol),
        _ => None,
    });
    DeltaCommitActions {
        metadata: metadata.map(delta_metadata_info),
        base_metadata: metadata
            .and(base)
            .map(|(metadata, _)| delta_metadata_info(metadata)),
        protocol: protocol.map(delta_protocol_info),
        base_protocol: protocol
            .and(base)
            .map(|(_, protocol)| delta_protocol_info(protocol)),
        domain_metadata: domain_metadata
            .iter()
            .map(|action| DeltaDomainMetadataInfo {
                domain: action.domain.clone(),
                configuration: action.configuration.clone(),
                removed: action.removed,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use sail_common_datafusion::catalog::delta::{DeltaMetadataInfo, DeltaProtocolInfo};
    use url::Url;

    use super::*;
    use crate::spec::{DataType as DeltaDataType, StructField, StructType, TableFeature};

    #[test]
    fn commit_actions_come_with_the_base_snapshot() -> Result<()> {
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
            Some("events".to_string()),
            schema(&["id"]).map_err(external)?,
            vec!["id".to_string()],
            0,
            [("owner".to_string(), "a".to_string())].into(),
        )
        .map_err(external)?;
        let base_protocol = Protocol::new(3, 7, Some(vec![]), Some(vec![]));
        let base = Some((&base_metadata, &base_protocol));
        let base_metadata_info = DeltaMetadataInfo {
            schema_string: base_metadata.schema_string().to_string(),
            partition_columns: vec!["id".to_string()],
            configuration: [("owner".to_string(), "a".to_string())].into(),
            description: Some("events".to_string()),
        };
        let base_protocol_info = DeltaProtocolInfo {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec![]),
            writer_features: Some(vec![]),
        };

        assert_eq!(
            delta_commit_actions(base, &[], &[]),
            DeltaCommitActions::default()
        );
        // The actions of the base snapshot written again come with the base snapshot, so
        // that the catalog can tell that they change nothing.
        let actions = vec![
            CommitAction::Protocol(base_protocol.clone()),
            CommitAction::Metadata(base_metadata.clone()),
        ];
        assert_eq!(
            delta_commit_actions(base, &actions, &[]),
            DeltaCommitActions {
                metadata: Some(base_metadata_info.clone()),
                base_metadata: Some(base_metadata_info.clone()),
                protocol: Some(base_protocol_info.clone()),
                base_protocol: Some(base_protocol_info.clone()),
                domain_metadata: vec![],
            }
        );

        let metadata = base_metadata
            .clone()
            .with_schema(&schema(&["id", "value"]).map_err(external)?)
            .map_err(external)?;
        let protocol = Protocol::new(
            3,
            7,
            Some(vec![TableFeature::DeletionVectors]),
            Some(vec![
                TableFeature::DeletionVectors,
                TableFeature::RowTracking,
            ]),
        );
        let metadata_info = DeltaMetadataInfo {
            schema_string: metadata.schema_string().to_string(),
            ..base_metadata_info.clone()
        };
        let protocol_info = DeltaProtocolInfo {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec!["deletionVectors".to_string()]),
            writer_features: Some(vec![
                "deletionVectors".to_string(),
                "rowTracking".to_string(),
            ]),
        };
        // The last metadata action and the last protocol action of the commit count.
        let actions = vec![
            CommitAction::Protocol(base_protocol.clone()),
            CommitAction::Metadata(base_metadata.clone()),
            CommitAction::Protocol(protocol.clone()),
            CommitAction::Metadata(metadata.clone()),
        ];
        assert_eq!(
            delta_commit_actions(base, &actions, &[]),
            DeltaCommitActions {
                metadata: Some(metadata_info.clone()),
                base_metadata: Some(base_metadata_info.clone()),
                protocol: Some(protocol_info.clone()),
                base_protocol: Some(base_protocol_info),
                domain_metadata: vec![],
            }
        );
        assert_eq!(
            delta_commit_actions(None, &actions, &[]),
            DeltaCommitActions {
                metadata: Some(metadata_info),
                protocol: Some(protocol_info),
                ..Default::default()
            }
        );
        assert_eq!(
            delta_commit_actions(base, &[CommitAction::Metadata(metadata.clone())], &[])
                .base_protocol,
            None
        );

        let domain = DomainMetadata {
            domain: "delta.rowTracking".to_string(),
            configuration: r#"{"rowIdHighWaterMark":41}"#.to_string(),
            removed: false,
        };
        assert_eq!(
            delta_commit_actions(base, &[], std::slice::from_ref(&domain)),
            DeltaCommitActions {
                domain_metadata: vec![DeltaDomainMetadataInfo {
                    domain: "delta.rowTracking".to_string(),
                    configuration: r#"{"rowIdHighWaterMark":41}"#.to_string(),
                    removed: false,
                }],
                ..Default::default()
            }
        );
        Ok(())
    }

    /// The commit carries the table metadata once, in the form of the Delta log, even for
    /// metadata that has no form in the legacy commit API of Unity Catalog.
    #[test]
    fn commit_carries_the_delta_log_actions() -> Result<()> {
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
            file_size: 2,
            file_modification_timestamp: 3,
            in_commit_timestamp: 4,
            staged_path: object_store::path::Path::from("t/_delta_log/_staged_commits/1.json"),
            domain_metadata: vec![],
            metrics: Default::default(),
        };
        let base_metadata = metadata(DeltaDataType::LONG).map_err(external)?;
        let base_protocol = Protocol::new(3, 7, Some(vec![]), Some(vec![]));
        let base = Some((&base_metadata, &base_protocol));

        let commit = delta_catalog_managed_commit(&table, base, &staged, &[], None);
        assert_eq!(
            commit,
            DeltaCatalogCommit {
                table_id: "id".to_string(),
                table_uri: "memory:///t".to_string(),
                commit_info: DeltaCatalogCommitInfo {
                    version: 1,
                    timestamp: 4,
                    file_name: "00000000000000000001.uuid.json".to_string(),
                    file_size: 2,
                    file_modification_timestamp: 3,
                },
                latest_backfilled_version: None,
                delta_log: DeltaCommitActions::default(),
            }
        );

        let variant = metadata(DeltaDataType::unshredded_variant()).map_err(external)?;
        let actions = vec![CommitAction::Metadata(variant.clone())];
        let commit = delta_catalog_managed_commit(&table, base, &staged, &actions, Some(0));
        assert_eq!(commit.latest_backfilled_version, Some(0));
        assert_eq!(
            commit.delta_log.metadata,
            Some(delta_metadata_info(&variant))
        );
        assert_eq!(
            commit.delta_log.base_metadata,
            Some(delta_metadata_info(&base_metadata))
        );
        Ok(())
    }

    /// A case of publishing the ratified commits before a commit.
    struct PublishCase {
        name: &'static str,
        /// The published commits and the staged commits (`<version>.<id>`) in the log. A
        /// published commit has the bytes of the staged commits of its version, unless it is
        /// written by another writer (`<version>!`).
        files: &'static [&'static str],
        /// The published commits whose lookup fails.
        failing: &'static [i64],
        latest_table_version: i64,
        /// The versions and the staged commits (`<version>.<id>`) that the catalog returns.
        commits: &'static [(i64, &'static str)],
        version: i64,
        expected: Option<i64>,
        /// The published commits after publishing. Each has the bytes of the staged commits
        /// of its version, unless it was written by another writer.
        published: &'static [i64],
    }

    /// A table URL of its own for each test, since the published ratified commits that this
    /// process remembers are keyed by the table URL.
    fn test_table_url() -> Result<Url> {
        Url::parse(&format!("memory:///{}/", Uuid::new_v4()))
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    fn log_file(name: &str) -> Path {
        let name = name.trim_end_matches('!');
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
                commits: &[(3, "3.a"), (5, "5.b")],
                version: 6,
                expected: Some(3),
                published: &[0, 1, 2, 3],
            },
            PublishCase {
                name: "publishing stops at a failed lookup",
                files: &["0", "1.a", "2.b"],
                failing: &[1],
                latest_table_version: 2,
                commits: &[(1, "1.a"), (2, "2.b")],
                version: 3,
                expected: Some(0),
                published: &[0],
            },
            PublishCase {
                name: "a published commit is not written again",
                files: &["0", "1", "1.a"],
                failing: &[],
                latest_table_version: 1,
                commits: &[(1, "1.a")],
                version: 2,
                expected: Some(1),
                published: &[0, 1],
            },
            PublishCase {
                name: "publishing stops at a published commit of another writer",
                files: &["0", "1!", "1.a", "2.b"],
                failing: &[],
                latest_table_version: 2,
                commits: &[(1, "1.a"), (2, "2.b")],
                version: 3,
                expected: Some(0),
                published: &[0, 1],
            },
            PublishCase {
                name: "a commit whose file name has another version is not published",
                files: &["0", "1.a"],
                failing: &[],
                latest_table_version: 2,
                // The file name of the commit has the version before it.
                commits: &[(2, "1.a")],
                version: 3,
                expected: Some(1),
                published: &[0],
            },
        ];
        for case in cases {
            let store = Arc::new(ObservedStore::default());
            for file in case.files {
                let bytes = match file.strip_suffix('!') {
                    Some(_) => "another writer\n".to_string(),
                    None => format!("{}\n", file.split('.').next().unwrap_or_default()),
                };
                store
                    .put(&log_file(file), bytes.into())
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            }
            for version in case.failing {
                store.fail_lookups(commit_path(*version));
            }
            let object_store = store.clone() as Arc<dyn object_store::ObjectStore>;
            let url = test_table_url()?;
            let log_store =
                default_logstore(object_store.clone(), object_store, &url, &StorageConfig);
            let response = DeltaRatifiedCommitResponse {
                latest_table_version: case.latest_table_version,
                commits: case
                    .commits
                    .iter()
                    .map(
                        |(version, file)| sail_catalog::lakehouse::DeltaRatifiedCommit {
                            version: *version,
                            timestamp: 1,
                            file_name: log_file(file).filename().unwrap_or_default().to_string(),
                            file_size: 1,
                            file_modification_timestamp: 1,
                        },
                    )
                    .collect(),
            };
            let actual = publish_ratified_commits_before(&log_store, &response, case.version).await;
            assert_eq!(actual.version, case.expected, "{}", case.name);
            let mut published = vec![];
            for version in 0..case.version {
                if case.failing.contains(&version) {
                    continue;
                }
                let Ok(commit) = store.get(&commit_path(version)).await else {
                    continue;
                };
                let bytes = commit
                    .bytes()
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let expected = if case.files.contains(&format!("{version}!").as_str()) {
                    "another writer\n".to_string()
                } else {
                    format!("{version}\n")
                };
                assert_eq!(bytes.as_ref(), expected.as_bytes(), "{}", case.name);
                published.push(version);
            }
            assert_eq!(published, case.published, "{}", case.name);
        }
        Ok(())
    }
    #[tokio::test]
    async fn a_published_commit_of_another_writer_is_not_the_ratified_commit() -> Result<()> {
        use crate::delta_log::{StorageConfig, default_logstore};

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let url = test_table_url()?;
        let log_store = default_logstore(store.clone(), store.clone(), &url, &StorageConfig);
        let put = async |name: &str, bytes: &'static str| {
            store
                .put(&log_file(name), bytes.to_string().into())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))
        };
        put("1.a", "ratified\n").await?;
        put("1", "ratified\n").await?;
        put("2.b", "ratified\n").await?;
        put("2", "another writer\n").await?;
        // A published commit with the bytes of the ratified commit is the ratified commit.
        publish_commit(&log_store, &log_file("1.a"), 1).await?;
        assert!(
            publish_commit(&log_store, &log_file("2.b"), 2)
                .await
                .is_err()
        );
        let published = read_object(&store, &commit_path(2)).await?;
        assert_eq!(published.as_ref(), b"another writer\n");
        Ok(())
    }

    #[tokio::test]
    async fn a_directory_at_the_published_path_is_not_a_published_commit() -> Result<()> {
        use crate::delta_log::{StorageConfig, default_logstore};

        let to_error = |e: std::io::Error| DataFusionError::External(Box::new(e));
        let root = std::env::temp_dir().join(format!("sail-publish-{}", Uuid::new_v4()));
        std::fs::create_dir_all(root.join("_delta_log/_staged_commits")).map_err(to_error)?;
        // A directory blocks the publishing of version 1.
        std::fs::create_dir_all(root.join(format!("_delta_log/{:020}.json", 1)))
            .map_err(to_error)?;
        std::fs::write(root.join(format!("_delta_log/{:020}.json", 0)), "0\n").map_err(to_error)?;
        for (version, id) in [(1, "a"), (2, "b")] {
            std::fs::write(
                root.join(format!(
                    "_delta_log/_staged_commits/{version:020}.{id}.json"
                )),
                format!("{version}\n"),
            )
            .map_err(to_error)?;
        }
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(&root)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );
        let url = test_table_url()?;
        let log_store = default_logstore(store.clone(), store, &url, &StorageConfig);
        let response = DeltaRatifiedCommitResponse {
            latest_table_version: 2,
            commits: [(1, "a"), (2, "b")]
                .into_iter()
                .map(
                    |(version, id)| sail_catalog::lakehouse::DeltaRatifiedCommit {
                        version,
                        timestamp: 1,
                        file_name: format!("{version:020}.{id}.json"),
                        file_size: 2,
                        file_modification_timestamp: 1,
                    },
                )
                .collect(),
        };
        let published = publish_ratified_commits_before(&log_store, &response, 3)
            .await
            .version;
        let commit_2_exists = root.join(format!("_delta_log/{:020}.json", 2)).exists();
        std::fs::remove_dir_all(&root).map_err(to_error)?;
        // Version 1 is not published, so it is not reported, and version 2 is not published.
        assert_eq!(published, Some(0));
        assert!(!commit_2_exists);
        Ok(())
    }

    #[test]
    fn the_ratified_commit_path_ignores_a_commit_with_another_version() {
        let response = |file_name: &str| DeltaRatifiedCommitResponse {
            latest_table_version: 2,
            commits: vec![sail_catalog::lakehouse::DeltaRatifiedCommit {
                version: 2,
                timestamp: 1,
                file_name: file_name.to_string(),
                file_size: 1,
                file_modification_timestamp: 1,
            }],
        };
        assert_eq!(
            ratified_commit_path(&response(&format!("{:020}.a.json", 2)), 2),
            log_file("2.a")
        );
        // The file name of the commit has version 1, so the catalog returned no commit for
        // version 2, and the published commit of version 2 is the one to compare.
        assert_eq!(
            ratified_commit_path(&response(&format!("{:020}.a.json", 1)), 2),
            commit_path(2)
        );
        assert_eq!(
            ratified_commit_path(&response(&format!("{:020}.a.json", 2)), 3),
            commit_path(3)
        );
    }
    /// A commit written by Sail, with no line terminator after the last action.
    fn commit_bytes(txn_id: Option<&str>, value: i64) -> String {
        let commit_info = match txn_id {
            Some(txn_id) => {
                serde_json::json!({"commitInfo": {"txnId": txn_id, "operation": "WRITE"}})
            }
            None => serde_json::json!({"commitInfo": {"operation": "WRITE"}}),
        };
        let add =
            serde_json::json!({"add": {"path": format!("part-{value}.parquet"), "size": value}});
        format!("{commit_info}\n{add}")
    }

    /// The commit written again with a line terminator after every action, as another writer
    /// may publish it.
    fn rewritten(bytes: &str, terminator: &str) -> String {
        bytes
            .lines()
            .map(|line| format!("{line}{terminator}"))
            .collect::<String>()
    }

    #[test]
    fn a_published_commit_is_identified_by_its_lines() {
        let ratified = commit_bytes(Some("t1"), 1);
        let cases = [
            ("the same bytes", ratified.clone(), true),
            (
                "a line feed after every line",
                rewritten(&ratified, "\n"),
                true,
            ),
            ("CRLF after every line", rewritten(&ratified, "\r\n"), true),
            (
                "an empty line at the end",
                rewritten(&ratified, "\n") + "\n",
                true,
            ),
            ("another transaction", commit_bytes(Some("t2"), 1), false),
            (
                "another transaction with the same actions",
                rewritten(&commit_bytes(Some("t2"), 1), "\n"),
                false,
            ),
            (
                "no transaction and other actions",
                commit_bytes(None, 2),
                false,
            ),
            ("no JSON", "another writer\n".to_string(), false),
        ];
        for (name, published, expected) in cases {
            assert_eq!(
                is_ratified_commit(published.as_bytes(), ratified.as_bytes()),
                expected,
                "{name}"
            );
        }
        // A commit written with another order of the keys has the same actions.
        assert!(is_ratified_commit(
            br#"{"commitInfo":{"operation":"WRITE","txnId":"t1"}}
"#,
            br#"{"commitInfo":{"txnId":"t1","operation":"WRITE"}}"#
        ));
        // Commits without a transaction are compared by their actions.
        let ratified = commit_bytes(None, 1);
        assert!(is_ratified_commit(
            rewritten(&ratified, "\n").as_bytes(),
            ratified.as_bytes()
        ));
        assert!(!is_ratified_commit(
            commit_bytes(None, 2).as_bytes(),
            ratified.as_bytes()
        ));
    }

    #[tokio::test]
    async fn a_rewritten_published_commit_is_the_ratified_commit() -> Result<()> {
        use crate::delta_log::{StorageConfig, default_logstore};

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let url = test_table_url()?;
        let log_store = default_logstore(store.clone(), store.clone(), &url, &StorageConfig);
        let put = async |name: &str, bytes: String| {
            store
                .put(&log_file(name), bytes.into())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))
        };
        put("0", commit_bytes(Some("t0"), 0)).await?;
        put("1.a", commit_bytes(Some("t1"), 1)).await?;
        put("1", rewritten(&commit_bytes(Some("t1"), 1), "\n")).await?;
        put("2.b", commit_bytes(Some("t2"), 2)).await?;
        put("2", rewritten(&commit_bytes(Some("other"), 2), "\n")).await?;
        put("3.c", commit_bytes(Some("t3"), 3)).await?;
        let response = DeltaRatifiedCommitResponse {
            latest_table_version: 3,
            commits: [(1, "a"), (2, "b"), (3, "c")]
                .into_iter()
                .map(
                    |(version, id)| sail_catalog::lakehouse::DeltaRatifiedCommit {
                        version,
                        timestamp: 1,
                        file_name: format!("{version:020}.{id}.json"),
                        file_size: 1,
                        file_modification_timestamp: 1,
                    },
                )
                .collect(),
        };
        // Version 1 is published by another writer, and the published commit of version 2 is
        // the commit of another transaction, which the failure names.
        let published = publish_ratified_commits_before(&log_store, &response, 4).await;
        assert_eq!(published.version, Some(1));
        let failure = published.failure.unwrap_or_default();
        assert!(
            failure.contains("version 2") && failure.contains(commit_path(2).as_ref()),
            "{failure}"
        );
        assert!(store.head(&commit_path(3)).await.is_err());
        // Publishing again finds version 1 published.
        publish_commit(&log_store, &log_file("1.a"), 1).await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_published_commit_that_is_overwritten_is_compared_again() -> Result<()> {
        use crate::delta_log::{StorageConfig, default_logstore};

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let url = test_table_url()?;
        let log_store = default_logstore(store.clone(), store.clone(), &url, &StorageConfig);
        let put = async |path: Path, bytes: String| {
            store
                .put(&path, bytes.into())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))
        };
        put(log_file("0"), commit_bytes(Some("t0"), 0)).await?;
        put(log_file("1.a"), commit_bytes(Some("t1"), 1)).await?;
        publish_commit(&log_store, &log_file("1.a"), 1).await?;
        let response = DeltaRatifiedCommitResponse {
            latest_table_version: 1,
            commits: vec![sail_catalog::lakehouse::DeltaRatifiedCommit {
                version: 1,
                timestamp: 1,
                file_name: format!("{:020}.a.json", 1),
                file_size: 1,
                file_modification_timestamp: 1,
            }],
        };
        let published = publish_ratified_commits_before(&log_store, &response, 2).await;
        assert_eq!(published.version, Some(1));
        put(commit_path(1), "{not valid delta json\n".to_string()).await?;
        let published = publish_ratified_commits_before(&log_store, &response, 2).await;
        assert_eq!(published.version, Some(0));
        assert!(published.failure.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn a_staged_commit_published_by_another_writer_is_ratified() -> Result<()> {
        use crate::delta_log::{StorageConfig, default_logstore};

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let url = test_table_url()?;
        let log_store = default_logstore(store.clone(), store.clone(), &url, &StorageConfig);
        let staged = |id: &str| CatalogManagedStagedCommit {
            version: 1,
            file_name: format!("{:020}.{id}.json", 1),
            file_size: 1,
            file_modification_timestamp: 1,
            in_commit_timestamp: 1,
            staged_path: log_file(&format!("1.{id}")),
            domain_metadata: vec![],
            metrics: Default::default(),
        };
        for (id, txn_id) in [("a", "t1"), ("b", "t2")] {
            store
                .put(
                    &log_file(&format!("1.{id}")),
                    commit_bytes(Some(txn_id), 1).into(),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }
        let response = |latest_table_version| DeltaRatifiedCommitResponse {
            latest_table_version,
            commits: vec![],
        };
        for terminator in ["\n", "\r\n"] {
            // The catalog returns no commit for version 1, since it is published and reported.
            store
                .put(
                    &commit_path(1),
                    rewritten(&commit_bytes(Some("t1"), 1), terminator).into(),
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            assert_eq!(
                staged_commit_state_in(&log_store, &response(1), &staged("a")).await?,
                StagedCommitState::Ratified
            );
            assert_eq!(
                staged_commit_state_in(&log_store, &response(1), &staged("b")).await?,
                StagedCommitState::Lost
            );
        }
        assert_eq!(
            staged_commit_state_in(&log_store, &response(0), &staged("b")).await?,
            StagedCommitState::Pending
        );
        Ok(())
    }
}
