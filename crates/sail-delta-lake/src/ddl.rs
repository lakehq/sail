use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion_common::{DataFusionError, Result, plan_err};
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::catalog::coordinator::{DeltaCatalogCommitCoordinator, DeltaCatalogManagedTable};
use crate::catalog_managed::catalog_managed_delta_table;
use crate::delta_log::{LogStoreRef, StorageConfig};
use crate::physical_plan::DeltaCommitExec;
use crate::snapshot::{DeltaSnapshot, DeltaSnapshotConfig};
use crate::spec::{CommitAction, DeltaOperation};
use crate::table::{
    DeltaTable, catalog_managed_commit_context, create_logstore_with_object_store,
    load_catalog_managed_commits_for_snapshot,
};
use crate::transaction::CommitBuilder;

pub(crate) async fn open_table(
    ctx: &TaskContext,
    path: &str,
    lakehouse: Option<&LakehouseExecutionContext>,
) -> Result<DeltaTable> {
    let url = crate::lake_source::parse_location_to_url(path)?;
    let store = ctx.runtime_env().object_store_registry.get_store(&url)?;
    let log_store = create_logstore_with_object_store(store, url.clone(), StorageConfig)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let mut config = DeltaSnapshotConfig {
        require_files: false,
        ..Default::default()
    };
    if let Some(lakehouse) = catalog_managed_commit_context(lakehouse) {
        config.catalog_managed_commits = load_catalog_managed_commits_for_snapshot(
            ctx,
            lakehouse,
            &url,
            log_store.clone(),
            None,
        )
        .await?;
    }
    let mut table = DeltaTable::new(log_store, config);
    table
        .load()
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    Ok(table)
}

pub(crate) async fn commit(
    ctx: &TaskContext,
    lakehouse: Option<&LakehouseExecutionContext>,
    snapshot: Arc<DeltaSnapshot>,
    log_store: LogStoreRef,
    actions: Vec<CommitAction>,
    operation: DeltaOperation,
) -> Result<()> {
    let Some(lakehouse) = catalog_managed_commit_context(lakehouse) else {
        return CommitBuilder::default()
            .with_actions(actions)
            .build(Some(snapshot), log_store, operation)
            .await
            .map(|_| ())
            .map_err(|error| DataFusionError::External(Box::new(error)));
    };
    let manager = ctx.extension::<CatalogManager>()?;
    let status = manager
        .get_table(lakehouse.catalog_table())
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let Some(table) = catalog_managed_delta_table(status.kind) else {
        return plan_err!("Missing Unity Catalog identity for managed Delta ALTER TABLE");
    };
    let table = DeltaCatalogManagedTable {
        table_id: table.table_id,
        table_uri: table.location.ok_or_else(|| {
            DataFusionError::Plan("Missing managed Delta table location".to_string())
        })?,
    };
    let coordinator = DeltaCatalogCommitCoordinator::new(ctx, lakehouse.catalog_table());
    let latest = coordinator.latest_table_version(lakehouse, &table).await?;
    let staged = CommitBuilder::default()
        .with_actions(actions.clone())
        .build(Some(snapshot), log_store.clone(), operation)
        .into_staged_commit_future_with_catalog_latest_version(latest)
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let backfilled =
        DeltaCommitExec::latest_published_backfilled_version(&log_store, staged.version - 1)
            .await?;
    // An uncertain catalog response must retain the staged file for reconciliation.
    coordinator
        .commit_staged(lakehouse, &table, &staged, &actions, backfilled)
        .await?;
    if let Err(error) = DeltaCommitExec::publish_staged_commit(&log_store, &staged).await {
        log::warn!(
            "Failed to publish ratified Delta ALTER commit {}: {error}",
            staged.version
        );
    }
    Ok(())
}
