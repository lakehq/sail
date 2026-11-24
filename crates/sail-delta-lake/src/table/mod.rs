// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: Credit: https://github.com/delta-io/delta-rs/blob/1f0b4d0965a85400c1effc6e9b4c7ebbb6795978/crates/core/src/table/mod.rs
// SPDX-FileAttributionText: License: https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
// SPDX-FileCopyrightText: Copyright (2020) QP Hou and a number of other contributors.
// SPDX-FileCopyrightText: Portions Copyright (2025) LakeSail, Inc.
// SPDX-FileContributor: Modified by LakeSail, Inc. in 2025

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion_common::{DataFusionError, Result};
use object_store::ObjectStore;
use sail_catalog::lakehouse::DeltaRatifiedCommitResponse;
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::catalog::delta::{
    DELTA_UNITY_TABLE_ID_KEY, DELTA_UNITY_TABLE_ID_LEGACY_KEY, DeltaCatalogManaged,
    DeltaCatalogState,
};
use sail_common_datafusion::catalog::{
    CommitAuthority, LakehouseExecutionContext, TableColumnStatus, identity_partition_fields,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakesource::LakeSourceMetadata;
use url::Url;

use crate::catalog::coordinator::DeltaCatalogCommitCoordinator;
use crate::catalog_managed::{
    CatalogManagedDeltaTable, catalog_managed_delta_table, metadata_with_catalog_managed,
    protocol_with_catalog_managed,
};
use crate::datasource::{DeltaScanConfig, df_logical_schema};
pub mod features;
pub use features::{
    ChangeDataFeedSupport, ChangeDataFeedToken, ColumnMappingToken, DeletionVectorToken,
    EnabledRowTrackingToken, RowTrackingToken, SupportedRowTrackingToken,
};

use crate::delta_log::{LogStoreRef, StorageConfig, default_logstore, resolve_version_timestamp};
use crate::logical::table_source::DeltaTableSource;
use crate::options::r#gen::DeltaReadOptions;
use crate::schema::{
    attach_column_mapping_metadata, metadata_for_create_with_struct_type, normalize_delta_schema,
    protocol_for_create, schema_has_column_defaults, schema_has_generated_columns,
    schema_has_identity_columns,
};
pub use crate::snapshot::DeltaSnapshot;
use crate::snapshot::{
    CatalogManagedCommitFile, CatalogManagedCommitSet, DeltaSnapshotConfig,
    catalog_managed_commit_version,
};
use crate::spec::{
    ColumnMappingMode, CommitAction, DeltaError, DeltaError as DeltaTableError, DeltaOperation,
    DeltaResult, SaveMode, StructType, contains_timestampntz_arrow, contains_variant_arrow,
};
use crate::transaction::{CommitBuilder, CommitProperties};

/// In memory representation of a Delta Table
///
/// A DeltaTable is a purely logical concept that represents a dataset that can evolve over time.
/// To attain concrete information about a table a snapshot need to be loaded.
/// Most commonly this is the latest state of the table, but may also loaded for a specific
/// version or point in time.
#[derive(Clone)]
pub struct DeltaTable {
    /// The state of the table as of the most recent loaded Delta log entry.
    pub state: Option<Arc<DeltaSnapshot>>,
    /// the load options used during load
    pub config: DeltaSnapshotConfig,
    /// log store
    pub(crate) log_store: LogStoreRef,
}

impl DeltaTable {
    /// Create a new Delta Table struct without loading any data from backing storage.
    ///
    /// NOTE: This is for advanced users. If you don't know why you need to use this method, please
    /// call one of the `open_table` helper methods instead.
    pub fn new(log_store: LogStoreRef, config: DeltaSnapshotConfig) -> Self {
        Self {
            state: None,
            log_store,
            config,
        }
    }

    /// Load DeltaTable with data from latest checkpoint
    pub async fn load(&mut self) -> Result<(), DeltaTableError> {
        self.update_incremental(None).await
    }

    /// Loads the DeltaTable state for the given version.
    pub async fn load_version(&mut self, version: i64) -> Result<(), DeltaTableError> {
        if let Some(snapshot) = &self.state
            && snapshot.version() > version
        {
            self.state = None;
        }
        self.update_incremental(Some(version)).await
    }

    /// Get the timestamp of a given version commit.
    pub(crate) async fn get_version_timestamp(&self, version: i64) -> Result<i64, DeltaTableError> {
        let snapshot = self.snapshot()?;
        resolve_version_timestamp(
            self.log_store.as_ref(),
            version,
            snapshot.version_timestamp(version),
            snapshot.protocol(),
            snapshot.metadata(),
        )
        .await
    }

    /// Updates the DeltaTable to the latest version by incrementally applying newer versions.
    /// It assumes that the table is already updated to the current version `self.version`.
    pub async fn update_incremental(
        &mut self,
        max_version: Option<i64>,
    ) -> Result<(), DeltaTableError> {
        match self.state.as_mut() {
            Some(state) => {
                Arc::make_mut(state)
                    .update(self.log_store.as_ref(), max_version.map(|v| v as u64))
                    .await?;
                Ok(())
            }
            _ => {
                let state = DeltaSnapshot::try_new(
                    self.log_store.as_ref(),
                    self.config.clone(),
                    max_version,
                    None,
                )
                .await?;
                self.state = Some(Arc::new(state));
                Ok(())
            }
        }
    }

    /// Returns the currently loaded state snapshot.
    pub fn snapshot(&self) -> DeltaResult<&Arc<DeltaSnapshot>> {
        self.state
            .as_ref()
            .ok_or_else(|| DeltaTableError::generic("Table has not yet been initialized"))
    }

    /// Currently loaded version of the table - if any.
    pub fn version(&self) -> Option<i64> {
        self.state.as_ref().map(|s| s.version())
    }

    /// The URI of the underlying data
    pub fn table_uri(&self) -> String {
        self.log_store.root_uri()
    }

    /// get a shared reference to the log store
    pub fn log_store(&self) -> LogStoreRef {
        self.log_store.clone()
    }
}

impl fmt::Display for DeltaTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "DeltaTable({})", self.table_uri())?;
        writeln!(f, "\tversion: {:?}", self.version())
    }
}

impl std::fmt::Debug for DeltaTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "DeltaTable <{}>", self.table_uri())
    }
}

pub async fn open_table_with_object_store(
    location: Url,
    object_store: Arc<dyn ObjectStore>,
    storage_options: StorageConfig,
) -> DeltaResult<DeltaTable> {
    let log_store =
        create_logstore_with_object_store(object_store.clone(), location, storage_options)?;

    let mut table = DeltaTable::new(log_store, Default::default());
    table.load().await?;

    Ok(table)
}

/// Open and load a Delta table with an explicit kernel load config.
///
/// This is primarily useful for planning-time code paths where we want to avoid eagerly loading
/// file-level metadata on the driver (e.g. `require_files=false`).
pub async fn open_table_with_object_store_and_table_config(
    location: Url,
    object_store: Arc<dyn ObjectStore>,
    storage_options: StorageConfig,
    table_config: DeltaSnapshotConfig,
) -> DeltaResult<DeltaTable> {
    let log_store =
        create_logstore_with_object_store(object_store.clone(), location, storage_options)?;

    let mut table = DeltaTable::new(log_store, table_config);
    table.load().await?;

    Ok(table)
}

/// Open and load a Delta table with an explicit kernel load config at a fixed version.
pub async fn open_table_with_object_store_and_table_config_at_version(
    location: Url,
    object_store: Arc<dyn ObjectStore>,
    storage_options: StorageConfig,
    table_config: DeltaSnapshotConfig,
    version: i64,
) -> DeltaResult<DeltaTable> {
    let log_store =
        create_logstore_with_object_store(object_store.clone(), location, storage_options)?;

    let mut table = DeltaTable::new(log_store, table_config);
    table.load_version(version).await?;

    Ok(table)
}

pub(crate) async fn create_delta_table_with_object_store(
    location: Url,
    object_store: Arc<dyn ObjectStore>,
    storage_options: StorageConfig,
) -> DeltaResult<DeltaTable> {
    let log_store = create_logstore_with_object_store(object_store, location, storage_options)?;

    let table = DeltaTable::new(log_store, Default::default());
    Ok(table)
}

pub(crate) fn create_logstore_with_object_store(
    object_store: Arc<dyn ObjectStore>,
    location: Url,
    storage_config: StorageConfig,
) -> DeltaResult<LogStoreRef> {
    let prefixed_store = storage_config.decorate_store(Arc::clone(&object_store), &location)?;

    let log_store = default_logstore(
        Arc::new(prefixed_store),
        object_store,
        &location,
        &storage_config,
    );

    Ok(log_store)
}

/// Creates a Delta Lake table source for logical planning.
pub async fn create_delta_source(
    ctx: &dyn Session,
    table_url: Url,
    schema: Option<Schema>,
    options: DeltaReadOptions,
    lakehouse_table: Option<LakehouseExecutionContext>,
) -> Result<Arc<dyn datafusion::logical_expr::TableSource>> {
    let read_options = options.clone();
    let (snapshot, log_store, scan_config) =
        load_delta_read_state(ctx, table_url, schema, options, false, lakehouse_table).await?;

    if read_options.read_change_feed {
        return Ok(Arc::new(
            crate::change_data_feed::ChangeDataFeedSource::try_new(
                snapshot,
                log_store,
                &read_options,
            )
            .await?,
        ));
    }
    Ok(Arc::new(DeltaTableSource::try_new(
        snapshot,
        log_store,
        scan_config,
    )?))
}

/// Infers the Delta logical schema for planning without constructing a logical read source.
pub async fn infer_delta_logical_schema(
    ctx: &dyn Session,
    table_url: Url,
    schema: Option<Schema>,
    options: DeltaReadOptions,
    lakehouse_table: Option<LakehouseExecutionContext>,
) -> Result<SchemaRef> {
    let metadata =
        infer_delta_logical_metadata(ctx, table_url, schema, options, lakehouse_table).await?;
    Ok(metadata.schema)
}

/// Infers the Delta logical schema, partitioning, and table configuration for planning.
pub async fn infer_delta_logical_metadata(
    ctx: &dyn Session,
    table_url: Url,
    schema: Option<Schema>,
    options: DeltaReadOptions,
    lakehouse_table: Option<LakehouseExecutionContext>,
) -> Result<LakeSourceMetadata> {
    let read_options = options.clone();
    let (snapshot, log_store, scan_config) =
        load_delta_read_state(ctx, table_url, schema, options, true, lakehouse_table).await?;

    let schema = if read_options.read_change_feed {
        use datafusion::logical_expr::TableSource;
        crate::change_data_feed::ChangeDataFeedSource::try_new(
            snapshot.clone(),
            log_store,
            &read_options,
        )
        .await?
        .schema()
    } else {
        df_logical_schema(
            snapshot.as_ref(),
            &scan_config.file_column_name,
            &scan_config.row_index_column_name,
            &scan_config.commit_version_column_name,
            &scan_config.commit_timestamp_column_name,
            scan_config.schema,
        )?
    };
    let properties = snapshot
        .metadata()
        .configuration()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    Ok(LakeSourceMetadata {
        schema,
        properties,
        partition_by: Some(identity_partition_fields(
            snapshot.metadata().partition_columns(),
        )),
    })
}

pub(crate) async fn delta_table_properties(
    ctx: &dyn Session,
    table_url: Url,
    options: DeltaReadOptions,
    lakehouse_table: Option<LakehouseExecutionContext>,
) -> Result<Vec<(String, String)>> {
    let (snapshot, _, _) =
        load_delta_read_state(ctx, table_url, None, options, true, lakehouse_table).await?;
    let mut properties = snapshot.metadata().configuration().clone();
    properties.remove("path");
    let protocol = snapshot.protocol();
    properties.insert(
        "delta.minReaderVersion".to_string(),
        protocol.min_reader_version().to_string(),
    );
    properties.insert(
        "delta.minWriterVersion".to_string(),
        protocol.min_writer_version().to_string(),
    );
    for feature in protocol
        .reader_features()
        .unwrap_or_default()
        .iter()
        .chain(protocol.writer_features().unwrap_or_default())
    {
        properties.insert(
            format!("delta.feature.{}", feature.as_str()),
            "supported".to_string(),
        );
    }
    Ok(properties.into_iter().collect())
}

fn delta_read_snapshot_config(
    metadata_only: bool,
    options: &DeltaReadOptions,
) -> DeltaSnapshotConfig {
    if metadata_only || options.metadata_as_data_read || options.read_change_feed {
        DeltaSnapshotConfig {
            require_files: false,
            ..Default::default()
        }
    } else {
        Default::default()
    }
}

fn effective_catalog_commit_end_version(
    latest_table_version: i64,
    requested_end_version: Option<i64>,
) -> Option<i64> {
    if latest_table_version < 1 {
        return None;
    }
    Some(
        requested_end_version
            .map(|version| version.min(latest_table_version))
            .unwrap_or(latest_table_version),
    )
}

/// Returns the start version of the next page of ratified commits, or `None` when the page
/// reaches the end version or has no commits.
fn next_catalog_commit_start_version(
    current_start_version: i64,
    effective_end_version: i64,
    versions: impl IntoIterator<Item = i64>,
) -> DeltaResult<Option<i64>> {
    let Some(page_max_version) = versions.into_iter().max() else {
        return Ok(None);
    };
    if page_max_version < current_start_version {
        return Err(DeltaTableError::generic(format!(
            "Catalog-managed Delta commit page ended at version {page_max_version}, before requested start version {current_start_version}"
        )));
    }
    if page_max_version >= effective_end_version {
        Ok(None)
    } else {
        Ok(Some(page_max_version + 1))
    }
}

fn is_missing_delta_log_error(error: &DeltaTableError) -> bool {
    matches!(
        error,
        DeltaTableError::InvalidTableLocation(message)
            if message.contains("No commit files found in _delta_log")
    )
}

pub(crate) fn catalog_managed_commit_context(
    lakehouse_table: Option<&LakehouseExecutionContext>,
) -> Option<&LakehouseExecutionContext> {
    lakehouse_table.filter(|context| context.commit == CommitAuthority::DeltaRatifiedCommit)
}

/// Whether a commit of the actions to the snapshot uses the catalog-managed protocol:
/// the last protocol action in the commit, or else the protocol of the snapshot.
pub(crate) fn is_catalog_managed_commit(
    snapshot: Option<&DeltaSnapshot>,
    actions: &[crate::spec::CommitAction],
) -> bool {
    let protocol = crate::spec::CommitAction::last_protocol(actions)
        .or_else(|| snapshot.map(|snapshot| snapshot.protocol()));
    protocol.is_some_and(|protocol| protocol.is_catalog_managed())
}

async fn load_catalog_managed_delta_bootstrap_info<C>(
    ctx: &C,
    catalog_table: &[String],
) -> Result<Option<CatalogManagedDeltaTable>>
where
    C: SessionExtensionAccessor + ?Sized,
{
    let manager = ctx.extension::<CatalogManager>()?;
    let status = manager
        .get_table(catalog_table)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(catalog_managed_delta_table(status.kind))
}

fn is_unity_catalog_response_property(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "comment"
            | "created_at"
            | "created_by"
            | "owner"
            | "table_type"
            | "updated_at"
            | "updated_by"
    ) || key.eq_ignore_ascii_case(DELTA_UNITY_TABLE_ID_KEY)
        || key.eq_ignore_ascii_case(DELTA_UNITY_TABLE_ID_LEGACY_KEY)
}

fn delta_bootstrap_configuration(properties: Vec<(String, String)>) -> HashMap<String, String> {
    properties
        .into_iter()
        .filter(|(key, _)| !is_unity_catalog_response_property(key))
        .collect()
}

fn catalog_managed_bootstrap_actions(
    info: CatalogManagedDeltaTable,
    table_url: &Url,
) -> Result<(Vec<CommitAction>, DeltaOperation)> {
    let CatalogManagedDeltaTable {
        columns,
        comment,
        location,
        partition_by,
        properties,
        table_id,
    } = info;
    let arrow_schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(TableColumnStatus::field)
            .collect::<Vec<_>>(),
    ));
    let normalized_schema = normalize_delta_schema(&arrow_schema);
    let kernel_schema = StructType::try_from(normalized_schema.as_ref())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let partition_columns = partition_by
        .into_iter()
        .map(|field| {
            if field.transform.is_some() {
                return Err(DataFusionError::Plan(
                    "partition transforms are not supported for Unity Delta bootstrap".to_string(),
                ));
            }
            Ok(field.column)
        })
        .collect::<Result<Vec<_>>>()?;
    let configuration = delta_bootstrap_configuration(properties);
    let protocol = protocol_for_create(
        false,
        contains_timestampntz_arrow(normalized_schema.as_ref()),
        false,
        schema_has_generated_columns(&kernel_schema),
        schema_has_column_defaults(&kernel_schema),
        schema_has_identity_columns(&kernel_schema),
        contains_variant_arrow(normalized_schema.as_ref()),
        &configuration,
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let protocol = protocol_with_catalog_managed(&protocol);
    let mut metadata = metadata_for_create_with_struct_type(
        kernel_schema,
        partition_columns,
        Utc::now().timestamp_millis(),
        configuration,
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    if let Some(comment) = comment {
        metadata = metadata.with_description(Some(comment));
    }
    let metadata = metadata_with_catalog_managed(metadata, &table_id);
    let operation = DeltaOperation::Create {
        mode: SaveMode::ErrorIfExists,
        location: location.unwrap_or_else(|| table_url.to_string()),
        protocol: Box::new(protocol.clone()),
        metadata: Box::new(metadata.clone()),
    };

    Ok((
        vec![
            CommitAction::Protocol(protocol),
            CommitAction::Metadata(metadata),
        ],
        operation,
    ))
}

/// Writes version 0 of a table that the catalog registered before the table has a Delta log.
/// Returns whether the table has version 0 now.
async fn bootstrap_catalog_managed_delta_table<C>(
    ctx: &C,
    lakehouse_table: &LakehouseExecutionContext,
    log_store: LogStoreRef,
) -> Result<bool>
where
    C: SessionExtensionAccessor + ?Sized,
{
    let catalog_table = lakehouse_table.catalog_table();
    let Some(info) = load_catalog_managed_delta_bootstrap_info(ctx, catalog_table).await? else {
        return Ok(false);
    };
    let (actions, operation) =
        catalog_managed_bootstrap_actions(info, &log_store.config().location)?;
    let commit_properties = CommitProperties::from_session_config(&ctx.session_config())?;
    match CommitBuilder::from(commit_properties)
        .with_actions(actions)
        .build(None, log_store.clone(), operation)
        .await
    {
        Ok(_) => Ok(true),
        // Another reader or the creator of the table may write version 0 first, and the
        // table is read from that version then.
        Err(error) => match log_store.get_latest_version(-1).await {
            Ok(_) => Ok(true),
            Err(_) => Err(DataFusionError::External(Box::new(error))),
        },
    }
}

/// Returns the catalog commits that a snapshot of a catalog-managed table is loaded with, up
/// to the version, or to the latest version of the table. The `response` has the ratified
/// commits of the table from version 1 when the catalog returned them already, and they are
/// requested from the catalog otherwise.
pub(crate) async fn load_catalog_managed_commits<C>(
    ctx: &C,
    lakehouse_table: &LakehouseExecutionContext,
    version_as_of: Option<i64>,
    response: Option<DeltaRatifiedCommitResponse>,
) -> Result<CatalogManagedCommitSet>
where
    C: SessionExtensionAccessor + ?Sized,
{
    if matches!(version_as_of, Some(version) if version < 1) {
        return Ok(CatalogManagedCommitSet {
            latest_table_version: version_as_of.unwrap_or(0),
            commits: Vec::new(),
        });
    }

    let coordinator = DeltaCatalogCommitCoordinator::new(ctx, lakehouse_table);
    let response = match response {
        Some(response) => response,
        None => coordinator.get_ratified_commits(1, version_as_of).await?,
    };

    let latest_table_version = response.latest_table_version;
    if let Some(version) = version_as_of
        && latest_table_version >= 0
        && version > latest_table_version
    {
        return Err(DataFusionError::External(Box::new(
            DeltaTableError::generic(format!(
                "Catalog-managed Delta table latest ratified version is {latest_table_version}, but version {version} was requested"
            )),
        )));
    }

    let Some(effective_end_version) =
        effective_catalog_commit_end_version(latest_table_version, version_as_of)
    else {
        return Ok(CatalogManagedCommitSet {
            latest_table_version,
            commits: Vec::new(),
        });
    };

    // The catalog returns the ratified commits after the latest version that is published and
    // reported, up to the latest version of the table. A response whose commits end before the
    // latest version is a page, such as a page of the legacy API, and the rest of the commits
    // are requested.
    let mut commits = response.commits;
    let mut next_start_version = next_catalog_commit_start_version(
        1,
        effective_end_version,
        commits.iter().map(|commit| commit.version),
    )?;
    while let Some(start_version) = next_start_version {
        let page = coordinator
            .get_ratified_commits(start_version, Some(effective_end_version))
            .await?;
        next_start_version = next_catalog_commit_start_version(
            start_version,
            effective_end_version,
            page.commits.iter().map(|commit| commit.version),
        )?;
        commits.extend(page.commits);
    }

    // The version in the file name of a commit is the version that the commit was written
    // for, so a commit that the catalog returns for another version is not read.
    if let Some(commit) = commits
        .iter()
        .find(|commit| catalog_managed_commit_version(&commit.file_name) != Some(commit.version))
    {
        return Err(DataFusionError::External(Box::new(
            DeltaTableError::generic(format!(
                "The catalog returned the commit file `{}` for version {} of the Delta table, \
                 which is not the name of a commit file of that version",
                commit.file_name, commit.version
            )),
        )));
    }

    // The catalog returns the ratified commits that are not published yet, and the log replay
    // reads every other version from the published log.
    let by_version = commits
        .into_iter()
        .filter(|commit| commit.version >= 1 && commit.version <= effective_end_version)
        .map(|commit| {
            (
                commit.version,
                CatalogManagedCommitFile {
                    version: commit.version,
                    file_name: commit.file_name,
                    file_size: commit.file_size,
                    file_modification_timestamp: commit.file_modification_timestamp,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    Ok(CatalogManagedCommitSet {
        latest_table_version,
        commits: by_version.into_values().collect(),
    })
}

/// Returns the context of a catalog-managed table with the ratified commits that the catalog
/// has now, for a table that the catalog resolved with its ratified commits.
///
/// A write that is not a blind append fails when another writer commits after the snapshot
/// that the write is planned from. Such a write is planned from the commits that the catalog
/// has when the write is planned, which costs one request, and not from the ones that the
/// catalog returned when the statement resolved the table.
pub(crate) async fn refresh_catalog_managed_commits<C>(
    ctx: &C,
    mut lakehouse_table: LakehouseExecutionContext,
) -> Result<LakehouseExecutionContext>
where
    C: SessionExtensionAccessor + ?Sized,
{
    let carried = catalog_managed_commit_context(Some(&lakehouse_table))
        .and_then(|context| context.delta.as_ref())
        .is_some_and(|state| {
            state.catalog_managed == DeltaCatalogManaged::Yes && state.commits.is_some()
        });
    if !carried {
        return Ok(lakehouse_table);
    }
    let commits = DeltaCatalogCommitCoordinator::new(ctx, &lakehouse_table)
        .get_ratified_commits(1, None)
        .await?;
    if let Some(state) = lakehouse_table.delta.as_mut() {
        state.commits = Some(commits);
    }
    Ok(lakehouse_table)
}

/// Opens a table and loads its snapshot of the version, or its latest snapshot. This is the
/// only way that a snapshot of a table of a catalog is loaded.
///
/// The catalog told whether the table is catalog-managed when it resolved the table. The
/// snapshot of a catalog-managed table is loaded with the ratified commits of the catalog,
/// which are the ones that the catalog returned then, so that every snapshot of a statement
/// is of the same state of the table, and the catalog is not asked again.
///
/// When the catalog did not tell, the protocol of the published Delta log decides. This is
/// the only fallback, and it is also followed when the catalog told that the table is not
/// catalog-managed, since the Delta log is loaded then anyway. A published log that cannot
/// be read does not decide, and the table is loaded only if the catalog has commits for it.
pub(crate) async fn open_catalog_table<C>(
    ctx: &C,
    lakehouse_table: Option<&LakehouseExecutionContext>,
    log_store: LogStoreRef,
    config: DeltaSnapshotConfig,
    version: Option<i64>,
) -> DeltaResult<DeltaTable>
where
    C: SessionExtensionAccessor + ?Sized,
{
    let config = DeltaSnapshotConfig {
        catalog_managed_commits: None,
        ..config
    };
    match catalog_managed_commit_context(lakehouse_table) {
        Some(lakehouse_table) => {
            open_resolved_table(ctx, lakehouse_table, log_store, config, version)
                .await
                .map_err(DeltaTableError::from)
        }
        None => {
            let mut table = DeltaTable::new(log_store, config);
            table.update_incremental(version).await?;
            Ok(table)
        }
    }
}

/// Returns the error of opening a table as the error of a query.
pub(crate) fn open_table_error(error: DeltaTableError) -> DataFusionError {
    match error {
        DeltaTableError::DataFusion(error) => error,
        error => DataFusionError::External(Box::new(error)),
    }
}

async fn open_resolved_table<C>(
    ctx: &C,
    lakehouse_table: &LakehouseExecutionContext,
    log_store: LogStoreRef,
    config: DeltaSnapshotConfig,
    version: Option<i64>,
) -> Result<DeltaTable>
where
    C: SessionExtensionAccessor + ?Sized,
{
    let state = lakehouse_table.delta.clone().unwrap_or_default();
    if state.catalog_managed == DeltaCatalogManaged::Yes {
        return load_catalog_managed_table(ctx, lakehouse_table, log_store, config, version, state)
            .await;
    }

    // The files of the table are loaded at once only when the table is expected not to be
    // catalog-managed, since a catalog-managed table is loaded again with its catalog
    // commits. The latest protocol decides, so only a table that the catalog told about
    // is loaded at an earlier version at once.
    let expected = state.catalog_managed == DeltaCatalogManaged::No;
    let direct = expected || (version.is_none() && !config.require_files);
    let detect_version = version.filter(|_| expected);
    let mut table = DeltaTable::new(
        log_store.clone(),
        if direct {
            config.clone()
        } else {
            DeltaSnapshotConfig {
                require_files: false,
                ..Default::default()
            }
        },
    );
    match table.update_incremental(detect_version).await {
        Ok(()) => {
            let snapshot = table.snapshot()?.clone();
            if snapshot.protocol().is_catalog_managed() {
                let state = DeltaCatalogState {
                    commits: None,
                    ..state
                };
                load_catalog_managed_table(ctx, lakehouse_table, log_store, config, version, state)
                    .await
            } else if direct {
                Ok(table)
            } else {
                // The log is replayed from the header that is loaded already.
                let snapshot = DeltaSnapshot::try_new(
                    log_store.as_ref(),
                    config.clone(),
                    version,
                    Some(&snapshot.replay_hint()),
                )
                .await?;
                let mut table = DeltaTable::new(log_store, config);
                table.state = Some(Arc::new(snapshot));
                Ok(table)
            }
        }
        Err(error) if is_missing_delta_log_error(&error) => {
            if !expected
                && state.registered_before_log
                && bootstrap_catalog_managed_delta_table(ctx, lakehouse_table, log_store.clone())
                    .await?
            {
                let state = DeltaCatalogState {
                    registered_before_log: false,
                    ..state
                };
                load_catalog_managed_table(ctx, lakehouse_table, log_store, config, version, state)
                    .await
            } else if state.registered_before_log {
                Err(DataFusionError::from(error))
            } else {
                Err(missing_delta_log_error(&log_store, lakehouse_table, "table"))
            }
        }
        // The ratified commits of a catalog-managed table replace its published commits,
        // so a table whose published log cannot be read may still be read with them when
        // the catalog did not tell whether the table is catalog-managed. The error of the
        // published log is the one reported when the catalog has no commits for the table.
        Err(error) if !expected && !is_missing_delta_log_error(&error) => {
            match load_catalog_managed_table(ctx, lakehouse_table, log_store, config, version, state)
                .await
            {
                Ok(table) => Ok(table),
                Err(catalog_error) => Err(DataFusionError::from(error).context(format!(
                    "Failed to load the published Delta log of {}, and the catalog commits are not available: {catalog_error}",
                    lakehouse_table.catalog_table().join(".")
                ))),
            }
        }
        Err(error) => Err(DataFusionError::from(error)),
    }
}

/// The error of a table without a Delta log in a catalog that registers a table after
/// version 0 of the table is published, where nothing is written for such a table.
fn missing_delta_log_error(
    log_store: &LogStoreRef,
    lakehouse_table: &LakehouseExecutionContext,
    kind: &str,
) -> DataFusionError {
    DataFusionError::External(Box::new(DeltaTableError::generic(format!(
        "Delta log not found at `{}` for the {kind} `{}`; \
         check the table location and the storage credentials",
        log_store.config().location,
        lakehouse_table.catalog_table().join(".")
    ))))
}

/// Loads the snapshot of a catalog-managed table with the ratified commits of the catalog.
async fn load_catalog_managed_table<C>(
    ctx: &C,
    lakehouse_table: &LakehouseExecutionContext,
    log_store: LogStoreRef,
    config: DeltaSnapshotConfig,
    version: Option<i64>,
    state: DeltaCatalogState,
) -> Result<DeltaTable>
where
    C: SessionExtensionAccessor + ?Sized,
{
    let commits =
        load_catalog_managed_commits(ctx, lakehouse_table, version, state.commits).await?;
    let mut table = DeltaTable::new(
        log_store.clone(),
        DeltaSnapshotConfig {
            catalog_managed_commits: Some(commits),
            ..config
        },
    );
    match table.update_incremental(version).await {
        Ok(()) => Ok(table),
        // The table is looked for only when it is not loaded, since the replay of a table
        // without a Delta log fails in more than one way with catalog commits.
        Err(_)
            if matches!(
                log_store.get_latest_version(-1).await,
                Err(DeltaError::MissingVersion)
            ) =>
        {
            if state.registered_before_log
                && bootstrap_catalog_managed_delta_table(ctx, lakehouse_table, log_store.clone())
                    .await?
            {
                table.update_incremental(version).await?;
                return Ok(table);
            }
            Err(missing_delta_log_error(
                &log_store,
                lakehouse_table,
                "catalog-managed table",
            ))
        }
        Err(error) => Err(DataFusionError::from(error)),
    }
}

pub(crate) async fn load_delta_read_state(
    ctx: &dyn Session,
    table_url: Url,
    schema: Option<Schema>,
    options: DeltaReadOptions,
    metadata_only: bool,
    lakehouse_table: Option<LakehouseExecutionContext>,
) -> Result<(Arc<DeltaSnapshot>, LogStoreRef, DeltaScanConfig)> {
    let reads_catalog_table = lakehouse_table.is_some();
    let url = ListingTableUrl::try_new(table_url.clone(), None)?;
    let object_store = ctx.runtime_env().object_store(&url)?;
    let storage_config = StorageConfig;
    let log_store =
        create_logstore_with_object_store(object_store, table_url.clone(), storage_config)?;

    let table_config = delta_read_snapshot_config(metadata_only, &options);
    let mut deltalake_table = open_catalog_table(
        &ctx,
        lakehouse_table.as_ref(),
        log_store.clone(),
        table_config,
        options.version_as_of,
    )
    .await
    .map_err(open_table_error)?;

    // The table is loaded at the version of the timestamp of the read, which is found
    // from the latest snapshot.
    if options.version_as_of.is_none()
        && let Some(timestamp) = &options.timestamp_as_of
    {
        load_table_as_of_timestamp(&mut deltalake_table, timestamp).await?;
    }

    let snapshot = deltalake_table.snapshot()?.clone();

    let scan_config = DeltaScanConfig {
        file_column_name: None,
        row_index_column_name: None,
        hash_partition_files: false,
        wrap_partition_values: false,
        enable_parquet_pushdown: true,
        schema: match schema {
            Some(ref s) if s.fields().is_empty() => None,
            Some(s) if reads_catalog_table => {
                let refreshed = refresh_catalog_delta_schema(s, snapshot.schema());
                // The catalog schema does not carry Delta column mapping metadata, but the
                // physical scan attaches it, so the logical schema must carry it as well.
                let refreshed =
                    if snapshot.effective_column_mapping_mode() == ColumnMappingMode::None {
                        refreshed
                    } else {
                        attach_column_mapping_metadata(&refreshed, snapshot.schema())
                    };
                Some(Arc::new(refreshed))
            }
            Some(s) => Some(Arc::new(s)),
            None => None,
        },
        commit_version_column_name: None,
        commit_timestamp_column_name: None,
        delta_log_replay_strategy: options.delta_log_replay_strategy,
        metadata_aggregate: None,
    };

    Ok((snapshot, log_store, scan_config))
}

fn refresh_catalog_delta_schema(catalog_schema: Schema, snapshot_schema: &Schema) -> Schema {
    let mut fields = catalog_schema.fields().iter().cloned().collect::<Vec<_>>();
    for snapshot_field in snapshot_schema.fields() {
        if let Some(index) = fields
            .iter()
            .position(|field| field.name().eq_ignore_ascii_case(snapshot_field.name()))
        {
            fields[index] = Arc::new(refresh_catalog_delta_field(
                fields[index].as_ref(),
                snapshot_field.as_ref(),
            ));
        } else {
            fields.push(snapshot_field.clone());
        }
    }
    Schema::new_with_metadata(fields, catalog_schema.metadata().clone())
}

fn refresh_catalog_delta_field(catalog_field: &Field, snapshot_field: &Field) -> Field {
    let data_type = match (catalog_field.data_type(), snapshot_field.data_type()) {
        (DataType::Struct(catalog_fields), DataType::Struct(snapshot_fields)) => {
            let mut fields = catalog_fields.iter().cloned().collect::<Vec<_>>();
            for snapshot_child in snapshot_fields {
                if let Some(index) = fields.iter().position(|catalog_child| {
                    catalog_child
                        .name()
                        .eq_ignore_ascii_case(snapshot_child.name())
                }) {
                    fields[index] = Arc::new(refresh_catalog_delta_field(
                        fields[index].as_ref(),
                        snapshot_child.as_ref(),
                    ));
                } else {
                    fields.push(snapshot_child.clone());
                }
            }
            DataType::Struct(fields.into())
        }
        (DataType::List(catalog_child), DataType::List(snapshot_child)) => DataType::List(
            Arc::new(refresh_catalog_delta_field(catalog_child, snapshot_child)),
        ),
        (DataType::LargeList(catalog_child), DataType::LargeList(snapshot_child)) => {
            DataType::LargeList(Arc::new(refresh_catalog_delta_field(
                catalog_child,
                snapshot_child,
            )))
        }
        (
            DataType::FixedSizeList(catalog_child, catalog_size),
            DataType::FixedSizeList(snapshot_child, _),
        ) => DataType::FixedSizeList(
            Arc::new(refresh_catalog_delta_field(catalog_child, snapshot_child)),
            *catalog_size,
        ),
        (DataType::Map(catalog_entries, sorted), DataType::Map(snapshot_entries, _)) => {
            DataType::Map(
                Arc::new(refresh_catalog_delta_map_entries(
                    catalog_entries,
                    snapshot_entries,
                )),
                *sorted,
            )
        }
        (DataType::Timestamp(..), DataType::Timestamp(..)) => catalog_field.data_type().clone(),
        _ => snapshot_field.data_type().clone(),
    };
    catalog_field
        .clone()
        .with_name(snapshot_field.name())
        .with_data_type(data_type)
        .with_nullable(snapshot_field.is_nullable())
}

/// Refreshes map entries by position, since the catalog and the Delta log may name
/// the entries, key, and value fields differently. The Delta log names are kept
/// because the Parquet files are read with them.
fn refresh_catalog_delta_map_entries(catalog_entries: &Field, snapshot_entries: &Field) -> Field {
    match (catalog_entries.data_type(), snapshot_entries.data_type()) {
        (DataType::Struct(catalog_fields), DataType::Struct(snapshot_fields))
            if catalog_fields.len() == 2 && snapshot_fields.len() == 2 =>
        {
            let fields = catalog_fields
                .iter()
                .zip(snapshot_fields.iter())
                .map(|(catalog_child, snapshot_child)| {
                    Arc::new(
                        refresh_catalog_delta_field(catalog_child, snapshot_child)
                            .with_name(snapshot_child.name())
                            .with_nullable(snapshot_child.is_nullable()),
                    )
                })
                .collect::<Vec<_>>();
            snapshot_entries
                .clone()
                .with_data_type(DataType::Struct(fields.into()))
        }
        _ => snapshot_entries.clone(),
    }
}

/// Loads the latest version of the table that was committed at or before the timestamp.
async fn load_table_as_of_timestamp(table: &mut DeltaTable, timestamp: &str) -> Result<()> {
    let datetime = parse_timestamp_as_of(timestamp)?;
    let target_version = find_version_for_timestamp(table, datetime)
        .await
        .map_err(|e| {
            if matches!(e, DeltaTableError::MissingVersion) {
                DeltaTableError::generic(format!(
                    "No version of the Delta table exists at or before timestamp {timestamp}"
                ))
            } else {
                e
            }
        })?;
    table.load_version(target_version).await?;
    Ok(())
}

/// Finds the latest version of the table that was committed at or before a given timestamp.
async fn find_version_for_timestamp(
    table: &mut DeltaTable,
    datetime: DateTime<Utc>,
) -> DeltaResult<i64> {
    let log_store = table.log_store();
    let latest_version = match table
        .config
        .catalog_managed_commits
        .as_ref()
        .and_then(|commits| commits.latest_replay_version())
    {
        Some(version) => version,
        None => log_store.get_latest_version(0).await?,
    };
    if table.version() != Some(latest_version) {
        table.load_version(latest_version).await?;
    }
    let snapshot = table.snapshot()?;
    let (mut min_version, mut max_version) =
        if let Some((enablement_version, enablement_timestamp)) =
            snapshot.in_commit_timestamp_enablement()
        {
            if datetime.timestamp_millis() >= enablement_timestamp {
                (enablement_version, latest_version)
            } else if enablement_version == 0 {
                return Err(DeltaError::MissingVersion);
            } else {
                (0, enablement_version - 1)
            }
        } else {
            (0, latest_version)
        };

    let target_ts = datetime.timestamp_millis();
    let mut target_version = -1;

    // Binary search to find the correct version
    while min_version <= max_version {
        let pivot = min_version + (max_version - min_version) / 2;
        let pivot_ts = table.get_version_timestamp(pivot).await?;

        if pivot_ts <= target_ts {
            // This version is a potential candidate, try to find a newer one
            target_version = pivot;
            min_version = pivot + 1;
        } else {
            // This version is too new, search in older versions
            max_version = pivot - 1;
        }
    }

    if target_version == -1 {
        // If no version was found, it means the provided timestamp is before the first commit.
        Err(DeltaError::MissingVersion)
    } else {
        Ok(target_version)
    }
}

pub(crate) fn parse_timestamp_as_of(timestamp: &str) -> DeltaResult<DateTime<Utc>> {
    let rfc3339_result = DateTime::parse_from_rfc3339(timestamp);
    if let Ok(datetime) = rfc3339_result {
        return Ok(datetime.with_timezone(&Utc));
    }

    let mut last_error = rfc3339_result
        .err()
        .map(|e| format!("RFC3339 parsing error: {e}"));

    for format in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ] {
        match NaiveDateTime::parse_from_str(timestamp, format) {
            Ok(naive) => return Ok(Utc.from_utc_datetime(&naive)),
            Err(e) => {
                last_error = Some(format!("Failed to parse with format '{format}': {e}"));
            }
        }
    }

    let detail = last_error
        .map(|e| format!(" Details: {e}"))
        .unwrap_or_default();

    Err(DeltaTableError::generic(format!(
        "Invalid timestamp string: {timestamp}. Supported formats are: RFC3339 (e.g. '2024-01-02T03:04:05Z'), '%Y-%m-%d %H:%M:%S%.f', '%Y-%m-%dT%H:%M:%S%.f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S'.{detail}",
    )))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use super::{
        effective_catalog_commit_end_version, next_catalog_commit_start_version,
        refresh_catalog_delta_schema,
    };
    use crate::schema::attach_column_mapping_metadata;

    #[test]
    fn catalog_schema_refresh_appends_snapshot_fields_without_replacing_existing_types() {
        let catalog_timestamp = DataType::Timestamp(TimeUnit::Microsecond, None);
        let snapshot_timestamp = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let catalog_schema = Schema::new(vec![Field::new(
            "event_time",
            catalog_timestamp.clone(),
            true,
        )]);
        let snapshot_schema = Schema::new(vec![
            Field::new("event_time", snapshot_timestamp, true),
            Field::new("extra", DataType::Int32, true),
        ]);

        let refreshed = refresh_catalog_delta_schema(catalog_schema, &snapshot_schema);

        assert_eq!(refreshed.fields().len(), 2);
        assert_eq!(refreshed.field(0).data_type(), &catalog_timestamp);
        assert_eq!(refreshed.field(1).name(), "extra");
    }

    #[test]
    fn catalog_schema_refresh_uses_snapshot_nullability_recursively() {
        let schema = |nullable, timestamp: DataType, source: &str| {
            let metadata = HashMap::from([("source".to_string(), source.to_string())]);
            let field = |name, data_type| {
                Field::new(name, data_type, nullable).with_metadata(metadata.clone())
            };
            let element = Arc::new(field("element", timestamp));
            let nested =
                DataType::Struct(vec![field("items", DataType::List(element.clone()))].into());
            Schema::new_with_metadata(
                vec![
                    field("scalar", DataType::Int32),
                    field("payload", nested.clone()),
                    field(
                        "list",
                        DataType::List(Arc::new(field("element", nested.clone()))),
                    ),
                    field("large_list", DataType::LargeList(element.clone())),
                    field("fixed_list", DataType::FixedSizeList(element, 2)),
                    field(
                        "map",
                        DataType::Map(
                            Arc::new(Field::new(
                                "entries",
                                DataType::Struct(
                                    vec![
                                        field("key", DataType::Utf8).with_nullable(false),
                                        field("value", nested),
                                    ]
                                    .into(),
                                ),
                                false,
                            )),
                            false,
                        ),
                    ),
                ],
                metadata,
            )
        };
        let catalog_timestamp = DataType::Timestamp(TimeUnit::Microsecond, None);
        let snapshot_timestamp = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        for nullable in [false, true] {
            let catalog = schema(!nullable, catalog_timestamp.clone(), "catalog");
            let snapshot = schema(nullable, snapshot_timestamp.clone(), "snapshot");
            let expected = schema(nullable, catalog_timestamp.clone(), "catalog");

            assert_eq!(refresh_catalog_delta_schema(catalog, &snapshot), expected);
        }
    }

    #[test]
    fn catalog_schema_refresh_appends_nested_snapshot_fields() {
        let catalog_timestamp = DataType::Timestamp(TimeUnit::Microsecond, None);
        let snapshot_timestamp = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let catalog_schema = Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(
                vec![Field::new("event_time", catalog_timestamp.clone(), true)].into(),
            ),
            true,
        )]);
        let snapshot_schema = Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(
                vec![
                    Field::new("event_time", snapshot_timestamp, true),
                    Field::new("extra", DataType::Int32, true),
                ]
                .into(),
            ),
            true,
        )]);

        let refreshed = refresh_catalog_delta_schema(catalog_schema, &snapshot_schema);
        assert_eq!(
            refreshed.field(0).data_type(),
            &DataType::Struct(
                vec![
                    Field::new("event_time", catalog_timestamp, true),
                    Field::new("extra", DataType::Int32, true),
                ]
                .into()
            )
        );
    }

    #[test]
    fn catalog_schema_refresh_canonicalizes_mapped_fields() {
        let mapped = |name: &str, physical: &str, id: u32, data_type: DataType| {
            Field::new(name, data_type, true).with_metadata(HashMap::from([
                ("delta.columnMapping.id".to_string(), id.to_string()),
                (
                    "delta.columnMapping.physicalName".to_string(),
                    physical.to_string(),
                ),
            ]))
        };
        let snapshot_schema = Schema::new(vec![
            mapped("EventID", "col-id", 1, DataType::Int32),
            mapped(
                "Payload",
                "col-payload",
                2,
                DataType::Struct(vec![mapped("Amount", "col-amount", 3, DataType::Int32)].into()),
            ),
        ]);
        let catalog_schema = Schema::new(vec![
            Field::new("eventid", DataType::Int32, true),
            Field::new(
                "payload",
                DataType::Struct(vec![Field::new("amount", DataType::Int32, true)].into()),
                true,
            ),
        ]);

        let refreshed = refresh_catalog_delta_schema(catalog_schema, &snapshot_schema);
        let refreshed = attach_column_mapping_metadata(&refreshed, &snapshot_schema);

        assert_eq!(refreshed, snapshot_schema);
    }

    #[test]
    fn catalog_schema_refresh_uses_snapshot_map_entry_names() {
        let catalog_map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("keys", DataType::Utf8, false),
                        Field::new("values", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let snapshot_map = DataType::Map(
            Arc::new(Field::new(
                "key_value",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let catalog_schema = Schema::new(vec![Field::new("properties", catalog_map, true)]);
        let snapshot_schema =
            Schema::new(vec![Field::new("properties", snapshot_map.clone(), true)]);

        let refreshed = refresh_catalog_delta_schema(catalog_schema, &snapshot_schema);

        assert_eq!(refreshed.fields().len(), 1);
        assert_eq!(refreshed.field(0).data_type(), &snapshot_map);
    }

    #[test]
    fn catalog_schema_refresh_keeps_catalog_map_value_types() {
        let catalog_timestamp = DataType::Timestamp(TimeUnit::Microsecond, None);
        let snapshot_timestamp = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let map_type = |entries: &str, key: &str, value: &str, value_type: DataType| {
            DataType::Map(
                Arc::new(Field::new(
                    entries,
                    DataType::Struct(
                        vec![
                            Field::new(key, DataType::Utf8, false),
                            Field::new(value, value_type, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        };
        let catalog_schema = Schema::new(vec![Field::new(
            "events",
            map_type("entries", "keys", "values", catalog_timestamp.clone()),
            true,
        )]);
        let snapshot_schema = Schema::new(vec![Field::new(
            "events",
            map_type("key_value", "key", "value", snapshot_timestamp),
            true,
        )]);

        let refreshed = refresh_catalog_delta_schema(catalog_schema, &snapshot_schema);

        assert_eq!(
            refreshed.field(0).data_type(),
            &map_type("key_value", "key", "value", catalog_timestamp)
        );
    }

    #[test]
    fn effective_catalog_commit_end_version_caps_requested_version() {
        assert_eq!(effective_catalog_commit_end_version(10, None), Some(10));
        assert_eq!(effective_catalog_commit_end_version(10, Some(7)), Some(7));
        assert_eq!(effective_catalog_commit_end_version(3, Some(7)), Some(3));
        assert_eq!(effective_catalog_commit_end_version(0, None), None);
        assert_eq!(effective_catalog_commit_end_version(-1, None), None);
    }

    #[test]
    fn next_catalog_commit_start_version_advances_from_page_max() {
        assert!(matches!(
            next_catalog_commit_start_version(1, 10, [5, 1, 3]),
            Ok(Some(6))
        ));
        assert!(matches!(
            next_catalog_commit_start_version(6, 10, [10, 8, 6]),
            Ok(None)
        ));
        assert!(matches!(
            next_catalog_commit_start_version(6, 10, []),
            Ok(None)
        ));
    }

    #[test]
    fn next_catalog_commit_start_version_rejects_non_progressing_page() {
        let result = next_catalog_commit_start_version(6, 10, [1, 5]);
        assert!(result.is_err());
        let error = result.err().map(|error| error.to_string());
        assert!(matches!(
            error.as_deref(),
            Some(message) if message.contains("before requested start version 6")
        ));
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod catalog_managed_tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt};
    use sail_catalog::error::{CatalogError, CatalogResult};
    use sail_catalog::lakehouse::{
        DeltaCommitOutcome, DeltaRatifiedCommit, DeltaRatifiedCommitRequest,
        DeltaRatifiedCommitResponse, LakehouseCapability, LakehouseResolvedTable,
        ResolveLakehouseTableRequest, resolve_lakehouse_table_status,
    };
    use sail_catalog::provider::*;
    use sail_catalog::testing::single_catalog_manager;
    use sail_common_datafusion::catalog::delta::DeltaCatalogManaged::{No, Undetermined, Yes};
    use sail_common_datafusion::catalog::delta::{
        DeltaCatalogCommit, DeltaCatalogManaged, DeltaCatalogState,
    };
    use sail_common_datafusion::catalog::*;
    use sail_common_datafusion::datasource::OptionLayer;
    use url::Url;

    use super::load_delta_read_state;
    use crate::options::ResolveOptions;
    use crate::options::r#gen::DeltaReadOptions;

    struct UnityManagedCatalog {
        /// What the catalog tells about the table when it resolves the table. A catalog
        /// without a state resolves the table as a catalog that tells nothing does.
        state: Option<DeltaCatalogState>,
        location: &'static str,
        /// The ratified commits that the catalog returns when it is asked for them, which
        /// may be newer than the ones that it returned when it resolved the table.
        ratified: Option<DeltaRatifiedCommitResponse>,
        /// The number of the requests for the ratified commits of the table.
        ratified_commit_requests: AtomicUsize,
        commit_error: Option<fn() -> CatalogError>,
        script: Option<CommitScript>,
        /// The number of ratified commits in a response, from the start version of the
        /// request, as in a page of the legacy API. All of them when it is not set.
        page_size: Option<usize>,
    }

    impl UnityManagedCatalog {
        fn new(
            state: Option<DeltaCatalogState>,
            ratified: Option<DeltaRatifiedCommitResponse>,
        ) -> Self {
            Self {
                state,
                location: "memory:///items/",
                ratified,
                ratified_commit_requests: AtomicUsize::new(0),
                commit_error: None,
                script: None,
                page_size: None,
            }
        }

        fn ratified_commit_requests(&self) -> usize {
            self.ratified_commit_requests.load(Ordering::SeqCst)
        }
    }

    /// The state of a table that the catalog told about when it resolved the table, with
    /// the ratified commits that it returned then. The catalog registers the table after
    /// version 0 of the table is written.
    fn told(
        catalog_managed: DeltaCatalogManaged,
        commits: Option<DeltaRatifiedCommitResponse>,
    ) -> Option<DeltaCatalogState> {
        Some(DeltaCatalogState {
            catalog_managed,
            commits,
            registered_before_log: false,
        })
    }

    /// The state of a table of a catalog that registers a table before version 0 of the
    /// table is written.
    fn told_before_log(catalog_managed: DeltaCatalogManaged) -> Option<DeltaCatalogState> {
        Some(DeltaCatalogState {
            catalog_managed,
            commits: None,
            registered_before_log: true,
        })
    }

    /// Returns a session with the catalog, and the table as the catalog resolves it for
    /// the operation, which is how a statement gets the table.
    async fn resolve_table(
        catalog: &Arc<UnityManagedCatalog>,
        operation: LakehouseOperation,
    ) -> (SessionContext, LakehouseExecutionContext) {
        let manager = single_catalog_manager(catalog.clone()).unwrap();
        let manager = Arc::new(manager);
        let session =
            SessionContext::new_with_config(SessionConfig::new().with_extension(manager.clone()));
        let table = vec!["unity".to_string(), "default".into(), "items".into()];
        let request = ResolveLakehouseTableRequest {
            catalog_table: table.clone(),
            operation,
            requested_format: None,
            options: vec![],
        };
        let resolved = manager
            .resolve_lakehouse_table(&session.state(), &table, request)
            .await
            .unwrap();
        let context = resolved.execution;
        assert_eq!(context.commit, CommitAuthority::DeltaRatifiedCommit);
        assert_eq!(context.delta, catalog.state);
        (session, context)
    }

    /// What the catalog does with a commit request. The catalog ratifies the commit request
    /// of the next version of the table when there is nothing else to do.
    enum CommitStep {
        /// The catalog does not ratify the commit and answers with the error.
        Refuse(fn() -> CatalogError),
        /// The catalog ratifies the commit and answers with the error.
        RatifyAndFail(fn() -> CatalogError),
        /// The catalog ratifies the commit, a writer publishes and reports it, and the
        /// catalog answers with the error.
        PublishAndFail(fn() -> CatalogError),
        /// The catalog ratifies the commit of another writer with the actions first, and
        /// answers with a conflict.
        Lose(&'static str),
        /// Another writer publishes the version before the commit, and the catalog does not
        /// ratify the commit since it has too many commits that are not published.
        PublishPredecessorAndExhaust,
    }

    /// The commits of a table in the catalog, and what the catalog does with the commit
    /// requests in the order of the requests.
    struct CommitScript {
        store: Arc<InMemory>,
        steps: Mutex<VecDeque<CommitStep>>,
        ratified: Mutex<DeltaRatifiedCommitResponse>,
        requests: Mutex<Vec<DeltaCatalogCommit>>,
    }

    impl CommitScript {
        /// Ratifies the commit if it is the next version of the table.
        fn ratify(&self, version: i64, file_name: &str) -> bool {
            let mut ratified = self.ratified.lock().unwrap();
            if ratified.latest_table_version + 1 != version {
                return false;
            }
            ratified.latest_table_version = version;
            ratified.commits.push(DeltaRatifiedCommit {
                version,
                timestamp: 1,
                file_name: file_name.to_string(),
                file_size: 1,
                file_modification_timestamp: 1,
            });
            true
        }

        /// Ratifies the commit of another writer with the actions as the next version of
        /// the table.
        async fn ratify_winner(&self, actions: &str) {
            let winner = self.ratified.lock().unwrap().latest_table_version + 1;
            let file_name = format!("{winner:020}.winner.json");
            let timestamp = winner + 1;
            let commit = format!(
                "{{\"commitInfo\":{{\"timestamp\":{timestamp},\"inCommitTimestamp\":{timestamp},\"isBlindAppend\":true}}}}\n{actions}\n"
            );
            let path = Path::from(format!("_delta_log/_staged_commits/{file_name}"));
            self.store.put(&path, commit.into()).await.unwrap();
            self.ratify(winner, &file_name);
        }

        async fn commit(&self, commit: &DeltaCatalogCommit) -> CatalogResult<()> {
            self.requests.lock().unwrap().push(commit.clone());
            let version = commit.commit_info.version;
            let file_name = commit.commit_info.file_name.as_str();
            let step = self.steps.lock().unwrap().pop_front();
            match step {
                Some(CommitStep::Refuse(error)) => return Err(error()),
                Some(CommitStep::RatifyAndFail(error)) => {
                    self.ratify(version, file_name);
                    return Err(error());
                }
                Some(CommitStep::PublishAndFail(error)) => {
                    self.ratify(version, file_name);
                    let staged = Path::from(format!("_delta_log/_staged_commits/{file_name}"));
                    let published = Path::from(format!("_delta_log/{version:020}.json"));
                    self.store.copy(&staged, &published).await.unwrap();
                    self.ratified.lock().unwrap().commits.clear();
                    return Err(error());
                }
                Some(CommitStep::Lose(actions)) => {
                    self.ratify_winner(actions).await;
                    return Err(CatalogError::Conflict("409".into()));
                }
                Some(CommitStep::PublishPredecessorAndExhaust) => {
                    let path = Path::from(format!("_delta_log/{}", published_commit(version - 1)));
                    let commit = "{\"commitInfo\":{\"timestamp\":2,\"inCommitTimestamp\":2}}\n";
                    self.store.put(&path, commit.into()).await.unwrap();
                    return Err(CatalogError::ResourceExhausted("429".into()));
                }
                None => {}
            }
            // The catalog recognizes the commit that it has ratified.
            let known = |commit: &DeltaRatifiedCommit| {
                commit.version == version && commit.file_name == file_name
            };
            let known = self.ratified.lock().unwrap().commits.iter().any(known);
            if known || self.ratify(version, file_name) {
                Ok(())
            } else {
                Err(CatalogError::Conflict("409".into()))
            }
        }
    }

    /// The answer of the catalog to a commit that it answers with the error of a request.
    fn commit_answer(error: CatalogError) -> CatalogResult<DeltaCommitOutcome> {
        match error {
            CatalogError::Conflict(message) => Ok(DeltaCommitOutcome::Conflict(message)),
            CatalogError::ResourceExhausted(message) => {
                Ok(DeltaCommitOutcome::BackfillRequired(message))
            }
            CatalogError::RateLimited(message) => Ok(DeltaCommitOutcome::Throttled(message)),
            CatalogError::CommitStateUnknown(error) => {
                Ok(DeltaCommitOutcome::Unknown(error.to_string()))
            }
            error => Err(error),
        }
    }

    fn unsupported<T>() -> CatalogResult<T> {
        Err(CatalogError::NotSupported(
            "unused test catalog operation".into(),
        ))
    }

    #[async_trait::async_trait]
    impl CatalogProvider for UnityManagedCatalog {
        fn get_name(&self) -> &str {
            "unity"
        }
        fn lakehouse_capabilities(&self) -> Vec<LakehouseCapability> {
            vec![LakehouseCapability::DeltaRatifiedCommits]
        }
        async fn resolve_lakehouse_table(
            &self,
            _: &Namespace,
            _: &str,
            status: &TableStatus,
            request: ResolveLakehouseTableRequest,
        ) -> CatalogResult<LakehouseResolvedTable> {
            let mut resolved = resolve_lakehouse_table_status(
                self.get_name(),
                request.catalog_table,
                status,
                request.operation,
                &self.lakehouse_capabilities(),
            );
            resolved.execution.delta = self.state.clone();
            Ok(resolved)
        }
        async fn get_table(&self, _: &Namespace, _: &str) -> CatalogResult<TableStatus> {
            Ok(TableStatus {
                catalog: Some("unity".into()),
                database: vec!["default".into()],
                name: "items".into(),
                kind: TableKind::Table {
                    columns: vec![],
                    comment: None,
                    constraints: vec![],
                    location: Some(self.location.into()),
                    format: "delta".into(),
                    partition_by: vec![],
                    sort_by: vec![],
                    bucket_by: None,
                    properties: [
                        ("table_type", "MANAGED"),
                        ("io.unitycatalog.tableId", "table-id"),
                    ]
                    .into_iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect(),
                    is_external: false,
                },
            })
        }
        async fn commit_delta_table(
            &self,
            _: &Namespace,
            _: &str,
            commit: DeltaCatalogCommit,
        ) -> CatalogResult<DeltaCommitOutcome> {
            let answer = match &self.script {
                Some(script) => script.commit(&commit).await,
                None => Ok(()),
            };
            match answer.and(self.commit_error.map_or(Ok(()), |error| Err(error()))) {
                Ok(()) => Ok(DeltaCommitOutcome::Committed),
                Err(error) => commit_answer(error),
            }
        }
        async fn get_delta_ratified_commits(
            &self,
            _: &Namespace,
            _: &str,
            request: DeltaRatifiedCommitRequest,
        ) -> CatalogResult<DeltaRatifiedCommitResponse> {
            self.ratified_commit_requests.fetch_add(1, Ordering::SeqCst);
            if let Some(script) = &self.script {
                let mut ratified = script.ratified.lock().unwrap().clone();
                ratified.commits.retain(|commit| {
                    commit.version >= request.start_version
                        && request.end_version.is_none_or(|end| commit.version <= end)
                });
                return Ok(ratified);
            }
            let mut ratified = self
                .ratified
                .clone()
                .ok_or_else(|| CatalogError::External("401 Unauthorized".into()))?;
            if let Some(page_size) = self.page_size {
                ratified.commits.retain(|commit| {
                    commit.version >= request.start_version
                        && request.end_version.is_none_or(|end| commit.version <= end)
                });
                ratified.commits.truncate(page_size);
            }
            Ok(ratified)
        }
        async fn alter_table(
            &self,
            _: &Namespace,
            _: &str,
            _: AlterTableOptions,
        ) -> CatalogResult<()> {
            unsupported()
        }
        async fn create_database(
            &self,
            _: &Namespace,
            _: CreateDatabaseOptions,
        ) -> CatalogResult<DatabaseStatus> {
            unsupported()
        }
        async fn get_database(&self, _: &Namespace) -> CatalogResult<DatabaseStatus> {
            unsupported()
        }
        async fn list_databases(
            &self,
            _: Option<&Namespace>,
        ) -> CatalogResult<Vec<DatabaseStatus>> {
            unsupported()
        }
        async fn drop_database(&self, _: &Namespace, _: DropDatabaseOptions) -> CatalogResult<()> {
            unsupported()
        }
        async fn create_table(
            &self,
            _: &Namespace,
            _: &str,
            _: CreateTableOptions,
        ) -> CatalogResult<TableStatus> {
            unsupported()
        }
        async fn list_tables(&self, _: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            unsupported()
        }
        async fn drop_table(
            &self,
            _: &Namespace,
            _: &str,
            _: DropTableOptions,
        ) -> CatalogResult<()> {
            unsupported()
        }
        async fn create_view(
            &self,
            _: &Namespace,
            _: &str,
            _: CreateViewOptions,
        ) -> CatalogResult<TableStatus> {
            unsupported()
        }
        async fn get_view(&self, _: &Namespace, _: &str) -> CatalogResult<TableStatus> {
            unsupported()
        }
        async fn list_views(&self, _: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            unsupported()
        }
        async fn drop_view(&self, _: &Namespace, _: &str, _: DropViewOptions) -> CatalogResult<()> {
            unsupported()
        }
    }

    async fn read_unity_managed_table(
        protocol: &str,
        corrupted_commit: Option<i64>,
        state: Option<DeltaCatalogState>,
    ) -> (usize, datafusion_common::Result<i64>) {
        let files = corrupted_commit
            .map(|version| (published_commit(version), CORRUPTED_COMMIT))
            .into_iter()
            .collect();
        read_unity_managed_table_with_log(protocol, files, state, None).await
    }

    const CORRUPTED_COMMIT: &str = "{not valid delta json\n";
    const EMPTY_COMMIT: &str = "{\"commitInfo\":{\"timestamp\":1,\"inCommitTimestamp\":1}}\n";

    fn published_commit(version: i64) -> String {
        format!("{version:020}.json")
    }

    fn staged_commit(version: i64, id: &str) -> String {
        format!("_staged_commits/{version:020}.{id}.json")
    }

    fn ratified(latest_table_version: i64, commits: &[(i64, &str)]) -> DeltaRatifiedCommitResponse {
        DeltaRatifiedCommitResponse {
            latest_table_version,
            commits: commits
                .iter()
                .map(|(version, id)| DeltaRatifiedCommit {
                    version: *version,
                    timestamp: 1,
                    file_name: format!("{version:020}.{id}.json"),
                    file_size: 1,
                    file_modification_timestamp: 1,
                })
                .collect(),
        }
    }

    async fn put_log_files(store: &dyn ObjectStore, log_files: &[(String, &str)]) {
        for (file, content) in log_files {
            store
                .put(
                    &Path::from(format!("items/_delta_log/{file}")),
                    content.to_string().into(),
                )
                .await
                .unwrap();
        }
    }

    async fn read_unity_managed_table_with_log(
        protocol: &str,
        log_files: Vec<(String, &str)>,
        state: Option<DeltaCatalogState>,
        ratified: Option<DeltaRatifiedCommitResponse>,
    ) -> (usize, datafusion_common::Result<i64>) {
        let store = store_with_published_version_0(protocol).await;
        put_log_files(store.as_ref(), &log_files).await;
        read_unity_managed_table_in_store(store, state, ratified).await
    }

    /// Reads the table of a catalog that resolves the table with the state, and that returns
    /// the ratified commits when it is asked for them. Returns the number of the requests for
    /// the ratified commits with the version of the table that is read.
    async fn read_unity_managed_table_in_store(
        store: Arc<dyn ObjectStore>,
        state: Option<DeltaCatalogState>,
        ratified: Option<DeltaRatifiedCommitResponse>,
    ) -> (usize, datafusion_common::Result<i64>) {
        load_unity_managed_table(store, UnityManagedCatalog::new(state, ratified), vec![]).await
    }

    /// Reads the table of the catalog like `read_unity_managed_table_in_store` with the
    /// read options.
    async fn load_unity_managed_table(
        store: Arc<dyn ObjectStore>,
        catalog: UnityManagedCatalog,
        options: Vec<(&str, &str)>,
    ) -> (usize, datafusion_common::Result<i64>) {
        let catalog = Arc::new(catalog);
        let (session, context) = resolve_table(&catalog, LakehouseOperation::Read).await;
        let table_url = Url::parse(catalog.location).unwrap();
        session
            .runtime_env()
            .register_object_store(&table_url, store);
        let state = session.state();
        let result = load_delta_read_state(
            &state,
            table_url,
            None,
            DeltaReadOptions::resolve(
                &state,
                vec![OptionLayer::OptionList {
                    items: options
                        .into_iter()
                        .map(|(key, value)| (key.to_string(), value.to_string()))
                        .collect(),
                }],
            )
            .unwrap(),
            false,
            Some(context),
        )
        .await
        .map(|(snapshot, _, _)| snapshot.version());
        (catalog.ratified_commit_requests(), result)
    }

    /// The protocol and the metadata of version 0 of a table with the column `id`, with the
    /// table properties (`,"<key>":"<value>"...`) in addition to the Unity Catalog table ID.
    fn version_0_actions(protocol: &str, configuration: &str) -> String {
        format!(
            concat!(
                "{{\"protocol\":{protocol}}}\n",
                "{{\"metaData\":{{\"id\":\"table-id\",\"format\":{{\"provider\":\"parquet\",",
                "\"options\":{{}}}},\"schemaString\":\"{{\\\"type\\\":\\\"struct\\\",",
                "\\\"fields\\\":[{{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",",
                "\\\"nullable\\\":true,\\\"metadata\\\":{{}}}}]}}\",\"partitionColumns\":[],",
                "\"configuration\":{{\"io.unitycatalog.tableId\":\"table-id\"{configuration}}},",
                "\"createdTime\":0}}}}\n"
            ),
            protocol = protocol,
            configuration = configuration,
        )
    }

    async fn store_with_published_version_0(protocol: &str) -> Arc<InMemory> {
        let store = Arc::new(InMemory::new());
        let version_0 = version_0_actions(protocol, "");
        put_log_files(store.as_ref(), &[(published_commit(0), version_0.as_str())]).await;
        store
    }

    #[tokio::test]
    async fn unity_managed_table_reads_catalog_commits_when_catalog_states_catalog_managed() {
        // The commit that enables the feature on an existing table is not published yet, so
        // the published protocol does not have the feature.
        let store = store_with_published_version_0(NON_CATALOG_MANAGED_PROTOCOL).await;
        let enabling_commit = format!(
            "{}{{\"protocol\":{CATALOG_MANAGED_PROTOCOL}}}\n",
            EMPTY_COMMIT
        );
        let files = [(staged_commit(1, "a"), enabling_commit.as_str())];
        put_log_files(store.as_ref(), &files).await;
        let commits = ratified(1, &[(1, "a")]);
        let (requests, result) =
            read_unity_managed_table_in_store(store, told(Yes, Some(commits)), None).await;
        assert_eq!(result.unwrap(), 1);
        // The commits that the catalog returned with the table are used without a request.
        assert_eq!(requests, 0);
    }

    #[tokio::test]
    async fn catalog_managed_table_is_read_at_the_version_that_the_catalog_resolved() {
        // The catalog has ratified version 2 after it resolved the table at version 1. The
        // statement reads the table that the catalog resolved, and does not ask again.
        let store = store_with_published_version_0(CATALOG_MANAGED_PROTOCOL).await;
        let files = [
            (staged_commit(1, "a"), EMPTY_COMMIT),
            (staged_commit(2, "b"), EMPTY_COMMIT),
        ];
        put_log_files(store.as_ref(), &files).await;
        let (requests, result) = read_unity_managed_table_in_store(
            store,
            told(Yes, Some(ratified(1, &[(1, "a")]))),
            Some(ratified(2, &[(1, "a"), (2, "b")])),
        )
        .await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(requests, 0);
    }

    #[tokio::test]
    async fn write_that_replaces_data_is_planned_from_the_commits_that_the_catalog_has_then() {
        // The catalog has ratified version 2 after it resolved the table at version 1. A
        // write that is not a blind append asks the catalog once when it is planned, so
        // that it does not conflict with the commit of version 2.
        let catalog = Arc::new(UnityManagedCatalog::new(
            told(Yes, Some(ratified(1, &[(1, "a")]))),
            Some(ratified(2, &[(1, "a"), (2, "b")])),
        ));
        let (session, context) = resolve_table(&catalog, LakehouseOperation::Write).await;
        let refreshed = super::refresh_catalog_managed_commits(&session.state(), context)
            .await
            .unwrap();
        assert_eq!(
            refreshed.delta.unwrap().commits,
            Some(ratified(2, &[(1, "a"), (2, "b")]))
        );
        assert_eq!(catalog.ratified_commit_requests(), 1);

        // The catalog is not asked about a table that it resolved without its commits,
        // since they are requested when the table is loaded.
        for state in [
            told(Yes, None),
            told(No, None),
            told_before_log(Undetermined),
        ] {
            let catalog = Arc::new(UnityManagedCatalog::new(
                state.clone(),
                Some(ratified(2, &[(1, "a"), (2, "b")])),
            ));
            let (session, context) = resolve_table(&catalog, LakehouseOperation::Write).await;
            let refreshed = super::refresh_catalog_managed_commits(&session.state(), context)
                .await
                .unwrap();
            assert_eq!(refreshed.delta, state);
            assert_eq!(catalog.ratified_commit_requests(), 0);
        }
    }

    #[tokio::test]
    async fn catalog_managed_table_without_resolved_commits_requests_them_once() {
        // The catalog tells that the table is catalog-managed without its commits, as a
        // catalog with the legacy API does, so the commits are requested.
        let store = store_with_published_version_0(CATALOG_MANAGED_PROTOCOL).await;
        let files = [(staged_commit(1, "a"), EMPTY_COMMIT)];
        put_log_files(store.as_ref(), &files).await;
        let (requests, result) = read_unity_managed_table_in_store(
            store,
            told(Yes, None),
            Some(ratified(1, &[(1, "a")])),
        )
        .await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(requests, 1);
    }

    #[tokio::test]
    async fn unity_managed_table_reads_published_log_when_catalog_states_not_catalog_managed() {
        let store = store_with_published_version_0(NON_CATALOG_MANAGED_PROTOCOL).await;
        let files = [(staged_commit(1, "a"), EMPTY_COMMIT)];
        put_log_files(store.as_ref(), &files).await;
        let (requests, result) = read_unity_managed_table_in_store(
            store,
            told(No, None),
            Some(ratified(1, &[(1, "a")])),
        )
        .await;
        assert_eq!(result.unwrap(), 0);
        assert_eq!(requests, 0);
    }

    #[tokio::test]
    async fn table_with_catalog_managed_protocol_reads_catalog_commits_whatever_the_catalog_states()
    {
        // The published protocol has the feature although the catalog told that the table is
        // not catalog-managed, so the commits are requested and read.
        let store = store_with_published_version_0(CATALOG_MANAGED_PROTOCOL).await;
        let files = [(staged_commit(1, "a"), EMPTY_COMMIT)];
        put_log_files(store.as_ref(), &files).await;
        let (requests, result) = read_unity_managed_table_in_store(
            store,
            told(No, None),
            Some(ratified(1, &[(1, "a")])),
        )
        .await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(requests, 1);
    }

    #[tokio::test]
    async fn published_protocol_decides_when_the_catalog_does_not_tell() {
        // Version 1 is ratified and not published. It is read only when the published
        // protocol has the feature.
        for state in [None, told(Undetermined, None)] {
            for (protocol, version, expected_requests) in [
                (NON_CATALOG_MANAGED_PROTOCOL, 0, 0),
                (CATALOG_MANAGED_PROTOCOL, 1, 1),
            ] {
                let (requests, result) = read_unity_managed_table_with_log(
                    protocol,
                    vec![(staged_commit(1, "a"), EMPTY_COMMIT)],
                    state.clone(),
                    Some(ratified(1, &[(1, "a")])),
                )
                .await;
                assert_eq!(result.unwrap(), version, "{state:?}");
                assert_eq!(requests, expected_requests, "{state:?}");
            }
        }
    }

    #[tokio::test]
    async fn ratified_commits_are_read_in_pages() {
        // The versions 1 to 3 are ratified and not published. Version 3 has two staged
        // commits, so only the catalog tells which one is ratified.
        let store = store_with_published_version_0(CATALOG_MANAGED_PROTOCOL).await;
        let files = [
            (staged_commit(1, "a"), EMPTY_COMMIT),
            (staged_commit(2, "b"), EMPTY_COMMIT),
            (staged_commit(3, "c"), EMPTY_COMMIT),
            (staged_commit(3, "d"), EMPTY_COMMIT),
        ];
        put_log_files(store.as_ref(), &files).await;
        let catalog = UnityManagedCatalog {
            page_size: Some(2),
            ..UnityManagedCatalog::new(
                told(Yes, None),
                Some(ratified(3, &[(1, "a"), (2, "b"), (3, "c")])),
            )
        };
        let (requests, result) = load_unity_managed_table(store, catalog, vec![]).await;
        assert_eq!(result.unwrap(), 3);
        assert_eq!(requests, 2);
    }

    #[tokio::test]
    async fn table_that_is_not_catalog_managed_is_loaded_once() {
        let source = store_with_published_version_0(NON_CATALOG_MANAGED_PROTOCOL).await;
        let store = Arc::new(crate::test_store::ObservedStore::default());
        let version_0 = Path::from("items/_delta_log/00000000000000000000.json");
        let bytes = source.get(&version_0).await.unwrap().bytes().await.unwrap();
        store.put(&version_0, bytes.into()).await.unwrap();
        let (requests, result) =
            read_unity_managed_table_in_store(store.clone(), told(No, None), None).await;
        assert_eq!(result.unwrap(), 0);
        assert_eq!(requests, 0);
        let reads = store
            .gets()
            .into_iter()
            .filter(|path| *path == version_0)
            .count();
        assert_eq!(reads, 1);
    }

    #[tokio::test]
    async fn table_that_the_catalog_states_is_not_catalog_managed_does_not_read_catalog_commits() {
        // The published log of a table that is not catalog-managed cannot be loaded, and its
        // error is not replaced with the catalog commits.
        let (requests, result) = read_unity_managed_table_with_log(
            NON_CATALOG_MANAGED_PROTOCOL,
            vec![(published_commit(1), CORRUPTED_COMMIT)],
            told(No, None),
            Some(ratified(1, &[])),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(requests, 0);
    }

    #[tokio::test]
    async fn unity_managed_table_with_unreadable_published_log_reads_catalog_commits() {
        // The catalog does not tell whether the table is catalog-managed, and the published
        // commit of version 1 is unreadable, so the published log does not decide. The
        // catalog has the ratified commit of the version, which is read in its place.
        for state in [
            None,
            told(Undetermined, None),
            told_before_log(Undetermined),
        ] {
            let files = vec![
                (published_commit(1), CORRUPTED_COMMIT),
                (staged_commit(1, "a"), EMPTY_COMMIT),
            ];
            let (requests, result) = read_unity_managed_table_with_log(
                CATALOG_MANAGED_PROTOCOL,
                files,
                state.clone(),
                Some(ratified(1, &[(1, "a")])),
            )
            .await;
            assert_eq!(result.unwrap(), 1, "{state:?}");
            assert_eq!(requests, 1, "{state:?}");

            // The catalog has no commits for the table, and the error of the published log
            // is the error, with the error of the catalog as context.
            let (requests, result) =
                read_unity_managed_table(CATALOG_MANAGED_PROTOCOL, Some(1), state).await;
            assert_eq!(requests, 1);
            let error = result.unwrap_err();
            let message = error.to_string();
            assert!(
                message.contains(
                    "Failed to load the published Delta log of unity.default.items, and the \
                     catalog commits are not available"
                ) && message.contains("401 Unauthorized"),
                "{message}"
            );
            let root = error.find_root().to_string();
            assert!(root.contains("Invalid JSON in log record"), "{root}");
            assert!(!root.contains("401 Unauthorized"), "{root}");
        }
    }

    #[tokio::test]
    async fn table_with_unreadable_published_log_is_not_read_when_catalog_states_not_catalog_managed()
     {
        // The catalog has a ratified commit of the version whose published commit is
        // unreadable, and it is not asked for it.
        let files = vec![
            (published_commit(1), CORRUPTED_COMMIT),
            (staged_commit(1, "a"), EMPTY_COMMIT),
        ];
        let (requests, result) = read_unity_managed_table_with_log(
            CATALOG_MANAGED_PROTOCOL,
            files,
            told(No, None),
            Some(ratified(1, &[(1, "a")])),
        )
        .await;
        let error = result.unwrap_err().to_string();
        assert!(error.contains("Invalid JSON in log record"), "{error}");
        assert!(!error.contains("catalog commits"), "{error}");
        assert_eq!(requests, 0);
    }

    #[tokio::test]
    async fn catalog_managed_table_is_read_with_vended_storage_credentials() {
        use datafusion::execution::object_store::{
            DefaultObjectStoreRegistry, ObjectStoreRegistry,
        };
        use sail_common_datafusion::catalog::{StorageCredential, TableStorageCredentials};
        use sail_object_store::{VendedObjectStores, VendedStoreFactory};

        // The table only exists in the store that is used with the vended credentials.
        let vended = store_with_published_version_0(CATALOG_MANAGED_PROTOCOL).await;
        put_log_files(vended.as_ref(), &[(staged_commit(1, "a"), EMPTY_COMMIT)]).await;
        let ambient: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (_, result) = read_unity_managed_table_in_store(
            ambient.clone(),
            told(Yes, None),
            Some(ratified(1, &[(1, "a")])),
        )
        .await;
        assert!(result.is_err());

        let registry = DefaultObjectStoreRegistry::new();
        let location = Url::parse("memory:///items/").unwrap();
        registry.register_store(&location, ambient);
        let factory: VendedStoreFactory = {
            let vended = vended.clone();
            Arc::new(move |_| Ok(vended.clone() as Arc<dyn ObjectStore>))
        };
        VendedObjectStores::with_factory(factory)
            .install(
                &registry,
                &location,
                "unity:unity.default.items",
                TableStorageCredentials {
                    credential: StorageCredential::GcsOauth {
                        token: "token".to_string(),
                    },
                    expires_at_ms: None,
                    access: sail_common_datafusion::catalog::StorageAccess::Read,
                },
                Arc::new(|_| Box::pin(async { Err("not expected".to_string()) })),
            )
            .unwrap();
        let (requests, result) = read_unity_managed_table_in_store(
            registry.get_store(&location).unwrap(),
            told(Yes, None),
            Some(ratified(1, &[(1, "a")])),
        )
        .await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(requests, 1);
    }

    #[tokio::test]
    async fn unity_managed_table_without_catalog_managed_feature_reads_published_log() {
        let (requests, result) =
            read_unity_managed_table(NON_CATALOG_MANAGED_PROTOCOL, None, None).await;
        assert_eq!(requests, 0);
        assert_eq!(result.unwrap(), 0);
    }

    const NON_CATALOG_MANAGED_PROTOCOL: &str = r#"{"minReaderVersion":1,"minWriterVersion":2}"#;

    const CATALOG_MANAGED_PROTOCOL: &str = concat!(
        r#"{"minReaderVersion":3,"minWriterVersion":7,"#,
        r#""readerFeatures":["catalogManaged","vacuumProtocolCheck"],"#,
        r#""writerFeatures":["catalogManaged","inCommitTimestamp","vacuumProtocolCheck"]}"#
    );

    #[tokio::test]
    async fn unity_managed_table_with_catalog_managed_feature_reads_catalog_commits() {
        let (requests, result) =
            read_unity_managed_table(CATALOG_MANAGED_PROTOCOL, None, None).await;
        assert_eq!(requests, 1);
        assert!(result.unwrap_err().to_string().contains("401 Unauthorized"));
    }

    #[tokio::test]
    async fn table_that_the_catalog_states_is_catalog_managed_reads_catalog_commits() {
        // The catalog decides without consulting the published protocol.
        let (requests, result) =
            read_unity_managed_table(NON_CATALOG_MANAGED_PROTOCOL, None, told(Yes, None)).await;
        assert_eq!(requests, 1);
        assert!(result.unwrap_err().to_string().contains("401 Unauthorized"));
    }

    #[tokio::test]
    async fn catalog_managed_table_reads_published_commits_not_returned_by_catalog() {
        // The catalog returns only the commits that are not published yet.
        let files = vec![
            (published_commit(1), EMPTY_COMMIT),
            (published_commit(2), EMPTY_COMMIT),
            (staged_commit(3, "a"), EMPTY_COMMIT),
        ];
        let (requests, result) = read_unity_managed_table_with_log(
            CATALOG_MANAGED_PROTOCOL,
            files,
            None,
            Some(ratified(3, &[(3, "a")])),
        )
        .await;
        assert_eq!(result.unwrap(), 3);
        // One request by the snapshot load, with no request per published version.
        assert_eq!(requests, 1);
    }

    #[tokio::test]
    async fn catalog_managed_table_reads_fully_published_log() {
        // Several staged files for a published version are not an error.
        let files = vec![
            (published_commit(1), EMPTY_COMMIT),
            (published_commit(2), EMPTY_COMMIT),
            (staged_commit(2, "a"), CORRUPTED_COMMIT),
            (staged_commit(2, "b"), CORRUPTED_COMMIT),
        ];
        let (requests, result) = read_unity_managed_table_with_log(
            CATALOG_MANAGED_PROTOCOL,
            files,
            None,
            Some(ratified(2, &[])),
        )
        .await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(requests, 1);
    }

    #[tokio::test]
    async fn catalog_managed_table_prefers_catalog_commit_over_published_commit() {
        let files = vec![
            (published_commit(1), CORRUPTED_COMMIT),
            (staged_commit(1, "a"), EMPTY_COMMIT),
        ];
        let (_, result) = read_unity_managed_table_with_log(
            CATALOG_MANAGED_PROTOCOL,
            files,
            told(Yes, None),
            Some(ratified(1, &[(1, "a")])),
        )
        .await;
        assert_eq!(result.unwrap(), 1);
    }

    #[tokio::test]
    async fn catalog_managed_table_does_not_read_staged_commit_not_returned_by_catalog() {
        // The catalog does not return version 1, so its staged commit is not known to be
        // ratified. It is not read in place of a published commit that is unreadable or
        // missing.
        for published in [Some(CORRUPTED_COMMIT), None] {
            let mut files = vec![
                (staged_commit(1, "a"), EMPTY_COMMIT),
                (staged_commit(2, "b"), EMPTY_COMMIT),
            ];
            files.extend(published.map(|content| (published_commit(1), content)));
            let (_, result) = read_unity_managed_table_with_log(
                CATALOG_MANAGED_PROTOCOL,
                files,
                told(Yes, None),
                Some(ratified(2, &[(2, "b")])),
            )
            .await;
            let error = result.unwrap_err().to_string();
            match published {
                Some(_) => assert!(error.contains("Invalid JSON in log record"), "{error}"),
                None => assert!(error.contains("version 1"), "{error}"),
            }
        }
    }

    #[tokio::test]
    async fn catalog_managed_table_rejects_commit_file_of_another_version() {
        // The catalog returns the staged commit of version 1 as the commit of version 2. The
        // actions of version 1 are not read as the actions of version 2.
        let files = vec![
            (published_commit(1), EMPTY_COMMIT),
            (staged_commit(1, "a"), EMPTY_COMMIT),
        ];
        let mut response = ratified(2, &[(2, "a")]);
        response.commits[0].file_name = staged_commit(1, "a")
            .trim_start_matches("_staged_commits/")
            .to_string();
        let (_, result) = read_unity_managed_table_with_log(
            CATALOG_MANAGED_PROTOCOL,
            files,
            told(Yes, None),
            Some(response),
        )
        .await;
        let error = result.unwrap_err().to_string();
        assert!(error.contains("for version 2"), "{error}");
        assert!(error.contains("00000000000000000001.a.json"), "{error}");
    }

    #[tokio::test]
    async fn catalog_managed_table_read_fails_when_catalog_commit_file_is_missing() {
        // The catalog returns the commit of version 1, and its file does not exist. The
        // published commit of the version is not read in its place.
        let files = vec![(published_commit(1), EMPTY_COMMIT)];
        let (_, result) = read_unity_managed_table_with_log(
            CATALOG_MANAGED_PROTOCOL,
            files,
            told(Yes, None),
            Some(ratified(1, &[(1, "a")])),
        )
        .await;
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("_delta_log/_staged_commits/00000000000000000001.a.json"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn catalog_managed_table_rejects_version_neither_published_nor_ratified() {
        let files = vec![
            (published_commit(1), EMPTY_COMMIT),
            (staged_commit(3, "a"), EMPTY_COMMIT),
        ];
        let (_, result) = read_unity_managed_table_with_log(
            CATALOG_MANAGED_PROTOCOL,
            files,
            None,
            Some(ratified(3, &[(3, "a")])),
        )
        .await;
        let error = result.unwrap_err().to_string();
        assert!(error.contains("version 2"), "{error}");
    }

    #[tokio::test]
    async fn catalog_managed_table_ignores_staged_commit_that_lost_the_commit_race() {
        // The catalog rejected the staged commit of version 1, and the commit of another
        // writer is published as version 1. The second staged commit has the size of the
        // published commit.
        let same_size = EMPTY_COMMIT.replacen('{', "x", 1);
        for orphan in [CORRUPTED_COMMIT, same_size.as_str()] {
            let files = vec![
                (published_commit(1), EMPTY_COMMIT),
                (staged_commit(1, "a"), orphan),
            ];
            let (_, result) = read_unity_managed_table_with_log(
                CATALOG_MANAGED_PROTOCOL,
                files,
                told(Yes, None),
                Some(ratified(1, &[])),
            )
            .await;
            assert_eq!(result.unwrap(), 1);
        }
    }

    /// Commits to a catalog-managed table whose catalog answers every commit request with
    /// the error, and returns whether the staged commit exists after the commit failed.
    async fn staged_commit_exists_after_catalog_error(error: fn() -> CatalogError) -> bool {
        let steps = (0..4).map(|_| CommitStep::Refuse(error)).collect();
        let commit = commit_with_script(steps, false).await;
        assert!(commit.result.is_err());
        assert_eq!(commit.latest_table_version(), 0);
        !commit.staged_commits().await.is_empty()
    }

    #[tokio::test(start_paused = true)]
    async fn rejected_catalog_commit_deletes_staged_commit() {
        for error in [
            (|| CatalogError::Conflict("409".into())) as fn() -> CatalogError,
            || CatalogError::InvalidArgument("400".into()),
            || CatalogError::Forbidden("403".into()),
            || CatalogError::RateLimited("429".into()),
        ] {
            assert!(!staged_commit_exists_after_catalog_error(error).await);
        }
    }

    /// Reads the table without a Delta log, and returns the files of the store after it.
    async fn read_unity_managed_table_without_log(
        state: Option<DeltaCatalogState>,
    ) -> (datafusion_common::Result<i64>, Vec<String>) {
        let store = Arc::new(InMemory::new());
        let (_, result) =
            read_unity_managed_table_in_store(store.clone(), state, Some(ratified(0, &[]))).await;
        let files = store.list(None).try_collect::<Vec<_>>().await.unwrap();
        let files = files.into_iter().map(|meta| meta.location.to_string());
        (result, files.collect())
    }

    #[tokio::test]
    async fn read_of_table_without_log_writes_nothing_when_catalog_publishes_version_0() {
        // The catalog registers a table after version 0 is written, so a table without a
        // Delta log is an error whatever the catalog tells about it.
        for state in [
            told(Yes, None),
            told(Yes, Some(ratified(0, &[]))),
            told(No, None),
            told(Undetermined, None),
            None,
        ] {
            let (result, files) = read_unity_managed_table_without_log(state.clone()).await;
            assert!(result.is_err(), "{state:?}");
            assert_eq!(files, Vec::<String>::new(), "{state:?}");
        }
    }

    #[tokio::test]
    async fn read_of_catalog_managed_table_without_log_names_the_table_and_its_location() {
        for commits in [None, Some(ratified(0, &[]))] {
            let (result, _) = read_unity_managed_table_without_log(told(Yes, commits)).await;
            let error = result.unwrap_err().to_string();
            assert!(
                error.contains(
                    "Delta log not found at `memory:///items/` for the catalog-managed table \
                     `unity.default.items`; check the table location and the storage credentials"
                ),
                "{error}"
            );
        }
    }

    /// Reads the table without a Delta log of a catalog that registers a table before
    /// version 0 of the table is written, and asserts that the read writes version 0.
    async fn assert_read_of_table_without_log_creates_version_0(
        catalog_managed: DeltaCatalogManaged,
    ) {
        let (result, files) =
            read_unity_managed_table_without_log(told_before_log(catalog_managed)).await;
        assert_eq!(result.unwrap(), 0);
        assert!(
            files.contains(&"items/_delta_log/00000000000000000000.json".to_string()),
            "{files:?}"
        );
    }

    #[tokio::test]
    async fn read_of_table_without_log_creates_version_0_for_legacy_catalog() {
        assert_read_of_table_without_log_creates_version_0(Yes).await;
    }

    #[tokio::test]
    async fn read_of_table_without_log_creates_version_0_when_the_catalog_does_not_tell() {
        assert_read_of_table_without_log_creates_version_0(Undetermined).await;
    }

    #[tokio::test]
    async fn read_of_table_without_log_uses_version_0_of_a_concurrent_bootstrap() {
        use crate::test_store::ObservedStore;

        // Another reader or the creator of the table writes version 0 after the catalog is
        // asked for the commits and before version 0 is written here.
        let version_0 = Path::from("items/_delta_log/00000000000000000000.json");
        let other = store_with_published_version_0(CATALOG_MANAGED_PROTOCOL).await;
        let bytes = other.get(&version_0).await.unwrap().bytes().await.unwrap();
        let store = Arc::new(ObservedStore::default());
        store.write_first(version_0.clone(), bytes.clone());
        let (_, result) = read_unity_managed_table_in_store(
            store.clone(),
            told_before_log(Yes),
            Some(ratified(0, &[])),
        )
        .await;
        assert_eq!(result.unwrap(), 0);
        let written = store.get(&version_0).await.unwrap().bytes().await.unwrap();
        assert_eq!(written, bytes);
        let version_1 = Path::from("items/_delta_log/00000000000000000001.json");
        assert!(store.head(&version_1).await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn catalog_commit_with_unknown_outcome_keeps_staged_commit() {
        // A server error, a timeout, or a network error may follow a ratified commit.
        assert!(staged_commit_exists_after_catalog_error(state_unknown).await);
    }
    const ADD_ACTION: &str = concat!(
        r#"{"add":{"path":"winner.parquet","partitionValues":{},"size":5,"#,
        r#""modificationTime":1,"dataChange":true}}"#
    );
    const METADATA_ACTION: &str = concat!(
        r#"{"metaData":{"id":"table-id","format":{"provider":"parquet","options":{}},"#,
        r#""schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\","#,
        r#"\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","#,
        r#""partitionColumns":[],"configuration":{"io.unitycatalog.tableId":"table-id","#,
        r#""delta.enableInCommitTimestamps":"true"},"createdTime":0}}"#
    );

    fn state_unknown() -> CatalogError {
        CatalogError::CommitStateUnknown("503".to_string().into())
    }

    struct ScriptedCommit {
        catalog: Arc<UnityManagedCatalog>,
        store: Arc<InMemory>,
        result: datafusion_common::Result<crate::transaction::CatalogManagedStagedCommit>,
        /// The versions of the commits that wrote a checkpoint.
        checkpoints: Vec<i64>,
    }

    impl ScriptedCommit {
        /// Returns the version and the file name of each commit request.
        fn requests(&self) -> Vec<(i64, String)> {
            let requests = self.script().requests.lock().unwrap();
            let commit = |commit: &DeltaCatalogCommit| {
                let info = &commit.commit_info;
                (info.version, info.file_name.clone())
            };
            requests.iter().map(commit).collect()
        }

        fn script(&self) -> &CommitScript {
            self.catalog.script.as_ref().unwrap()
        }

        fn latest_table_version(&self) -> i64 {
            self.script().ratified.lock().unwrap().latest_table_version
        }

        /// Returns the number of the requests for the ratified commits of the table.
        fn ratified_commit_requests(&self) -> usize {
            self.catalog.ratified_commit_requests()
        }

        async fn staged_commits(&self) -> Vec<String> {
            let prefix = Path::from("_delta_log/_staged_commits");
            let files = self.store.list(Some(&prefix)).try_collect::<Vec<_>>();
            let files = files.await.unwrap().into_iter();
            let mut files = files
                .map(|meta| meta.location.filename().unwrap().to_string())
                .collect::<Vec<_>>();
            files.sort();
            files
        }
    }

    /// Commits a file to a catalog-managed table at version 0, where the catalog follows the
    /// steps for the commit requests. The commit is a blind append, or a write that depends
    /// on what it read from the table.
    async fn commit_with_script(steps: Vec<CommitStep>, blind_append: bool) -> ScriptedCommit {
        commit_with_script_after(steps, blind_append, UnpublishedCommit::None).await
    }

    /// The ratified commits that the catalog returned when it resolved the table of a
    /// commit.
    #[derive(Clone, Copy, PartialEq)]
    enum Resolved {
        /// The commits of the table as it is when the commit is sent.
        Current,
        /// No commits, as a catalog with the legacy API.
        NotReturned,
        /// The commits of the table before the catalog ratified the commit of another
        /// writer with the actions, which it did before the commit is sent.
        Stale(&'static str),
    }

    /// Commits a file like `commit_with_script`, to a table that the catalog resolved with
    /// the ratified commits.
    async fn commit_resolved(resolved: Resolved, blind_append: bool) -> ScriptedCommit {
        commit_with_script_in(
            vec![],
            blind_append,
            UnpublishedCommit::None,
            CATALOG_MANAGED_PROTOCOL,
            "",
            1,
            resolved,
        )
        .await
    }

    /// The commit of another writer that the catalog ratified as version 1, and that is not
    /// published.
    #[derive(PartialEq)]
    enum UnpublishedCommit {
        None,
        /// The staged commit of version 1 exists.
        Staged,
        /// The staged commit of version 1 is gone after the commit was read.
        Missing,
    }

    const OTHER_WRITER_COMMIT: &str = "00000000000000000001.other.json";

    /// Commits a file like `commit_with_script`, with a reference snapshot that has the
    /// unpublished commit of another writer. The snapshot has only the state of the table,
    /// as the snapshot of a write that does not read the files of the table.
    async fn commit_with_script_after(
        steps: Vec<CommitStep>,
        blind_append: bool,
        unpublished: UnpublishedCommit,
    ) -> ScriptedCommit {
        commit_with_script_in(
            steps,
            blind_append,
            unpublished,
            CATALOG_MANAGED_PROTOCOL,
            "",
            1,
            Resolved::Current,
        )
        .await
    }

    /// Commits files like `commit_with_script_after`, one after another, to a table with the
    /// protocol and the table properties (`,"<key>":"<value>"...`) in addition to the table
    /// properties of a catalog-managed table. Every commit is a commit of one file, and is
    /// a commit to the table as the catalog resolved it once, with the ratified commits.
    async fn commit_with_script_in(
        steps: Vec<CommitStep>,
        blind_append: bool,
        unpublished: UnpublishedCommit,
        protocol: &str,
        configuration: &str,
        commits: usize,
        resolved: Resolved,
    ) -> ScriptedCommit {
        use crate::catalog::coordinator::{
            DeltaCatalogCommitCoordinator, DeltaCatalogManagedTable,
        };
        use crate::delta_log::{StorageConfig, default_logstore};
        use crate::physical_plan::DeltaSnapshotContext;
        use crate::snapshot::{
            CatalogManagedCommitFile, CatalogManagedCommitSet, DeltaSnapshotConfig,
        };
        use crate::spec::{Add, CommitAction, DeltaOperation, SaveMode};
        use crate::table::DeltaSnapshot;
        use crate::transaction::CommitProperties;

        let store = Arc::new(InMemory::new());
        let configuration = format!(r#","delta.enableInCommitTimestamps":"true"{configuration}"#);
        let version_0 = format!(
            "{EMPTY_COMMIT}{}",
            version_0_actions(protocol, &configuration)
        );
        store
            .put(
                &Path::from(format!("_delta_log/{}", published_commit(0))),
                version_0.into(),
            )
            .await
            .unwrap();
        let latest = if unpublished == UnpublishedCommit::None {
            ratified(0, &[])
        } else {
            ratified(1, &[(1, "other")])
        };
        let catalog = Arc::new(UnityManagedCatalog {
            location: "memory:///",
            script: Some(CommitScript {
                store: store.clone(),
                steps: Mutex::new(steps.into()),
                ratified: Mutex::new(latest.clone()),
                requests: Mutex::new(vec![]),
            }),
            ..UnityManagedCatalog::new(
                told(Yes, (resolved != Resolved::NotReturned).then_some(latest)),
                None,
            )
        });
        let (session, context) = resolve_table(&catalog, LakehouseOperation::Write).await;
        let table_url = Url::parse(catalog.location).unwrap();
        let object_store = store.clone() as Arc<dyn ObjectStore>;
        let log_store = default_logstore(
            object_store.clone(),
            object_store,
            &table_url,
            &StorageConfig,
        );
        let table = DeltaCatalogManagedTable::from_context(&context).unwrap();
        let snapshot = if unpublished == UnpublishedCommit::None {
            let config = DeltaSnapshotConfig {
                require_files: false,
                ..Default::default()
            };
            DeltaSnapshot::try_new(log_store.as_ref(), config, Some(0), None)
                .await
                .unwrap()
        } else {
            let path = Path::from(format!("_delta_log/_staged_commits/{OTHER_WRITER_COMMIT}"));
            let commit = format!(
                "{{\"commitInfo\":{{\"timestamp\":2,\"inCommitTimestamp\":2,\"isBlindAppend\":true}}}}\n{ADD_ACTION}\n"
            );
            store.put(&path, commit.into()).await.unwrap();
            let config = DeltaSnapshotConfig {
                require_files: false,
                catalog_managed_commits: Some(CatalogManagedCommitSet {
                    latest_table_version: 1,
                    commits: vec![CatalogManagedCommitFile {
                        version: 1,
                        file_name: OTHER_WRITER_COMMIT.to_string(),
                        file_size: 0,
                        file_modification_timestamp: 0,
                    }],
                }),
                ..Default::default()
            };
            let snapshot = DeltaSnapshot::try_new(log_store.as_ref(), config, Some(1), None)
                .await
                .unwrap();
            if unpublished == UnpublishedCommit::Missing {
                store.delete(&path).await.unwrap();
            }
            let config = DeltaSnapshotConfig {
                require_files: false,
                ..Default::default()
            };
            DeltaSnapshotContext::from_snapshot(&snapshot)
                .to_snapshot(log_store.as_ref(), config)
                .unwrap()
        };
        let operation = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: None,
            predicate: (!blind_append).then(|| "id > 0".to_string()),
        };
        if let Resolved::Stale(actions) = resolved {
            catalog
                .script
                .as_ref()
                .unwrap()
                .ratify_winner(actions)
                .await;
        }
        let state = session.state();
        let snapshot = Arc::new(snapshot);
        let mut checkpoints = vec![];
        let mut result = None;
        for commit in 0..commits {
            let add = Add {
                path: if commit == 0 {
                    "new.parquet".to_string()
                } else {
                    format!("new-{commit}.parquet")
                },
                size: 5,
                data_change: true,
                ..Default::default()
            };
            let commit_result = DeltaCatalogCommitCoordinator::new(&state, &context)
                .commit_actions(
                    &table,
                    &log_store,
                    Some(snapshot.clone()),
                    &[CommitAction::Add(add)],
                    &operation,
                    CommitProperties::default(),
                )
                .await;
            if let Ok(staged) = &commit_result
                && staged.metrics.new_checkpoint_created
            {
                checkpoints.push(staged.version);
            }
            result = Some(commit_result);
        }
        ScriptedCommit {
            catalog,
            store,
            result: result.unwrap(),
            checkpoints,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn catalog_commit_with_unknown_outcome_is_sent_again_unchanged() {
        for step in [
            CommitStep::Refuse(state_unknown),
            // The catalog recognizes the commit that it ratified for the first request.
            CommitStep::RatifyAndFail(state_unknown),
            CommitStep::Refuse(|| CatalogError::RateLimited("429".into())),
        ] {
            let commit = commit_with_script(vec![step], false).await;
            let staged = commit.result.as_ref().unwrap();
            assert_eq!(staged.version, 1);
            let request = (1, staged.file_name.clone());
            assert_eq!(commit.requests(), vec![request.clone(), request]);
            assert_eq!(commit.latest_table_version(), 1);
            assert_eq!(
                commit.staged_commits().await,
                vec![staged.file_name.clone()]
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn catalog_commit_with_unknown_outcome_is_found_in_the_commits_of_the_table() {
        for ratify in [CommitStep::RatifyAndFail, CommitStep::PublishAndFail] {
            // The catalog does not answer any request, and has ratified the commit.
            let steps = vec![
                ratify(state_unknown),
                CommitStep::Refuse(state_unknown),
                CommitStep::Refuse(state_unknown),
                CommitStep::Refuse(state_unknown),
            ];
            let commit = commit_with_script(steps, false).await;
            let staged = commit.result.as_ref().unwrap();
            assert_eq!(commit.requests(), vec![(1, staged.file_name.clone()); 4]);
            assert_eq!(
                commit.staged_commits().await,
                vec![staged.file_name.clone()]
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn catalog_commit_that_is_not_found_has_unknown_state() {
        let steps = (0..4).map(|_| CommitStep::Refuse(state_unknown)).collect();
        let commit = commit_with_script(steps, true).await;
        let error = commit.result.as_ref().err().unwrap().to_string();
        assert!(error.contains("commit state unknown"), "{error}");
        // The same commit is sent each time, and its file is kept.
        let requests = commit.requests();
        assert_eq!(requests, vec![requests[0].clone(); 4]);
        assert_eq!(commit.staged_commits().await, vec![requests[0].1.clone()]);
        assert_eq!(commit.latest_table_version(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn catalog_commit_with_unknown_outcome_is_not_reported_as_refused() {
        // The catalog does not answer the first request, and then throttles the same request,
        // refuses to authorize it, or is at its limit when the request is sent again. The
        // catalog may still ratify the first request, so the outcome is not known and the
        // staged commit is kept.
        let refusals: [fn() -> CatalogError; 4] = [
            || CatalogError::RateLimited("429".into()),
            || CatalogError::Unauthorized("401".into()),
            || CatalogError::Forbidden("403".into()),
            || CatalogError::ResourceExhausted("429".into()),
        ];
        for refusal in refusals {
            let steps = std::iter::once(CommitStep::Refuse(state_unknown))
                .chain((0..3).map(|_| CommitStep::Refuse(refusal)))
                .collect();
            let commit = commit_with_script(steps, true).await;
            let error = commit.result.as_ref().err().unwrap().to_string();
            assert!(error.contains("commit state unknown"), "{error}");
            let requests = commit.requests();
            assert_eq!(commit.staged_commits().await, vec![requests[0].1.clone()]);
            assert_eq!(commit.latest_table_version(), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn catalog_commit_with_unknown_outcome_that_another_writer_won_is_a_conflict() {
        let steps = vec![
            CommitStep::Refuse(state_unknown),
            CommitStep::Lose(ADD_ACTION),
        ];
        let commit = commit_with_script(steps, false).await;
        let error = commit.result.as_ref().err().unwrap().to_string();
        assert!(error.contains("Delta catalog commit conflict"), "{error}");
        assert_eq!(commit.requests().len(), 2);
        assert_eq!(
            commit.staged_commits().await,
            vec![format!("{:020}.winner.json", 1)]
        );
    }

    #[tokio::test]
    async fn blind_append_is_committed_after_the_commit_of_another_writer() {
        let commit = commit_with_script(vec![CommitStep::Lose(ADD_ACTION)], true).await;
        let staged = commit.result.as_ref().unwrap();
        assert_eq!((staged.version, staged.metrics.num_retries), (2, 1));
        let requests = commit.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            (requests[0].0, &requests[1]),
            (1, &(2, staged.file_name.clone()))
        );
        // The staged commit that lost is deleted.
        assert_eq!(
            commit.staged_commits().await,
            vec![format!("{:020}.winner.json", 1), staged.file_name.clone()]
        );
        // The commits are requested once, to stage the commit after the other writer.
        assert_eq!(commit.ratified_commit_requests(), 1);
    }

    #[tokio::test]
    async fn blind_append_conflicts_with_metadata_change_of_another_writer() {
        let commit = commit_with_script(vec![CommitStep::Lose(METADATA_ACTION)], true).await;
        let error = commit.result.as_ref().err().unwrap().to_string();
        assert!(
            error.contains("Metadata changed since last commit"),
            "{error}"
        );
        assert_eq!(commit.requests().len(), 1);
        assert_eq!(commit.latest_table_version(), 1);
    }

    #[tokio::test]
    async fn write_that_is_not_a_blind_append_fails_on_conflict() {
        let commit = commit_with_script(vec![CommitStep::Lose(ADD_ACTION)], false).await;
        let error = commit.result.as_ref().err().unwrap().to_string();
        assert!(error.contains("Delta catalog commit conflict"), "{error}");
        assert_eq!(commit.requests().len(), 1);
        assert_eq!(
            commit.staged_commits().await,
            vec![format!("{:020}.winner.json", 1)]
        );
    }

    #[tokio::test]
    async fn commit_to_the_resolved_table_does_not_request_the_ratified_commits() {
        for blind_append in [true, false] {
            let commit = commit_resolved(Resolved::Current, blind_append).await;
            let staged = commit.result.as_ref().unwrap();
            assert_eq!((staged.version, staged.metrics.num_retries), (1, 0));
            assert_eq!(commit.requests(), vec![(1, staged.file_name.clone())]);
            assert_eq!(commit.ratified_commit_requests(), 0);
        }
        // The reference snapshot has the unpublished commit of another writer that the
        // catalog returned when it resolved the table.
        let commit = commit_with_script_after(vec![], true, UnpublishedCommit::Staged).await;
        assert_eq!(commit.result.as_ref().unwrap().version, 2);
        assert_eq!(commit.ratified_commit_requests(), 0);
    }

    #[tokio::test]
    async fn commit_to_a_table_resolved_without_commits_requests_them_once() {
        for blind_append in [true, false] {
            let commit = commit_resolved(Resolved::NotReturned, blind_append).await;
            let staged = commit.result.as_ref().unwrap();
            assert_eq!((staged.version, staged.metrics.num_retries), (1, 0));
            assert_eq!(commit.ratified_commit_requests(), 1);
        }
    }

    #[tokio::test]
    async fn blind_append_to_a_table_that_changed_since_it_was_resolved_is_committed() {
        // Another writer committed version 1 after the catalog resolved the table at
        // version 0. The catalog refuses the commit that is staged as version 1, and the
        // commits are requested once to stage the commit again as version 2.
        let commit = commit_resolved(Resolved::Stale(ADD_ACTION), true).await;
        let staged = commit.result.as_ref().unwrap();
        assert_eq!((staged.version, staged.metrics.num_retries), (2, 1));
        let requests = commit.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            (requests[0].0, &requests[1]),
            (1, &(2, staged.file_name.clone()))
        );
        assert_eq!(commit.ratified_commit_requests(), 1);
        // The staged commit that the catalog refused is deleted.
        assert_eq!(
            commit.staged_commits().await,
            vec![format!("{:020}.winner.json", 1), staged.file_name.clone()]
        );
        assert_eq!(commit.latest_table_version(), 2);
    }

    #[tokio::test]
    async fn blind_append_to_a_table_whose_metadata_changed_since_it_was_resolved_fails() {
        let commit = commit_resolved(Resolved::Stale(METADATA_ACTION), true).await;
        let error = commit.result.as_ref().err().unwrap().to_string();
        assert!(
            error.contains("Metadata changed since last commit"),
            "{error}"
        );
        assert_eq!(commit.requests().len(), 1);
        assert_eq!(commit.ratified_commit_requests(), 1);
        assert_eq!(commit.latest_table_version(), 1);
        assert_eq!(
            commit.staged_commits().await,
            vec![format!("{:020}.winner.json", 1)]
        );
    }

    #[tokio::test]
    async fn write_that_is_not_a_blind_append_fails_when_the_table_changed_since_it_was_resolved() {
        let commit = commit_resolved(Resolved::Stale(ADD_ACTION), false).await;
        let error = commit.result.as_ref().err().unwrap().to_string();
        assert!(error.contains("Delta catalog commit conflict"), "{error}");
        assert_eq!(commit.requests().len(), 1);
        assert_eq!(commit.ratified_commit_requests(), 0);
        assert_eq!(
            commit.staged_commits().await,
            vec![format!("{:020}.winner.json", 1)]
        );
        assert_eq!(commit.latest_table_version(), 1);
    }

    fn backfilled_versions(commit: &ScriptedCommit) -> Vec<serde_json::Value> {
        let requests = commit.script().requests.lock().unwrap();
        requests
            .iter()
            .map(|request| serde_json::json!(request.latest_backfilled_version))
            .collect()
    }

    async fn published_commit_bytes(commit: &ScriptedCommit, version: i64) -> Option<Vec<u8>> {
        let path = Path::from(format!("_delta_log/{}", published_commit(version)));
        let result = commit.store.get(&path).await;
        if matches!(result, Err(object_store::Error::NotFound { .. })) {
            return None;
        }
        Some(result.unwrap().bytes().await.unwrap().to_vec())
    }

    #[tokio::test]
    async fn blind_append_publishes_the_commit_of_another_writer_first() {
        let commit = commit_with_script(vec![CommitStep::Lose(ADD_ACTION)], true).await;
        let staged = commit.result.as_ref().unwrap();
        assert_eq!(staged.version, 2);
        let published = published_commit_bytes(&commit, 1).await.unwrap();
        assert!(published.ends_with(format!("{ADD_ACTION}\n").as_bytes()));
        assert_eq!(
            backfilled_versions(&commit),
            vec![serde_json::json!(0), serde_json::json!(1)]
        );
        assert!(published_commit_bytes(&commit, 2).await.is_some());
    }

    #[tokio::test]
    async fn commit_publishes_the_unpublished_commit_before_it() {
        let commit = commit_with_script_after(vec![], true, UnpublishedCommit::Staged).await;
        let staged = commit.result.as_ref().unwrap();
        assert_eq!(staged.version, 2);
        let path = Path::from(format!("_delta_log/_staged_commits/{OTHER_WRITER_COMMIT}"));
        let other = commit
            .store
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(
            published_commit_bytes(&commit, 1).await,
            Some(other.to_vec())
        );
        assert_eq!(backfilled_versions(&commit), vec![serde_json::json!(1)]);
        assert!(published_commit_bytes(&commit, 2).await.is_some());
    }

    #[tokio::test]
    async fn commit_does_not_report_an_unpublished_commit_as_backfilled() {
        // Version 1 cannot be published, so the commit request reports version 0 as the
        // latest backfilled version, and version 2 is not published before version 1.
        let commit = commit_with_script_after(vec![], true, UnpublishedCommit::Missing).await;
        let staged = commit.result.as_ref().unwrap();
        assert_eq!(staged.version, 2);
        assert_eq!(backfilled_versions(&commit), vec![serde_json::json!(0)]);
        assert_eq!(published_commit_bytes(&commit, 1).await, None);
        assert_eq!(published_commit_bytes(&commit, 2).await, None);
    }

    const CATALOG_MANAGED_V2_CHECKPOINT_PROTOCOL: &str = concat!(
        r#"{"minReaderVersion":3,"minWriterVersion":7,"#,
        r#""readerFeatures":["catalogManaged","v2Checkpoint","vacuumProtocolCheck"],"#,
        r#""writerFeatures":["catalogManaged","inCommitTimestamp","v2Checkpoint","vacuumProtocolCheck"]}"#
    );

    /// Returns the versions of the checkpoints in the log, and the version in the last
    /// checkpoint file.
    async fn checkpoint_versions(commit: &ScriptedCommit) -> (Vec<i64>, Option<i64>) {
        let prefix = Path::from("_delta_log");
        let files = commit.store.list(Some(&prefix)).try_collect::<Vec<_>>();
        let mut versions = files
            .await
            .unwrap()
            .into_iter()
            .filter_map(|meta| {
                let name = meta.location.filename()?.to_string();
                let (version, rest) = name.split_once('.')?;
                rest.starts_with("checkpoint.")
                    .then(|| version.parse::<i64>().unwrap())
            })
            .collect::<Vec<_>>();
        versions.sort();
        versions.dedup();
        let last = match commit
            .store
            .get(&Path::from("_delta_log/_last_checkpoint"))
            .await
        {
            Ok(result) => {
                let bytes = result.bytes().await.unwrap();
                let hint: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                hint["version"].as_i64()
            }
            Err(_) => None,
        };
        (versions, last)
    }

    /// Opens the table at the latest version of the catalog after the commits before the
    /// latest checkpoint are deleted, which only succeeds when the open reads the checkpoint.
    /// Returns the version and the number of files of the table.
    async fn open_from_latest_checkpoint(commit: &ScriptedCommit) -> (i64, usize) {
        use crate::delta_log::{StorageConfig, default_logstore};
        use crate::snapshot::{
            CatalogManagedCommitFile, CatalogManagedCommitSet, DeltaSnapshotConfig,
        };
        use crate::table::DeltaSnapshot;

        let checkpoint = checkpoint_versions(commit).await.1.unwrap();
        let ratified = commit.script().ratified.lock().unwrap().clone();
        for version in 0..=checkpoint {
            let path = Path::from(format!("_delta_log/{}", published_commit(version)));
            commit.store.delete(&path).await.unwrap();
        }
        for staged in &ratified.commits {
            if staged.version <= checkpoint {
                let path = Path::from(format!("_delta_log/_staged_commits/{}", staged.file_name));
                commit.store.delete(&path).await.unwrap();
            }
        }
        let table_url = Url::parse("memory:///").unwrap();
        let object_store = commit.store.clone() as Arc<dyn ObjectStore>;
        let log_store = default_logstore(
            object_store.clone(),
            object_store,
            &table_url,
            &StorageConfig,
        );
        let config = DeltaSnapshotConfig {
            require_files: true,
            catalog_managed_commits: Some(CatalogManagedCommitSet {
                latest_table_version: ratified.latest_table_version,
                commits: ratified
                    .commits
                    .iter()
                    .map(|commit| CatalogManagedCommitFile {
                        version: commit.version,
                        file_name: commit.file_name.clone(),
                        file_size: commit.file_size,
                        file_modification_timestamp: commit.file_modification_timestamp,
                    })
                    .collect(),
            }),
            ..Default::default()
        };
        let snapshot = DeltaSnapshot::try_new(
            log_store.as_ref(),
            config,
            Some(ratified.latest_table_version),
            None,
        )
        .await
        .unwrap();
        (snapshot.version(), snapshot.adds().len())
    }

    #[tokio::test]
    async fn catalog_managed_commits_are_checkpointed_at_the_checkpoint_interval() {
        // The classic checkpoint policy writes a classic checkpoint, and the v2 checkpoint
        // policy of a table with the v2 checkpoint feature writes a v2 checkpoint.
        for (protocol, configuration) in [
            (
                CATALOG_MANAGED_PROTOCOL,
                r#","delta.checkpointInterval":"2""#,
            ),
            (
                CATALOG_MANAGED_V2_CHECKPOINT_PROTOCOL,
                r#","delta.checkpointInterval":"2","delta.checkpointPolicy":"v2""#,
            ),
        ] {
            let commit = commit_with_script_in(
                vec![],
                true,
                UnpublishedCommit::None,
                protocol,
                configuration,
                5,
                Resolved::Current,
            )
            .await;
            assert_eq!(commit.result.as_ref().unwrap().version, 5);
            assert_eq!(commit.checkpoints, vec![2, 4], "{configuration}");
            assert_eq!(
                checkpoint_versions(&commit).await,
                (vec![2, 4], Some(4)),
                "{configuration}"
            );
            assert_eq!(
                open_from_latest_checkpoint(&commit).await,
                (5, 5),
                "{configuration}"
            );
        }
    }

    #[tokio::test]
    async fn catalog_managed_commit_is_checkpointed_only_when_it_is_published() {
        // Version 2 is at the checkpoint interval. It is published once the unpublished
        // version 1 of another writer is published before it, and it is not published when
        // version 1 cannot be published, so it is not checkpointed.
        for (unpublished, checkpoints) in [
            (UnpublishedCommit::Staged, vec![2]),
            (UnpublishedCommit::Missing, vec![]),
        ] {
            let commit = commit_with_script_in(
                vec![],
                true,
                unpublished,
                CATALOG_MANAGED_PROTOCOL,
                r#","delta.checkpointInterval":"2""#,
                1,
                Resolved::Current,
            )
            .await;
            assert_eq!(commit.result.as_ref().unwrap().version, 2);
            assert_eq!(commit.checkpoints, checkpoints);
            let last = checkpoints.last().copied();
            assert_eq!(checkpoint_versions(&commit).await, (checkpoints, last));
        }
    }

    #[tokio::test]
    async fn catalog_commit_at_the_limit_of_the_catalog_is_not_sent_again_without_backfill() {
        // Every ratified commit is published and reported, so the catalog refuses the commit
        // request again.
        let steps = vec![CommitStep::Refuse(|| {
            CatalogError::ResourceExhausted("429".into())
        })];
        let commit = commit_with_script(steps, true).await;
        let error = commit.result.as_ref().err().unwrap().to_string();
        assert!(error.contains("429"), "{error}");
        assert_eq!(backfilled_versions(&commit), vec![serde_json::json!(0)]);
        assert_eq!(commit.staged_commits().await, Vec::<String>::new());
    }

    #[tokio::test(start_paused = true)]
    async fn catalog_commit_refused_as_a_conflict_after_it_was_ratified_is_committed() {
        // The catalog ratifies the first request without an answer, and answers the request
        // that is sent again with a conflict, as a catalog that does not recognize a request
        // that it has ratified does. A blind append must not be committed a second time.
        for blind_append in [false, true] {
            let steps = vec![
                CommitStep::RatifyAndFail(state_unknown),
                CommitStep::Refuse(|| CatalogError::Conflict("409".into())),
            ];
            let commit = commit_with_script(steps, blind_append).await;
            let staged = commit.result.as_ref().unwrap();
            assert_eq!((staged.version, staged.metrics.num_retries), (1, 0));
            let request = (1, staged.file_name.clone());
            assert_eq!(commit.requests(), vec![request.clone(), request]);
            assert_eq!(commit.latest_table_version(), 1);
            assert_eq!(
                commit.staged_commits().await,
                vec![staged.file_name.clone()]
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn catalog_commit_that_is_rate_limited_for_every_request_is_not_committed() {
        let steps = (0..4)
            .map(|_| CommitStep::Refuse(|| CatalogError::RateLimited("429".into())))
            .collect();
        let commit = commit_with_script(steps, true).await;
        let error = commit.result.as_ref().err().unwrap().to_string();
        assert!(error.contains("429"), "{error}");
        // The request is sent again three times, and the staged commit is deleted since the
        // catalog answered that it did not ratify it.
        let requests = commit.requests();
        assert_eq!(requests, vec![requests[0].clone(); 4]);
        assert_eq!(commit.staged_commits().await, Vec::<String>::new());
        assert_eq!(commit.latest_table_version(), 0);
    }

    /// Reads the catalog-managed table with the log files as of the version, and returns the
    /// number of the requests for the ratified commits with the version that is read. The
    /// catalog resolves the table with the ratified commits, or without them.
    async fn read_catalog_managed_table_as_of(
        version: &str,
        log_files: &[(String, &str)],
        ratified: DeltaRatifiedCommitResponse,
        resolved_with_commits: bool,
    ) -> (usize, datafusion_common::Result<i64>) {
        let store = store_with_published_version_0(CATALOG_MANAGED_PROTOCOL).await;
        put_log_files(store.as_ref(), log_files).await;
        let catalog = if resolved_with_commits {
            // The commits are not requested, so the catalog has none to return.
            UnityManagedCatalog::new(told(Yes, Some(ratified)), None)
        } else {
            UnityManagedCatalog::new(told(Yes, None), Some(ratified))
        };
        load_unity_managed_table(store, catalog, vec![("versionAsOf", version)]).await
    }

    #[tokio::test]
    async fn catalog_managed_table_is_read_as_of_a_version() {
        // Version 1 is published, the versions 2 and 3 are ratified and not published yet.
        let files = [
            (published_commit(1), EMPTY_COMMIT),
            (staged_commit(2, "a"), EMPTY_COMMIT),
            (staged_commit(3, "b"), EMPTY_COMMIT),
        ];
        let latest = || ratified(3, &[(2, "a"), (3, "b")]);
        for resolved_with_commits in [true, false] {
            // The commits that the catalog resolved the table with are not requested again.
            let expected_requests = if resolved_with_commits { 0 } else { 1 };
            for version in [1, 2, 3] {
                let (requests, result) = read_catalog_managed_table_as_of(
                    &version.to_string(),
                    &files,
                    latest(),
                    resolved_with_commits,
                )
                .await;
                assert_eq!(result.unwrap(), version);
                assert_eq!(requests, expected_requests);
            }
            // The version is above the latest version that the catalog ratified, although a
            // staged commit of the version exists.
            let mut files = files.to_vec();
            files.push((staged_commit(4, "c"), EMPTY_COMMIT));
            let (requests, result) =
                read_catalog_managed_table_as_of("4", &files, latest(), resolved_with_commits)
                    .await;
            let error = result.unwrap_err().to_string();
            assert!(
                error.contains("latest ratified version is 3, but version 4 was requested"),
                "{error}"
            );
            assert_eq!(requests, expected_requests);
            // Version 0 is read from the published log without the catalog.
            let (requests, result) =
                read_catalog_managed_table_as_of("0", &files, latest(), resolved_with_commits)
                    .await;
            assert_eq!(result.unwrap(), 0);
            assert_eq!(requests, 0);
        }
    }

    #[tokio::test]
    async fn catalog_managed_table_is_read_as_of_a_version_below_an_unreadable_commit() {
        // The staged commit of version 3 is ratified and unreadable. A read as of version 2 does
        // not read it, and a read of the latest version fails.
        let files = [
            (published_commit(1), EMPTY_COMMIT),
            (staged_commit(2, "a"), EMPTY_COMMIT),
            (staged_commit(3, "b"), CORRUPTED_COMMIT),
        ];
        let latest = || ratified(3, &[(2, "a"), (3, "b")]);
        for resolved_with_commits in [true, false] {
            let (_, result) =
                read_catalog_managed_table_as_of("2", &files, latest(), resolved_with_commits)
                    .await;
            assert_eq!(result.unwrap(), 2);
            let (_, result) =
                read_catalog_managed_table_as_of("3", &files, latest(), resolved_with_commits)
                    .await;
            assert!(result.is_err());
        }
    }

    #[tokio::test]
    async fn catalog_managed_table_is_read_as_of_a_version_with_only_the_commits_up_to_it() {
        // Version 2 has a published commit that is unreadable, and its ratified commit is
        // staged. Version 1 is read as of version 1 from its ratified commit, without the
        // ratified commit of version 2, whose file does not exist.
        let files = [
            (staged_commit(1, "a"), EMPTY_COMMIT),
            (published_commit(2), CORRUPTED_COMMIT),
        ];
        let latest = || ratified(2, &[(1, "a"), (2, "b")]);
        let (requests, result) =
            read_catalog_managed_table_as_of("1", &files, latest(), true).await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(requests, 0);
        let (_, result) = read_catalog_managed_table_as_of("2", &files, latest(), true).await;
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("_staged_commits/00000000000000000002.b.json"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn catalog_managed_table_with_checkpoint_is_read_with_catalog_commits() {
        use crate::checkpoint::create_checkpoint_for;

        // The versions 1 to 11 are published with a checkpoint at version 10, the commits
        // before the checkpoint are cleaned up, and version 12 is ratified and not published.
        // A staged commit that lost the race for version 3 is unreadable.
        let store = store_with_published_version_0(CATALOG_MANAGED_PROTOCOL).await;
        for version in 1..=11 {
            store
                .put(
                    &Path::from(format!("items/_delta_log/{}", published_commit(version))),
                    EMPTY_COMMIT.to_string().into(),
                )
                .await
                .unwrap();
        }
        let log_store = super::create_logstore_with_object_store(
            store.clone(),
            Url::parse("memory:///items/").unwrap(),
            super::StorageConfig,
        )
        .unwrap();
        create_checkpoint_for(10, log_store.as_ref(), uuid::Uuid::new_v4())
            .await
            .unwrap();
        for version in 0..10 {
            let path = Path::from(format!("items/_delta_log/{}", published_commit(version)));
            store.delete(&path).await.unwrap();
        }
        for (file, content) in [
            (staged_commit(3, "orphan"), CORRUPTED_COMMIT),
            (staged_commit(12, "a"), EMPTY_COMMIT),
        ] {
            store
                .put(
                    &Path::from(format!("items/_delta_log/{file}")),
                    content.to_string().into(),
                )
                .await
                .unwrap();
        }
        let latest = || ratified(12, &[(12, "a")]);
        let (requests, result) =
            read_unity_managed_table_in_store(store.clone(), told(Yes, None), Some(latest())).await;
        assert_eq!(result.unwrap(), 12);
        assert_eq!(requests, 1);
        for version in [10, 11, 12] {
            let (_, result) = load_unity_managed_table(
                store.clone(),
                UnityManagedCatalog::new(told(Yes, None), Some(latest())),
                vec![("versionAsOf", version.to_string().as_str())],
            )
            .await;
            assert_eq!(result.unwrap(), version);
        }
    }

    #[tokio::test]
    async fn catalog_commit_at_the_limit_of_the_catalog_is_sent_again_after_backfill() {
        // Version 1 cannot be published before the first request. Another writer publishes
        // it before the catalog refuses the request at its limit, so the request is sent
        // again with version 1 reported as backfilled.
        let commit = commit_with_script_after(
            vec![CommitStep::PublishPredecessorAndExhaust],
            true,
            UnpublishedCommit::Missing,
        )
        .await;
        let staged = commit.result.as_ref().unwrap();
        assert_eq!(staged.version, 2);
        let request = (2, staged.file_name.clone());
        assert_eq!(commit.requests(), vec![request.clone(), request]);
        assert_eq!(
            backfilled_versions(&commit),
            vec![serde_json::json!(0), serde_json::json!(1)]
        );
        assert_eq!(commit.latest_table_version(), 2);
        assert!(published_commit_bytes(&commit, 2).await.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn catalog_commit_with_unknown_outcome_at_the_limit_of_the_catalog_reports_backfill() {
        // The catalog does not answer the first request, then refuses the same request at
        // its limit after another writer publishes version 1. The request is sent again
        // with version 1 reported as backfilled.
        let commit = commit_with_script_after(
            vec![
                CommitStep::Refuse(state_unknown),
                CommitStep::PublishPredecessorAndExhaust,
            ],
            true,
            UnpublishedCommit::Missing,
        )
        .await;
        let staged = commit.result.as_ref().unwrap();
        assert_eq!(staged.version, 2);
        let request = (2, staged.file_name.clone());
        assert_eq!(commit.requests(), vec![request.clone(); 3]);
        let backfilled = [0, 0, 1].map(|version| serde_json::json!(version));
        assert_eq!(backfilled_versions(&commit), backfilled);
        assert_eq!(commit.latest_table_version(), 2);
        assert!(published_commit_bytes(&commit, 1).await.is_some());
        assert!(published_commit_bytes(&commit, 2).await.is_some());

        // The request with the backfill is ratified but not answered, and the catalog
        // recognizes the same request when it is sent again.
        let commit = commit_with_script_after(
            vec![
                CommitStep::Refuse(state_unknown),
                CommitStep::PublishPredecessorAndExhaust,
                CommitStep::RatifyAndFail(state_unknown),
            ],
            true,
            UnpublishedCommit::Missing,
        )
        .await;
        let staged = commit.result.as_ref().unwrap();
        assert_eq!(commit.requests(), vec![(2, staged.file_name.clone()); 4]);
        let backfilled = [0, 0, 1, 1].map(|version| serde_json::json!(version));
        assert_eq!(backfilled_versions(&commit), backfilled);
        assert_eq!(commit.latest_table_version(), 2);
        let ratified = commit.script().ratified.lock().unwrap().commits.clone();
        let versions: Vec<_> = ratified.iter().map(|commit| commit.version).collect();
        assert_eq!(versions, vec![1, 2]);
        assert!(published_commit_bytes(&commit, 2).await.is_some());

        // The first request is ratified but not answered. The commits of the table show
        // that the commit is ratified, so the request is not sent again.
        let commit = commit_with_script_after(
            vec![
                CommitStep::RatifyAndFail(state_unknown),
                CommitStep::PublishPredecessorAndExhaust,
            ],
            true,
            UnpublishedCommit::Missing,
        )
        .await;
        let staged = commit.result.as_ref().unwrap();
        assert_eq!(commit.requests(), vec![(2, staged.file_name.clone()); 2]);
        assert_eq!(commit.latest_table_version(), 2);
        let ratified = commit.script().ratified.lock().unwrap().commits.clone();
        let versions: Vec<_> = ratified.iter().map(|commit| commit.version).collect();
        assert_eq!(versions, vec![1, 2]);
    }

    #[tokio::test]
    async fn catalog_commit_at_the_limit_of_the_catalog_names_the_commit_that_is_not_published() {
        // Another writer publishes other actions as version 1 before the catalog refuses the
        // request at its limit, so the ratified version 1 cannot be published.
        let commit = commit_with_script_after(
            vec![CommitStep::PublishPredecessorAndExhaust],
            true,
            UnpublishedCommit::Staged,
        )
        .await;
        let error = commit.result.as_ref().err().unwrap().to_string();
        let published = crate::spec::commit_path(1).to_string();
        assert!(error.contains("429"), "{error}");
        assert!(error.contains("of version 1"), "{error}");
        assert!(error.contains(&published), "{error}");
    }

    /// Alters a Unity managed table that is published at version 1, with the protocol, the
    /// table properties (`,"<key>":"<value>"...`), and the commit of version 1. The catalog
    /// resolves the table with the state, and fails every commit request. Returns the result
    /// of the ALTER, the number of the requests for ratified commits, and the commits in the
    /// Delta log.
    async fn alter_unity_managed_table(
        protocol: &str,
        configuration: &str,
        version_1: &str,
        state: Option<DeltaCatalogState>,
        operation: sail_common_datafusion::lakesource::LakeSourceAlterTableOperation,
    ) -> (
        datafusion_common::Result<sail_common_datafusion::lakesource::LakeSourceAlterTableResult>,
        usize,
        Vec<String>,
    ) {
        use sail_common_datafusion::lakesource::LakeSource;

        let store = Arc::new(InMemory::new());
        let version_0 = format!(
            "{EMPTY_COMMIT}{}",
            version_0_actions(protocol, configuration)
        );
        let files = [
            (published_commit(0), version_0.as_str()),
            (published_commit(1), version_1),
        ];
        put_log_files(store.as_ref(), &files).await;
        let catalog = Arc::new(UnityManagedCatalog {
            commit_error: Some(|| CatalogError::External("unexpected commit request".into())),
            ..UnityManagedCatalog::new(state, Some(ratified(1, &[])))
        });
        let (session, context) = resolve_table(&catalog, LakehouseOperation::Alter).await;
        let table_url = Url::parse(catalog.location).unwrap();
        session
            .runtime_env()
            .register_object_store(&table_url, store.clone());
        let result = crate::lake_source::DeltaLakeSource
            .alter_table(
                session.task_ctx().as_ref(),
                table_url.as_str(),
                operation,
                Some(context),
            )
            .await;
        let prefix = Path::from("items/_delta_log");
        let mut published = store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|meta| meta.location.filename().unwrap().to_string())
            .filter(|name| name.ends_with(".json"))
            .collect::<Vec<_>>();
        published.sort();
        (result, catalog.ratified_commit_requests(), published)
    }

    fn set_table_property(
        key: &str,
        value: &str,
    ) -> sail_common_datafusion::lakesource::LakeSourceAlterTableOperation {
        sail_common_datafusion::lakesource::LakeSourceAlterTableOperation::SetTableProperties {
            changes: vec![(key.to_string(), Some(value.to_string()))],
            if_exists: false,
        }
    }

    #[tokio::test]
    async fn alter_of_unity_managed_table_that_is_not_catalog_managed_commits_to_the_log() {
        let comment =
            sail_common_datafusion::lakesource::LakeSourceAlterTableOperation::AlterColumns {
                changes: vec![sail_common::spec::ColumnChange::Comment {
                    path: vec!["id".into()],
                    comment: Some("the id".into()),
                }],
                case_sensitive: false,
            };
        for operation in [set_table_property("k", "v"), comment] {
            let (result, requests, published) = alter_unity_managed_table(
                NON_CATALOG_MANAGED_PROTOCOL,
                "",
                EMPTY_COMMIT,
                told(No, None),
                operation,
            )
            .await;
            assert!(!result.unwrap().catalog_updated);
            assert_eq!(requests, 0);
            assert_eq!(
                published,
                [
                    published_commit(0),
                    published_commit(1),
                    published_commit(2)
                ]
            );
        }
    }

    #[tokio::test]
    async fn alter_of_unity_managed_table_that_is_not_catalog_managed_changes_unity_properties() {
        let (result, requests, published) = alter_unity_managed_table(
            r#"{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}"#,
            r#","delta.enableDeletionVectors":"true""#,
            EMPTY_COMMIT,
            told(No, None),
            set_table_property("delta.enableDeletionVectors", "false"),
        )
        .await;
        assert!(!result.unwrap().catalog_updated);
        assert_eq!(requests, 0);
        assert_eq!(published.last(), Some(&published_commit(2)));
    }

    #[tokio::test]
    async fn alter_of_catalog_managed_table_keeps_unity_properties() {
        let (result, requests, published) = alter_unity_managed_table(
            CATALOG_MANAGED_PROTOCOL,
            r#","delta.enableInCommitTimestamps":"true","delta.enableDeletionVectors":"true""#,
            EMPTY_COMMIT,
            told(Yes, Some(ratified(1, &[]))),
            set_table_property("delta.enableDeletionVectors", "false"),
        )
        .await;
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("Unity Catalog requires the table property"),
            "{error}"
        );
        assert_eq!(requests, 0);
        assert_eq!(published.last(), Some(&published_commit(1)));
    }

    #[tokio::test]
    async fn alter_of_table_with_unreadable_published_log_writes_nothing() {
        // The catalog does not tell whether the table is catalog-managed, and the published
        // log, which decides, cannot be read. The catalog is asked for its commits once and
        // has none for the unreadable version, so the table is not written, to the Delta
        // log or through the catalog.
        for protocol in [NON_CATALOG_MANAGED_PROTOCOL, CATALOG_MANAGED_PROTOCOL] {
            for state in [
                None,
                told(Undetermined, None),
                told_before_log(Undetermined),
            ] {
                let (result, requests, published) = alter_unity_managed_table(
                    protocol,
                    "",
                    CORRUPTED_COMMIT,
                    state,
                    set_table_property("k", "v"),
                )
                .await;
                let error = result.unwrap_err().to_string();
                assert!(error.contains("Invalid JSON in log record"), "{error}");
                assert!(
                    error.contains("the catalog commits are not available"),
                    "{error}"
                );
                assert_eq!(requests, 1);
                assert_eq!(published, [published_commit(0), published_commit(1)]);
            }
        }
    }

    /// Runs a write that creates the table that the catalog staged at `memory:///items/`,
    /// with the storage credentials of the staged table, and returns the error of the
    /// write, whether the write still uses the credentials, and the store of the table.
    async fn write_staged_unity_table(
        input: datafusion::arrow::record_batch::RecordBatch,
    ) -> (datafusion_common::DataFusionError, bool, Arc<InMemory>) {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::physical_plan::{ExecutionPlan, collect};
        use sail_common_datafusion::datasource::PhysicalSinkMode;
        use sail_object_store::{VendedObjectStores, VendedStoreFactory};

        use crate::physical_plan::{DeltaCommitContext, DeltaCommitExec};

        let catalog = Arc::new(UnityManagedCatalog::new(None, None));
        let manager = single_catalog_manager(catalog.clone()).unwrap();
        let table = vec!["unity".to_string(), "default".into(), "items".into()];
        let owner = format!(
            "{:?}",
            manager.resolve_fully_qualified_object_name(&table).unwrap()
        );
        let store = Arc::new(InMemory::new());
        let factory: VendedStoreFactory = {
            let store = store.clone();
            Arc::new(move |_| Ok(store.clone() as Arc<dyn ObjectStore>))
        };
        let stores = Arc::new(VendedObjectStores::with_factory(factory));
        let session = SessionContext::new_with_config(
            SessionConfig::new()
                .with_extension(Arc::new(manager))
                .with_extension(stores.clone()),
        );
        let table_url = Url::parse("memory:///items/").unwrap();
        session
            .runtime_env()
            .register_object_store(&table_url, Arc::new(InMemory::new()));
        stores
            .install(
                session.runtime_env().object_store_registry.as_ref(),
                &table_url,
                &owner,
                TableStorageCredentials {
                    credential: StorageCredential::GcsOauth {
                        token: "token".to_string(),
                    },
                    expires_at_ms: None,
                    access: sail_common_datafusion::catalog::StorageAccess::Read,
                },
                Arc::new(|_| Box::pin(async { Err("not expected".to_string()) })),
            )
            .unwrap();
        assert!(stores.contains(&table_url, &owner));
        let mut context = LakehouseExecutionContext::catalog_table_context(
            CatalogProviderId("unity".into()),
            table,
            CatalogTableIdentity {
                table_id: Some("table-id".into()),
                table_uri: Some(table_url.to_string()),
            },
            LakehouseOperation::Create,
            LakehouseFormat::Delta,
            LakehouseAuthority::PathManaged,
            ScanAuthority::ClientLakeSource,
        );
        context.commit = CommitAuthority::DeltaRatifiedCommit;
        let schema = input.schema();
        let input = MemorySourceConfig::try_new_exec(&[vec![input]], schema, None).unwrap();
        let exec: Arc<dyn ExecutionPlan> = Arc::new(DeltaCommitExec::new(
            input,
            table_url.clone(),
            vec![],
            false,
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
            PhysicalSinkMode::ErrorIfExists,
            None,
            DeltaCommitContext::default(),
            Some(context),
        ));
        let error = collect(exec, session.task_ctx()).await.unwrap_err();
        (error, stores.contains(&table_url, &owner), store)
    }

    #[tokio::test]
    async fn failed_write_of_staged_table_stops_using_its_storage_credentials() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;

        // The write fails before the table is created.
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let input =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let (error, uses_credentials, store) = write_staged_unity_table(input).await;
        assert!(
            error.to_string().contains("must be delta action rows"),
            "{error}"
        );
        assert!(!uses_credentials);
        let path = Path::from(format!("items/_delta_log/{}", published_commit(0)));
        assert!(store.head(&path).await.is_err());
    }

    #[tokio::test]
    async fn staged_table_that_the_catalog_fails_to_register_stops_using_its_storage_credentials() {
        use crate::physical_plan::encode_actions;
        use crate::schema::metadata_for_create_with_struct_type;
        use crate::spec::{
            Action, DataType as DeltaDataType, Protocol, StructField, StructType, TableFeature,
        };

        // Version 0 of the table is written, and the catalog then fails to register the
        // table, so the error is the error of the catalog.
        let schema =
            StructType::try_new([StructField::nullable("id", DeltaDataType::LONG)]).unwrap();
        let metadata = metadata_for_create_with_struct_type(
            schema,
            vec![],
            0,
            HashMap::from([(
                "delta.enableInCommitTimestamps".to_string(),
                "true".to_string(),
            )]),
        )
        .unwrap();
        let protocol = Protocol::new(
            3,
            7,
            Some(vec![]),
            Some(vec![TableFeature::InCommitTimestamp]),
        );
        let input = encode_actions(
            vec![Action::Protocol(protocol), Action::Metadata(metadata)],
            None,
        )
        .unwrap();
        let (error, uses_credentials, store) = write_staged_unity_table(input).await;
        assert!(
            error.to_string().contains("unused test catalog operation"),
            "{error}"
        );
        assert!(!uses_credentials);
        let path = Path::from(format!("items/_delta_log/{}", published_commit(0)));
        assert!(store.head(&path).await.is_ok());
    }
}
