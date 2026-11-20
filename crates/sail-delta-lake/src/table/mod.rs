// SPDX-License-Identifier: Apache-2.0
// SPDX-FileComment: Credit: https://github.com/delta-io/delta-rs/blob/1f0b4d0965a85400c1effc6e9b4c7ebbb6795978/crates/core/src/table/mod.rs
// SPDX-FileAttributionText: License: https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
// SPDX-FileCopyrightText: Copyright (2020) QP Hou and a number of other contributors.
// SPDX-FileCopyrightText: Portions Copyright (2025) LakeSail, Inc.
// SPDX-FileContributor: Modified by LakeSail, Inc. in 2025

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, LazyLock, Mutex};

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion_common::{DataFusionError, Result};
use log::{debug, warn};
use object_store::ObjectStore;
use sail_catalog::error::CatalogError;
use sail_catalog::lakehouse::DeltaRatifiedCommitResponse;
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::catalog::delta::{
    DELTA_UNITY_TABLE_ID_KEY, DELTA_UNITY_TABLE_ID_LEGACY_KEY,
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

use crate::delta_log::{
    LogStoreRef, ReplayedTableHeader, StorageConfig, default_logstore, resolve_version_timestamp,
};
use crate::logical::table_source::DeltaTableSource;
use crate::options::r#gen::DeltaReadOptions;
use crate::schema::{
    attach_column_mapping_metadata, metadata_for_create_with_struct_type, normalize_delta_schema,
    protocol_for_create, schema_has_column_defaults, schema_has_generated_columns,
    schema_has_identity_columns,
};
pub use crate::snapshot::DeltaSnapshot;
use crate::snapshot::{CatalogManagedCommitFile, CatalogManagedCommitSet, DeltaSnapshotConfig};
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
    /// The header of an earlier version of the table that the first load replays from.
    replay_hint: Option<ReplayedTableHeader>,
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
            replay_hint: None,
        }
    }

    /// Sets the header of an earlier version of the table that the first load of a snapshot
    /// without files replays from, instead of reading the log up to that version again.
    pub(crate) fn with_replay_hint(mut self, replay_hint: Option<ReplayedTableHeader>) -> Self {
        self.replay_hint = replay_hint;
        self
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
                let replay_hint = self.replay_hint.take();
                let state = DeltaSnapshot::try_new(
                    self.log_store.as_ref(),
                    self.config.clone(),
                    max_version,
                    replay_hint.as_ref(),
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
    let (snapshot, log_store, scan_config) = load_delta_read_state(
        ctx,
        table_url,
        schema,
        options,
        false,
        lakehouse_table,
        SnapshotIntent::Read,
    )
    .await?;

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
    let (snapshot, log_store, scan_config) = load_delta_read_state(
        ctx,
        table_url,
        schema,
        options,
        true,
        lakehouse_table,
        SnapshotIntent::Read,
    )
    .await?;

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
    let (snapshot, _, _) = load_delta_read_state(
        ctx,
        table_url,
        None,
        options,
        true,
        lakehouse_table,
        SnapshotIntent::Read,
    )
    .await?;
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
    let protocol = actions
        .iter()
        .rev()
        .find_map(|action| match action {
            crate::spec::CommitAction::Protocol(protocol) => Some(protocol),
            _ => None,
        })
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

/// Writes version 0 of a table that the catalog registers before the table has a Delta log.
/// A catalog that registers a table only after version 0 is published has no such table, so
/// a missing Delta log is an error there, and nothing is written.
async fn bootstrap_catalog_managed_delta_table<C>(
    ctx: &C,
    lakehouse_table: &LakehouseExecutionContext,
    table_url: &Url,
    log_store: LogStoreRef,
) -> Result<bool>
where
    C: SessionExtensionAccessor + ?Sized,
{
    let catalog_table = lakehouse_table.catalog_table();
    let Some(info) = load_catalog_managed_delta_bootstrap_info(ctx, catalog_table).await? else {
        return Ok(false);
    };
    let response = DeltaCatalogCommitCoordinator::new(ctx, catalog_table)
        .get_ratified_commits(lakehouse_table, table_url.to_string(), 1, None)
        .await?;
    if response.requires_published_log {
        let kind = if response.catalog_managed {
            "catalog-managed table"
        } else {
            "table"
        };
        return Err(DataFusionError::External(Box::new(
            DeltaTableError::generic(format!(
                "Delta log not found at `{table_url}` for the {kind} `{}`; \
                 check the table location and the storage credentials",
                catalog_table.join(".")
            )),
        )));
    }
    let (actions, operation) = catalog_managed_bootstrap_actions(info, table_url)?;
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

async fn load_catalog_managed_commits<C>(
    ctx: &C,
    lakehouse_table: &LakehouseExecutionContext,
    table_url: &Url,
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

    let response = match response {
        Some(response) => response,
        None => {
            DeltaCatalogCommitCoordinator::new(ctx, lakehouse_table.catalog_table())
                .get_ratified_commits(lakehouse_table, table_url.to_string(), 1, version_as_of)
                .await?
        }
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
        let page = DeltaCatalogCommitCoordinator::new(ctx, lakehouse_table.catalog_table())
            .get_ratified_commits(
                lakehouse_table,
                table_url.to_string(),
                start_version,
                Some(effective_end_version),
            )
            .await?;
        next_start_version = next_catalog_commit_start_version(
            start_version,
            effective_end_version,
            page.commits.iter().map(|commit| commit.version),
        )?;
        commits.extend(page.commits);
    }

    // The catalog returns the ratified commits that are not published yet, and the log replay
    // reads every other version from the published log. The snapshot falls back to the staged
    // commit of a version only when the replay of the published log fails.
    let by_version = commits
        .into_iter()
        .filter(|commit| commit.version >= 1 && commit.version <= effective_end_version)
        .map(|commit| {
            (
                commit.version,
                CatalogManagedCommitFile {
                    version: commit.version,
                    file_name: commit.file_name,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    Ok(CatalogManagedCommitSet {
        latest_table_version,
        commits: by_version.into_values().collect(),
    })
}

/// What a snapshot of a Unity managed Delta table is loaded for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotIntent {
    /// A read uses the published Delta protocol when the catalog cannot be asked whether the
    /// table is catalog-managed.
    Read,
    /// A write, a DDL statement, or a commit fails then, since the table may be catalog-managed,
    /// and a catalog-managed table is not committed to the file system.
    Write,
}

/// What a snapshot of a Unity managed Delta table is loaded with.
#[derive(Debug, Default)]
pub(crate) struct CatalogSnapshotSource {
    /// The catalog commits of a catalog-managed table.
    pub commits: Option<CatalogManagedCommitSet>,
    /// The header of the latest published version of a table that is not catalog-managed,
    /// which was loaded to read its protocol.
    pub replay_hint: Option<ReplayedTableHeader>,
    /// The latest snapshot of a table that is not catalog-managed, which was loaded with the
    /// configuration of the caller to read its protocol.
    pub snapshot: Option<Arc<DeltaSnapshot>>,
}

impl CatalogSnapshotSource {
    fn catalog_managed(commits: CatalogManagedCommitSet) -> Self {
        Self {
            commits: Some(commits),
            ..Default::default()
        }
    }

    /// Creates the table that a snapshot is loaded from, with the latest snapshot of the
    /// table when it is loaded already.
    pub(crate) fn into_table(
        self,
        log_store: LogStoreRef,
        config: DeltaSnapshotConfig,
    ) -> DeltaTable {
        let mut table = DeltaTable::new(
            log_store,
            DeltaSnapshotConfig {
                catalog_managed_commits: self.commits,
                ..config
            },
        )
        .with_replay_hint(self.replay_hint);
        table.state = self.snapshot;
        table
    }
}

/// Warns once for each table that the catalog cannot be asked whether the table is
/// catalog-managed, since a snapshot of the table is loaded for every query of the table.
fn warn_discovery_failure(table_url: &Url, error: &DataFusionError) {
    static REPORTED: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(Default::default);
    let first = REPORTED
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(table_url.to_string());
    if first {
        warn!(
            "Failed to ask the catalog whether {table_url} is a catalog-managed Delta table, \
             using the published Delta protocol: {error}"
        );
    } else {
        debug!(
            "Failed to ask the catalog whether {table_url} is a catalog-managed Delta table, \
             using the published Delta protocol: {error}"
        );
    }
}

/// The error of a write that is refused because the catalog could not be asked whether the
/// table is catalog-managed. The message of a catalog error is the message that the client
/// sees, so the context is added to it, and the error stays a catalog error.
fn refuse_write_without_catalog(
    catalog_table: &[String],
    error: DataFusionError,
) -> DataFusionError {
    let message = |error: &dyn fmt::Display| {
        format!(
            "cannot determine whether table {} is catalog-managed because the Unity Catalog \
             request failed: {error}; refusing to write without the catalog",
            catalog_table.join(".")
        )
    };
    match error {
        DataFusionError::External(error)
            if error
                .downcast_ref::<CatalogError>()
                .is_some_and(|error| std::error::Error::source(error).is_none()) =>
        {
            DataFusionError::External(Box::new(CatalogError::External(message(&error))))
        }
        error => DataFusionError::Context(message(&error), Box::new(error)),
    }
}

/// Loads what a snapshot of a Unity managed Delta table is loaded with. When the latest version
/// of the table is requested with a configuration, the latest snapshot of a table that is not
/// catalog-managed is loaded with that configuration.
pub(crate) async fn load_catalog_managed_commits_for_snapshot<C>(
    ctx: &C,
    lakehouse_table: &LakehouseExecutionContext,
    table_url: &Url,
    log_store: LogStoreRef,
    version_as_of: Option<i64>,
    intent: SnapshotIntent,
    config: Option<&DeltaSnapshotConfig>,
) -> Result<CatalogSnapshotSource>
where
    C: SessionExtensionAccessor + ?Sized,
{
    let catalog_table = lakehouse_table.catalog_table();
    let bootstrap_info = load_catalog_managed_delta_bootstrap_info(ctx, catalog_table).await?;
    let load_commits = |response| {
        load_catalog_managed_commits(ctx, lakehouse_table, table_url, version_as_of, response)
    };
    // A Unity table id alone does not make a table catalog-managed. The catalog property
    // `delta.feature.catalogManaged` does, and otherwise the published Delta protocol decides.
    let catalog_managed_property = bootstrap_info
        .as_ref()
        .is_some_and(|info| catalog_properties_are_catalog_managed(&info.properties));
    // The commit that enables the feature on an existing table may not be published yet, and
    // the table properties may not have the catalog property. So the catalog is asked about
    // a Unity managed table, and its answer has the ratified commits of the table as well.
    // When the catalog cannot be asked, the published Delta protocol decides for a read, so
    // that a table that is not catalog-managed can be read without the catalog commits. A
    // catalog-managed table still needs them, and the failure to load them is the error then.
    // A write fails, since the table may be catalog-managed.
    // A catalog that registers a table only after version 0 is published serves the table
    // with the Delta REST API, whose answer states whether the table is catalog-managed.
    let mut catalog_states_not_catalog_managed = false;
    let discovered = if bootstrap_info.is_some() && !catalog_managed_property {
        match DeltaCatalogCommitCoordinator::new(ctx, catalog_table)
            .discover_ratified_commits(lakehouse_table, table_url.to_string(), 1, version_as_of)
            .await
        {
            Ok(response) => {
                catalog_states_not_catalog_managed =
                    response.requires_published_log && !response.catalog_managed;
                response.catalog_managed.then_some(response)
            }
            Err(error) if intent == SnapshotIntent::Write => {
                return Err(refuse_write_without_catalog(catalog_table, error));
            }
            Err(error) => {
                warn_discovery_failure(table_url, &error);
                None
            }
        }
    } else {
        None
    };
    if catalog_managed_property || discovered.is_some() {
        match log_store.get_latest_version(-1).await {
            Ok(_) => {}
            Err(DeltaError::MissingVersion) => {
                bootstrap_catalog_managed_delta_table(
                    ctx,
                    lakehouse_table,
                    table_url,
                    log_store.clone(),
                )
                .await?;
            }
            Err(err) => return Err(DataFusionError::from(err)),
        }
        return Ok(CatalogSnapshotSource::catalog_managed(
            load_commits(discovered).await?,
        ));
    }

    // The latest snapshot that the caller asks for is loaded at once, and is used when the
    // table is not catalog-managed. The files of the table are loaded only when the table is
    // expected not to be catalog-managed, since the catalog commits of a catalog-managed table
    // are loaded afterwards.
    let latest_config = config.filter(|config| {
        version_as_of.is_none()
            && (!config.require_files
                || bootstrap_info.is_none()
                || catalog_states_not_catalog_managed)
    });
    let mut detector = DeltaTable::new(
        log_store.clone(),
        match latest_config {
            Some(config) => DeltaSnapshotConfig {
                catalog_managed_commits: None,
                ..config.clone()
            },
            None => DeltaSnapshotConfig {
                require_files: false,
                ..Default::default()
            },
        },
    );
    match detector.load().await {
        Ok(()) => {
            let snapshot = detector.snapshot()?;
            if snapshot.protocol().is_catalog_managed() {
                Ok(CatalogSnapshotSource::catalog_managed(
                    load_commits(None).await?,
                ))
            } else if latest_config.is_some() {
                Ok(CatalogSnapshotSource {
                    snapshot: Some(snapshot.clone()),
                    ..Default::default()
                })
            } else {
                Ok(CatalogSnapshotSource {
                    replay_hint: Some(snapshot.replay_hint()),
                    ..Default::default()
                })
            }
        }
        Err(err) if is_missing_delta_log_error(&err) => {
            if bootstrap_catalog_managed_delta_table(
                ctx,
                lakehouse_table,
                table_url,
                log_store.clone(),
            )
            .await?
            {
                Ok(CatalogSnapshotSource::catalog_managed(
                    load_commits(None).await?,
                ))
            } else {
                Err(DataFusionError::from(err))
            }
        }
        // Under the catalog-managed protocol the catalog is authoritative for table versions,
        // and published commits may be unreadable or superseded by ratified ones. Unity tables
        // created by Sail do not carry the catalog property, so a Unity managed table whose
        // published log cannot be loaded uses the catalog commits, unless the catalog states
        // that the table is not catalog-managed. When the catalog has no commits for the table
        // either, the error of the published log is the one reported.
        Err(err) if bootstrap_info.is_some() && !catalog_states_not_catalog_managed => {
            warn!(
                "Failed to load the published Delta log of {table_url}, reading catalog commits: {err}"
            );
            match load_commits(None).await {
                Ok(commits) => Ok(CatalogSnapshotSource::catalog_managed(commits)),
                Err(catalog_err) => Err(DataFusionError::from(err).context(format!(
                    "Failed to load the published Delta log of {table_url}, and the catalog commits are not available: {catalog_err}"
                ))),
            }
        }
        Err(err) => Err(DataFusionError::from(err)),
    }
}

fn catalog_properties_are_catalog_managed(properties: &[(String, String)]) -> bool {
    properties.iter().any(|(key, value)| {
        key.eq_ignore_ascii_case("delta.feature.catalogManaged")
            && value.eq_ignore_ascii_case("supported")
    })
}

pub(crate) async fn load_delta_read_state(
    ctx: &dyn Session,
    table_url: Url,
    schema: Option<Schema>,
    options: DeltaReadOptions,
    metadata_only: bool,
    lakehouse_table: Option<LakehouseExecutionContext>,
    intent: SnapshotIntent,
) -> Result<(Arc<DeltaSnapshot>, LogStoreRef, DeltaScanConfig)> {
    let reads_catalog_table = lakehouse_table.is_some();
    let url = ListingTableUrl::try_new(table_url.clone(), None)?;
    let object_store = ctx.runtime_env().object_store(&url)?;
    let storage_config = StorageConfig;
    let log_store =
        create_logstore_with_object_store(object_store, table_url.clone(), storage_config)?;

    let table_config = delta_read_snapshot_config(metadata_only, &options);
    // The latest snapshot of the table is loaded with the catalog commits when the read has
    // no time travel options.
    let reads_latest_version = options.version_as_of.is_none() && options.timestamp_as_of.is_none();
    let source = match catalog_managed_commit_context(lakehouse_table.as_ref()) {
        Some(lakehouse_table) => {
            load_catalog_managed_commits_for_snapshot(
                &ctx,
                lakehouse_table,
                &table_url,
                log_store.clone(),
                options.version_as_of,
                intent,
                reads_latest_version.then_some(&table_config),
            )
            .await?
        }
        None => CatalogSnapshotSource::default(),
    };

    let mut deltalake_table = source.into_table(log_store.clone(), table_config);

    // Load the table state according to the provided time travel options, unless the latest
    // snapshot is loaded already.
    if deltalake_table.state.is_none() {
        load_table_by_options(&mut deltalake_table, &options).await?;
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

/// Helper function to load a DeltaTable based on version or timestamp options.
async fn load_table_by_options(table: &mut DeltaTable, options: &DeltaReadOptions) -> Result<()> {
    // Precedence: version > timestamp > latest.
    if let Some(version) = options.version_as_of {
        table.load_version(version).await?;
    } else if let Some(timestamp_str) = &options.timestamp_as_of {
        let datetime = parse_timestamp_as_of(timestamp_str)?;

        let target_version = find_version_for_timestamp(table, datetime)
            .await
            .map_err(|e| {
                if matches!(e, DeltaTableError::MissingVersion) {
                    DeltaTableError::generic(format!(
                        "No version of the Delta table exists at or before timestamp {}",
                        timestamp_str
                    ))
                } else {
                    e
                }
            })?;

        table.load_version(target_version).await?;
    } else {
        // Default behavior: load the latest version.
        table.load().await?;
    }
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
        DeltaRatifiedCommit, DeltaRatifiedCommitRequest, DeltaRatifiedCommitResponse,
        LakehouseCommitOutcome, LakehouseCommitRequest,
    };
    use sail_catalog::manager::{CatalogManager, CatalogManagerOptions};
    use sail_catalog::provider::*;
    use sail_common_datafusion::catalog::*;
    use sail_common_datafusion::datasource::OptionLayer;
    use url::Url;

    use super::{SnapshotIntent, load_delta_read_state};
    use crate::options::ResolveOptions;
    use crate::options::r#gen::DeltaReadOptions;

    struct UnityManagedCatalog {
        catalog_managed_property: bool,
        ratified: Option<DeltaRatifiedCommitResponse>,
        commit_requests: AtomicUsize,
        discover_requests: AtomicUsize,
        /// The error of a discovery request. A catalog without an error answers that it
        /// does not state whether the table is catalog-managed unless `ratified` states it.
        discover_error: Option<fn() -> CatalogError>,
        commit_error: Option<fn() -> CatalogError>,
        script: Option<CommitScript>,
        /// The number of ratified commits in a response, from the start version of the
        /// request, as in a page of the legacy API. All of them when it is not set.
        page_size: Option<usize>,
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
        requests: Mutex<Vec<serde_json::Value>>,
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

        async fn commit(&self, update: &serde_json::Value) -> CatalogResult<()> {
            self.requests.lock().unwrap().push(update.clone());
            let version = update["commit_info"]["version"].as_i64().unwrap();
            let file_name = update["commit_info"]["file_name"].as_str().unwrap();
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
                    let winner = self.ratified.lock().unwrap().latest_table_version + 1;
                    let file_name = format!("{winner:020}.winner.json");
                    let timestamp = winner + 1;
                    let commit = format!(
                        "{{\"commitInfo\":{{\"timestamp\":{timestamp},\"inCommitTimestamp\":{timestamp},\"isBlindAppend\":true}}}}\n{actions}\n"
                    );
                    let path = Path::from(format!("_delta_log/_staged_commits/{file_name}"));
                    self.store.put(&path, commit.into()).await.unwrap();
                    self.ratify(winner, &file_name);
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
        async fn get_table(&self, _: &Namespace, _: &str) -> CatalogResult<TableStatus> {
            Ok(TableStatus {
                catalog: Some("unity".into()),
                database: vec!["default".into()],
                name: "items".into(),
                kind: TableKind::Table {
                    columns: vec![],
                    comment: None,
                    constraints: vec![],
                    location: Some("memory:///items/".into()),
                    format: "delta".into(),
                    partition_by: vec![],
                    sort_by: vec![],
                    bucket_by: None,
                    properties: [
                        ("table_type", "MANAGED"),
                        ("io.unitycatalog.tableId", "table-id"),
                    ]
                    .into_iter()
                    .chain(
                        self.catalog_managed_property
                            .then_some(("Delta.Feature.CatalogManaged", "supported")),
                    )
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect(),
                    is_external: false,
                },
            })
        }
        async fn commit_lakehouse_table(
            &self,
            _: &Namespace,
            _: &str,
            request: LakehouseCommitRequest,
        ) -> CatalogResult<LakehouseCommitOutcome> {
            if let Some(script) = &self.script {
                script.commit(&request.updates[0]).await?;
            }
            match self.commit_error {
                Some(error) => Err(error()),
                None => Ok(LakehouseCommitOutcome::Committed {
                    context: request.context,
                    payload: None,
                }),
            }
        }
        async fn get_delta_ratified_commits(
            &self,
            _: &Namespace,
            _: &str,
            request: DeltaRatifiedCommitRequest,
        ) -> CatalogResult<DeltaRatifiedCommitResponse> {
            if request.discover {
                self.discover_requests.fetch_add(1, Ordering::SeqCst);
                if let Some(error) = self.discover_error {
                    return Err(error());
                }
                // A catalog that serves the table with the Delta REST API states whether the
                // table is catalog-managed.
                return match &self.ratified {
                    Some(ratified)
                        if ratified.catalog_managed || ratified.requires_published_log =>
                    {
                        Ok(ratified.clone())
                    }
                    _ => Ok(DeltaRatifiedCommitResponse::default()),
                };
            }
            self.commit_requests.fetch_add(1, Ordering::SeqCst);
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
        catalog_managed_property: bool,
    ) -> (usize, datafusion_common::Result<i64>) {
        let files = corrupted_commit
            .map(|version| (published_commit(version), CORRUPTED_COMMIT))
            .into_iter()
            .collect();
        read_unity_managed_table_with_log(protocol, files, catalog_managed_property, None).await
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
            requires_published_log: false,
            catalog_managed: false,
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

    async fn read_unity_managed_table_with_log(
        protocol: &str,
        log_files: Vec<(String, &str)>,
        catalog_managed_property: bool,
        ratified: Option<DeltaRatifiedCommitResponse>,
    ) -> (usize, datafusion_common::Result<i64>) {
        let store = store_with_published_version_0(protocol).await;
        for (file, content) in log_files {
            store
                .put(
                    &Path::from(format!("items/_delta_log/{file}")),
                    content.to_string().into(),
                )
                .await
                .unwrap();
        }
        read_unity_managed_table_in_store(store, catalog_managed_property, ratified).await
    }

    async fn read_unity_managed_table_in_store(
        store: Arc<dyn ObjectStore>,
        catalog_managed_property: bool,
        ratified: Option<DeltaRatifiedCommitResponse>,
    ) -> (usize, datafusion_common::Result<i64>) {
        let (commit_requests, _, result) =
            read_unity_managed_table_with_requests(store, catalog_managed_property, ratified, None)
                .await;
        (commit_requests, result)
    }

    /// Returns the number of the commit requests and of the discovery requests to the catalog
    /// with the version of the table that is read.
    async fn read_unity_managed_table_with_requests(
        store: Arc<dyn ObjectStore>,
        catalog_managed_property: bool,
        ratified: Option<DeltaRatifiedCommitResponse>,
        discover_error: Option<fn() -> CatalogError>,
    ) -> (usize, usize, datafusion_common::Result<i64>) {
        read_unity_managed_table_with_options(
            store,
            catalog_managed_property,
            ratified,
            discover_error,
            vec![],
        )
        .await
    }

    /// Reads the table like `read_unity_managed_table_with_requests` with the read options.
    async fn read_unity_managed_table_with_options(
        store: Arc<dyn ObjectStore>,
        catalog_managed_property: bool,
        ratified: Option<DeltaRatifiedCommitResponse>,
        discover_error: Option<fn() -> CatalogError>,
        options: Vec<(&str, &str)>,
    ) -> (usize, usize, datafusion_common::Result<i64>) {
        load_unity_managed_table(
            store,
            catalog_managed_property,
            ratified,
            discover_error,
            options,
            SnapshotIntent::Read,
            None,
        )
        .await
    }

    /// Loads the table like `read_unity_managed_table_with_options` for the intent, from a
    /// catalog that returns the ratified commits in pages of the size.
    async fn load_unity_managed_table(
        store: Arc<dyn ObjectStore>,
        catalog_managed_property: bool,
        ratified: Option<DeltaRatifiedCommitResponse>,
        discover_error: Option<fn() -> CatalogError>,
        options: Vec<(&str, &str)>,
        intent: SnapshotIntent,
        page_size: Option<usize>,
    ) -> (usize, usize, datafusion_common::Result<i64>) {
        let catalog = Arc::new(UnityManagedCatalog {
            catalog_managed_property,
            ratified,
            commit_requests: AtomicUsize::new(0),
            discover_requests: AtomicUsize::new(0),
            discover_error,
            commit_error: None,
            script: None,
            page_size,
        });
        let manager = CatalogManager::try_new(CatalogManagerOptions {
            catalogs: HashMap::from([(
                "unity".into(),
                catalog.clone() as Arc<dyn CatalogProvider>,
            )]),
            default_catalog: "unity".into(),
            default_database: vec!["default".into()],
            global_temporary_database: vec!["global_temp".into()],
        })
        .unwrap();
        let session =
            SessionContext::new_with_config(SessionConfig::new().with_extension(Arc::new(manager)));
        let table_url = Url::parse("memory:///items/").unwrap();
        session
            .runtime_env()
            .register_object_store(&table_url, store);
        let mut context = LakehouseExecutionContext::catalog_table_context(
            CatalogProviderId("unity".into()),
            vec!["unity".into(), "default".into(), "items".into()],
            CatalogTableIdentity {
                table_id: Some("table-id".into()),
                table_uri: Some(table_url.to_string()),
            },
            LakehouseOperation::Read,
            LakehouseFormat::Delta,
            LakehouseAuthority::PathManaged,
            ScanAuthority::ClientLakeSource,
        );
        context.commit = CommitAuthority::DeltaRatifiedCommit;
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
            intent,
        )
        .await
        .map(|(snapshot, _, _)| snapshot.version());
        (
            catalog.commit_requests.load(Ordering::SeqCst),
            catalog.discover_requests.load(Ordering::SeqCst),
            result,
        )
    }

    async fn store_with_published_version_0(protocol: &str) -> Arc<InMemory> {
        let store = Arc::new(InMemory::new());
        let commit = format!(
            concat!(
                "{{\"protocol\":{protocol}}}\n",
                "{{\"metaData\":{{\"id\":\"table-id\",\"format\":{{\"provider\":\"parquet\",",
                "\"options\":{{}}}},\"schemaString\":\"{{\\\"type\\\":\\\"struct\\\",",
                "\\\"fields\\\":[{{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",",
                "\\\"nullable\\\":true,\\\"metadata\\\":{{}}}}]}}\",\"partitionColumns\":[],",
                "\"configuration\":{{\"io.unitycatalog.tableId\":\"table-id\"}},",
                "\"createdTime\":0}}}}\n"
            ),
            protocol = protocol
        );
        store
            .put(
                &Path::from("items/_delta_log/00000000000000000000.json"),
                commit.into(),
            )
            .await
            .unwrap();
        store
    }

    #[tokio::test]
    async fn unity_managed_table_reads_catalog_commits_when_catalog_states_catalog_managed() {
        // The commit that enables the feature on an existing table is not published yet, and
        // neither the table properties nor the published protocol have the feature.
        let store = store_with_published_version_0(NON_CATALOG_MANAGED_PROTOCOL).await;
        let enabling_commit = format!(
            "{}{{\"protocol\":{CATALOG_MANAGED_PROTOCOL}}}\n",
            EMPTY_COMMIT
        );
        store
            .put(
                &Path::from(format!("items/_delta_log/{}", staged_commit(1, "a"))),
                enabling_commit.into(),
            )
            .await
            .unwrap();
        let ratified = DeltaRatifiedCommitResponse {
            catalog_managed: true,
            requires_published_log: true,
            ..ratified(1, &[(1, "a")])
        };
        let (commit_requests, discover_requests, result) =
            read_unity_managed_table_with_requests(store, false, Some(ratified), None).await;
        assert_eq!(result.unwrap(), 1);
        // The commits of the discovery response are used without another request.
        assert_eq!((commit_requests, discover_requests), (0, 1));
    }

    #[tokio::test]
    async fn unity_managed_table_reads_published_log_when_catalog_states_not_catalog_managed() {
        let store = store_with_published_version_0(NON_CATALOG_MANAGED_PROTOCOL).await;
        store
            .put(
                &Path::from(format!("items/_delta_log/{}", staged_commit(1, "a"))),
                EMPTY_COMMIT.to_string().into(),
            )
            .await
            .unwrap();
        let (commit_requests, discover_requests, result) = read_unity_managed_table_with_requests(
            store,
            false,
            Some(ratified(1, &[(1, "a")])),
            None,
        )
        .await;
        assert_eq!(result.unwrap(), 0);
        assert_eq!((commit_requests, discover_requests), (0, 1));
    }

    #[tokio::test]
    async fn unity_managed_table_with_catalog_managed_property_is_not_discovered() {
        let store = store_with_published_version_0(CATALOG_MANAGED_PROTOCOL).await;
        let (commit_requests, discover_requests, result) =
            read_unity_managed_table_with_requests(store, true, Some(ratified(0, &[])), None).await;
        assert_eq!(result.unwrap(), 0);
        assert_eq!((commit_requests, discover_requests), (1, 0));
    }

    #[tokio::test]
    async fn unity_managed_table_reads_published_log_when_discovery_fails() {
        // The catalog cannot tell whether the table is catalog-managed, so the published
        // protocol, which does not have the feature, decides. A table that is not
        // catalog-managed is readable without the catalog, as it is with other engines.
        for error in [
            (|| CatalogError::External("503 Service Unavailable".into())) as fn() -> CatalogError,
            || CatalogError::Forbidden("403 Forbidden".into()),
            || CatalogError::Unauthorized("401 Unauthorized".into()),
        ] {
            let store = store_with_published_version_0(NON_CATALOG_MANAGED_PROTOCOL).await;
            let (commit_requests, discover_requests, result) =
                read_unity_managed_table_with_requests(
                    store,
                    false,
                    Some(ratified(0, &[])),
                    Some(error),
                )
                .await;
            assert_eq!(result.unwrap(), 0);
            assert_eq!((commit_requests, discover_requests), (0, 1));
        }
    }

    #[tokio::test]
    async fn unity_managed_table_is_not_written_when_discovery_fails() {
        // The table may be catalog-managed with the commit that enables the feature not
        // published yet, so a write does not use the published protocol.
        let store = store_with_published_version_0(NON_CATALOG_MANAGED_PROTOCOL).await;
        let (commit_requests, discover_requests, result) = load_unity_managed_table(
            store,
            false,
            Some(ratified(0, &[])),
            Some(|| CatalogError::External("503 Service Unavailable".into())),
            vec![],
            SnapshotIntent::Write,
            None,
        )
        .await;
        // The client sees the message of the innermost error, so the context is in it.
        let cause = match result.unwrap_err() {
            datafusion_common::DataFusionError::External(cause) => {
                cause.downcast::<CatalogError>().ok().map(|cause| *cause)
            }
            _ => None,
        };
        assert!(
            matches!(&cause, Some(CatalogError::External(_))),
            "{cause:?}"
        );
        let message = cause.unwrap().to_string();
        assert!(
            message.contains(
                "cannot determine whether table unity.default.items is catalog-managed \
                 because the Unity Catalog request failed"
            ) && message.contains("503 Service Unavailable")
                && message.contains("refusing to write without the catalog"),
            "{message}"
        );
        assert_eq!((commit_requests, discover_requests), (0, 1));
    }

    #[tokio::test]
    async fn ratified_commits_are_read_in_pages() {
        // The versions 1 to 3 are ratified and not published. Version 3 has two staged
        // commits, so only the catalog tells which one is ratified.
        let store = store_with_published_version_0(CATALOG_MANAGED_PROTOCOL).await;
        for file in [
            staged_commit(1, "a"),
            staged_commit(2, "b"),
            staged_commit(3, "c"),
            staged_commit(3, "d"),
        ] {
            store
                .put(
                    &Path::from(format!("items/_delta_log/{file}")),
                    EMPTY_COMMIT.to_string().into(),
                )
                .await
                .unwrap();
        }
        let (commit_requests, _, result) = load_unity_managed_table(
            store,
            true,
            Some(ratified(3, &[(1, "a"), (2, "b"), (3, "c")])),
            None,
            vec![],
            SnapshotIntent::Read,
            Some(2),
        )
        .await;
        assert_eq!(result.unwrap(), 3);
        assert_eq!(commit_requests, 2);
    }

    #[tokio::test]
    async fn table_that_is_not_catalog_managed_is_loaded_once() {
        let source = store_with_published_version_0(NON_CATALOG_MANAGED_PROTOCOL).await;
        let store = Arc::new(crate::test_store::ObservedStore::default());
        let version_0 = Path::from("items/_delta_log/00000000000000000000.json");
        let bytes = source.get(&version_0).await.unwrap().bytes().await.unwrap();
        store.put(&version_0, bytes.into()).await.unwrap();
        // The catalog serves the table with the Delta REST API, and states that the table is
        // not catalog-managed.
        let ratified = DeltaRatifiedCommitResponse {
            requires_published_log: true,
            ..ratified(0, &[])
        };
        let (_, _, result) =
            read_unity_managed_table_with_requests(store.clone(), false, Some(ratified), None)
                .await;
        assert_eq!(result.unwrap(), 0);
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
        let (commit_requests, result) = read_unity_managed_table_with_log(
            NON_CATALOG_MANAGED_PROTOCOL,
            vec![(published_commit(1), CORRUPTED_COMMIT)],
            false,
            Some(DeltaRatifiedCommitResponse {
                requires_published_log: true,
                ..ratified(1, &[])
            }),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(commit_requests, 0);
    }

    #[tokio::test]
    async fn catalog_managed_table_reads_catalog_commits_when_discovery_fails() {
        // The published protocol has the feature, so the catalog commits are read.
        let store = store_with_published_version_0(CATALOG_MANAGED_PROTOCOL).await;
        let (commit_requests, discover_requests, result) = read_unity_managed_table_with_requests(
            store,
            false,
            Some(ratified(0, &[])),
            Some(|| CatalogError::External("503 Service Unavailable".into())),
        )
        .await;
        assert_eq!(result.unwrap(), 0);
        assert_eq!((commit_requests, discover_requests), (1, 1));
    }

    #[tokio::test]
    async fn catalog_managed_table_is_read_with_vended_storage_credentials() {
        use datafusion::execution::object_store::{
            DefaultObjectStoreRegistry, ObjectStoreRegistry,
        };
        use sail_common_datafusion::catalog::{StorageCredential, TableStorageCredentials};
        use sail_object_store::{VendedObjectStores, VendedStoreFactory};

        // The table only exists in the store that is used with the vended credentials.
        let vended = Arc::new(InMemory::new());
        let commit = format!(
            concat!(
                "{{\"protocol\":{protocol}}}\n",
                "{{\"metaData\":{{\"id\":\"table-id\",\"format\":{{\"provider\":\"parquet\",",
                "\"options\":{{}}}},\"schemaString\":\"{{\\\"type\\\":\\\"struct\\\",",
                "\\\"fields\\\":[]}}\",\"partitionColumns\":[],",
                "\"configuration\":{{\"io.unitycatalog.tableId\":\"table-id\"}},",
                "\"createdTime\":0}}}}\n"
            ),
            protocol = CATALOG_MANAGED_PROTOCOL
        );
        vended
            .put(
                &Path::from("items/_delta_log/00000000000000000000.json"),
                commit.into(),
            )
            .await
            .unwrap();
        vended
            .put(
                &Path::from(format!("items/_delta_log/{}", staged_commit(1, "a"))),
                EMPTY_COMMIT.to_string().into(),
            )
            .await
            .unwrap();
        let ambient: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (_, result) = read_unity_managed_table_in_store(
            ambient.clone(),
            true,
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
                },
                Arc::new(|_| Box::pin(async { Err("not expected".to_string()) })),
            )
            .unwrap();
        let (commit_requests, result) = read_unity_managed_table_in_store(
            registry.get_store(&location).unwrap(),
            true,
            Some(ratified(1, &[(1, "a")])),
        )
        .await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(commit_requests, 1);
    }

    #[tokio::test]
    async fn unity_managed_table_without_catalog_managed_feature_reads_published_log() {
        let (commit_requests, result) =
            read_unity_managed_table(NON_CATALOG_MANAGED_PROTOCOL, None, false).await;
        assert_eq!(commit_requests, 0);
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
        let (commit_requests, result) =
            read_unity_managed_table(CATALOG_MANAGED_PROTOCOL, None, false).await;
        assert_eq!(commit_requests, 1);
        assert!(result.unwrap_err().to_string().contains("401 Unauthorized"));
    }

    #[tokio::test]
    async fn unity_managed_table_with_unreadable_published_log_reads_catalog_commits() {
        let (commit_requests, result) =
            read_unity_managed_table(CATALOG_MANAGED_PROTOCOL, Some(1), false).await;
        assert_eq!(commit_requests, 1);
        // The catalog has no commits for the table, and the error of the published log is
        // the error, with the error of the catalog as context.
        let error = result.unwrap_err();
        assert!(error.to_string().contains("401 Unauthorized"), "{error}");
        let root = error.find_root().to_string();
        assert!(!root.contains("401 Unauthorized"), "{root}");
    }

    #[tokio::test]
    async fn unity_managed_table_with_catalog_managed_property_reads_catalog_commits() {
        // The catalog property decides without consulting the published protocol.
        let (commit_requests, result) =
            read_unity_managed_table(NON_CATALOG_MANAGED_PROTOCOL, None, true).await;
        assert_eq!(commit_requests, 1);
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
        let (commit_requests, result) = read_unity_managed_table_with_log(
            CATALOG_MANAGED_PROTOCOL,
            files,
            false,
            Some(ratified(3, &[(3, "a")])),
        )
        .await;
        assert_eq!(result.unwrap(), 3);
        // One request by the snapshot load, with no request per published version.
        assert_eq!(commit_requests, 1);
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
        let (commit_requests, result) = read_unity_managed_table_with_log(
            CATALOG_MANAGED_PROTOCOL,
            files,
            false,
            Some(ratified(2, &[])),
        )
        .await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(commit_requests, 1);
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
            true,
            Some(ratified(1, &[(1, "a")])),
        )
        .await;
        assert_eq!(result.unwrap(), 1);
    }

    #[tokio::test]
    async fn catalog_managed_table_reads_staged_commit_dropped_by_catalog() {
        // A backfilled version is no longer returned by the catalog. Its only staged commit
        // is used in place of the published commit.
        let files = vec![
            (published_commit(1), CORRUPTED_COMMIT),
            (staged_commit(1, "a"), EMPTY_COMMIT),
            (staged_commit(2, "b"), EMPTY_COMMIT),
        ];
        let (_, result) = read_unity_managed_table_with_log(
            CATALOG_MANAGED_PROTOCOL,
            files,
            true,
            Some(ratified(2, &[(2, "b")])),
        )
        .await;
        assert_eq!(result.unwrap(), 2);
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
            false,
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
                true,
                Some(ratified(1, &[])),
            )
            .await;
            assert_eq!(result.unwrap(), 1);
        }
    }

    #[tokio::test]
    async fn catalog_managed_table_reads_staged_commit_without_published_commit() {
        let files = vec![
            (staged_commit(1, "a"), EMPTY_COMMIT),
            (staged_commit(2, "b"), EMPTY_COMMIT),
        ];
        let (_, result) = read_unity_managed_table_with_log(
            CATALOG_MANAGED_PROTOCOL,
            files,
            true,
            Some(ratified(2, &[(2, "b")])),
        )
        .await;
        assert_eq!(result.unwrap(), 2);
    }

    #[tokio::test]
    async fn catalog_managed_table_with_many_staged_commits_reads_published_commits() {
        // Every version has a staged commit. The staged commits of the versions 1 to 39 are
        // unreadable and must not be used, the published commit of version 40 is unreadable,
        // and the published commit of version 41 is missing.
        let mut files = Vec::new();
        for version in 1..=41 {
            match version {
                40 => {
                    files.push((published_commit(version), CORRUPTED_COMMIT));
                    files.push((staged_commit(version, "a"), EMPTY_COMMIT));
                }
                41 => files.push((staged_commit(version, "a"), EMPTY_COMMIT)),
                _ => {
                    files.push((published_commit(version), EMPTY_COMMIT));
                    files.push((staged_commit(version, "a"), CORRUPTED_COMMIT));
                }
            }
        }
        files.push((staged_commit(42, "a"), EMPTY_COMMIT));
        let (commit_requests, result) = read_unity_managed_table_with_log(
            CATALOG_MANAGED_PROTOCOL,
            files,
            true,
            Some(ratified(42, &[(42, "a")])),
        )
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(commit_requests, 1);
    }

    async fn staged_commit_exists_after_catalog_error(error: fn() -> CatalogError) -> bool {
        use crate::catalog::coordinator::{
            DeltaCatalogCommitCoordinator, DeltaCatalogManagedTable,
        };
        use crate::delta_log::{StorageConfig, default_logstore};
        use crate::transaction::CatalogManagedStagedCommit;

        let catalog = Arc::new(UnityManagedCatalog {
            catalog_managed_property: true,
            ratified: None,
            commit_requests: AtomicUsize::new(0),
            discover_requests: AtomicUsize::new(0),
            discover_error: None,
            commit_error: Some(error),
            script: None,
            page_size: None,
        });
        let manager = CatalogManager::try_new(CatalogManagerOptions {
            catalogs: HashMap::from([("unity".into(), catalog as Arc<dyn CatalogProvider>)]),
            default_catalog: "unity".into(),
            default_database: vec!["default".into()],
            global_temporary_database: vec!["global_temp".into()],
        })
        .unwrap();
        let session =
            SessionContext::new_with_config(SessionConfig::new().with_extension(Arc::new(manager)));
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let staged_path = Path::from(format!("_delta_log/{}", staged_commit(1, "a")));
        store
            .put(&staged_path, EMPTY_COMMIT.to_string().into())
            .await
            .unwrap();
        let table_url = Url::parse("memory:///").unwrap();
        let log_store = default_logstore(store.clone(), store.clone(), &table_url, &StorageConfig);
        let catalog_table = vec!["unity".to_string(), "default".into(), "items".into()];
        let mut context = LakehouseExecutionContext::catalog_table_context(
            CatalogProviderId("unity".into()),
            catalog_table.clone(),
            CatalogTableIdentity {
                table_id: Some("table-id".into()),
                table_uri: Some(table_url.to_string()),
            },
            LakehouseOperation::Write,
            LakehouseFormat::Delta,
            LakehouseAuthority::PathManaged,
            ScanAuthority::ClientLakeSource,
        );
        context.commit = CommitAuthority::DeltaRatifiedCommit;
        let staged = CatalogManagedStagedCommit {
            version: 1,
            file_name: format!("{:020}.a.json", 1),
            file_size: EMPTY_COMMIT.len() as i64,
            file_modification_timestamp: 1,
            in_commit_timestamp: 1,
            staged_path: staged_path.clone(),
            domain_metadata: vec![],
            metrics: Default::default(),
        };
        let state = session.state();
        let result = DeltaCatalogCommitCoordinator::new(&state, &catalog_table)
            .commit_staged(
                &context,
                &DeltaCatalogManagedTable {
                    table_id: "table-id".into(),
                    table_uri: table_url.to_string(),
                },
                &log_store,
                None,
                &staged,
                &[],
                None,
            )
            .await;
        assert!(result.is_err());
        store.head(&staged_path).await.is_ok()
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

    async fn read_unity_managed_table_without_log(
        requires_published_log: bool,
        catalog_managed: bool,
    ) -> (datafusion_common::Result<i64>, Vec<String>) {
        let store = Arc::new(InMemory::new());
        let ratified = DeltaRatifiedCommitResponse {
            requires_published_log,
            catalog_managed,
            ..ratified(0, &[])
        };
        let (_, result) =
            read_unity_managed_table_in_store(store.clone(), true, Some(ratified)).await;
        let files = store.list(None).try_collect::<Vec<_>>().await.unwrap();
        let files = files.into_iter().map(|meta| meta.location.to_string());
        (result, files.collect())
    }

    #[tokio::test]
    async fn read_of_table_without_log_writes_nothing_when_catalog_publishes_version_0() {
        for (catalog_managed, kind) in [(true, "catalog-managed table"), (false, "table")] {
            let (result, files) = read_unity_managed_table_without_log(true, catalog_managed).await;
            let error = result.unwrap_err().to_string();
            // The table is called catalog-managed only when the catalog says it is.
            assert!(
                error.contains(&format!(
                    "Delta log not found at `memory:///items/` for the {kind} `unity.default.items`"
                )),
                "{error}"
            );
            assert_eq!(files, Vec::<String>::new());
        }
    }

    #[tokio::test]
    async fn read_of_table_without_log_creates_version_0_for_legacy_catalog() {
        let (result, files) = read_unity_managed_table_without_log(false, false).await;
        assert_eq!(result.unwrap(), 0);
        assert!(
            files.contains(&"items/_delta_log/00000000000000000000.json".to_string()),
            "{files:?}"
        );
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
        let ratified = DeltaRatifiedCommitResponse {
            requires_published_log: false,
            ..ratified(0, &[])
        };
        let (_, result) =
            read_unity_managed_table_in_store(store.clone(), true, Some(ratified)).await;
        assert_eq!(result.unwrap(), 0);
        let written = store.get(&version_0).await.unwrap().bytes().await.unwrap();
        assert_eq!(written, bytes);
        let version_1 = Path::from("items/_delta_log/00000000000000000001.json");
        assert!(store.head(&version_1).await.is_err());
    }

    #[tokio::test]
    async fn catalog_commit_with_unknown_outcome_keeps_staged_commit() {
        // A server error, a timeout, or a network error may follow a ratified commit.
        assert!(
            staged_commit_exists_after_catalog_error(|| CatalogError::External("503".into())).await
        );
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
            let commit = |update: &serde_json::Value| {
                let info = &update["commit_info"];
                let file_name = info["file_name"].as_str().unwrap();
                (info["version"].as_i64().unwrap(), file_name.to_string())
            };
            requests.iter().map(commit).collect()
        }

        fn script(&self) -> &CommitScript {
            self.catalog.script.as_ref().unwrap()
        }

        fn latest_table_version(&self) -> i64 {
            self.script().ratified.lock().unwrap().latest_table_version
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
        )
        .await
    }

    /// Commits files like `commit_with_script_after`, one after another, to a table with the
    /// protocol and the table properties (`,"<key>":"<value>"...`) in addition to the table
    /// properties of a catalog-managed table. Every commit is a commit of one file.
    async fn commit_with_script_in(
        steps: Vec<CommitStep>,
        blind_append: bool,
        unpublished: UnpublishedCommit,
        protocol: &str,
        configuration: &str,
        commits: usize,
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
        let version_0 = format!(
            concat!(
                "{{\"commitInfo\":{{\"timestamp\":1,\"inCommitTimestamp\":1}}}}\n",
                "{{\"protocol\":{protocol}}}\n{metadata}\n"
            ),
            protocol = protocol,
            metadata = METADATA_ACTION
                .replace(r#"\"type\":\"string\""#, r#"\"type\":\"long\""#)
                .replace(
                    r#""delta.enableInCommitTimestamps":"true""#,
                    &format!(r#""delta.enableInCommitTimestamps":"true"{configuration}"#),
                ),
        );
        store
            .put(
                &Path::from(format!("_delta_log/{}", published_commit(0))),
                version_0.into(),
            )
            .await
            .unwrap();
        let catalog = Arc::new(UnityManagedCatalog {
            catalog_managed_property: true,
            ratified: None,
            commit_requests: AtomicUsize::new(0),
            discover_requests: AtomicUsize::new(0),
            discover_error: None,
            commit_error: None,
            script: Some(CommitScript {
                store: store.clone(),
                steps: Mutex::new(steps.into()),
                ratified: Mutex::new(if unpublished == UnpublishedCommit::None {
                    ratified(0, &[])
                } else {
                    ratified(1, &[(1, "other")])
                }),
                requests: Mutex::new(vec![]),
            }),
            page_size: None,
        });
        let manager = CatalogManager::try_new(CatalogManagerOptions {
            catalogs: HashMap::from([(
                "unity".into(),
                catalog.clone() as Arc<dyn CatalogProvider>,
            )]),
            default_catalog: "unity".into(),
            default_database: vec!["default".into()],
            global_temporary_database: vec!["global_temp".into()],
        })
        .unwrap();
        let session =
            SessionContext::new_with_config(SessionConfig::new().with_extension(Arc::new(manager)));
        let table_url = Url::parse("memory:///").unwrap();
        let object_store = store.clone() as Arc<dyn ObjectStore>;
        let log_store = default_logstore(
            object_store.clone(),
            object_store,
            &table_url,
            &StorageConfig,
        );
        let catalog_table = vec!["unity".to_string(), "default".into(), "items".into()];
        let mut context = LakehouseExecutionContext::catalog_table_context(
            CatalogProviderId("unity".into()),
            catalog_table.clone(),
            CatalogTableIdentity {
                table_id: Some("table-id".into()),
                table_uri: Some(table_url.to_string()),
            },
            LakehouseOperation::Write,
            LakehouseFormat::Delta,
            LakehouseAuthority::PathManaged,
            ScanAuthority::ClientLakeSource,
        );
        context.commit = CommitAuthority::DeltaRatifiedCommit;
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
            let commit_result = DeltaCatalogCommitCoordinator::new(&state, &catalog_table)
                .commit_actions(
                    &context,
                    &DeltaCatalogManagedTable {
                        table_id: "table-id".into(),
                        table_uri: table_url.to_string(),
                    },
                    &table_url,
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

    fn backfilled_versions(commit: &ScriptedCommit) -> Vec<serde_json::Value> {
        let requests = commit.script().requests.lock().unwrap();
        requests
            .iter()
            .map(|request| request["latest_backfilled_version"].clone())
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
    /// number of the commit requests to the catalog with the version that is read.
    async fn read_catalog_managed_table_as_of(
        version: &str,
        log_files: &[(String, &str)],
        ratified: DeltaRatifiedCommitResponse,
    ) -> (usize, datafusion_common::Result<i64>) {
        let store = store_with_published_version_0(CATALOG_MANAGED_PROTOCOL).await;
        for (file, content) in log_files {
            store
                .put(
                    &Path::from(format!("items/_delta_log/{file}")),
                    content.to_string().into(),
                )
                .await
                .unwrap();
        }
        let (commit_requests, _, result) = read_unity_managed_table_with_options(
            store,
            true,
            Some(ratified),
            None,
            vec![("versionAsOf", version)],
        )
        .await;
        (commit_requests, result)
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
        for version in [1, 2, 3] {
            let (commit_requests, result) =
                read_catalog_managed_table_as_of(&version.to_string(), &files, latest()).await;
            assert_eq!(result.unwrap(), version);
            assert_eq!(commit_requests, 1);
        }
        // The version is above the latest version that the catalog ratified, although a staged
        // commit of the version exists.
        let mut files = files.to_vec();
        files.push((staged_commit(4, "c"), EMPTY_COMMIT));
        let (_, result) = read_catalog_managed_table_as_of("4", &files, latest()).await;
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("latest ratified version is 3, but version 4 was requested"),
            "{error}"
        );
        // Version 0 is read from the published log without the catalog.
        let (commit_requests, result) =
            read_catalog_managed_table_as_of("0", &files, latest()).await;
        assert_eq!(result.unwrap(), 0);
        assert_eq!(commit_requests, 0);
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
        let (_, result) = read_catalog_managed_table_as_of("2", &files, latest()).await;
        assert_eq!(result.unwrap(), 2);
        let (_, result) = read_catalog_managed_table_as_of("3", &files, latest()).await;
        assert!(result.is_err());
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
        let (commit_requests, result) =
            read_unity_managed_table_in_store(store.clone(), true, Some(latest())).await;
        assert_eq!(result.unwrap(), 12);
        assert_eq!(commit_requests, 1);
        for version in [10, 11, 12] {
            let (_, _, result) = read_unity_managed_table_with_options(
                store.clone(),
                true,
                Some(latest()),
                None,
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

    /// Returns a store with the published version 0 of a catalog-managed table and the files
    /// of its log, and the log store of the table.
    async fn observed_log_store(
        log_files: Vec<(String, &str)>,
    ) -> (
        Arc<crate::test_store::ObservedStore>,
        crate::delta_log::LogStoreRef,
    ) {
        use crate::delta_log::{StorageConfig, default_logstore};

        let version_0 = Path::from(format!("_delta_log/{}", published_commit(0)));
        let source = store_with_published_version_0(CATALOG_MANAGED_PROTOCOL).await;
        let version_0_bytes = source
            .get(&Path::from(format!("items/{version_0}")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let store = Arc::new(crate::test_store::ObservedStore::default());
        store.put(&version_0, version_0_bytes.into()).await.unwrap();
        for (file, content) in log_files {
            store
                .put(
                    &Path::from(format!("_delta_log/{file}")),
                    content.to_string().into(),
                )
                .await
                .unwrap();
        }
        let object_store = store.clone() as Arc<dyn ObjectStore>;
        let log_store = default_logstore(
            object_store.clone(),
            object_store,
            &Url::parse("memory:///").unwrap(),
            &StorageConfig,
        );
        (store, log_store)
    }

    /// Loads the snapshot of a catalog-managed table with the catalog commits.
    async fn load_catalog_managed_snapshot(
        log_store: &crate::delta_log::LogStoreRef,
        latest_table_version: i64,
        commits: &[(i64, &str)],
        version: Option<i64>,
    ) -> crate::spec::DeltaResult<i64> {
        use crate::snapshot::{
            CatalogManagedCommitFile, CatalogManagedCommitSet, DeltaSnapshotConfig,
        };
        let config = DeltaSnapshotConfig {
            require_files: false,
            catalog_managed_commits: Some(CatalogManagedCommitSet {
                latest_table_version,
                commits: commits
                    .iter()
                    .map(|(version, id)| CatalogManagedCommitFile {
                        version: *version,
                        file_name: format!("{version:020}.{id}.json"),
                    })
                    .collect(),
            }),
            ..Default::default()
        };
        crate::table::DeltaSnapshot::try_new(log_store.as_ref(), config, version, None)
            .await
            .map(|snapshot| snapshot.version())
    }

    fn lists_staged_commits(store: &crate::test_store::ObservedStore) -> usize {
        store
            .lists()
            .iter()
            .filter(|prefix| prefix.starts_with("_delta_log/_staged_commits"))
            .count()
    }

    #[tokio::test]
    async fn staged_commit_after_the_latest_catalog_version_is_not_read() {
        // A legacy catalog has the latest version -1 before the first commit through it, so
        // the staged commit of version 1 is not ratified.
        let (_, log_store) = observed_log_store(vec![(staged_commit(1, "a"), EMPTY_COMMIT)]).await;
        let result = load_catalog_managed_snapshot(&log_store, -1, &[], Some(1)).await;
        assert!(result.is_err(), "{result:?}");

        // Version 1 is ratified and backfilled, and the staged commit of version 2 is not
        // ratified.
        let (_, log_store) = observed_log_store(vec![
            (staged_commit(1, "a"), EMPTY_COMMIT),
            (staged_commit(2, "b"), EMPTY_COMMIT),
        ])
        .await;
        let result = load_catalog_managed_snapshot(&log_store, 1, &[], Some(2)).await;
        assert!(result.is_err(), "{result:?}");
        let result = load_catalog_managed_snapshot(&log_store, 1, &[], None).await;
        assert_eq!(result.unwrap(), 1);
    }

    #[tokio::test]
    async fn staged_commits_are_listed_only_when_a_published_commit_is_missing() {
        let (store, log_store) = observed_log_store(vec![
            (published_commit(1), EMPTY_COMMIT),
            (staged_commit(1, "a"), EMPTY_COMMIT),
            (staged_commit(2, "b"), EMPTY_COMMIT),
        ])
        .await;
        let result = load_catalog_managed_snapshot(&log_store, 1, &[], None).await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(lists_staged_commits(&store), 0);

        // The published commit of version 2 is missing.
        let result = load_catalog_managed_snapshot(&log_store, 2, &[], None).await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(lists_staged_commits(&store), 1);
    }

    #[tokio::test]
    async fn staged_commit_is_not_read_after_a_storage_error() {
        let (store, log_store) = observed_log_store(vec![
            (published_commit(1), EMPTY_COMMIT),
            (staged_commit(1, "a"), EMPTY_COMMIT),
        ])
        .await;
        store.fail_lookups(Path::from(format!("_delta_log/{}", published_commit(1))));
        let error = load_catalog_managed_snapshot(&log_store, 1, &[], None)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("503 Service Unavailable"), "{error}");
        assert_eq!(lists_staged_commits(&store), 0);
    }

    /// Alters a Unity managed table that is published at version 1, with the protocol and
    /// the table properties (`,"<key>":"<value>"...`). The catalog answers the discovery
    /// request with the response, and fails every commit request. Returns the result of
    /// the ALTER, the number of the requests for ratified commits, and the published commits.
    async fn alter_unity_managed_table(
        protocol: &str,
        configuration: &str,
        ratified: DeltaRatifiedCommitResponse,
        operation: sail_common_datafusion::lakesource::LakeSourceAlterTableOperation,
    ) -> (
        datafusion_common::Result<sail_common_datafusion::lakesource::LakeSourceAlterTableResult>,
        usize,
        Vec<String>,
    ) {
        use sail_common_datafusion::lakesource::LakeSource;

        let store = Arc::new(InMemory::new());
        let version_0 = format!(
            concat!(
                "{{\"commitInfo\":{{\"timestamp\":1,\"inCommitTimestamp\":1}}}}\n",
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
        );
        for (version, commit) in [(0, version_0.as_str()), (1, EMPTY_COMMIT)] {
            let path = Path::from(format!("items/_delta_log/{}", published_commit(version)));
            store.put(&path, commit.to_string().into()).await.unwrap();
        }
        let catalog = Arc::new(UnityManagedCatalog {
            catalog_managed_property: false,
            ratified: Some(ratified),
            commit_requests: AtomicUsize::new(0),
            discover_requests: AtomicUsize::new(0),
            discover_error: None,
            commit_error: Some(|| CatalogError::External("unexpected commit request".into())),
            script: None,
            page_size: None,
        });
        let manager = CatalogManager::try_new(CatalogManagerOptions {
            catalogs: HashMap::from([(
                "unity".into(),
                catalog.clone() as Arc<dyn CatalogProvider>,
            )]),
            default_catalog: "unity".into(),
            default_database: vec!["default".into()],
            global_temporary_database: vec!["global_temp".into()],
        })
        .unwrap();
        let session =
            SessionContext::new_with_config(SessionConfig::new().with_extension(Arc::new(manager)));
        let table_url = Url::parse("memory:///items/").unwrap();
        session
            .runtime_env()
            .register_object_store(&table_url, store.clone());
        let mut context = LakehouseExecutionContext::catalog_table_context(
            CatalogProviderId("unity".into()),
            vec!["unity".into(), "default".into(), "items".into()],
            CatalogTableIdentity {
                table_id: Some("table-id".into()),
                table_uri: Some(table_url.to_string()),
            },
            LakehouseOperation::Alter,
            LakehouseFormat::Delta,
            LakehouseAuthority::PathManaged,
            ScanAuthority::ClientLakeSource,
        );
        context.commit = CommitAuthority::DeltaRatifiedCommit;
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
        (
            result,
            catalog.commit_requests.load(Ordering::SeqCst),
            published,
        )
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

    /// The response of a catalog that serves the table with the Delta REST API and states
    /// that the table is not catalog-managed. The catalog has no commits of the table.
    fn not_catalog_managed() -> DeltaRatifiedCommitResponse {
        DeltaRatifiedCommitResponse {
            requires_published_log: true,
            ..ratified(0, &[])
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
            let (result, commit_requests, published) = alter_unity_managed_table(
                NON_CATALOG_MANAGED_PROTOCOL,
                "",
                not_catalog_managed(),
                operation,
            )
            .await;
            assert!(!result.unwrap().catalog_updated);
            assert_eq!(commit_requests, 0);
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
        let (result, commit_requests, published) = alter_unity_managed_table(
            r#"{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}"#,
            r#","delta.enableDeletionVectors":"true""#,
            not_catalog_managed(),
            set_table_property("delta.enableDeletionVectors", "false"),
        )
        .await;
        assert!(!result.unwrap().catalog_updated);
        assert_eq!(commit_requests, 0);
        assert_eq!(published.last(), Some(&published_commit(2)));
    }

    #[tokio::test]
    async fn alter_of_catalog_managed_table_keeps_unity_properties() {
        let (result, commit_requests, published) = alter_unity_managed_table(
            CATALOG_MANAGED_PROTOCOL,
            r#","delta.enableInCommitTimestamps":"true","delta.enableDeletionVectors":"true""#,
            DeltaRatifiedCommitResponse {
                catalog_managed: true,
                ..ratified(1, &[])
            },
            set_table_property("delta.enableDeletionVectors", "false"),
        )
        .await;
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("Unity Catalog requires the table property"),
            "{error}"
        );
        assert_eq!(commit_requests, 0);
        assert_eq!(published.last(), Some(&published_commit(1)));
    }
}
