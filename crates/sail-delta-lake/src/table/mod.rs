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

// [Credit]: https://github.com/delta-io/delta-rs/blob/1f0b4d0965a85400c1effc6e9b4c7ebbb6795978/crates/core/src/table/mod.rs

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion_common::{DataFusionError, Result};
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::catalog::delta::{
    DELTA_UNITY_TABLE_ID_KEY, DELTA_UNITY_TABLE_ID_LEGACY_KEY,
};
use sail_common_datafusion::catalog::{
    CommitAuthority, LakehouseExecutionContext, TableColumnStatus,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use url::Url;

use crate::catalog::coordinator::DeltaCatalogCommitCoordinator;
use crate::catalog_managed::{
    catalog_managed_delta_table, metadata_with_catalog_managed, protocol_with_catalog_managed,
    CatalogManagedDeltaTable,
};
use crate::datasource::{df_logical_schema, DeltaScanConfig};
pub mod features;
pub use features::{
    ChangeDataFeedSupport, ChangeDataFeedToken, ColumnMappingToken, DeletionVectorToken,
    EnabledRowTrackingToken, RowTrackingToken, SupportedRowTrackingToken,
};

use crate::delta_log::{default_logstore, resolve_version_timestamp, LogStoreRef, StorageConfig};
use crate::logical::table_source::DeltaTableSource;
use crate::options::gen::DeltaReadOptions;
use crate::schema::{
    metadata_for_create_with_struct_type, normalize_delta_schema, protocol_for_create,
    schema_has_column_defaults, schema_has_generated_columns, schema_has_identity_columns,
};
pub use crate::snapshot::DeltaSnapshot;
use crate::snapshot::{
    catalog_managed_commit_file_name, CatalogManagedCommitFile, CatalogManagedCommitSet,
    DeltaSnapshotConfig,
};
use crate::spec::{
    contains_timestampntz_arrow, contains_variant_arrow, CommitAction, DeltaError,
    DeltaError as DeltaTableError, DeltaOperation, DeltaResult, Protocol, SaveMode, StructType,
    TableFeature,
};
use crate::transaction::CommitBuilder;

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
        if let Some(snapshot) = &self.state {
            if snapshot.version() > version {
                self.state = None;
            }
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
    let (snapshot, log_store, scan_config) =
        load_delta_read_state(ctx, table_url, schema, options, false, lakehouse_table).await?;

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
    let (schema, _) =
        infer_delta_logical_metadata(ctx, table_url, schema, options, lakehouse_table).await?;
    Ok(schema)
}

/// Infers the Delta logical schema and table configuration for planning.
pub async fn infer_delta_logical_metadata(
    ctx: &dyn Session,
    table_url: Url,
    schema: Option<Schema>,
    options: DeltaReadOptions,
    lakehouse_table: Option<LakehouseExecutionContext>,
) -> Result<(SchemaRef, Vec<(String, String)>)> {
    let (snapshot, _log_store, scan_config) =
        load_delta_read_state(ctx, table_url, schema, options, true, lakehouse_table).await?;

    let schema = df_logical_schema(
        snapshot.as_ref(),
        &scan_config.file_column_name,
        &scan_config.row_index_column_name,
        &scan_config.commit_version_column_name,
        &scan_config.commit_timestamp_column_name,
        scan_config.schema,
    )?;
    let properties = snapshot
        .metadata()
        .configuration()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    Ok((schema, properties))
}

fn delta_read_snapshot_config(
    metadata_only: bool,
    options: &DeltaReadOptions,
) -> DeltaSnapshotConfig {
    if metadata_only || options.metadata_as_data_read {
        DeltaSnapshotConfig {
            require_files: false,
            ..Default::default()
        }
    } else {
        Default::default()
    }
}

fn protocol_is_catalog_managed(protocol: &Protocol) -> bool {
    protocol.has_reader_feature(&TableFeature::CatalogManaged)
        || protocol.has_writer_feature(&TableFeature::CatalogManaged)
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

#[cfg(test)]
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
        metadata = metadata.with_description(comment);
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

async fn bootstrap_catalog_managed_delta_table<C>(
    ctx: &C,
    catalog_table: &[String],
    table_url: &Url,
    log_store: LogStoreRef,
) -> Result<bool>
where
    C: SessionExtensionAccessor + ?Sized,
{
    let Some(info) = load_catalog_managed_delta_bootstrap_info(ctx, catalog_table).await? else {
        return Ok(false);
    };
    let (actions, operation) = catalog_managed_bootstrap_actions(info, table_url)?;
    CommitBuilder::default()
        .with_actions(actions)
        .build(None, log_store, operation)
        .await
        .map(|_| true)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

async fn load_catalog_managed_commits<C>(
    ctx: &C,
    lakehouse_table: &LakehouseExecutionContext,
    table_url: &Url,
    log_store: LogStoreRef,
    version_as_of: Option<i64>,
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

    let catalog_table = lakehouse_table.catalog_table();
    let coordinator = DeltaCatalogCommitCoordinator::new(ctx, catalog_table);
    let response = coordinator
        .get_ratified_commits(lakehouse_table, table_url.to_string(), 1, version_as_of)
        .await?;

    let latest_table_version = response.latest_table_version;
    if let Some(version) = version_as_of {
        if latest_table_version >= 0 && version > latest_table_version {
            return Err(DataFusionError::External(Box::new(DeltaTableError::generic(
                format!(
                    "Catalog-managed Delta table latest ratified version is {latest_table_version}, but version {version} was requested"
                ),
            ))));
        }
    }

    let Some(effective_end_version) =
        effective_catalog_commit_end_version(latest_table_version, version_as_of)
    else {
        return Ok(CatalogManagedCommitSet {
            latest_table_version,
            commits: Vec::new(),
        });
    };

    let mut by_version = response
        .commits
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

    for version in 1..=effective_end_version {
        if by_version.contains_key(&version) {
            continue;
        }
        let response = coordinator
            .get_ratified_commits(
                lakehouse_table,
                table_url.to_string(),
                version,
                Some(version),
            )
            .await?;
        if let Some(commit) = response
            .commits
            .into_iter()
            .find(|commit| commit.version == version)
        {
            by_version.insert(
                version,
                CatalogManagedCommitFile {
                    version: commit.version,
                    file_name: commit.file_name,
                },
            );
            continue;
        }
        if let Some(file_name) = staged_catalog_managed_commit_file(&log_store, version).await? {
            by_version.insert(version, CatalogManagedCommitFile { version, file_name });
        } else {
            return Err(DataFusionError::External(Box::new(DeltaTableError::generic(
                format!("Catalog-managed Delta commit version {version} was not returned by the catalog"),
            ))));
        }
    }

    Ok(CatalogManagedCommitSet {
        latest_table_version,
        commits: by_version.into_values().collect(),
    })
}

async fn staged_catalog_managed_commit_file(
    log_store: &LogStoreRef,
    version: i64,
) -> Result<Option<String>> {
    let prefix = Path::from("_delta_log/_staged_commits");
    let store = log_store.object_store(None);
    let staged = store
        .list(Some(&prefix))
        .try_filter_map(|meta| async move {
            let path = meta.location.as_ref();
            let file_name = path.rsplit('/').next().unwrap_or(path);
            Ok(
                (file_name.starts_with(&format!("{version:020}")) && file_name.ends_with(".json"))
                    .then(|| catalog_managed_commit_file_name(path)),
            )
        })
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    match staged.as_slice() {
        [] => Ok(None),
        [file_name] => Ok(Some(file_name.clone())),
        _ => Err(DataFusionError::External(Box::new(
            DeltaTableError::generic(format!(
                "Multiple staged Delta commits found for catalog-managed version {version}"
            )),
        ))),
    }
}

pub(crate) async fn load_catalog_managed_commits_for_snapshot<C>(
    ctx: &C,
    lakehouse_table: &LakehouseExecutionContext,
    table_url: &Url,
    log_store: LogStoreRef,
    version_as_of: Option<i64>,
) -> Result<Option<CatalogManagedCommitSet>>
where
    C: SessionExtensionAccessor + ?Sized,
{
    let catalog_table = lakehouse_table.catalog_table();
    if load_catalog_managed_delta_bootstrap_info(ctx, catalog_table)
        .await?
        .is_some()
    {
        match log_store.get_latest_version(-1).await {
            Ok(_) => {}
            Err(DeltaError::MissingVersion) => {
                bootstrap_catalog_managed_delta_table(
                    ctx,
                    catalog_table,
                    table_url,
                    log_store.clone(),
                )
                .await?;
            }
            Err(err) => return Err(DataFusionError::from(err)),
        }
        return Ok(Some(
            load_catalog_managed_commits(
                ctx,
                lakehouse_table,
                table_url,
                log_store.clone(),
                version_as_of,
            )
            .await?,
        ));
    }

    let mut detector = DeltaTable::new(
        log_store.clone(),
        DeltaSnapshotConfig {
            require_files: false,
            ..Default::default()
        },
    );
    match detector.load().await {
        Ok(()) => {
            if protocol_is_catalog_managed(detector.snapshot()?.protocol()) {
                Ok(Some(
                    load_catalog_managed_commits(
                        ctx,
                        lakehouse_table,
                        table_url,
                        log_store.clone(),
                        version_as_of,
                    )
                    .await?,
                ))
            } else {
                Ok(None)
            }
        }
        Err(err) if is_missing_delta_log_error(&err) => {
            if bootstrap_catalog_managed_delta_table(
                ctx,
                catalog_table,
                table_url,
                log_store.clone(),
            )
            .await?
            {
                Ok(Some(
                    load_catalog_managed_commits(
                        ctx,
                        lakehouse_table,
                        table_url,
                        log_store.clone(),
                        version_as_of,
                    )
                    .await?,
                ))
            } else {
                Err(DataFusionError::from(err))
            }
        }
        Err(err) => Err(DataFusionError::from(err)),
    }
}

async fn load_delta_read_state(
    ctx: &dyn Session,
    table_url: Url,
    schema: Option<Schema>,
    options: DeltaReadOptions,
    metadata_only: bool,
    lakehouse_table: Option<LakehouseExecutionContext>,
) -> Result<(Arc<DeltaSnapshot>, LogStoreRef, DeltaScanConfig)> {
    let url = ListingTableUrl::try_new(table_url.clone(), None)?;
    let object_store = ctx.runtime_env().object_store(&url)?;
    let storage_config = StorageConfig;
    let log_store =
        create_logstore_with_object_store(object_store, table_url.clone(), storage_config)?;

    let catalog_managed_commits = match catalog_managed_commit_context(lakehouse_table.as_ref()) {
        Some(lakehouse_table) => {
            load_catalog_managed_commits_for_snapshot(
                &ctx,
                lakehouse_table,
                &table_url,
                log_store.clone(),
                options.version_as_of,
            )
            .await?
        }
        None => None,
    };

    let mut table_config = delta_read_snapshot_config(metadata_only, &options);
    table_config.catalog_managed_commits = catalog_managed_commits;
    let mut deltalake_table = DeltaTable::new(log_store.clone(), table_config);

    // Load the table state according to the provided time travel options.
    load_table_by_options(&mut deltalake_table, &options).await?;

    let snapshot = deltalake_table.snapshot()?.clone();

    let scan_config = DeltaScanConfig {
        file_column_name: None,
        row_index_column_name: None,
        wrap_partition_values: false,
        enable_parquet_pushdown: true,
        schema: match schema {
            Some(ref s) if s.fields().is_empty() => None,
            Some(s) => Some(Arc::new(s)),
            None => None,
        },
        commit_version_column_name: None,
        commit_timestamp_column_name: None,
        delta_log_replay_strategy: options.delta_log_replay_strategy,
        delta_log_replay_hash_threshold: options.delta_log_replay_hash_threshold.get(),
    };

    Ok((snapshot, log_store, scan_config))
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

fn parse_timestamp_as_of(timestamp: &str) -> DeltaResult<DateTime<Utc>> {
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
    use super::{effective_catalog_commit_end_version, next_catalog_commit_start_version};

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
