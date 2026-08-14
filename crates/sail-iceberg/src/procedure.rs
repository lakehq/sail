use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::common::{DataFusionError, Result, not_impl_err, plan_err};
use datafusion::execution::TaskContext;
use object_store::ObjectStoreExt;
use sail_common_datafusion::datasource::{OptionLayer, SourceInfo};
use sail_common_datafusion::lakeprocedure::{
    LakeProcedure, LakeProcedureAccess, LakeProcedureDataType, LakeProcedureField,
    LakeProcedureInvocation, LakeProcedureParameter, LakeProcedureProvider,
    LakeProcedureResolution, LakeProcedureValue,
};

use crate::catalog_support::commit::{
    CatalogCommitOutcome, CatalogTableInfo, IcebergCatalogCommitCoordinator,
    IcebergCatalogCommitMode, catalog_requirements, table_metadata_location,
};
use crate::io::StoreContext;
use crate::lake_source::{
    IcebergLakeSource, metadata_location_from_properties, validate_iceberg_lakehouse_storage_access,
};
use crate::spec::metadata::table_metadata::{MetadataLog, SnapshotLog};
use crate::spec::snapshots::{MAIN_BRANCH, SnapshotReference, SnapshotRetention};
use crate::spec::{Snapshot, TableMetadata, TableRequirement, TableUpdate};
use crate::table::metadata_loader::{
    encode_metadata_file, load_metadata_file_bytes, metadata_file_extension_from_properties,
    metadata_file_version_from_path, metadata_location_to_object_path_string, write_version_hint,
};
use crate::utils::metadata::metadata_files_for_version;

const MAX_PROCEDURE_COMMIT_RETRIES: usize = 5;

const ICEBERG_PROCEDURES: &[&str] = &[
    "rollback_to_snapshot",
    "rollback_to_timestamp",
    "set_current_snapshot",
    "cherrypick_snapshot",
    "rewrite_data_files",
    "rewrite_manifests",
    "remove_orphan_files",
    "expire_snapshots",
    "migrate",
    "snapshot",
    "add_files",
    "ancestors_of",
    "register_table",
    "publish_changes",
    "create_changelog_view",
    "rewrite_position_delete_files",
    "fast_forward",
    "compute_table_stats",
    "compute_partition_stats",
    "rewrite_table_path",
];

#[async_trait]
impl LakeProcedureProvider for IcebergLakeSource {
    fn resolve_procedure(&self, name: &str) -> LakeProcedureResolution {
        if let Some(procedure) = supported_procedure(name) {
            return LakeProcedureResolution::Supported(procedure);
        }
        if ICEBERG_PROCEDURES
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(name))
        {
            return LakeProcedureResolution::Unsupported {
                reason: format!(
                    "Iceberg system procedure '{}' is recognized but not implemented",
                    name.to_ascii_lowercase()
                ),
            };
        }
        LakeProcedureResolution::Unrecognized
    }

    async fn execute_procedure(
        &self,
        ctx: &TaskContext,
        info: SourceInfo,
        invocation: LakeProcedureInvocation,
    ) -> Result<RecordBatch> {
        let ProcedureTable {
            table_url,
            table_properties,
            lakehouse_table,
        } = ProcedureTable::from_source_info(info).await?;
        match invocation.procedure.name.as_str() {
            "ancestors_of" => {
                let metadata = load_current_metadata(
                    ctx,
                    &table_url,
                    &table_properties,
                    lakehouse_table.as_ref(),
                )
                .await?;
                ancestors_output(&metadata, &invocation)
            }
            "rollback_to_snapshot" => {
                let snapshot_id = required_i64(&invocation, "snapshot_id")?;
                commit_snapshot_operation(
                    ctx,
                    &table_url,
                    &table_properties,
                    lakehouse_table.as_ref(),
                    SnapshotOperation::RollbackToSnapshot(snapshot_id),
                    invocation.procedure.schema(),
                )
                .await
            }
            "rollback_to_timestamp" => {
                let timestamp_micros = required_timestamp_micros(&invocation, "timestamp")?;
                commit_snapshot_operation(
                    ctx,
                    &table_url,
                    &table_properties,
                    lakehouse_table.as_ref(),
                    SnapshotOperation::RollbackToTimestamp(timestamp_micros.div_euclid(1_000)),
                    invocation.procedure.schema(),
                )
                .await
            }
            "set_current_snapshot" => {
                let snapshot_id = optional_i64(&invocation, "snapshot_id")?;
                let reference = optional_string(&invocation, "ref")?;
                if snapshot_id.is_some() == reference.is_some() {
                    return plan_err!(
                        "Exactly one of snapshot_id or ref must be provided to set_current_snapshot"
                    );
                }
                commit_snapshot_operation(
                    ctx,
                    &table_url,
                    &table_properties,
                    lakehouse_table.as_ref(),
                    SnapshotOperation::SetCurrentSnapshot {
                        snapshot_id,
                        reference,
                    },
                    invocation.procedure.schema(),
                )
                .await
            }
            "fast_forward" => {
                let branch = required_string(&invocation, "branch")?;
                let to = required_string(&invocation, "to")?;
                commit_snapshot_operation(
                    ctx,
                    &table_url,
                    &table_properties,
                    lakehouse_table.as_ref(),
                    SnapshotOperation::FastForward { branch, to },
                    invocation.procedure.schema(),
                )
                .await
            }
            name => not_impl_err!("Iceberg system procedure '{name}' is not implemented"),
        }
    }
}

fn supported_procedure(name: &str) -> Option<LakeProcedure> {
    let name = name.to_ascii_lowercase();
    let string = LakeProcedureDataType::Utf8;
    let long = LakeProcedureDataType::Int64;
    let timestamp = LakeProcedureDataType::TimestampMicros;
    let (parameters, output, access) = match name.as_str() {
        "ancestors_of" => (
            vec![
                LakeProcedureParameter::required("table", string),
                LakeProcedureParameter::optional("snapshot_id", long),
            ],
            vec![
                LakeProcedureField::new("snapshot_id", long, true),
                LakeProcedureField::new("timestamp", long, true),
            ],
            LakeProcedureAccess::MetadataRead,
        ),
        "rollback_to_snapshot" => (
            vec![
                LakeProcedureParameter::required("table", string),
                LakeProcedureParameter::required("snapshot_id", long),
            ],
            snapshot_change_output(false),
            LakeProcedureAccess::MetadataCommit,
        ),
        "rollback_to_timestamp" => (
            vec![
                LakeProcedureParameter::required("table", string),
                LakeProcedureParameter::required("timestamp", timestamp),
            ],
            snapshot_change_output(false),
            LakeProcedureAccess::MetadataCommit,
        ),
        "set_current_snapshot" => (
            vec![
                LakeProcedureParameter::required("table", string),
                LakeProcedureParameter::optional("snapshot_id", long),
                LakeProcedureParameter::optional("ref", string),
            ],
            snapshot_change_output(true),
            LakeProcedureAccess::MetadataCommit,
        ),
        "fast_forward" => (
            vec![
                LakeProcedureParameter::required("table", string),
                LakeProcedureParameter::required("branch", string),
                LakeProcedureParameter::required("to", string),
            ],
            vec![
                LakeProcedureField::new("branch_updated", string, false),
                LakeProcedureField::new("previous_ref", long, true),
                LakeProcedureField::new("updated_ref", long, false),
            ],
            LakeProcedureAccess::MetadataCommit,
        ),
        _ => return None,
    };
    Some(LakeProcedure {
        name,
        parameters,
        output,
        access,
    })
}

fn snapshot_change_output(previous_nullable: bool) -> Vec<LakeProcedureField> {
    vec![
        LakeProcedureField::new(
            "previous_snapshot_id",
            LakeProcedureDataType::Int64,
            previous_nullable,
        ),
        LakeProcedureField::new("current_snapshot_id", LakeProcedureDataType::Int64, false),
    ]
}

struct ProcedureTable {
    table_url: url::Url,
    table_properties: Vec<(String, String)>,
    lakehouse_table: Option<sail_common_datafusion::catalog::LakehouseExecutionContext>,
}

impl ProcedureTable {
    async fn from_source_info(info: SourceInfo) -> Result<Self> {
        validate_iceberg_lakehouse_storage_access(info.lakehouse_table.as_ref())?;
        let table_url = IcebergLakeSource::parse_table_url(info.paths).await?;
        let table_properties = info
            .options
            .iter()
            .filter_map(|layer| match layer {
                OptionLayer::TablePropertyList { items } => Some(items.as_slice()),
                _ => None,
            })
            .flatten()
            .cloned()
            .collect();
        Ok(Self {
            table_url,
            table_properties,
            lakehouse_table: info.lakehouse_table,
        })
    }
}

async fn load_current_metadata(
    ctx: &TaskContext,
    table_url: &url::Url,
    table_properties: &[(String, String)],
    lakehouse_table: Option<&sail_common_datafusion::catalog::LakehouseExecutionContext>,
) -> Result<TableMetadata> {
    let object_store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let catalog_table = lakehouse_table.map(|context| context.catalog_table());
    let catalog_info = match catalog_table {
        Some(table) => IcebergCatalogCommitCoordinator::load_table_info(ctx, table).await?,
        None => CatalogTableInfo::default(),
    };
    let mode = IcebergCatalogCommitMode::resolve(lakehouse_table, &catalog_info, table_properties);
    let metadata_location = if mode.uses_catalog_metadata() {
        catalog_info
            .metadata_location
            .or_else(|| metadata_location_from_properties(table_properties))
    } else {
        None
    };
    let metadata_file = match metadata_location {
        Some(location) => metadata_location_to_object_path_string(&location)?,
        None => crate::table::find_latest_metadata_file(&object_store, table_url).await?,
    };
    let bytes = load_metadata_file_bytes(&object_store, &metadata_file).await?;
    TableMetadata::from_json(&bytes).map_err(|error| DataFusionError::External(Box::new(error)))
}

fn ancestors_output(
    metadata: &TableMetadata,
    invocation: &LakeProcedureInvocation,
) -> Result<RecordBatch> {
    let start = optional_i64(invocation, "snapshot_id")?.or_else(|| main_snapshot_id(metadata));
    let Some(start) = start else {
        return Ok(RecordBatch::new_empty(invocation.procedure.schema()));
    };
    let ancestors = ancestor_chain(metadata, start)?;
    let snapshot_ids = ancestors
        .iter()
        .map(|snapshot| Some(snapshot.snapshot_id()))
        .collect::<Vec<_>>();
    let timestamps = ancestors
        .iter()
        .map(|snapshot| Some(snapshot.timestamp_ms()))
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        invocation.procedure.schema(),
        vec![
            Arc::new(Int64Array::from(snapshot_ids)),
            Arc::new(Int64Array::from(timestamps)),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

#[derive(Clone)]
enum SnapshotOperation {
    RollbackToSnapshot(i64),
    RollbackToTimestamp(i64),
    SetCurrentSnapshot {
        snapshot_id: Option<i64>,
        reference: Option<String>,
    },
    FastForward {
        branch: String,
        to: String,
    },
}

struct PreparedSnapshotOperation {
    requirement: TableRequirement,
    update: TableUpdate,
    output: RecordBatch,
    changed: bool,
}

impl SnapshotOperation {
    fn prepare(
        &self,
        metadata: &TableMetadata,
        schema: datafusion::arrow::datatypes::SchemaRef,
    ) -> Result<PreparedSnapshotOperation> {
        match self {
            Self::RollbackToSnapshot(snapshot_id) => {
                let previous = required_current_snapshot(metadata)?;
                snapshot(metadata, *snapshot_id)?;
                if !is_ancestor(metadata, *snapshot_id, previous)? {
                    return plan_err!(
                        "Cannot roll back to snapshot {snapshot_id}: it is not an ancestor of the current snapshot {previous}"
                    );
                }
                prepare_main_update(metadata, previous, *snapshot_id, schema, false)
            }
            Self::RollbackToTimestamp(timestamp_ms) => {
                let previous = required_current_snapshot(metadata)?;
                let target = ancestor_chain(metadata, previous)?
                    .into_iter()
                    .filter(|snapshot| snapshot.timestamp_ms() < *timestamp_ms)
                    .max_by_key(|snapshot| snapshot.timestamp_ms())
                    .map(Snapshot::snapshot_id)
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "Cannot roll back: no ancestor snapshot is older than timestamp {timestamp_ms}"
                        ))
                    })?;
                prepare_main_update(metadata, previous, target, schema, false)
            }
            Self::SetCurrentSnapshot {
                snapshot_id,
                reference,
            } => {
                let previous = main_snapshot_id(metadata);
                let target = match (snapshot_id, reference) {
                    (Some(snapshot_id), None) => *snapshot_id,
                    (None, Some(reference)) => reference_snapshot_id(metadata, reference)?,
                    _ => {
                        return plan_err!(
                            "Exactly one of snapshot_id or ref must be provided to set_current_snapshot"
                        );
                    }
                };
                snapshot(metadata, target)?;
                prepare_main_update_nullable(metadata, previous, target, schema)
            }
            Self::FastForward { branch, to } => {
                let target = reference_snapshot_id(metadata, to)?;
                snapshot(metadata, target)?;
                let previous_reference = metadata.refs.get(branch);
                if let Some(reference) = previous_reference {
                    if !reference.is_branch() {
                        return plan_err!("Ref {branch} is a tag, not a branch");
                    }
                    if !is_ancestor(metadata, reference.snapshot_id, target)? {
                        return plan_err!(
                            "Cannot fast-forward: {branch} is not an ancestor of {to}"
                        );
                    }
                }
                let previous = previous_reference
                    .map(|reference| reference.snapshot_id)
                    .or_else(|| {
                        (branch == MAIN_BRANCH)
                            .then_some(main_snapshot_id(metadata))
                            .flatten()
                    });
                if previous_reference.is_none()
                    && let Some(previous) = previous
                    && !is_ancestor(metadata, previous, target)?
                {
                    return plan_err!("Cannot fast-forward: {branch} is not an ancestor of {to}");
                }
                let retention = previous_reference
                    .map(|reference| reference.retention.clone())
                    .unwrap_or_else(default_branch_retention);
                let requirement = TableRequirement::RefSnapshotIdMatch {
                    r#ref: branch.clone(),
                    snapshot_id: previous,
                };
                let update = TableUpdate::SetSnapshotRef {
                    ref_name: branch.clone(),
                    reference: SnapshotReference {
                        snapshot_id: target,
                        retention,
                    },
                };
                let output = fast_forward_output(schema, branch, previous, target)?;
                Ok(PreparedSnapshotOperation {
                    requirement,
                    update,
                    output,
                    changed: previous != Some(target),
                })
            }
        }
    }
}

async fn commit_snapshot_operation(
    ctx: &TaskContext,
    table_url: &url::Url,
    table_properties: &[(String, String)],
    lakehouse_table: Option<&sail_common_datafusion::catalog::LakehouseExecutionContext>,
    operation: SnapshotOperation,
    output_schema: datafusion::arrow::datatypes::SchemaRef,
) -> Result<RecordBatch> {
    let object_store = ctx
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let store_context = StoreContext::new(object_store.clone(), table_url)?;
    let catalog_table = lakehouse_table.map(|context| context.catalog_table().to_vec());

    for attempt in 1..=MAX_PROCEDURE_COMMIT_RETRIES {
        let catalog_info = match catalog_table.as_ref() {
            Some(table) => IcebergCatalogCommitCoordinator::load_table_info(ctx, table).await?,
            None => CatalogTableInfo::default(),
        };
        let commit_mode =
            IcebergCatalogCommitMode::resolve(lakehouse_table, &catalog_info, table_properties);
        let recorded_metadata_location = catalog_info
            .metadata_location
            .clone()
            .or_else(|| metadata_location_from_properties(table_properties));
        let metadata_location = commit_mode
            .uses_catalog_metadata()
            .then(|| recorded_metadata_location.clone())
            .flatten();
        let metadata_file = match metadata_location.as_deref() {
            Some(location) => metadata_location_to_object_path_string(location)?,
            None => crate::table::find_latest_metadata_file(&object_store, table_url).await?,
        };
        let bytes = load_metadata_file_bytes(&object_store, &metadata_file).await?;
        let mut metadata = TableMetadata::from_json(&bytes)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let prepared = operation.prepare(&metadata, output_schema.clone())?;
        if !prepared.changed {
            return Ok(prepared.output);
        }

        let requirements =
            catalog_requirements(&metadata, std::slice::from_ref(&prepared.requirement), &[]);
        let updates = vec![prepared.update.clone()];
        let mut use_metadata_location_fallback = commit_mode.uses_metadata_location_update();
        if commit_mode.uses_catalog_commit() {
            let table = catalog_table.as_ref().ok_or_else(|| {
                DataFusionError::Internal(
                    "missing catalog table for Iceberg procedure commit".to_string(),
                )
            })?;
            let context = lakehouse_table.ok_or_else(|| {
                DataFusionError::Internal(
                    "missing lakehouse context for Iceberg procedure commit".to_string(),
                )
            })?;
            match IcebergCatalogCommitCoordinator::new(ctx, table)
                .commit(context, requirements, updates)
                .await?
            {
                CatalogCommitOutcome::Committed(_) => return Ok(prepared.output),
                CatalogCommitOutcome::Conflict if attempt < MAX_PROCEDURE_COMMIT_RETRIES => {
                    continue;
                }
                CatalogCommitOutcome::Conflict => return Err(procedure_commit_conflict()),
                CatalogCommitOutcome::NotSupported
                    if matches!(
                        commit_mode,
                        IcebergCatalogCommitMode::CompatibilityCatalogCommit
                    ) =>
                {
                    use_metadata_location_fallback = true;
                }
                CatalogCommitOutcome::NotSupported => {
                    return plan_err!(
                        "Iceberg catalog commit is not supported by the resolved catalog authority"
                    );
                }
            }
        }

        let current_version = metadata_file_version_from_path(&metadata_file).unwrap_or(0);
        let next_version = current_version + 1;
        if !use_metadata_location_fallback {
            let existing = metadata_files_for_version(&store_context, next_version).await?;
            if !existing.is_empty() {
                if attempt < MAX_PROCEDURE_COMMIT_RETRIES {
                    continue;
                }
                return Err(procedure_commit_conflict());
            }
        }
        apply_snapshot_update(
            &mut metadata,
            &prepared.update,
            metadata_location.as_deref().unwrap_or(&metadata_file),
        )?;
        let metadata_json = metadata
            .to_json()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let extension = metadata_file_extension_from_properties(&metadata.properties)?;
        let new_metadata_file = if use_metadata_location_fallback {
            format!(
                "metadata/{next_version:05}-{}{extension}",
                uuid::Uuid::new_v4()
            )
        } else {
            format!("metadata/v{next_version}{extension}")
        };
        let new_metadata_location = table_metadata_location(table_url, &new_metadata_file)?;
        let encoded = encode_metadata_file(&new_metadata_file, &metadata_json)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let metadata_path = object_store::path::Path::from(new_metadata_file.as_str());
        let result = store_context
            .prefixed
            .put_opts(
                &metadata_path,
                object_store::PutPayload::from(Bytes::from(encoded)),
                object_store::PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                },
            )
            .await;
        match result {
            Ok(_) => {}
            Err(object_store::Error::AlreadyExists { .. })
                if attempt < MAX_PROCEDURE_COMMIT_RETRIES =>
            {
                continue;
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                return Err(procedure_commit_conflict());
            }
            Err(error) => return Err(DataFusionError::External(Box::new(error))),
        }
        if !use_metadata_location_fallback {
            let version_files = metadata_files_for_version(&store_context, next_version).await?;
            if version_files
                .iter()
                .any(|candidate| candidate != &new_metadata_file)
            {
                let _ = store_context.prefixed.delete(&metadata_path).await;
                if attempt < MAX_PROCEDURE_COMMIT_RETRIES {
                    continue;
                }
                return Err(procedure_commit_conflict());
            }
        }

        if let Some(table) = catalog_table.as_ref()
            && (use_metadata_location_fallback
                || matches!(commit_mode, IcebergCatalogCommitMode::Filesystem))
        {
            IcebergCatalogCommitCoordinator::new(ctx, table)
                .update_metadata_location(
                    table_properties,
                    recorded_metadata_location.as_deref(),
                    &new_metadata_location,
                )
                .await?;
        }
        let version_hint = if use_metadata_location_fallback {
            new_metadata_file
                .rsplit('/')
                .next()
                .unwrap_or(new_metadata_file.as_str())
                .to_string()
        } else {
            next_version.to_string()
        };
        write_version_hint(&store_context.prefixed, &version_hint).await;
        return Ok(prepared.output);
    }
    Err(procedure_commit_conflict())
}

fn apply_snapshot_update(
    metadata: &mut TableMetadata,
    update: &TableUpdate,
    previous_metadata_file: &str,
) -> Result<()> {
    let TableUpdate::SetSnapshotRef {
        ref_name,
        reference,
    } = update
    else {
        return Err(DataFusionError::Internal(
            "Iceberg snapshot procedure produced a non-reference update".to_string(),
        ));
    };
    let previous_timestamp = metadata.last_updated_ms;
    let timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();
    if ref_name == MAIN_BRANCH {
        metadata.current_snapshot_id = Some(reference.snapshot_id);
        metadata.snapshot_log.push(SnapshotLog {
            timestamp_ms,
            snapshot_id: reference.snapshot_id,
        });
    }
    metadata.refs.insert(ref_name.clone(), reference.clone());
    metadata.last_updated_ms = timestamp_ms;
    metadata.metadata_log.push(MetadataLog {
        timestamp_ms: previous_timestamp,
        metadata_file: previous_metadata_file.to_string(),
    });
    Ok(())
}

fn prepare_main_update(
    metadata: &TableMetadata,
    previous: i64,
    target: i64,
    schema: datafusion::arrow::datatypes::SchemaRef,
    previous_nullable: bool,
) -> Result<PreparedSnapshotOperation> {
    let output = snapshot_change_batch(schema, Some(previous), target, previous_nullable)?;
    prepare_main_reference_update(metadata, Some(previous), target, output)
}

fn prepare_main_update_nullable(
    metadata: &TableMetadata,
    previous: Option<i64>,
    target: i64,
    schema: datafusion::arrow::datatypes::SchemaRef,
) -> Result<PreparedSnapshotOperation> {
    let output = snapshot_change_batch(schema, previous, target, true)?;
    prepare_main_reference_update(metadata, previous, target, output)
}

fn prepare_main_reference_update(
    metadata: &TableMetadata,
    previous: Option<i64>,
    target: i64,
    output: RecordBatch,
) -> Result<PreparedSnapshotOperation> {
    if let Some(reference) = metadata.refs.get(MAIN_BRANCH)
        && !reference.is_branch()
    {
        return plan_err!("Ref {MAIN_BRANCH} is a tag, not a branch");
    }
    let retention = metadata
        .refs
        .get(MAIN_BRANCH)
        .map(|reference| reference.retention.clone())
        .unwrap_or_else(default_branch_retention);
    Ok(PreparedSnapshotOperation {
        requirement: TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_string(),
            snapshot_id: previous,
        },
        update: TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: SnapshotReference {
                snapshot_id: target,
                retention,
            },
        },
        output,
        changed: previous != Some(target),
    })
}

fn snapshot_change_batch(
    schema: datafusion::arrow::datatypes::SchemaRef,
    previous: Option<i64>,
    current: i64,
    previous_nullable: bool,
) -> Result<RecordBatch> {
    if !previous_nullable && previous.is_none() {
        return plan_err!("Iceberg table has no current snapshot");
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![previous])),
            Arc::new(Int64Array::from(vec![current])),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

fn fast_forward_output(
    schema: datafusion::arrow::datatypes::SchemaRef,
    branch: &str,
    previous: Option<i64>,
    current: i64,
) -> Result<RecordBatch> {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![branch])),
            Arc::new(Int64Array::from(vec![previous])),
            Arc::new(Int64Array::from(vec![current])),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

fn default_branch_retention() -> SnapshotRetention {
    SnapshotRetention::Branch {
        min_snapshots_to_keep: None,
        max_snapshot_age_ms: None,
        max_ref_age_ms: None,
    }
}

fn required_current_snapshot(metadata: &TableMetadata) -> Result<i64> {
    main_snapshot_id(metadata)
        .ok_or_else(|| DataFusionError::Plan("Iceberg table has no current snapshot".to_string()))
}

fn main_snapshot_id(metadata: &TableMetadata) -> Option<i64> {
    metadata
        .refs
        .get(MAIN_BRANCH)
        .map(|reference| reference.snapshot_id)
        .or(metadata.current_snapshot_id)
        .filter(|snapshot_id| *snapshot_id >= 0)
}

fn snapshot(metadata: &TableMetadata, snapshot_id: i64) -> Result<&Snapshot> {
    metadata
        .snapshots
        .iter()
        .find(|snapshot| snapshot.snapshot_id() == snapshot_id)
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Cannot find Iceberg snapshot with id {snapshot_id}"
            ))
        })
}

fn reference_snapshot_id(metadata: &TableMetadata, reference: &str) -> Result<i64> {
    if reference == MAIN_BRANCH {
        return required_current_snapshot(metadata);
    }
    metadata
        .refs
        .get(reference)
        .map(|reference| reference.snapshot_id)
        .ok_or_else(|| DataFusionError::Plan(format!("Ref does not exist: {reference}")))
}

fn ancestor_chain(metadata: &TableMetadata, start: i64) -> Result<Vec<&Snapshot>> {
    let mut current = Some(start);
    let mut visited = HashSet::new();
    let mut ancestors = Vec::new();
    while let Some(snapshot_id) = current {
        if !visited.insert(snapshot_id) {
            return plan_err!("Cycle detected in Iceberg snapshot ancestry at {snapshot_id}");
        }
        let current_snapshot = snapshot(metadata, snapshot_id)?;
        current = current_snapshot.parent_snapshot_id();
        ancestors.push(current_snapshot);
    }
    Ok(ancestors)
}

fn is_ancestor(metadata: &TableMetadata, ancestor: i64, descendant: i64) -> Result<bool> {
    Ok(ancestor_chain(metadata, descendant)?
        .iter()
        .any(|snapshot| snapshot.snapshot_id() == ancestor))
}

fn required_i64(invocation: &LakeProcedureInvocation, name: &str) -> Result<i64> {
    optional_i64(invocation, name)?.ok_or_else(|| {
        DataFusionError::Plan(format!("Missing required procedure argument '{name}'"))
    })
}

fn optional_i64(invocation: &LakeProcedureInvocation, name: &str) -> Result<Option<i64>> {
    match invocation.argument(name) {
        Some(LakeProcedureValue::Int64(value)) => Ok(Some(*value)),
        Some(LakeProcedureValue::Null) | None => Ok(None),
        value => plan_err!("Procedure argument '{name}' is not an int64: {value:?}"),
    }
}

fn required_timestamp_micros(invocation: &LakeProcedureInvocation, name: &str) -> Result<i64> {
    match invocation.argument(name) {
        Some(LakeProcedureValue::TimestampMicros(value)) => Ok(*value),
        value => plan_err!("Procedure argument '{name}' is not a timestamp: {value:?}"),
    }
}

fn required_string(invocation: &LakeProcedureInvocation, name: &str) -> Result<String> {
    optional_string(invocation, name)?.ok_or_else(|| {
        DataFusionError::Plan(format!("Missing required procedure argument '{name}'"))
    })
}

fn optional_string(invocation: &LakeProcedureInvocation, name: &str) -> Result<Option<String>> {
    match invocation.argument(name) {
        Some(LakeProcedureValue::Utf8(value)) => Ok(Some(value.clone())),
        Some(LakeProcedureValue::Null) | None => Ok(None),
        value => plan_err!("Procedure argument '{name}' is not a string: {value:?}"),
    }
}

fn procedure_commit_conflict() -> DataFusionError {
    DataFusionError::Execution(format!(
        "Iceberg procedure commit failed after {MAX_PROCEDURE_COMMIT_RETRIES} retries due to concurrent metadata updates"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{FormatVersion, Operation, Summary};

    fn snapshot_with_parent(id: i64, parent: Option<i64>, timestamp_ms: i64) -> Snapshot {
        Snapshot {
            snapshot_id: id,
            parent_snapshot_id: parent,
            sequence_number: id,
            timestamp_ms,
            manifest_list: String::new(),
            manifests: None,
            summary: Summary::new(Operation::Append),
            schema_id: None,
            first_row_id: None,
            added_rows: None,
            key_id: None,
        }
    }

    fn metadata_with_snapshots() -> TableMetadata {
        TableMetadata {
            format_version: FormatVersion::V2,
            table_uuid: None,
            location: "file:///tmp/table".to_string(),
            last_sequence_number: 3,
            last_updated_ms: 30,
            last_column_id: 0,
            schemas: vec![],
            current_schema_id: 0,
            partition_specs: vec![],
            default_spec_id: 0,
            last_partition_id: 0,
            properties: Default::default(),
            current_snapshot_id: Some(3),
            next_row_id: None,
            encryption_keys: vec![],
            snapshots: vec![
                snapshot_with_parent(1, None, 10),
                snapshot_with_parent(2, Some(1), 20),
                snapshot_with_parent(3, Some(2), 30),
            ],
            snapshot_log: vec![],
            metadata_log: vec![],
            sort_orders: vec![],
            default_sort_order_id: None,
            refs: Default::default(),
            statistics: vec![],
            partition_statistics: vec![],
        }
    }

    #[test]
    fn snapshot_ancestry_is_ordered_from_start_to_root() -> Result<()> {
        let metadata = metadata_with_snapshots();
        let ids = ancestor_chain(&metadata, 3)?
            .into_iter()
            .map(Snapshot::snapshot_id)
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![3, 2, 1]);
        assert!(is_ancestor(&metadata, 1, 3)?);
        assert!(!is_ancestor(&metadata, 3, 1)?);
        Ok(())
    }

    #[test]
    fn recognized_procedures_are_distinct_from_unknown_procedures() {
        let source = IcebergLakeSource;
        assert!(matches!(
            source.resolve_procedure("rollback_to_snapshot"),
            LakeProcedureResolution::Supported(_)
        ));
        assert!(matches!(
            source.resolve_procedure("expire_snapshots"),
            LakeProcedureResolution::Unsupported { .. }
        ));
        assert_eq!(
            source.resolve_procedure("not_an_iceberg_procedure"),
            LakeProcedureResolution::Unrecognized
        );
    }

    #[test]
    fn rollback_requires_ancestry_but_set_current_does_not() -> Result<()> {
        let mut metadata = metadata_with_snapshots();
        metadata
            .snapshots
            .push(snapshot_with_parent(4, Some(1), 40));
        let Some(rollback_procedure) = supported_procedure("rollback_to_snapshot") else {
            return plan_err!("rollback_to_snapshot should be supported");
        };
        let Err(error) = SnapshotOperation::RollbackToSnapshot(4)
            .prepare(&metadata, rollback_procedure.schema())
        else {
            return plan_err!("sibling snapshot cannot be a rollback target");
        };
        assert!(error.to_string().contains("not an ancestor"));

        let Some(set_current_procedure) = supported_procedure("set_current_snapshot") else {
            return plan_err!("set_current_snapshot should be supported");
        };
        let prepared = SnapshotOperation::SetCurrentSnapshot {
            snapshot_id: Some(4),
            reference: None,
        }
        .prepare(&metadata, set_current_procedure.schema())?;
        assert!(matches!(
            prepared.update,
            TableUpdate::SetSnapshotRef { reference, .. } if reference.snapshot_id == 4
        ));
        Ok(())
    }

    #[test]
    fn fast_forward_preserves_branch_retention_and_checks_ancestry() -> Result<()> {
        let mut metadata = metadata_with_snapshots();
        let retention = SnapshotRetention::Branch {
            min_snapshots_to_keep: Some(2),
            max_snapshot_age_ms: Some(10_000),
            max_ref_age_ms: Some(20_000),
        };
        metadata.refs.insert(
            "audit".to_string(),
            SnapshotReference {
                snapshot_id: 1,
                retention: retention.clone(),
            },
        );
        metadata.refs.insert(
            "tip".to_string(),
            SnapshotReference {
                snapshot_id: 3,
                retention: default_branch_retention(),
            },
        );
        let Some(procedure) = supported_procedure("fast_forward") else {
            return plan_err!("fast_forward should be supported");
        };
        let schema = procedure.schema();
        let prepared = SnapshotOperation::FastForward {
            branch: "audit".to_string(),
            to: "tip".to_string(),
        }
        .prepare(&metadata, schema.clone())?;
        assert!(matches!(
            prepared.update,
            TableUpdate::SetSnapshotRef { reference, .. }
                if reference.snapshot_id == 3 && reference.retention == retention
        ));

        let Err(error) = SnapshotOperation::FastForward {
            branch: "tip".to_string(),
            to: "audit".to_string(),
        }
        .prepare(&metadata, schema) else {
            return plan_err!("a branch cannot be moved backward by fast-forward");
        };
        assert!(error.to_string().contains("not an ancestor"));
        Ok(())
    }
}
