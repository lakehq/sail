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

pub(crate) async fn drop_check_constraint(
    ctx: &TaskContext,
    path: &str,
    lakehouse: Option<&LakehouseExecutionContext>,
    name: String,
    if_exists: bool,
) -> Result<()> {
    let table = open_table(ctx, path, lakehouse).await?;
    let snapshot = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .clone();
    if catalog_managed_commit_context(lakehouse).is_none() {
        crate::lake_source::ensure_not_catalog_managed_delta(
            &snapshot,
            "ALTER TABLE DROP CONSTRAINT",
        )?;
    }
    let name = name.to_lowercase();
    let key = format!("delta.constraints.{name}");
    let expr = snapshot.metadata().configuration().get(&key).cloned();
    if expr.is_none() && !if_exists {
        return plan_err!("Constraint '{name}' does not exist");
    }
    let metadata = snapshot.metadata().clone().remove_config_key(&key);
    commit(
        ctx,
        lakehouse,
        snapshot,
        table.log_store(),
        vec![CommitAction::Metadata(metadata)],
        DeltaOperation::DropConstraint { name, expr },
    )
    .await
}

pub(crate) async fn alter_columns(
    ctx: &TaskContext,
    path: &str,
    lakehouse: Option<&LakehouseExecutionContext>,
    changes: &[sail_common::spec::ColumnChange<datafusion::arrow::datatypes::DataType>],
) -> Result<sail_common_datafusion::lakesource::LakeSourceAlterTableResult> {
    use datafusion::arrow::datatypes::Schema;
    use sail_common::spec::ColumnChange;

    use crate::spec::{ColumnMappingMode, StructType};

    let table = open_table(ctx, path, lakehouse).await?;
    let snapshot = table
        .snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .clone();
    if catalog_managed_commit_context(lakehouse).is_none() {
        crate::lake_source::ensure_not_catalog_managed_delta(&snapshot, "ALTER TABLE COLUMNS")?;
    }
    let current = StructType::try_from(snapshot.schema())?;
    let mut partitions = snapshot.metadata().partition_columns().clone();
    for change in changes {
        if matches!(
            change,
            ColumnChange::Comment { .. } | ColumnChange::Nullability { .. }
        ) && is_collection_slot(snapshot.schema().fields(), change.path())
        {
            return plan_err!(
                "Delta does not support changing comments or nullability of collection elements"
            );
        }
        if let ColumnChange::Add { data_type, .. } = change {
            reject_nested_required_fields(data_type)?;
        }
        if matches!(
            change,
            ColumnChange::Rename { .. } | ColumnChange::Drop { .. }
        ) {
            if snapshot.effective_column_mapping_mode() == ColumnMappingMode::None {
                return plan_err!("Delta RENAME/DROP COLUMN requires column mapping");
            }
            // Dependencies must be rewritten or removed before their columns can change.
            if snapshot
                .metadata()
                .configuration()
                .keys()
                .any(|key| key.starts_with("delta.constraints."))
                || snapshot.schema().fields().iter().any(|field| {
                    sail_common_datafusion::column_features::ColumnFeatures::from_field(field)
                        .generation_expression()
                        .is_some()
                })
            {
                return plan_err!(
                    "Cannot rename or drop columns while CHECK constraints or generated columns depend on the schema"
                );
            }
            if let [name] = change.path()
                && let Some(index) = partitions.iter().position(|p| p.eq_ignore_ascii_case(name))
            {
                match change {
                    ColumnChange::Rename { name, .. } => partitions[index] = name.clone(),
                    ColumnChange::Drop { .. } => {
                        return plan_err!("Cannot drop a partition column");
                    }
                    _ => {}
                }
            }
        }
    }
    let arrow = Schema::try_from(&current)?;
    let updated = sail_common_datafusion::column_change::alter_schema(&arrow, changes)?;
    if snapshot.effective_column_mapping_mode() == ColumnMappingMode::None {
        validate_unmapped_names(updated.fields())?;
    }
    let candidate =
        preserve_schema_metadata(&current, &StructType::try_from(&updated)?, updated.fields())?;
    let (candidate, metadata) = crate::schema::manager::evolve_schema(
        &current,
        &candidate,
        snapshot.metadata(),
        snapshot.effective_column_mapping_mode(),
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let mut metadata = metadata.with_partition_columns(partitions);
    let mut properties = Vec::new();
    const STATS_COLUMNS: &str = "delta.dataSkippingStatsColumns";
    if let Some(value) = metadata.configuration().get(STATS_COLUMNS)
        && let Some(value) = update_statistics_columns(value, changes)?
    {
        metadata = metadata.add_config_key(STATS_COLUMNS.into(), value.clone());
        properties.push((STATS_COLUMNS.into(), value));
    }
    let operation = match changes {
        [ColumnChange::Rename { path, name }] => {
            let mut renamed = path.clone();
            if let Some(last) = renamed.last_mut() {
                *last = name.clone();
            }
            DeltaOperation::RenameColumn {
                old_column_path: path.join("."),
                new_column_path: renamed.join("."),
            }
        }
        changes
            if changes
                .iter()
                .all(|c| matches!(c, ColumnChange::Drop { .. })) =>
        {
            DeltaOperation::DropColumns {
                columns: changes.iter().map(|c| c.path().join(".")).collect(),
            }
        }
        changes
            if changes
                .iter()
                .all(|c| matches!(c, ColumnChange::Add { .. })) =>
        {
            DeltaOperation::AddColumns {
                columns: changes
                    .iter()
                    .map(|change| {
                        let mut column =
                            crate::lake_source::operation_column_json(&candidate, change.path())
                                .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        if let Some(metadata) = column
                            .get_mut("metadata")
                            .and_then(serde_json::Value::as_object_mut)
                        {
                            metadata.retain(|key, _| !key.starts_with("delta.columnMapping."));
                        }
                        let mut value = serde_json::json!({"column": column});
                        if let ColumnChange::Add {
                            position: Some(position),
                            ..
                        } = change
                        {
                            value["position"] =
                                serde_json::Value::String(column_position(position));
                        }
                        Ok(value)
                    })
                    .collect::<Result<Vec<_>>>()?,
            }
        }
        [change] => DeltaOperation::AlterColumn {
            column: crate::lake_source::operation_column_json(&candidate, change.path())
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
            position: match change {
                ColumnChange::Position { position, .. } => Some(column_position(position)),
                _ => None,
            },
        },
        _ => return plan_err!("Invalid combination of column changes"),
    };
    let schema = Arc::new(Schema::try_from(&candidate)?);
    let desired = crate::schema::protocol_for_metadata(&metadata)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let desired = crate::lake_source::avoid_stable_type_widening_auto_upgrade_for_preview_tables(
        snapshot.protocol(),
        &desired,
        metadata.configuration(),
    );
    let protocol = snapshot.protocol().merge_upgrade_requirements(&desired);
    let mut actions = Vec::new();
    if &protocol != snapshot.protocol() {
        actions.push(CommitAction::Protocol(protocol));
    }
    actions.push(CommitAction::Metadata(metadata));
    commit(
        ctx,
        lakehouse,
        snapshot,
        table.log_store(),
        actions,
        operation,
    )
    .await?;
    Ok(
        sail_common_datafusion::lakesource::LakeSourceAlterTableResult {
            catalog_updated: catalog_managed_commit_context(lakehouse).is_some(),
            schema: Some(schema),
            properties,
        },
    )
}

fn update_statistics_columns(
    value: &str,
    changes: &[sail_common::spec::ColumnChange<datafusion::arrow::datatypes::DataType>],
) -> Result<Option<String>> {
    use sail_common::spec::ColumnChange;
    let mut columns = crate::spec::ColumnName::parse_column_name_list(value)
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .into_iter()
        .map(crate::spec::ColumnName::into_inner)
        .collect::<Vec<_>>();
    let mut changed = false;
    for change in changes {
        let path = change.path();
        columns.retain_mut(|column| {
            if column.len() < path.len()
                || !column
                    .iter()
                    .zip(path)
                    .all(|(a, b)| a.eq_ignore_ascii_case(b))
            {
                return true;
            }
            match change {
                ColumnChange::Rename { name, .. } => {
                    column[path.len() - 1] = name.clone();
                    changed = true;
                    true
                }
                ColumnChange::Drop { .. } => {
                    changed = true;
                    false
                }
                _ => true,
            }
        });
    }
    Ok(changed.then(|| {
        columns
            .iter()
            .map(|path| {
                path.iter()
                    .map(|part| format!("`{}`", part.replace('`', "``")))
                    .collect::<Vec<_>>()
                    .join(".")
            })
            .collect::<Vec<_>>()
            .join(",")
    }))
}

fn is_collection_slot(fields: &datafusion::arrow::datatypes::Fields, path: &[String]) -> bool {
    use datafusion::arrow::datatypes::DataType;
    let mut ty = DataType::Struct(fields.clone());
    for (i, name) in path.iter().enumerate() {
        ty = match ty {
            DataType::Struct(fields) => {
                let Some(field) = fields.iter().find(|f| f.name().eq_ignore_ascii_case(name))
                else {
                    return false;
                };
                field.data_type().clone()
            }
            DataType::List(field) | DataType::LargeList(field)
                if name.eq_ignore_ascii_case("element") =>
            {
                if i + 1 == path.len() {
                    return true;
                }
                field.data_type().clone()
            }
            DataType::Map(entries, _) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return false;
                };
                let index = if name.eq_ignore_ascii_case("key") {
                    0
                } else if name.eq_ignore_ascii_case("value") {
                    1
                } else {
                    return false;
                };
                if i + 1 == path.len() {
                    return true;
                }
                fields[index].data_type().clone()
            }
            _ => return false,
        };
    }
    false
}

fn reject_nested_required_fields(ty: &datafusion::arrow::datatypes::DataType) -> Result<()> {
    use datafusion::arrow::datatypes::DataType;
    match ty {
        DataType::Struct(fields) => {
            for field in fields {
                if !field.is_nullable() {
                    return plan_err!("Cannot add a required column to an existing Delta table");
                }
                reject_nested_required_fields(field.data_type())?;
            }
        }
        DataType::List(field) | DataType::LargeList(field) => {
            reject_nested_required_fields(field.data_type())?
        }
        DataType::Map(field, _) => {
            if let DataType::Struct(fields) = field.data_type() {
                for field in fields {
                    reject_nested_required_fields(field.data_type())?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_unmapped_names(fields: &datafusion::arrow::datatypes::Fields) -> Result<()> {
    use datafusion::arrow::datatypes::DataType;
    fn nested(ty: &DataType) -> Result<()> {
        match ty {
            DataType::Struct(fields) => validate_unmapped_names(fields),
            DataType::List(field) | DataType::LargeList(field) | DataType::Map(field, _) => {
                nested(field.data_type())
            }
            _ => Ok(()),
        }
    }
    for field in fields {
        if field
            .name()
            .contains([' ', ',', ';', '{', '}', '(', ')', '\n', '\t', '='])
        {
            return plan_err!(
                "Invalid Delta column name '{}'; enable column mapping to use special characters",
                field.name()
            );
        }
        nested(field.data_type())?;
    }
    Ok(())
}

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

fn column_position(position: &sail_common::spec::ColumnPosition) -> String {
    match position {
        sail_common::spec::ColumnPosition::First => "FIRST".into(),
        sail_common::spec::ColumnPosition::After(name) => format!("AFTER {name}"),
    }
}

fn preserve_schema_metadata(
    current: &crate::spec::StructType,
    candidate: &crate::spec::StructType,
    arrow: &datafusion::arrow::datatypes::Fields,
) -> Result<crate::spec::StructType> {
    use crate::spec::{ColumnMetadataKey, MetadataValue, StructType};
    let fields = candidate
        .fields()
        .zip(arrow)
        .map(|(field, arrow)| {
            let id = field.get_config_value(&ColumnMetadataKey::ColumnMappingId);
            let old = current.fields().find(|old| {
                if let Some(id) = id {
                    old.get_config_value(&ColumnMetadataKey::ColumnMappingId) == Some(id)
                } else {
                    old.name() == field.name()
                }
            });
            let mut field = field.clone();
            if let Some(old) = old {
                field.metadata = old.metadata.clone();
            }
            if let Some(comment) = arrow.metadata().get("comment") {
                field
                    .metadata
                    .insert("comment".into(), MetadataValue::String(comment.clone()));
            }
            preserve_type_metadata(
                old.map(|f| f.data_type()),
                &mut field.data_type,
                arrow.data_type(),
            )?;
            Ok(field)
        })
        .collect::<Result<Vec<_>>>()?;
    StructType::try_new(fields).map_err(|e| DataFusionError::External(Box::new(e)))
}

fn preserve_type_metadata(
    current: Option<&crate::spec::DataType>,
    target: &mut crate::spec::DataType,
    arrow: &datafusion::arrow::datatypes::DataType,
) -> Result<()> {
    use datafusion::arrow::datatypes::DataType as ArrowType;

    use crate::spec::{DataType, StructType};
    match (target, arrow) {
        (DataType::Struct(target), ArrowType::Struct(fields)) => {
            let empty =
                StructType::try_new(vec![]).map_err(|e| DataFusionError::External(Box::new(e)))?;
            let current = match current {
                Some(DataType::Struct(value)) => value.as_ref(),
                _ => &empty,
            };
            **target = preserve_schema_metadata(current, target, fields)?;
        }
        (DataType::Array(target), ArrowType::List(element) | ArrowType::LargeList(element)) => {
            let current = match current {
                Some(DataType::Array(value)) => Some(value.element_type()),
                _ => None,
            };
            preserve_type_metadata(current, &mut target.element_type, element.data_type())?;
        }
        (DataType::Map(target), ArrowType::Map(entries, _)) => {
            let ArrowType::Struct(fields) = entries.data_type() else {
                return plan_err!("Invalid map schema");
            };
            let (key, value) = match current {
                Some(DataType::Map(value)) => (Some(value.key_type()), Some(value.value_type())),
                _ => (None, None),
            };
            preserve_type_metadata(key, &mut target.key_type, fields[0].data_type())?;
            preserve_type_metadata(value, &mut target.value_type, fields[1].data_type())?;
        }
        _ => {}
    }
    Ok(())
}
