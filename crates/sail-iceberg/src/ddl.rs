use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion_common::{DataFusionError, Result, not_impl_err, plan_err};
use object_store::{PutMode, PutOptions};
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::AlterTableOptions;
use sail_common_datafusion::catalog::{CommitAuthority, LakehouseExecutionContext, TableKind};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakesource::LakeSourceAlterTableOperation;

use crate::catalog_support::commit::IcebergCatalogCommitCoordinator;
use crate::datasource::type_converter::arrow_type_to_iceberg;
use crate::lake_source::{IcebergLakeSource, metadata_location_from_properties};
use crate::spec::{
    FormatVersion, Literal, MetadataLog, NestedField, PrimitiveType, Schema, TableMetadata, Type,
};
use crate::table::metadata_loader::{
    encode_metadata_file, load_metadata_file_bytes, metadata_file_extension_from_properties,
    metadata_file_version_from_path, metadata_location_to_object_path_string,
};

pub(crate) async fn alter_catalog_table(
    ctx: &TaskContext,
    path: &str,
    operation: &LakeSourceAlterTableOperation,
    context: &LakehouseExecutionContext,
) -> Result<()> {
    if context.commit != CommitAuthority::IcebergMetadataLocationCas {
        return not_impl_err!(
            "ALTER TABLE is not yet supported for this Iceberg catalog commit protocol"
        );
    }
    let manager = ctx.extension::<CatalogManager>()?;
    let status = manager
        .get_table(context.catalog_table())
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let TableKind::Table { properties, .. } = status.kind else {
        return plan_err!("ALTER TABLE requires an Iceberg table");
    };
    let previous = metadata_location_from_properties(&properties).ok_or_else(|| {
        DataFusionError::Plan(
            "catalog-authoritative Iceberg table has no metadata location".to_string(),
        )
    })?;
    let table_url = IcebergLakeSource::parse_table_url(vec![path.to_string()]).await?;
    let runtime = ctx.runtime_env();
    let store = runtime.object_store_registry.get_store(&table_url)?;
    // Read exactly the committed pointer; directory listings may contain failed commits.
    let metadata_path = metadata_location_to_object_path_string(&previous)?;
    let bytes = load_metadata_file_bytes(&store, &metadata_path).await?;
    let mut metadata = TableMetadata::from_json(&bytes)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    apply_operation(&mut metadata, operation)?;
    metadata.metadata_log.push(MetadataLog {
        timestamp_ms: metadata.last_updated_ms,
        metadata_file: previous.clone(),
    });
    metadata.last_updated_ms = crate::utils::timestamp::monotonic_timestamp_ms();
    let version = metadata_file_version_from_path(&previous).unwrap_or(0) + 1;
    let extension = metadata_file_extension_from_properties(&metadata.properties)?;
    let next = table_url
        .join(&format!(
            "metadata/{version:05}-{}{extension}",
            uuid::Uuid::new_v4()
        ))
        .map_err(|error| DataFusionError::External(Box::new(error)))?
        .to_string();
    let json = metadata
        .to_json()
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let bytes = encode_metadata_file(&next, &json)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    store
        .put_opts(
            &object_store::path::Path::from(metadata_location_to_object_path_string(&next)?),
            bytes.into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await?;
    // Leave the file on an ambiguous failure: HMS may already have published it.
    IcebergCatalogCommitCoordinator::new(ctx, context.catalog_table())
        .update_metadata_location_with_alter(
            &properties,
            &previous,
            &next,
            catalog_alter_options(operation)?,
        )
        .await
}

fn catalog_alter_options(
    operation: &LakeSourceAlterTableOperation,
) -> Result<Vec<AlterTableOptions>> {
    Ok(match operation {
        LakeSourceAlterTableOperation::SetTableProperties { changes, .. } => {
            let mut properties = Vec::new();
            let mut keys = Vec::new();
            for (key, value) in changes {
                match value {
                    Some(value) => properties.push((key.clone(), value.clone())),
                    None => keys.push(key.clone()),
                }
            }
            vec![
                AlterTableOptions::SetTableProperties { properties },
                AlterTableOptions::UnsetTableProperties {
                    keys,
                    // Existence was checked against the committed format metadata.
                    if_exists: true,
                },
            ]
        }
        LakeSourceAlterTableOperation::AlterColumnType {
            column_path,
            data_type,
        } => {
            vec![AlterTableOptions::AlterColumnType {
                name: column_path.clone(),
                data_type: data_type.clone(),
            }]
        }
        LakeSourceAlterTableOperation::AlterColumnDefault {
            column_path,
            default,
        } => {
            vec![AlterTableOptions::AlterColumnDefault {
                name: column_path.clone(),
                default: default.clone(),
            }]
        }
        LakeSourceAlterTableOperation::AddCheckConstraint { .. } => {
            return not_impl_err!("CHECK constraints for Iceberg tables");
        }
    })
}

pub(crate) fn apply_operation(
    metadata: &mut TableMetadata,
    operation: &LakeSourceAlterTableOperation,
) -> Result<()> {
    match operation {
        LakeSourceAlterTableOperation::SetTableProperties { changes, if_exists } => {
            for (key, _) in changes {
                if (key != "format-version"
                    && crate::properties::is_reserved_iceberg_table_property(key))
                    || key.eq_ignore_ascii_case("table_type")
                    || key.eq_ignore_ascii_case("EXTERNAL")
                    || key.starts_with("spark.sql.")
                {
                    return plan_err!(
                        "Cannot alter reserved property '{key}' on catalog-managed Iceberg tables"
                    );
                }
            }
            crate::properties::apply_table_property_changes(metadata, changes, *if_exists)
        }
        LakeSourceAlterTableOperation::AlterColumnType {
            column_path,
            data_type,
        } => {
            let target = arrow_type_to_iceberg(data_type)?;
            update_column(metadata, column_path, |field| {
                let allowed = *field.field_type == target
                    || matches!(
                        (field.field_type.as_ref(), &target),
                        (
                            Type::Primitive(PrimitiveType::Int),
                            Type::Primitive(PrimitiveType::Long)
                        ) | (
                            Type::Primitive(PrimitiveType::Float),
                            Type::Primitive(PrimitiveType::Double)
                        )
                    );
                let decimal = matches!((field.field_type.as_ref(), &target),
                    (Type::Primitive(PrimitiveType::Decimal { precision: p1, scale: s1 }),
                     Type::Primitive(PrimitiveType::Decimal { precision: p2, scale: s2 })) if p2 >= p1 && s1 == s2);
                if !allowed && !decimal {
                    return plan_err!(
                        "Cannot change Iceberg column '{}' from {} to {}",
                        field.name,
                        field.field_type,
                        target
                    );
                }
                if let Type::Primitive(target) = &target {
                    for default in [&mut field.initial_default, &mut field.write_default]
                        .into_iter()
                        .flatten()
                    {
                        if let Literal::Primitive(value) = default {
                            *value = target
                                .promote_literal(value)
                                .ok_or_else(|| {
                                    DataFusionError::Plan(format!(
                                        "Cannot promote Iceberg default to {target}"
                                    ))
                                })?
                                .into_owned();
                        }
                    }
                }
                *field.field_type = target;
                Ok(())
            })
        }
        LakeSourceAlterTableOperation::AlterColumnDefault {
            column_path,
            default,
        } => {
            if metadata.format_version < FormatVersion::V3 {
                return plan_err!("Iceberg column defaults require format-version=3");
            }
            update_column(metadata, column_path, |field| {
                field.write_default = default
                    .as_deref()
                    .map(|default| parse_default(default, field))
                    .transpose()?;
                Ok(())
            })
        }
        LakeSourceAlterTableOperation::AddCheckConstraint { .. } => {
            not_impl_err!("CHECK constraints for Iceberg tables")
        }
    }
}

/// Build protocol updates without accessing storage; REST catalogs publish their own metadata.
pub fn catalog_alter_updates(
    metadata: &TableMetadata,
    operation: &LakeSourceAlterTableOperation,
) -> Result<(
    Vec<crate::spec::TableRequirement>,
    Vec<crate::spec::catalog::TableUpdate>,
)> {
    use crate::spec::catalog::{TableRequirement, TableUpdate};
    let mut updated = metadata.clone();
    apply_operation(&mut updated, operation)?;
    let mut requirements = Vec::new();
    if let Some(uuid) = metadata.table_uuid {
        requirements.push(TableRequirement::UuidMatch { uuid });
    }
    let mut updates = Vec::new();
    if updated.format_version != metadata.format_version {
        updates.push(TableUpdate::UpgradeFormatVersion {
            format_version: updated.format_version,
        });
    }
    if updated.current_schema_id != metadata.current_schema_id {
        requirements.push(TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: metadata.current_schema_id,
        });
        requirements.push(TableRequirement::LastAssignedFieldIdMatch {
            last_assigned_field_id: metadata.last_column_id,
        });
        let schema = updated
            .current_schema()
            .ok_or_else(|| DataFusionError::Plan("Missing Iceberg schema".to_string()))?;
        if let Some(existing) = metadata.schemas.iter().find(|existing| {
            existing.as_struct() == schema.as_struct()
                && existing
                    .identifier_field_ids()
                    .collect::<std::collections::HashSet<_>>()
                    == schema
                        .identifier_field_ids()
                        .collect::<std::collections::HashSet<_>>()
        }) {
            updates.push(TableUpdate::SetCurrentSchema {
                schema_id: existing.schema_id(),
            });
        } else {
            updates.push(TableUpdate::AddSchema {
                schema: Box::new(schema.clone()),
            });
            updates.push(TableUpdate::SetCurrentSchema { schema_id: -1 });
        }
    }
    let properties = updated
        .properties
        .iter()
        .filter(|(key, value)| metadata.properties.get(*key) != Some(*value))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<std::collections::HashMap<_, _>>();
    if !properties.is_empty() {
        updates.push(TableUpdate::SetProperties {
            updates: properties,
        });
    }
    let removals = metadata
        .properties
        .keys()
        .filter(|key| !updated.properties.contains_key(*key))
        .cloned()
        .collect::<Vec<_>>();
    if !removals.is_empty() {
        updates.push(TableUpdate::RemoveProperties { removals });
    }
    Ok((requirements, updates))
}

fn update_column(
    metadata: &mut TableMetadata,
    path: &[String],
    update: impl FnOnce(&mut NestedField) -> Result<()>,
) -> Result<()> {
    let [name] = path else {
        return not_impl_err!("Iceberg ALTER COLUMN currently requires a top-level column");
    };
    let current = metadata
        .current_schema()
        .ok_or_else(|| DataFusionError::Plan("Missing Iceberg schema".to_string()))?;
    let mut fields = current.fields().to_vec();
    let field = fields
        .iter_mut()
        .find(|field| field.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| DataFusionError::Plan(format!("Column '{name}' does not exist")))?;
    update(Arc::make_mut(field))?;
    let schema_id = metadata
        .schemas
        .iter()
        .map(Schema::schema_id)
        .max()
        .unwrap_or(0)
        + 1;
    let schema = Schema::builder()
        .with_schema_id(schema_id)
        .with_identifier_field_ids(current.identifier_field_ids())
        .with_fields(fields)
        .build()
        .map_err(DataFusionError::Plan)?;
    metadata.schemas.push(schema);
    metadata.current_schema_id = schema_id;
    Ok(())
}

fn parse_default(expression: &str, field: &NestedField) -> Result<Literal> {
    let literal = Literal::try_from_str(expression, &field.field_type)
        .map_err(DataFusionError::Plan)?
        .unwrap_or(Literal::Null);
    if field.required && matches!(literal, Literal::Null) {
        return plan_err!(
            "Required Iceberg column '{}' cannot default to null",
            field.name
        );
    }
    Ok(literal)
}
