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
        LakeSourceAlterTableOperation::AlterIdentifierFields { names, drop } => {
            vec![AlterTableOptions::AlterIdentifierFields {
                names: names.clone(),
                drop: *drop,
            }]
        }
        LakeSourceAlterTableOperation::AlterPartitionSpec { add, drop } => {
            vec![AlterTableOptions::AlterPartitionSpec {
                add: add.clone(),
                drop: drop.clone(),
            }]
        }
        LakeSourceAlterTableOperation::SetPartitioning { partition_by } => {
            vec![AlterTableOptions::SetPartitioning {
                partition_by: partition_by.clone(),
            }]
        }
        LakeSourceAlterTableOperation::SetWriteOrder {
            order,
            distribution,
        } => vec![AlterTableOptions::SetWriteOrder {
            order: order.clone(),
            distribution: distribution.clone(),
        }],
        LakeSourceAlterTableOperation::SetLocation { location } => {
            vec![AlterTableOptions::SetLocation {
                location: location.clone(),
            }]
        }
        LakeSourceAlterTableOperation::ReplaceColumns { schema } => {
            vec![AlterTableOptions::ReplaceColumns {
                fields: schema.fields().clone(),
            }]
        }
        LakeSourceAlterTableOperation::AlterColumns { changes } => {
            vec![AlterTableOptions::AlterColumns {
                changes: changes.clone(),
            }]
        }
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
        LakeSourceAlterTableOperation::AddCheckConstraint { .. }
        | LakeSourceAlterTableOperation::DropCheckConstraint { .. } => {
            return not_impl_err!("CHECK constraints for Iceberg tables");
        }
    })
}

pub(crate) fn apply_operation(
    metadata: &mut TableMetadata,
    operation: &LakeSourceAlterTableOperation,
) -> Result<()> {
    match operation {
        LakeSourceAlterTableOperation::AlterIdentifierFields { names, drop } => {
            alter_identifier_fields(metadata, names, *drop)
        }
        LakeSourceAlterTableOperation::AlterPartitionSpec { add, drop } => {
            alter_partition_spec(metadata, add.as_ref(), drop.as_ref())
        }
        LakeSourceAlterTableOperation::SetPartitioning { partition_by } => {
            let schema = metadata
                .current_schema()
                .ok_or_else(|| DataFusionError::Plan("Missing Iceberg schema".into()))?;
            let spec = crate::lake_source::create_table_partition_spec(schema, partition_by)?
                .assign_ids(metadata);
            commit_partition_spec(metadata, spec);
            Ok(())
        }
        LakeSourceAlterTableOperation::SetWriteOrder {
            order,
            distribution,
        } => set_write_order(metadata, order, distribution.as_deref()),
        LakeSourceAlterTableOperation::SetLocation { location } => {
            if location.trim().is_empty() {
                return plan_err!("Table location cannot be empty");
            }
            metadata.location = location.trim_end_matches('/').to_string();
            Ok(())
        }
        LakeSourceAlterTableOperation::ReplaceColumns { schema } => {
            if schema.fields().iter().any(|field| !field.is_nullable()) {
                return plan_err!("Cannot add a required column to an existing Iceberg table");
            }
            commit_schema(metadata, schema)
        }
        LakeSourceAlterTableOperation::AlterColumns { changes } => alter_columns(metadata, changes),
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
        LakeSourceAlterTableOperation::AddCheckConstraint { .. }
        | LakeSourceAlterTableOperation::DropCheckConstraint { .. } => {
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
    if updated.location != metadata.location {
        updates.push(TableUpdate::SetLocation {
            location: updated.location.clone(),
        });
    }
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
    if updated.default_spec_id != metadata.default_spec_id {
        requirements.push(TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: metadata.current_schema_id,
        });
        requirements.push(TableRequirement::DefaultSpecIdMatch {
            default_spec_id: metadata.default_spec_id,
        });
        requirements.push(TableRequirement::LastAssignedPartitionIdMatch {
            last_assigned_partition_id: metadata.last_partition_id,
        });
        let spec = updated
            .default_partition_spec()
            .ok_or_else(|| DataFusionError::Plan("Missing Iceberg partition spec".into()))?;
        let spec_id = if metadata
            .partition_specs
            .iter()
            .any(|old| old.spec_id() == spec.spec_id())
        {
            spec.spec_id()
        } else {
            updates.push(TableUpdate::AddSpec { spec: spec.into() });
            -1
        };
        updates.push(TableUpdate::SetDefaultSpec { spec_id });
    }
    if updated.default_sort_order_id != metadata.default_sort_order_id {
        requirements.push(TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: metadata.current_schema_id,
        });
        requirements.push(TableRequirement::DefaultSortOrderIdMatch {
            default_sort_order_id: i64::from(metadata.default_sort_order_id.unwrap_or(0)),
        });
        let order = updated
            .sort_orders
            .iter()
            .find(|order| Some(order.order_id as i32) == updated.default_sort_order_id)
            .ok_or_else(|| DataFusionError::Plan("Missing Iceberg sort order".into()))?;
        let sort_order_id = if metadata
            .sort_orders
            .iter()
            .any(|old| old.order_id == order.order_id)
        {
            order.order_id
        } else {
            updates.push(TableUpdate::AddSortOrder {
                sort_order: order.clone(),
            });
            -1
        };
        updates.push(TableUpdate::SetDefaultSortOrder { sort_order_id });
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
    let current = metadata
        .current_schema()
        .ok_or_else(|| DataFusionError::Plan("Missing Iceberg schema".to_string()))?;
    let mut fields = current.fields().to_vec();
    update_nested_field(&mut fields, path, update)?;
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

fn commit_partition_spec(metadata: &mut TableMetadata, spec: crate::spec::PartitionSpec) {
    metadata.default_spec_id = spec.spec_id();
    metadata.last_partition_id = metadata
        .last_partition_id
        .max(spec.last_assigned_field_id());
    if !metadata
        .partition_specs
        .iter()
        .any(|old| old.spec_id() == spec.spec_id())
    {
        metadata.partition_specs.push(spec);
    }
}

fn alter_partition_spec(
    metadata: &mut TableMetadata,
    add: Option<&(
        sail_common_datafusion::catalog::CatalogPartitionField,
        Option<String>,
    )>,
    drop: Option<&sail_common_datafusion::catalog::CatalogPartitionField>,
) -> Result<()> {
    use crate::spec::{PartitionField, PartitionSpec, Transform};
    use crate::utils::partition_transform::{
        iceberg_transform_from_partition_field, partition_field_name,
    };
    let schema = metadata
        .current_schema()
        .ok_or_else(|| DataFusionError::Plan("Missing Iceberg schema".into()))?;
    let mut fields = metadata
        .default_partition_spec()
        .map(|spec| spec.fields().to_vec())
        .unwrap_or_default();
    if fields.iter().any(|field| !field.source_ids.is_empty()) {
        return not_impl_err!("Evolution of multi-source partition specs");
    }
    // V1 tombstones are retained by assign_ids when constructing the next spec.
    fields.retain(|field| field.transform != Transform::Void);
    if let Some(drop) = drop {
        let source = schema.field_id_by_name(&drop.column);
        let transform = iceberg_transform_from_partition_field(drop);
        let index = fields
            .iter()
            .position(|field| match source {
                Some(source) => field.source_id == source && field.transform == transform,
                None => transform == Transform::Identity && field.name == drop.column,
            })
            .ok_or_else(|| {
                DataFusionError::Plan(format!("Partition field '{}' does not exist", drop.column))
            })?;
        fields.remove(index);
    }
    if let Some((add, name)) = add {
        let source = schema.field_by_name(&add.column).ok_or_else(|| {
            DataFusionError::Plan(format!("Partition source '{}' does not exist", add.column))
        })?;
        if !matches!(source.field_type.as_ref(), Type::Primitive(_))
            || schema.field_path_by_id(source.id).is_none()
        {
            return plan_err!("Cannot partition by non-primitive column '{}'", add.column);
        }
        let transform = iceberg_transform_from_partition_field(add);
        if matches!(transform, Transform::Bucket(0) | Transform::Truncate(0)) {
            return plan_err!("Bucket count and truncate width must be positive");
        }
        transform
            .result_type(&source.field_type)
            .map_err(DataFusionError::Plan)?;
        let name = name.clone().unwrap_or_else(|| partition_field_name(add));
        if fields.iter().any(|field| {
            field.name == name || (field.source_id == source.id && field.transform == transform)
        }) {
            return plan_err!("Duplicate partition field '{name}'");
        }
        if let Some(named) = schema.field_by_name(&name)
            && (named.id != source.id || transform != Transform::Identity)
        {
            return plan_err!("Partition field name '{name}' conflicts with a schema field");
        }
        fields.push(PartitionField::new(source.id, 0, name, transform));
    }
    let mut builder = PartitionSpec::builder();
    for field in fields {
        builder =
            builder.add_field_with_id(field.source_id, field.field_id, field.name, field.transform);
    }
    let spec = builder.build().assign_ids(metadata);
    commit_partition_spec(metadata, spec);
    Ok(())
}

fn set_write_order(
    metadata: &mut TableMetadata,
    requested: &[sail_common_datafusion::catalog::CatalogSortField],
    distribution: Option<&str>,
) -> Result<()> {
    use crate::spec::{NullOrder, SortDirection, SortField, SortOrder};
    use crate::utils::partition_transform::iceberg_transform_from_partition_field;
    let schema = metadata
        .current_schema()
        .ok_or_else(|| DataFusionError::Plan("Missing Iceberg schema".into()))?;
    let fields = requested
        .iter()
        .map(|field| {
            let source = schema.field_by_name(&field.field.column).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Sort column '{}' does not exist",
                    field.field.column
                ))
            })?;
            if !matches!(source.field_type.as_ref(), Type::Primitive(_))
                || schema.field_path_by_id(source.id).is_none()
            {
                return plan_err!(
                    "Cannot sort by non-primitive column '{}'",
                    field.field.column
                );
            }
            let transform = iceberg_transform_from_partition_field(&field.field);
            if matches!(
                transform,
                crate::spec::Transform::Bucket(0) | crate::spec::Transform::Truncate(0)
            ) {
                return plan_err!("Bucket count and truncate width must be positive");
            }
            transform
                .result_type(&source.field_type)
                .map_err(DataFusionError::Plan)?;
            Ok(SortField {
                source_id: source.id,
                source_ids: vec![],
                transform,
                direction: if field.ascending {
                    SortDirection::Ascending
                } else {
                    SortDirection::Descending
                },
                null_order: if field.nulls_first {
                    NullOrder::First
                } else {
                    NullOrder::Last
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let order_id = if let Some(order) = metadata
        .sort_orders
        .iter()
        .find(|order| order.fields == fields)
    {
        order.order_id
    } else {
        let order_id = if fields.is_empty() {
            0
        } else {
            metadata
                .sort_orders
                .iter()
                .map(|order| order.order_id)
                .max()
                .unwrap_or(0)
                + 1
        };
        metadata.sort_orders.push(SortOrder { order_id, fields });
        order_id
    };
    metadata.default_sort_order_id = Some(
        order_id
            .try_into()
            .map_err(|_| DataFusionError::Plan("Iceberg sort order ID overflow".into()))?,
    );
    if let Some(distribution) = distribution {
        metadata
            .properties
            .insert("write.distribution-mode".into(), distribution.into());
    }
    Ok(())
}

fn alter_identifier_fields(
    metadata: &mut TableMetadata,
    names: &[Vec<String>],
    drop: bool,
) -> Result<()> {
    use std::collections::HashSet;
    let current = metadata
        .current_schema()
        .ok_or_else(|| DataFusionError::Plan("Missing Iceberg schema".into()))?;
    let mut ids = if drop {
        current.identifier_field_ids().collect::<HashSet<_>>()
    } else {
        HashSet::new()
    };
    for name in names {
        let name = name.join(".");
        let field = current.field_by_name(&name).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Identifier field '{name}' not found in current schema"
            ))
        })?;
        if drop {
            if !ids.remove(&field.id) {
                return plan_err!("'{name}' is not an identifier field");
            }
        } else {
            ids.insert(field.id);
        }
    }
    // Reuse the native schema validator: identifiers must be required primitive
    // fields, outside lists/maps, with required struct ancestors, excluding floats.
    let schema = Schema::builder()
        .with_schema_id(
            metadata
                .schemas
                .iter()
                .map(Schema::schema_id)
                .max()
                .unwrap_or(0)
                + 1,
        )
        .with_fields(current.fields().to_vec())
        .with_identifier_field_ids(ids.iter().copied())
        .build()
        .map_err(DataFusionError::Plan)?;
    if let Some(existing) = metadata.schemas.iter().find(|existing| {
        existing.as_struct() == schema.as_struct()
            && existing.identifier_field_ids().collect::<HashSet<_>>() == ids
    }) {
        metadata.current_schema_id = existing.schema_id();
    } else {
        metadata.current_schema_id = schema.schema_id();
        metadata.schemas.push(schema);
    }
    Ok(())
}

fn alter_columns(
    metadata: &mut TableMetadata,
    changes: &[sail_common::spec::ColumnChange<datafusion::arrow::datatypes::DataType>],
) -> Result<()> {
    use crate::datasource::type_converter::iceberg_schema_to_arrow;
    let current = metadata
        .current_schema()
        .ok_or_else(|| DataFusionError::Plan("Missing Iceberg schema".into()))?;
    let arrow = iceberg_schema_to_arrow(current)?;
    let updated = sail_common_datafusion::column_change::alter_schema(&arrow, changes)?;
    commit_schema(metadata, &updated)
}

fn commit_schema(
    metadata: &mut TableMetadata,
    updated: &datafusion::arrow::datatypes::Schema,
) -> Result<()> {
    use crate::datasource::type_converter::arrow_field_to_iceberg;
    let current = metadata
        .current_schema()
        .ok_or_else(|| DataFusionError::Plan("Missing Iceberg schema".into()))?;
    let mut last_id = metadata.last_column_id;
    let fields = updated
        .fields()
        .iter()
        .map(|field| {
            let mut field = arrow_field_to_iceberg(field)?;
            assign_new_ids(&mut field, &mut last_id, current);
            Ok(Arc::new(field))
        })
        .collect::<Result<Vec<_>>>()?;
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
    for spec in &metadata.partition_specs {
        if spec.spec_id() == metadata.default_spec_id {
            for field in spec.fields() {
                if field.transform != crate::spec::Transform::Void
                    && schema.field_by_id(field.source_id).is_none()
                {
                    return plan_err!("Cannot drop a column used by the current partition spec");
                }
            }
        }
    }
    for order in &metadata.sort_orders {
        if Some(order.order_id) == metadata.default_sort_order_id.map(i64::from) {
            for field in &order.fields {
                if field.transform != crate::spec::Transform::Void
                    && schema.field_by_id(field.source_id).is_none()
                {
                    return plan_err!("Cannot drop a column used by the current sort order");
                }
            }
        }
    }
    if let Some(mapping) = metadata
        .properties
        .get(crate::spec::DEFAULT_SCHEMA_NAME_MAPPING)
    {
        let mapping: crate::spec::NameMapping = serde_json::from_str(mapping)
            .map_err(|e| DataFusionError::Plan(format!("Invalid Iceberg name mapping: {e}")))?;
        mapping.field_names().map_err(DataFusionError::Plan)?;
        let mapping = crate::spec::NameMapping::new(update_name_mapping(
            mapping.fields().iter(),
            schema.fields(),
        ));
        metadata.properties.insert(
            crate::spec::DEFAULT_SCHEMA_NAME_MAPPING.into(),
            serde_json::to_string(&mapping).map_err(|e| DataFusionError::External(Box::new(e)))?,
        );
    }
    metadata.schemas.push(schema);
    metadata.current_schema_id = schema_id;
    metadata.last_column_id = last_id;
    Ok(())
}

fn update_name_mapping<'a>(
    mapping: impl Iterator<Item = &'a crate::spec::MappedField>,
    fields: &[Arc<NestedField>],
) -> Vec<crate::spec::MappedField> {
    use crate::spec::MappedField;
    let mut result = Vec::new();
    for mapped in mapping {
        let field = fields.iter().find(|f| Some(f.id) == mapped.field_id());
        // Keep historical aliases unless another field has claimed the name.
        let mut names = mapped
            .names()
            .iter()
            .filter(|name| {
                !fields
                    .iter()
                    .any(|f| &f.name == *name && Some(f.id) != mapped.field_id())
            })
            .cloned()
            .collect::<Vec<_>>();
        let nested = if let Some(field) = field {
            if !names.contains(&field.name) {
                names.push(field.name.clone());
            }
            update_name_mapping(
                mapped.fields().iter().map(Arc::as_ref),
                &nested_fields(&field.field_type),
            )
        } else {
            mapped.fields().iter().map(|f| f.as_ref().clone()).collect()
        };
        result.push(MappedField::new(mapped.field_id(), names, nested));
    }
    for field in fields {
        if !result
            .iter()
            .any(|mapped| mapped.field_id() == Some(field.id))
        {
            result.push(MappedField::new(
                Some(field.id),
                vec![field.name.clone()],
                update_name_mapping(std::iter::empty(), &nested_fields(&field.field_type)),
            ));
        }
    }
    result
}

fn nested_fields(ty: &Type) -> Vec<Arc<NestedField>> {
    match ty {
        Type::Struct(value) => value.fields().to_vec(),
        Type::List(value) => vec![value.element_field.clone()],
        Type::Map(value) => vec![value.key_field.clone(), value.value_field.clone()],
        _ => vec![],
    }
}

fn assign_new_ids(field: &mut NestedField, last_id: &mut i32, current: &Schema) {
    // Column DDL does not change existing primitive types. Preserve format-specific
    // annotations that Arrow cannot represent (for example geometry CRS).
    if let Some(old) = current.field_by_id(field.id)
        && matches!(old.field_type.as_ref(), Type::Primitive(_))
    {
        field.field_type = old.field_type.clone();
    }
    if field.id == 0 {
        *last_id += 1;
        field.id = *last_id;
    }
    match field.field_type.as_mut() {
        Type::Struct(value) => {
            let mut fields = value.fields().to_vec();
            for field in &mut fields {
                assign_new_ids(Arc::make_mut(field), last_id, current);
            }
            *value = crate::spec::StructType::new(fields);
        }
        Type::List(value) => {
            assign_new_ids(Arc::make_mut(&mut value.element_field), last_id, current)
        }
        Type::Map(value) => {
            assign_new_ids(Arc::make_mut(&mut value.key_field), last_id, current);
            assign_new_ids(Arc::make_mut(&mut value.value_field), last_id, current);
        }
        _ => {}
    }
}

fn update_nested_field(
    fields: &mut [Arc<NestedField>],
    path: &[String],
    update: impl FnOnce(&mut NestedField) -> Result<()>,
) -> Result<()> {
    let Some((name, rest)) = path.split_first() else {
        return plan_err!("Column name must not be empty");
    };
    let field = fields
        .iter_mut()
        .find(|field| field.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| DataFusionError::Plan(format!("Column '{name}' does not exist")))?;
    let field = Arc::make_mut(field);
    if rest.is_empty() {
        return update(field);
    }
    match field.field_type.as_mut() {
        Type::Struct(value) => {
            let mut fields = value.fields().to_vec();
            update_nested_field(&mut fields, rest, update)?;
            *value = crate::spec::StructType::new(fields);
            Ok(())
        }
        Type::List(value) if rest[0].eq_ignore_ascii_case("element") => {
            update_nested_field(std::slice::from_mut(&mut value.element_field), rest, update)
        }
        Type::Map(value) if rest[0].eq_ignore_ascii_case("key") => {
            update_nested_field(std::slice::from_mut(&mut value.key_field), rest, update)
        }
        Type::Map(value) if rest[0].eq_ignore_ascii_case("value") => {
            update_nested_field(std::slice::from_mut(&mut value.value_field), rest, update)
        }
        _ => plan_err!("Column '{}' is not a nested field", path.join(".")),
    }
}
