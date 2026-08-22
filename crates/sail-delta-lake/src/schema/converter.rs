use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray,
    new_null_array,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, Fields, Schema as ArrowSchema, SchemaRef,
};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion_common::{DataFusionError, Result, exec_err};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use sail_common_datafusion::array::record_batch::cast_array_recursively;
use sail_common_datafusion::schema_evolution::{
    StructFieldMatching, cast_array_with_schema_evolution_relaxed_tz,
};
use serde::{Deserialize, Serialize};

use crate::spec::{
    ColumnMappingMode, ColumnMetadataKey, DataType, DeltaError as DeltaTableError, DeltaResult,
    MetadataValue, StructField, StructType,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalPartitionColumn {
    pub logical_name: String,
    pub physical_name: String,
}

impl PhysicalPartitionColumn {
    pub fn new(logical_name: impl Into<String>, physical_name: impl Into<String>) -> Self {
        Self {
            logical_name: logical_name.into(),
            physical_name: physical_name.into(),
        }
    }
}

pub fn arrow_schema_from_struct_type(
    schema: &StructType,
    partition_columns: &[String],
    wrap_partitions: bool,
) -> DeltaResult<SchemaRef> {
    let fields = schema
        .fields()
        .filter(|f| !partition_columns.contains(&f.name().to_string()))
        .map(field_from_struct_field)
        .chain(partition_columns.iter().map(|partition_col| {
            let f = schema
                .field(partition_col)
                .ok_or_else(|| DeltaTableError::missing_column(partition_col))?;
            let field = field_from_struct_field(f)?;
            let corrected = if wrap_partitions {
                wrap_partition_type(field.data_type())
            } else {
                field.data_type().clone()
            };
            Ok(field.with_data_type(corrected))
        }))
        .collect::<Result<Vec<Field>, DeltaTableError>>()?;

    Ok(Arc::new(ArrowSchema::new(fields)))
}

/// Build the physical Arrow schema used for Delta Parquet data files.
///
/// Name mode strips Delta-side field ID metadata, then re-adds each ID under Arrow's
/// `PARQUET:field_id` key so nested IDs reach the Parquet footer.
pub fn get_physical_arrow_schema(
    logical: &StructType,
    mode: ColumnMappingMode,
) -> DeltaResult<ArrowSchema> {
    // Build the stripped Delta physical schema first, then restore IDs only for Parquet.
    let physical_kernel = logical.make_physical(mode);
    let physical_arrow = ArrowSchema::try_from(&physical_kernel)?;
    match mode {
        ColumnMappingMode::Name | ColumnMappingMode::Id => {
            enrich_arrow_with_parquet_field_ids(&physical_arrow, logical)
        }
        ColumnMappingMode::None => Ok(physical_arrow),
    }
}

/// Build a physical schema for metadata and statistics without Parquet footer field IDs.
///
/// This is the Arrow-native equivalent of `StructType::make_physical`. It reads the
/// `delta.columnMapping.physicalName` metadata from each Arrow field and renames the
/// field accordingly.
pub fn make_physical_arrow_schema(logical: &ArrowSchema, mode: ColumnMappingMode) -> ArrowSchema {
    let new_fields: Vec<Field> = logical
        .fields()
        .iter()
        .map(|f| make_physical_arrow_field(f.as_ref(), mode))
        .collect();
    ArrowSchema::new(new_fields).with_metadata(logical.metadata().clone())
}

pub fn attach_column_mapping_metadata(requested: &ArrowSchema, table: &ArrowSchema) -> ArrowSchema {
    let fields = requested
        .fields()
        .iter()
        .map(|requested| {
            table
                .field_with_name(requested.name())
                .map(|table| attach_field_mapping_metadata(requested, table))
                .unwrap_or_else(|_| requested.as_ref().clone())
        })
        .collect::<Vec<_>>();
    ArrowSchema::new(fields).with_metadata(requested.metadata().clone())
}

fn attach_field_mapping_metadata(requested: &Field, table: &Field) -> Field {
    let mut metadata = requested.metadata().clone();
    for key in [
        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
        ColumnMetadataKey::ColumnMappingId.as_ref(),
        ColumnMetadataKey::ParquetFieldId.as_ref(),
        PARQUET_FIELD_ID_META_KEY,
    ] {
        if let Some(value) = table.metadata().get(key) {
            metadata.insert(key.to_string(), value.clone());
        }
    }

    requested
        .clone()
        .with_data_type(attach_data_type_mapping_metadata(
            requested.data_type(),
            table.data_type(),
        ))
        .with_metadata(metadata)
}

fn attach_data_type_mapping_metadata(
    requested: &ArrowDataType,
    table: &ArrowDataType,
) -> ArrowDataType {
    match (requested, table) {
        (ArrowDataType::Struct(requested), ArrowDataType::Struct(table)) => {
            let fields = requested
                .iter()
                .map(|requested| {
                    table
                        .iter()
                        .find(|table| table.name() == requested.name())
                        .map(|table| attach_field_mapping_metadata(requested, table))
                        .unwrap_or_else(|| requested.as_ref().clone())
                })
                .collect::<Vec<_>>();
            ArrowDataType::Struct(fields.into())
        }
        (ArrowDataType::List(requested), ArrowDataType::List(table)) => {
            ArrowDataType::List(Arc::new(attach_field_mapping_metadata(requested, table)))
        }
        (ArrowDataType::LargeList(requested), ArrowDataType::LargeList(table)) => {
            ArrowDataType::LargeList(Arc::new(attach_field_mapping_metadata(requested, table)))
        }
        (
            ArrowDataType::FixedSizeList(requested, requested_len),
            ArrowDataType::FixedSizeList(table, _),
        ) => ArrowDataType::FixedSizeList(
            Arc::new(attach_field_mapping_metadata(requested, table)),
            *requested_len,
        ),
        (ArrowDataType::Map(requested, sorted), ArrowDataType::Map(table, _)) => {
            ArrowDataType::Map(
                Arc::new(attach_field_mapping_metadata(requested, table)),
                *sorted,
            )
        }
        _ => requested.clone(),
    }
}

/// Get the physical name of an Arrow field under a given column mapping mode.
///
/// This is the Arrow-native equivalent of `StructField::physical_name`.
pub fn arrow_field_physical_name(field: &Field, mode: ColumnMappingMode) -> &str {
    match mode {
        ColumnMappingMode::None => field.name().as_str(),
        ColumnMappingMode::Id | ColumnMappingMode::Name => field
            .metadata()
            .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
            .map(|s| s.as_str())
            .unwrap_or_else(|| field.name().as_str()),
    }
}

/// Resolve every logical struct field path to its physical Delta column path.
pub fn logical_to_physical_arrow_paths(
    schema: &ArrowSchema,
    mode: ColumnMappingMode,
) -> HashMap<Vec<String>, Vec<String>> {
    fn add_fields(
        fields: &Fields,
        mode: ColumnMappingMode,
        logical_prefix: &[String],
        physical_prefix: &[String],
        paths: &mut HashMap<Vec<String>, Vec<String>>,
    ) {
        for field in fields {
            let mut logical_path = logical_prefix.to_vec();
            logical_path.push(field.name().clone());
            let mut physical_path = physical_prefix.to_vec();
            physical_path.push(arrow_field_physical_name(field, mode).to_string());
            paths.insert(logical_path.clone(), physical_path.clone());
            if let ArrowDataType::Struct(children) = field.data_type() {
                add_fields(children, mode, &logical_path, &physical_path, paths);
            }
        }
    }

    let mut paths = HashMap::new();
    add_fields(schema.fields(), mode, &[], &[], &mut paths);
    paths
}

pub fn restore_logical_record_batch(
    batch: &RecordBatch,
    target_schema: &SchemaRef,
    mode: ColumnMappingMode,
) -> Result<RecordBatch> {
    if batch.schema_ref() == target_schema {
        return Ok(batch.clone());
    }

    let matching = match mode {
        ColumnMappingMode::None => StructFieldMatching::Name,
        ColumnMappingMode::Name => StructFieldMatching::PhysicalName,
        ColumnMappingMode::Id => StructFieldMatching::FieldId,
    };
    let source_schema = batch.schema();
    let columns = target_schema
        .fields()
        .iter()
        .map(|target| match source_schema.index_of(target.name()) {
            Ok(index) => cast_array_with_schema_evolution_relaxed_tz(
                batch.column(index),
                target.as_ref(),
                &datafusion_common::format::DEFAULT_CAST_OPTIONS,
                matching,
            ),
            Err(_) if target.is_nullable() => {
                Ok(new_null_array(target.data_type(), batch.num_rows()))
            }
            Err(_) => Err(DataFusionError::Plan(format!(
                "missing required column '{}' while restoring Delta logical schema",
                target.name()
            ))),
        })
        .collect::<Result<Vec<_>>>()?;

    if columns.is_empty() {
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(target_schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )?)
    } else {
        Ok(RecordBatch::try_new(Arc::clone(target_schema), columns)?)
    }
}

pub(crate) fn adapt_array_to_physical_field(
    source: &ArrayRef,
    logical_field: &Field,
    physical_field: &Field,
) -> Result<ArrayRef> {
    adapt_array_to_physical_field_inner(source, logical_field, physical_field, None)
}

fn adapt_array_to_physical_field_inner(
    source: &ArrayRef,
    logical_field: &Field,
    physical_field: &Field,
    parent_nulls: Option<&NullBuffer>,
) -> Result<ArrayRef> {
    if !physical_field.is_nullable()
        && (0..source.len()).any(|index| {
            parent_nulls.is_none_or(|nulls| nulls.is_valid(index))
                && (source.data_type() == &ArrowDataType::Null || source.is_null(index))
        })
    {
        return exec_err!(
            "required field '{}' contains null values",
            logical_field.name()
        );
    }

    if source.data_type() == &ArrowDataType::Null
        || (!source.is_empty() && source.null_count() == source.len())
    {
        return Ok(new_null_array(physical_field.data_type(), source.len()));
    }

    match (logical_field.data_type(), physical_field.data_type()) {
        (ArrowDataType::Struct(logical_fields), ArrowDataType::Struct(physical_fields)) => {
            let Some(source_struct) = source.as_any().downcast_ref::<StructArray>() else {
                return exec_err!("expected struct array for field '{}'", logical_field.name());
            };
            if logical_fields.len() != physical_fields.len() {
                return exec_err!(
                    "cannot adapt struct field '{}': logical field count {} differs from physical field count {}",
                    logical_field.name(),
                    logical_fields.len(),
                    physical_fields.len()
                );
            }
            let arrays = logical_fields
                .iter()
                .zip(physical_fields)
                .map(
                    |(logical, physical)| match source_struct.column_by_name(logical.name()) {
                        Some(source) => adapt_array_to_physical_field_inner(
                            source,
                            logical.as_ref(),
                            physical.as_ref(),
                            source_struct.nulls(),
                        ),
                        None if physical.is_nullable()
                            || source_struct
                                .nulls()
                                .is_some_and(|nulls| nulls.null_count() == source_struct.len()) =>
                        {
                            Ok(new_null_array(physical.data_type(), source_struct.len()))
                        }
                        None => exec_err!(
                            "required nested field '{}' is missing from input",
                            logical.name()
                        ),
                    },
                )
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::new(
                physical_fields.clone(),
                arrays,
                source_struct.nulls().cloned(),
            )))
        }
        (ArrowDataType::List(logical), ArrowDataType::List(physical)) => {
            let Some(source_list) = source.as_any().downcast_ref::<ListArray>() else {
                return exec_err!("expected list array for field '{}'", logical_field.name());
            };
            let values =
                adapt_array_to_physical_field_inner(source_list.values(), logical, physical, None)?;
            Ok(Arc::new(ListArray::new(
                Arc::clone(physical),
                source_list.offsets().clone(),
                values,
                source_list.nulls().cloned(),
            )))
        }
        (ArrowDataType::LargeList(logical), ArrowDataType::LargeList(physical)) => {
            let Some(source_list) = source.as_any().downcast_ref::<LargeListArray>() else {
                return exec_err!(
                    "expected large list array for field '{}'",
                    logical_field.name()
                );
            };
            let values =
                adapt_array_to_physical_field_inner(source_list.values(), logical, physical, None)?;
            Ok(Arc::new(LargeListArray::new(
                Arc::clone(physical),
                source_list.offsets().clone(),
                values,
                source_list.nulls().cloned(),
            )))
        }
        (
            ArrowDataType::FixedSizeList(logical, logical_len),
            ArrowDataType::FixedSizeList(physical, physical_len),
        ) => {
            let Some(source_list) = source.as_any().downcast_ref::<FixedSizeListArray>() else {
                return exec_err!(
                    "expected fixed-size list array for field '{}'",
                    logical_field.name()
                );
            };
            if logical_len != physical_len || source_list.value_length() != *physical_len {
                return exec_err!(
                    "cannot adapt fixed-size list field '{}' from length {} to {}",
                    logical_field.name(),
                    source_list.value_length(),
                    physical_len
                );
            }
            let values =
                adapt_array_to_physical_field_inner(source_list.values(), logical, physical, None)?;
            Ok(Arc::new(FixedSizeListArray::new(
                Arc::clone(physical),
                *physical_len,
                values,
                source_list.nulls().cloned(),
            )))
        }
        (ArrowDataType::Map(logical, _), ArrowDataType::Map(physical, sorted)) => {
            let Some(source_map) = source.as_any().downcast_ref::<MapArray>() else {
                return exec_err!("expected map array for field '{}'", logical_field.name());
            };
            let (ArrowDataType::Struct(logical_entries), ArrowDataType::Struct(physical_entries)) =
                (logical.data_type(), physical.data_type())
            else {
                return exec_err!("map field '{}' has invalid entries", logical_field.name());
            };
            if logical_entries.len() != 2 || physical_entries.len() != 2 {
                return exec_err!(
                    "cannot adapt map field '{}': expected key/value entries, got {} logical and {} physical entries",
                    logical_field.name(),
                    logical_entries.len(),
                    physical_entries.len()
                );
            }
            let arrays = physical_entries
                .iter()
                .map(|physical| {
                    if !matches!(physical.name().as_str(), "key" | "value") {
                        return exec_err!(
                            "map field '{}' has unexpected physical entry '{}'",
                            logical_field.name(),
                            physical.name()
                        );
                    }
                    let logical = logical_entries
                        .iter()
                        .find(|logical| logical.name() == physical.name())
                        .ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "map entry '{}' is missing from logical schema",
                                physical.name()
                            ))
                        })?;
                    let source = source_map
                        .entries()
                        .column_by_name(logical.name())
                        .ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "map entry '{}' is missing from input",
                                logical.name()
                            ))
                        })?;
                    adapt_array_to_physical_field_inner(
                        source,
                        logical.as_ref(),
                        physical.as_ref(),
                        source_map.entries().nulls(),
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let entries = StructArray::new(physical_entries.clone(), arrays, None);
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(physical),
                source_map.offsets().clone(),
                entries,
                source_map.nulls().cloned(),
                *sorted,
            )?))
        }
        _ => cast_array_recursively(source, physical_field.data_type()),
    }
}

fn make_physical_arrow_field(field: &Field, mode: ColumnMappingMode) -> Field {
    let physical_name_key = ColumnMetadataKey::ColumnMappingPhysicalName.as_ref();
    let field_id_key = ColumnMetadataKey::ColumnMappingId.as_ref();
    let parquet_field_id_key = ColumnMetadataKey::ParquetFieldId.as_ref();

    let mut meta = field.metadata().clone();

    let name = match mode {
        ColumnMappingMode::None => field.name().clone(),
        ColumnMappingMode::Id | ColumnMappingMode::Name => meta
            .get(physical_name_key)
            .cloned()
            .unwrap_or_else(|| field.name().clone()),
    };

    match mode {
        ColumnMappingMode::Id => {
            if let Some(fid) = meta.get(field_id_key).cloned() {
                meta.insert(parquet_field_id_key.to_string(), fid);
            }
        }
        ColumnMappingMode::Name => {
            meta.remove(field_id_key);
            meta.remove(parquet_field_id_key);
        }
        ColumnMappingMode::None => {
            meta.remove(physical_name_key);
            meta.remove(field_id_key);
            meta.remove(parquet_field_id_key);
        }
    }

    let new_dt = make_physical_arrow_data_type(field.data_type(), mode);
    Field::new(name, new_dt, field.is_nullable()).with_metadata(meta)
}

fn make_physical_arrow_data_type(dt: &ArrowDataType, mode: ColumnMappingMode) -> ArrowDataType {
    match dt {
        ArrowDataType::Struct(fields) => {
            let new_fields: Fields = fields
                .iter()
                .map(|f| Arc::new(make_physical_arrow_field(f.as_ref(), mode)))
                .collect();
            ArrowDataType::Struct(new_fields)
        }
        ArrowDataType::List(field) => ArrowDataType::List(Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(make_physical_arrow_data_type(field.data_type(), mode)),
        )),
        ArrowDataType::LargeList(field) => ArrowDataType::LargeList(Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(make_physical_arrow_data_type(field.data_type(), mode)),
        )),
        ArrowDataType::FixedSizeList(field, len) => ArrowDataType::FixedSizeList(
            Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(make_physical_arrow_data_type(field.data_type(), mode)),
            ),
            *len,
        ),
        ArrowDataType::Map(entries, sorted) => {
            let ArrowDataType::Struct(fields) = entries.data_type() else {
                return dt.clone();
            };
            let fields: Fields = fields
                .iter()
                .map(|field| {
                    Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_data_type(make_physical_arrow_data_type(field.data_type(), mode)),
                    )
                })
                .collect();
            ArrowDataType::Map(
                Arc::new(
                    entries
                        .as_ref()
                        .clone()
                        .with_data_type(ArrowDataType::Struct(fields)),
                ),
                *sorted,
            )
        }
        other => other.clone(),
    }
}

/// Build an Arrow schema from an Arrow schema, reordering partition columns to the end
/// and optionally wrapping partition column types in a dictionary type.
pub fn arrow_schema_reorder_partitions(
    schema: &ArrowSchema,
    partition_columns: &[String],
    wrap_partitions: bool,
) -> DeltaResult<SchemaRef> {
    let mut non_partition_fields: Vec<Field> = schema
        .fields()
        .iter()
        .filter(|f| !partition_columns.contains(f.name()))
        .map(|f| f.as_ref().clone())
        .collect();

    let partition_fields: Vec<Field> =
        partition_columns
            .iter()
            .map(|col| {
                let f = schema
                    .field_with_name(col)
                    .map_err(|_| DeltaTableError::missing_column(col))?;
                let corrected = if wrap_partitions {
                    wrap_partition_type(f.data_type())
                } else {
                    f.data_type().clone()
                };
                Ok(Field::new(f.name(), corrected, f.is_nullable())
                    .with_metadata(f.metadata().clone()))
            })
            .collect::<Result<Vec<Field>, DeltaTableError>>()?;

    non_partition_fields.extend(partition_fields);
    Ok(Arc::new(ArrowSchema::new(non_partition_fields)))
}

fn field_from_struct_field(field: &StructField) -> Result<Field, DeltaTableError> {
    let arrow_field: Field = Field::try_from(field)?;
    let field_type = arrow_field.data_type().clone();
    Ok(Field::new(
        field.name().to_string(),
        field_type,
        field.is_nullable(),
    ))
}

fn wrap_partition_type(data_type: &ArrowDataType) -> ArrowDataType {
    match data_type {
        ArrowDataType::Utf8
        | ArrowDataType::LargeUtf8
        | ArrowDataType::Binary
        | ArrowDataType::LargeBinary => {
            datafusion::datasource::physical_plan::wrap_partition_type_in_dict(data_type.clone())
        }
        _ => data_type.clone(),
    }
}

fn enrich_arrow_with_parquet_field_ids(
    physical_arrow: &ArrowSchema,
    logical_kernel: &StructType,
) -> DeltaResult<ArrowSchema> {
    fn enrich_data_type(
        physical: &ArrowDataType,
        logical: &DataType,
    ) -> DeltaResult<ArrowDataType> {
        match (physical, logical) {
            (ArrowDataType::Struct(physical_fields), DataType::Struct(logical_fields)) => {
                let logical_fields = logical_fields.fields().collect::<Vec<_>>();
                if physical_fields.len() != logical_fields.len() {
                    return Err(DeltaTableError::schema(format!(
                        "physical struct field count {} differs from logical field count {}",
                        physical_fields.len(),
                        logical_fields.len()
                    )));
                }
                Ok(ArrowDataType::Struct(
                    physical_fields
                        .iter()
                        .zip(logical_fields)
                        .map(|(physical, logical)| {
                            enrich_field(physical.as_ref(), logical).map(Arc::new)
                        })
                        .collect::<DeltaResult<Fields>>()?,
                ))
            }
            (ArrowDataType::List(physical), DataType::Array(logical)) => Ok(ArrowDataType::List(
                Arc::new(physical.as_ref().clone().with_data_type(enrich_data_type(
                    physical.data_type(),
                    logical.element_type(),
                )?)),
            )),
            (ArrowDataType::LargeList(physical), DataType::Array(logical)) => Ok(
                ArrowDataType::LargeList(Arc::new(physical.as_ref().clone().with_data_type(
                    enrich_data_type(physical.data_type(), logical.element_type())?,
                ))),
            ),
            (ArrowDataType::FixedSizeList(physical, len), DataType::Array(logical)) => {
                Ok(ArrowDataType::FixedSizeList(
                    Arc::new(physical.as_ref().clone().with_data_type(enrich_data_type(
                        physical.data_type(),
                        logical.element_type(),
                    )?)),
                    *len,
                ))
            }
            (ArrowDataType::Map(physical, sorted), DataType::Map(logical)) => {
                let ArrowDataType::Struct(physical_entries) = physical.data_type() else {
                    return Err(DeltaTableError::schema(
                        "physical map entries must be a struct",
                    ));
                };
                if physical_entries.len() != 2 {
                    return Err(DeltaTableError::schema(format!(
                        "physical map must contain key/value entries, got {}",
                        physical_entries.len()
                    )));
                }
                let entries = physical_entries
                    .iter()
                    .map(|entry| {
                        let logical_type = match entry.name().as_str() {
                            "key" => logical.key_type(),
                            "value" => logical.value_type(),
                            name => {
                                return Err(DeltaTableError::schema(format!(
                                    "unexpected physical map entry '{name}'"
                                )));
                            }
                        };
                        Ok(Arc::new(entry.as_ref().clone().with_data_type(
                            enrich_data_type(entry.data_type(), logical_type)?,
                        )))
                    })
                    .collect::<DeltaResult<Fields>>()?;
                Ok(ArrowDataType::Map(
                    Arc::new(
                        physical
                            .as_ref()
                            .clone()
                            .with_data_type(ArrowDataType::Struct(entries)),
                    ),
                    *sorted,
                ))
            }
            _ => Ok(physical.clone()),
        }
    }

    fn enrich_field(physical: &Field, logical: &StructField) -> DeltaResult<Field> {
        let mut metadata = physical.metadata().clone();
        if let Some(MetadataValue::Number(id)) =
            logical.get_config_value(&ColumnMetadataKey::ColumnMappingId)
        {
            metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
        }
        Ok(Field::new(
            physical.name(),
            enrich_data_type(physical.data_type(), logical.data_type())?,
            physical.is_nullable(),
        )
        .with_metadata(metadata))
    }

    let logical_fields = logical_kernel.fields().collect::<Vec<_>>();
    if physical_arrow.fields().len() != logical_fields.len() {
        return Err(DeltaTableError::schema(format!(
            "physical schema field count {} differs from logical field count {}",
            physical_arrow.fields().len(),
            logical_fields.len()
        )));
    }
    let new_fields = physical_arrow
        .fields()
        .iter()
        .zip(logical_fields)
        .map(|(physical, logical)| enrich_field(physical.as_ref(), logical))
        .collect::<DeltaResult<Vec<_>>>()?;

    Ok(ArrowSchema::new(new_fields))
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::panic)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::array::{Int64Array, NullArray};
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::record_batch::RecordBatchOptions;

    use super::*;
    use crate::spec::{ArrayType, MapType};

    fn mapped_field(name: &str, physical_name: &str, id: i64, data_type: DataType) -> StructField {
        StructField::new(name, data_type, true).with_metadata([
            ("delta.columnMapping.id", MetadataValue::Number(id)),
            (
                "delta.columnMapping.physicalName",
                MetadataValue::String(physical_name.to_string()),
            ),
        ])
    }

    #[test]
    fn explicit_schema_inherits_nested_column_mapping_metadata() {
        let physical_name_key = ColumnMetadataKey::ColumnMappingPhysicalName.as_ref();
        let table_value = Arc::new(
            Field::new("value", ArrowDataType::Int64, true).with_metadata(HashMap::from([(
                physical_name_key.to_string(),
                "col-value".to_string(),
            )])),
        );
        let table_level = Arc::new(
            Field::new(
                "level",
                ArrowDataType::Struct(vec![table_value].into()),
                true,
            )
            .with_metadata(HashMap::from([(
                physical_name_key.to_string(),
                "col-level".to_string(),
            )])),
        );
        let table = ArrowSchema::new(vec![
            Field::new(
                "payload",
                ArrowDataType::Struct(vec![table_level].into()),
                true,
            )
            .with_metadata(HashMap::from([(
                physical_name_key.to_string(),
                "col-payload".to_string(),
            )])),
        ]);
        let requested = ArrowSchema::new(vec![Field::new(
            "payload",
            ArrowDataType::Struct(
                vec![Arc::new(Field::new(
                    "level",
                    ArrowDataType::Struct(
                        vec![Arc::new(Field::new("value", ArrowDataType::Int64, true))].into(),
                    ),
                    true,
                ))]
                .into(),
            ),
            true,
        )]);

        let attached = attach_column_mapping_metadata(&requested, &table);
        let payload = attached.field_with_name("payload").expect("payload");
        assert_eq!(
            payload
                .metadata()
                .get(physical_name_key)
                .map(String::as_str),
            Some("col-payload")
        );
        let ArrowDataType::Struct(payload_fields) = payload.data_type() else {
            panic!("payload must be struct");
        };
        let level = payload_fields.first().expect("level");
        assert_eq!(
            level.metadata().get(physical_name_key).map(String::as_str),
            Some("col-level")
        );
        let ArrowDataType::Struct(level_fields) = level.data_type() else {
            panic!("level must be struct");
        };
        assert_eq!(
            level_fields[0]
                .metadata()
                .get(physical_name_key)
                .map(String::as_str),
            Some("col-value")
        );
    }

    #[test]
    fn physical_name_schema_enriches_nested_field_ids() {
        let nested = StructType::try_new(vec![mapped_field("rate", "col-rate", 2, DataType::LONG)])
            .expect("nested schema");
        let logical = StructType::try_new(vec![mapped_field(
            "details",
            "col-details",
            1,
            DataType::Struct(Box::new(nested)),
        )])
        .expect("logical schema");

        let physical =
            get_physical_arrow_schema(&logical, ColumnMappingMode::Name).expect("physical schema");
        let details = physical
            .field_with_name("col-details")
            .expect("physical details field");
        let ArrowDataType::Struct(children) = details.data_type() else {
            panic!("details must be a struct");
        };
        let rate = children.first().expect("nested rate field");

        assert_eq!(rate.name(), "col-rate");
        assert_eq!(
            rate.metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .map(String::as_str),
            Some("2")
        );
    }

    #[test]
    fn physical_schema_recurses_through_arrays_and_maps() {
        let event =
            StructType::try_new(vec![mapped_field("code", "col-code", 2, DataType::STRING)])
                .expect("event schema");
        let amount = StructType::try_new(vec![mapped_field(
            "amount",
            "col-amount",
            4,
            DataType::LONG,
        )])
        .expect("amount schema");
        let logical = StructType::try_new(vec![
            mapped_field(
                "events",
                "col-events",
                1,
                DataType::Array(Box::new(ArrayType::new(
                    DataType::Struct(Box::new(event)),
                    true,
                ))),
            ),
            mapped_field(
                "lookup",
                "col-lookup",
                3,
                DataType::Map(Box::new(MapType::new(
                    DataType::STRING,
                    DataType::Struct(Box::new(amount)),
                    true,
                ))),
            ),
        ])
        .expect("logical schema");

        let physical =
            get_physical_arrow_schema(&logical, ColumnMappingMode::Name).expect("physical schema");
        let events = physical
            .field_with_name("col-events")
            .expect("events field");
        assert_eq!(
            events.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"1".to_string())
        );
        let ArrowDataType::List(element) = events.data_type() else {
            panic!("events must be a list");
        };
        let ArrowDataType::Struct(event_fields) = element.data_type() else {
            panic!("events element must be a struct");
        };
        let code = event_fields.first().expect("code field");
        assert_eq!(code.name(), "col-code");
        assert_eq!(
            code.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"2".to_string())
        );

        let lookup = physical
            .field_with_name("col-lookup")
            .expect("lookup field");
        let ArrowDataType::Map(entries, _) = lookup.data_type() else {
            panic!("lookup must be a map");
        };
        let ArrowDataType::Struct(entries) = entries.data_type() else {
            panic!("map entries must be a struct");
        };
        let value = entries
            .iter()
            .find(|field| field.name() == "value")
            .expect("map value");
        let ArrowDataType::Struct(value_fields) = value.data_type() else {
            panic!("map value must be a struct");
        };
        let amount = value_fields.first().expect("amount field");
        assert_eq!(amount.name(), "col-amount");
        assert_eq!(
            amount.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"4".to_string())
        );
    }

    #[test]
    fn restore_missing_nullable_column_as_nulls() {
        let batch = RecordBatch::try_new_with_options(
            Arc::new(ArrowSchema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(2)),
        )
        .expect("batch");
        let target = Arc::new(ArrowSchema::new(vec![Field::new(
            "optional",
            ArrowDataType::Int64,
            true,
        )]));

        let restored = restore_logical_record_batch(&batch, &target, ColumnMappingMode::Name)
            .expect("restore");

        assert_eq!(restored.column(0).null_count(), 2);
    }

    #[test]
    fn restore_missing_required_column_fails() {
        let batch = RecordBatch::try_new_with_options(
            Arc::new(ArrowSchema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .expect("batch");
        let target = Arc::new(ArrowSchema::new(vec![Field::new(
            "required",
            ArrowDataType::Int64,
            false,
        )]));

        let error = restore_logical_record_batch(&batch, &target, ColumnMappingMode::Name)
            .expect_err("missing required column must fail");

        assert!(
            error
                .to_string()
                .contains("missing required column 'required'")
        );
    }

    #[test]
    fn parquet_field_id_enrichment_rejects_field_count_mismatch() {
        let physical = ArrowSchema::new(vec![Field::new("physical", ArrowDataType::Int64, true)]);
        let logical = StructType::try_new([
            StructField::new("first", DataType::LONG, true),
            StructField::new("second", DataType::LONG, true),
        ])
        .expect("logical schema");

        let error = enrich_arrow_with_parquet_field_ids(&physical, &logical)
            .expect_err("field count mismatch must fail");

        assert!(error.to_string().contains("field count"));
    }

    #[test]
    fn physical_adaptation_rejects_required_child_null_under_valid_parent() {
        let logical_child = Arc::new(Field::new("child", ArrowDataType::Int64, true));
        let physical_child = Arc::new(Field::new("col-child", ArrowDataType::Int64, false));
        let source = Arc::new(StructArray::new(
            vec![Arc::clone(&logical_child)].into(),
            vec![Arc::new(Int64Array::from(vec![None]))],
            None,
        )) as ArrayRef;
        let logical = Field::new(
            "parent",
            ArrowDataType::Struct(vec![logical_child].into()),
            true,
        );
        let physical = Field::new(
            "col-parent",
            ArrowDataType::Struct(vec![physical_child].into()),
            true,
        );

        let error = adapt_array_to_physical_field(&source, &logical, &physical)
            .expect_err("required child null must fail");

        assert!(
            error
                .to_string()
                .contains("required field 'child' contains null")
        );
    }

    #[test]
    fn physical_adaptation_rejects_all_null_required_field() {
        let source = Arc::new(NullArray::new(1)) as ArrayRef;
        let logical = Field::new("required", ArrowDataType::Int64, true);
        let physical = Field::new("col-required", ArrowDataType::Int64, false);

        let error = adapt_array_to_physical_field(&source, &logical, &physical)
            .expect_err("required field must reject nulls");

        assert!(
            error
                .to_string()
                .contains("required field 'required' contains null")
        );
    }

    #[test]
    fn physical_adaptation_allows_required_child_under_null_parent() {
        let logical_child = Arc::new(Field::new("child", ArrowDataType::Int64, true));
        let physical_child = Arc::new(Field::new("col-child", ArrowDataType::Int64, false));
        let source = Arc::new(StructArray::new(
            vec![Arc::clone(&logical_child)].into(),
            vec![Arc::new(Int64Array::from(vec![None]))],
            Some(NullBuffer::from(vec![false])),
        )) as ArrayRef;
        let logical = Field::new(
            "parent",
            ArrowDataType::Struct(vec![logical_child].into()),
            true,
        );
        let physical = Field::new(
            "col-parent",
            ArrowDataType::Struct(vec![physical_child].into()),
            true,
        );

        let adapted =
            adapt_array_to_physical_field(&source, &logical, &physical).expect("adaptation");

        assert_eq!(adapted.null_count(), 1);
    }
}
