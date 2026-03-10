use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, Fields, Schema as ArrowSchema, SchemaRef,
};

use crate::spec::{
    ColumnMappingMode, ColumnMetadataKey, DataType, DeltaError as DeltaTableError, DeltaResult,
    MetadataValue, StructField, StructType,
};

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

pub fn get_physical_arrow_schema(logical: &StructType, mode: ColumnMappingMode) -> ArrowSchema {
    let physical_kernel = logical.make_physical(mode);
    let physical_arrow: ArrowSchema =
        ArrowSchema::try_from(&physical_kernel).unwrap_or_else(|_| ArrowSchema::empty());
    match mode {
        ColumnMappingMode::Name | ColumnMappingMode::Id => {
            enrich_arrow_with_parquet_field_ids(&physical_arrow, logical)
        }
        ColumnMappingMode::None => physical_arrow,
    }
}

/// Apply Delta column mapping to an Arrow schema, renaming logical→physical column names.
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

pub fn enrich_arrow_with_parquet_field_ids(
    physical_arrow: &ArrowSchema,
    logical_kernel: &StructType,
) -> ArrowSchema {
    fn build_path_map(
        st: &StructType,
        path: &mut Vec<String>,
        out: &mut HashMap<Vec<String>, (Option<i64>, String)>,
    ) {
        for f in st.fields() {
            let phys = match f.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName) {
                Some(MetadataValue::String(s)) => s.clone(),
                _ => f.name().clone(),
            };
            path.push(phys);
            let id = match f.get_config_value(&ColumnMetadataKey::ColumnMappingId) {
                Some(MetadataValue::Number(fid)) => Some(*fid),
                _ => None,
            };
            out.insert(path.clone(), (id, f.name().clone()));

            match f.data_type() {
                DataType::Struct(nst) => build_path_map(nst.as_ref(), path, out),
                DataType::Array(at) => {
                    if let DataType::Struct(nst) = at.element_type() {
                        build_path_map(nst.as_ref(), path, out)
                    }
                }
                DataType::Map(mt) => {
                    if let DataType::Struct(nst) = mt.value_type() {
                        build_path_map(nst.as_ref(), path, out)
                    }
                }
                _ => {}
            }
            path.pop();
        }
    }

    fn enrich_field(
        af: &datafusion::arrow::datatypes::Field,
        path: &mut Vec<String>,
        map: &HashMap<Vec<String>, (Option<i64>, String)>,
        rename_current: bool,
    ) -> datafusion::arrow::datatypes::Field {
        let mut meta = af.metadata().clone();
        let mut new_name = af.name().clone();
        if let Some((maybe_id, logical_name)) = map.get(path) {
            if let Some(fid) = maybe_id {
                meta.insert("PARQUET:field_id".to_string(), fid.to_string());
            }
            if rename_current {
                new_name = logical_name.clone();
            }
        }

        let new_dt = match af.data_type() {
            datafusion::arrow::datatypes::DataType::Struct(children) => {
                let new_children: Vec<datafusion::arrow::datatypes::Field> = children
                    .iter()
                    .map(|child| {
                        path.push(child.name().clone());
                        let updated = enrich_field(child, path, map, true);
                        path.pop();
                        updated
                    })
                    .collect();
                datafusion::arrow::datatypes::DataType::Struct(new_children.into())
            }
            datafusion::arrow::datatypes::DataType::List(elem) => {
                let updated_elem = enrich_field(elem.as_ref(), path, map, false);
                datafusion::arrow::datatypes::DataType::List(Arc::new(updated_elem))
            }
            datafusion::arrow::datatypes::DataType::LargeList(elem) => {
                let updated_elem = enrich_field(elem.as_ref(), path, map, false);
                datafusion::arrow::datatypes::DataType::LargeList(Arc::new(updated_elem))
            }
            datafusion::arrow::datatypes::DataType::FixedSizeList(elem, len) => {
                let updated_elem = enrich_field(elem.as_ref(), path, map, false);
                datafusion::arrow::datatypes::DataType::FixedSizeList(Arc::new(updated_elem), *len)
            }
            datafusion::arrow::datatypes::DataType::Map(kv, is_sorted) => {
                let kv_struct = match kv.data_type() {
                    datafusion::arrow::datatypes::DataType::Struct(children) => children,
                    _ => {
                        return datafusion::arrow::datatypes::Field::new(
                            af.name(),
                            af.data_type().clone(),
                            af.is_nullable(),
                        )
                        .with_metadata(meta)
                    }
                };
                let mut new_kv_children: Vec<datafusion::arrow::datatypes::Field> =
                    Vec::with_capacity(2);
                for child in kv_struct.iter() {
                    if child.name() == "value" {
                        let value_dt = match child.data_type() {
                            datafusion::arrow::datatypes::DataType::Struct(value_children) => {
                                let new_value_children: Vec<datafusion::arrow::datatypes::Field> =
                                    value_children
                                        .iter()
                                        .map(|grandchild| {
                                            path.push(grandchild.name().clone());
                                            let updated = enrich_field(grandchild, path, map, true);
                                            path.pop();
                                            updated
                                        })
                                        .collect();
                                datafusion::arrow::datatypes::DataType::Struct(
                                    new_value_children.into(),
                                )
                            }
                            _ => child.data_type().clone(),
                        };
                        let updated = datafusion::arrow::datatypes::Field::new(
                            "value",
                            value_dt,
                            child.is_nullable(),
                        )
                        .with_metadata(child.metadata().clone());
                        new_kv_children.push(updated);
                    } else {
                        new_kv_children.push(child.as_ref().clone());
                    }
                }
                let new_kv = datafusion::arrow::datatypes::Field::new(
                    kv.name(),
                    datafusion::arrow::datatypes::DataType::Struct(new_kv_children.into()),
                    kv.is_nullable(),
                );
                datafusion::arrow::datatypes::DataType::Map(Arc::new(new_kv), *is_sorted)
            }
            _ => af.data_type().clone(),
        };

        datafusion::arrow::datatypes::Field::new(&new_name, new_dt, af.is_nullable())
            .with_metadata(meta)
    }

    let mut path_to_info: HashMap<Vec<String>, (Option<i64>, String)> = HashMap::new();
    build_path_map(logical_kernel, &mut Vec::new(), &mut path_to_info);

    let new_fields: Vec<datafusion::arrow::datatypes::Field> = physical_arrow
        .fields()
        .iter()
        .map(|af| {
            let mut path = vec![af.name().clone()];
            enrich_field(af, &mut path, &path_to_info, false)
        })
        .collect();

    ArrowSchema::new(new_fields)
}
