use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, Schema as ArrowSchema, SchemaRef,
};
use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use delta_kernel::schema::{
    ColumnMetadataKey, MetadataValue, StructField as KernelStructField, StructType,
};
use delta_kernel::table_features::ColumnMappingMode;

use crate::kernel::{DeltaResult, DeltaTableError};

pub fn logical_arrow_to_kernel(arrow: &ArrowSchema) -> DeltaResult<StructType> {
    Ok(arrow.try_into_kernel()?)
}

pub fn kernel_to_logical_arrow(schema: &StructType) -> ArrowSchema {
    schema
        .try_into_arrow()
        .unwrap_or_else(|_| ArrowSchema::empty())
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

pub fn get_physical_arrow_schema(logical: &StructType, mode: ColumnMappingMode) -> ArrowSchema {
    let physical_kernel = logical.make_physical(mode);
    let physical_arrow: ArrowSchema = (&physical_kernel)
        .try_into_arrow()
        .unwrap_or_else(|_| ArrowSchema::empty());
    match mode {
        ColumnMappingMode::Name | ColumnMappingMode::Id => {
            enrich_arrow_with_parquet_field_ids(&physical_arrow, logical)
        }
        ColumnMappingMode::None => physical_arrow,
    }
}

fn field_from_struct_field(field: &KernelStructField) -> Result<Field, DeltaTableError> {
    let arrow_field: Field = field.try_into_arrow()?;
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
                delta_kernel::schema::DataType::Struct(nst) => {
                    build_path_map(nst.as_ref(), path, out)
                }
                delta_kernel::schema::DataType::Array(at) => {
                    if let delta_kernel::schema::DataType::Struct(nst) = at.element_type() {
                        build_path_map(nst.as_ref(), path, out)
                    }
                }
                delta_kernel::schema::DataType::Map(mt) => {
                    if let delta_kernel::schema::DataType::Struct(nst) = mt.value_type() {
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
