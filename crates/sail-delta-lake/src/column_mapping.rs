use std::sync::atomic::{AtomicI64, Ordering};

use crate::options::ColumnMappingModeOption;
use delta_kernel::schema::{ArrayType, DataType, MapType, MetadataValue, StructField, StructType};
use delta_kernel::table_features::ColumnMappingMode;

/// Annotate a logical kernel schema with column mapping metadata (id + physicalName)
/// using a sequential id assignment. Intended only for new table creation (name mode).
pub fn annotate_schema_for_column_mapping(schema: &StructType) -> StructType {
    let counter = AtomicI64::new(1);
    let annotated_fields = schema
        .fields()
        .map(|f| -> Result<StructField, delta_kernel::Error> { Ok(annotate_field(f, &counter)) });
    // Safe: we preserve existing names and structure
    StructType::try_new(annotated_fields).expect("failed to build annotated schema")
}

fn annotate_field(field: &StructField, counter: &AtomicI64) -> StructField {
    match field.data_type() {
        DataType::Struct(struct_type) => {
            let next_id = counter.fetch_add(1, Ordering::Relaxed);
            let physical_name = format!("col-{}", uuid::Uuid::new_v4());
            let annotated = field.clone().add_metadata([
                ("delta.columnMapping.id", MetadataValue::Number(next_id)),
                (
                    "delta.columnMapping.physicalName",
                    MetadataValue::String(physical_name),
                ),
            ]);
            let nested = annotate_struct(struct_type.as_ref(), counter);
            StructField {
                name: annotated.name().clone(),
                data_type: nested.into(),
                nullable: annotated.is_nullable(),
                metadata: annotated.metadata().clone(),
            }
        }
        DataType::Array(array_type) => {
            let next_id = counter.fetch_add(1, Ordering::Relaxed);
            let physical_name = format!("col-{}", uuid::Uuid::new_v4());
            let annotated = field.clone().add_metadata([
                ("delta.columnMapping.id", MetadataValue::Number(next_id)),
                (
                    "delta.columnMapping.physicalName",
                    MetadataValue::String(physical_name),
                ),
            ]);
            let new_element = match array_type.element_type() {
                DataType::Struct(st) => annotate_struct(st.as_ref(), counter).into(),
                other => other.clone(),
            };
            StructField {
                name: annotated.name().clone(),
                data_type: ArrayType::new(new_element, array_type.contains_null()).into(),
                nullable: annotated.is_nullable(),
                metadata: annotated.metadata().clone(),
            }
        }
        DataType::Map(map_type) => {
            let next_id = counter.fetch_add(1, Ordering::Relaxed);
            let physical_name = format!("col-{}", uuid::Uuid::new_v4());
            let annotated = field.clone().add_metadata([
                ("delta.columnMapping.id", MetadataValue::Number(next_id)),
                (
                    "delta.columnMapping.physicalName",
                    MetadataValue::String(physical_name),
                ),
            ]);
            let new_key = match map_type.key_type() {
                DataType::Struct(st) => annotate_struct(st.as_ref(), counter).into(),
                other => other.clone(),
            };
            let new_value = match map_type.value_type() {
                DataType::Struct(st) => annotate_struct(st.as_ref(), counter).into(),
                other => other.clone(),
            };
            StructField {
                name: annotated.name().clone(),
                data_type: MapType::new(new_key, new_value, map_type.value_contains_null()).into(),
                nullable: annotated.is_nullable(),
                metadata: annotated.metadata().clone(),
            }
        }
        _ => {
            let next_id = counter.fetch_add(1, Ordering::Relaxed);
            let physical_name = format!("col-{}", uuid::Uuid::new_v4());
            field.clone().add_metadata([
                ("delta.columnMapping.id", MetadataValue::Number(next_id)),
                (
                    "delta.columnMapping.physicalName",
                    MetadataValue::String(physical_name),
                ),
            ])
        }
    }
}

fn annotate_struct(struct_type: &StructType, counter: &AtomicI64) -> StructType {
    let fields = struct_type
        .fields()
        .map(|f| -> Result<StructField, delta_kernel::Error> { Ok(annotate_field(f, counter)) });
    StructType::try_new(fields).expect("failed to build nested annotated struct")
}

/// Build the physical schema used for file writes according to the column mapping mode.
/// - None: return unchanged.
/// - Name: rename fields to physicalName, remove id/parquet id metadata.
/// - Id: rename fields to physicalName, set parquet.field.id from delta.columnMapping.id.
pub fn make_physical_schema_for_writes(
    logical_schema: &StructType,
    mode: ColumnMappingModeOption,
) -> StructType {
    let kernel_mode = match mode {
        ColumnMappingModeOption::None => ColumnMappingMode::None,
        ColumnMappingModeOption::Name => ColumnMappingMode::Name,
        ColumnMappingModeOption::Id => ColumnMappingMode::Id,
    };
    logical_schema.make_physical(kernel_mode)
}
