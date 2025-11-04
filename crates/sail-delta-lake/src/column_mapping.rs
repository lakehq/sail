use std::sync::atomic::{AtomicI64, Ordering};

use datafusion::arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use delta_kernel::schema::{ArrayType, DataType, MapType, MetadataValue, StructField, StructType};
use delta_kernel::table_features::ColumnMappingMode;

use crate::options::ColumnMappingModeOption;

/// Annotate a logical kernel schema with column mapping metadata (id + physicalName)
/// using a sequential id assignment. Intended only for new table creation (name mode).
pub fn annotate_schema_for_column_mapping(schema: &StructType) -> StructType {
    let counter = AtomicI64::new(1);
    let annotated_fields = schema
        .fields()
        .map(|f| -> Result<StructField, delta_kernel::Error> { Ok(annotate_field(f, &counter)) });
    // Safe: we preserve existing names and structure
    #[allow(clippy::expect_used)]
    let result = StructType::try_new(annotated_fields).expect("failed to build annotated schema");
    result
}

fn annotate_field(field: &StructField, counter: &AtomicI64) -> StructField {
    // Assign column mapping metadata once per field
    let next_id = counter.fetch_add(1, Ordering::Relaxed);
    let physical_name = format!("col-{}", uuid::Uuid::new_v4());
    let annotated = field.clone().add_metadata([
        ("delta.columnMapping.id", MetadataValue::Number(next_id)),
        (
            "delta.columnMapping.physicalName",
            MetadataValue::String(physical_name),
        ),
    ]);

    let new_data_type = merge_types(None, annotated.data_type(), counter);
    StructField {
        name: annotated.name().clone(),
        data_type: new_data_type,
        nullable: annotated.is_nullable(),
        metadata: annotated.metadata().clone(),
    }
}

/// Merge `new_type` into `existing_type` while preserving metadata on existing fields
/// and annotating only newly introduced nested fields. When `existing_type` is None,
/// annotate nested struct parts as needed. Used both for full-field annotation and
/// for schema evolution of nested types.
fn merge_types(
    existing_type: Option<&DataType>,
    new_type: &DataType,
    counter: &AtomicI64,
) -> DataType {
    match (existing_type, new_type) {
        (Some(DataType::Struct(prev_st)), DataType::Struct(new_st)) => DataType::Struct(Box::new(
            merge_struct(prev_st.as_ref(), new_st.as_ref(), counter),
        )),
        (None, DataType::Struct(new_st)) => {
            let fields = new_st
                .fields()
                .map(|f| -> Result<StructField, delta_kernel::Error> {
                    Ok(annotate_field(f, counter))
                });
            #[allow(clippy::expect_used)]
            let result =
                StructType::try_new(fields).expect("failed to build nested annotated struct");
            DataType::Struct(Box::new(result))
        }
        (Some(DataType::Array(prev_arr)), DataType::Array(new_arr)) => {
            let merged_elem = merge_types(
                Some(prev_arr.element_type()),
                new_arr.element_type(),
                counter,
            );
            DataType::Array(Box::new(ArrayType::new(
                merged_elem,
                new_arr.contains_null(),
            )))
        }
        (None, DataType::Array(new_arr)) => {
            let new_elem = match new_arr.element_type() {
                DataType::Struct(st) => {
                    let fields = st
                        .fields()
                        .map(|f| -> Result<StructField, delta_kernel::Error> {
                            Ok(annotate_field(f, counter))
                        });
                    #[allow(clippy::expect_used)]
                    let result = StructType::try_new(fields)
                        .expect("failed to build nested annotated struct");
                    DataType::Struct(Box::new(result))
                }
                other => other.clone(),
            };
            DataType::Array(Box::new(ArrayType::new(new_elem, new_arr.contains_null())))
        }
        (Some(DataType::Map(prev_map)), DataType::Map(new_map)) => {
            let new_key = merge_types(Some(prev_map.key_type()), new_map.key_type(), counter);
            let new_value = merge_types(Some(prev_map.value_type()), new_map.value_type(), counter);
            DataType::Map(Box::new(MapType::new(
                new_key,
                new_value,
                new_map.value_contains_null(),
            )))
        }
        (None, DataType::Map(new_map)) => {
            let new_key = match new_map.key_type() {
                DataType::Struct(st) => {
                    let fields = st
                        .fields()
                        .map(|f| -> Result<StructField, delta_kernel::Error> {
                            Ok(annotate_field(f, counter))
                        });
                    #[allow(clippy::expect_used)]
                    let result = StructType::try_new(fields)
                        .expect("failed to build nested annotated struct");
                    DataType::Struct(Box::new(result))
                }
                other => other.clone(),
            };
            let new_value = match new_map.value_type() {
                DataType::Struct(st) => {
                    let fields = st
                        .fields()
                        .map(|f| -> Result<StructField, delta_kernel::Error> {
                            Ok(annotate_field(f, counter))
                        });
                    #[allow(clippy::expect_used)]
                    let result = StructType::try_new(fields)
                        .expect("failed to build nested annotated struct");
                    DataType::Struct(Box::new(result))
                }
                other => other.clone(),
            };
            DataType::Map(Box::new(MapType::new(
                new_key,
                new_value,
                new_map.value_contains_null(),
            )))
        }
        (_, _) => new_type.clone(),
    }
}

/// Compute the maximum `delta.columnMapping.id` present in a logical kernel schema.
pub fn compute_max_column_id(schema: &StructType) -> i64 {
    fn max_in_field(field: &StructField) -> i64 {
        let mut max_id = field
            .metadata()
            .get("delta.columnMapping.id")
            .and_then(|v| match v {
                MetadataValue::Number(n) => Some(*n),
                _ => None,
            })
            .unwrap_or_default();

        match field.data_type() {
            DataType::Struct(st) => {
                for f in st.fields() {
                    max_id = max_id.max(max_in_field(f));
                }
            }
            DataType::Array(at) => {
                if let DataType::Struct(st) = at.element_type() {
                    for f in st.fields() {
                        max_id = max_id.max(max_in_field(f));
                    }
                }
            }
            DataType::Map(mt) => {
                if let DataType::Struct(st) = mt.key_type() {
                    for f in st.fields() {
                        max_id = max_id.max(max_in_field(f));
                    }
                }
                if let DataType::Struct(st) = mt.value_type() {
                    for f in st.fields() {
                        max_id = max_id.max(max_in_field(f));
                    }
                }
            }
            _ => {}
        }

        max_id
    }

    let mut max_id = 0i64;
    for f in schema.fields() {
        max_id = max_id.max(max_in_field(f));
    }
    max_id
}

fn merge_struct(existing: &StructType, candidate: &StructType, counter: &AtomicI64) -> StructType {
    let merged_fields = candidate.fields().map(|nf| {
        let prev = existing.fields().find(|f| f.name() == nf.name());
        let field = if let Some(prev_field) = prev {
            let merged_dtype = merge_types(Some(prev_field.data_type()), nf.data_type(), counter);
            StructField {
                name: prev_field.name().clone(),
                data_type: merged_dtype,
                nullable: nf.is_nullable(),
                metadata: prev_field.metadata().clone(),
            }
        } else {
            annotate_field(nf, counter)
        };
        Ok::<StructField, delta_kernel::Error>(field)
    });
    #[allow(clippy::expect_used)]
    StructType::try_new(merged_fields).expect("failed to build merged annotated struct")
}

/// Annotate only new fields (compared to `existing`) within `candidate` with
/// column mapping metadata, starting from `start_id` (inclusive).
/// Returns the updated schema and the last used id.
pub fn annotate_new_fields_for_column_mapping(
    existing: &StructType,
    candidate: &StructType,
    start_id: i64,
) -> (StructType, i64) {
    let counter = AtomicI64::new(start_id);

    let merged = merge_struct(existing, candidate, &counter);
    let last = counter.load(Ordering::Relaxed);
    // If we started at N and annotated K fields, counter now is N+K.
    // We return last_used = max(last-1, start_id-1) to represent the last assigned id.
    let last_used = if last > start_id {
        last - 1
    } else {
        start_id - 1
    };
    (merged, last_used)
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

/// Enrich a physical Arrow schema with PARQUET:field_id metadata for each field whose
/// logical field in the kernel schema carries a `delta.columnMapping.id`.
///
/// This is needed so Parquet readers can resolve columns by field id when names are
/// physical (e.g., `col-<uuid>`) under column mapping Name/Id modes.
pub fn enrich_arrow_with_parquet_field_ids(
    physical_arrow: &ArrowSchema,
    logical_kernel: &StructType,
) -> ArrowSchema {
    use delta_kernel::schema::ColumnMetadataKey;

    // Build map: physical_name -> Option<id>
    let mut physical_to_id: std::collections::HashMap<String, Option<i64>> =
        std::collections::HashMap::new();
    for kf in logical_kernel.fields() {
        let id = match kf.get_config_value(&ColumnMetadataKey::ColumnMappingId) {
            Some(MetadataValue::Number(fid)) => Some(*fid),
            _ => None,
        };
        let phys = match kf.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName) {
            Some(MetadataValue::String(s)) => s.clone(),
            _ => kf.name().clone(),
        };
        physical_to_id.insert(phys, id);
    }

    let new_fields: Vec<ArrowField> = physical_arrow
        .fields()
        .iter()
        .map(|af| {
            let mut meta = af.metadata().clone();
            if let Some(Some(fid)) = physical_to_id.get(af.name()) {
                meta.insert("PARQUET:field_id".to_string(), fid.to_string());
            }
            ArrowField::new(af.name(), af.data_type().clone(), af.is_nullable()).with_metadata(meta)
        })
        .collect();

    ArrowSchema::new(new_fields)
}
