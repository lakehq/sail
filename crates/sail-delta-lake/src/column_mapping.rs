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

    match annotated.data_type() {
        DataType::Struct(struct_type) => {
            let nested = annotate_struct(struct_type.as_ref(), counter);
            StructField {
                name: annotated.name().clone(),
                data_type: nested.into(),
                nullable: annotated.is_nullable(),
                metadata: annotated.metadata().clone(),
            }
        }
        DataType::Array(array_type) => {
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
        _ => annotated,
    }
}

// TODO: Consider inlining the struct-field iteration into the DataType::Struct branch
// of annotate_field and removing this helper to reduce indirection.
fn annotate_struct(struct_type: &StructType, counter: &AtomicI64) -> StructType {
    let fields = struct_type
        .fields()
        .map(|f| -> Result<StructField, delta_kernel::Error> { Ok(annotate_field(f, counter)) });
    #[allow(clippy::expect_used)]
    let result = StructType::try_new(fields).expect("failed to build nested annotated struct");
    result
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

/// Annotate only new fields (compared to `existing`) within `candidate` with
/// column mapping metadata, starting from `start_id` (inclusive).
/// Returns the updated schema and the last used id.
pub fn annotate_new_fields_for_column_mapping(
    existing: &StructType,
    candidate: &StructType,
    start_id: i64,
) -> (StructType, i64) {
    let counter = AtomicI64::new(start_id);

    fn find_child<'a>(st: &'a StructType, name: &str) -> Option<&'a StructField> {
        st.fields().find(|f| f.name() == name)
    }

    // TODO: Consider extracting a generic recursive type merger (e.g.,
    // merge_types(existing: Option<&DataType>, new: &DataType, ...)) to reduce
    // nested matches for Array/Map/Struct while preserving field-level metadata.
    fn merge_datatype(
        existing_field: Option<&StructField>,
        new_field: &StructField,
        counter: &AtomicI64,
    ) -> (StructField, ()) {
        match (existing_field, new_field.data_type()) {
            (Some(prev), DataType::Struct(new_st)) => {
                // Merge nested struct: keep existing field metadata, adopt new nullability, and annotate only new nested columns
                let prev_st = match prev.data_type() {
                    DataType::Struct(s) => Some(s),
                    _ => None,
                };
                let merged_struct = if let Some(prev_st) = prev_st {
                    merge_struct(prev_st.as_ref(), new_st.as_ref(), counter)
                } else {
                    // Structure type changed to Struct: treat all nested fields as new
                    annotate_struct(new_st.as_ref(), counter)
                };
                (
                    StructField {
                        name: prev.name().clone(),
                        data_type: DataType::Struct(Box::new(merged_struct)),
                        nullable: new_field.is_nullable(),
                        metadata: prev.metadata().clone(),
                    },
                    (),
                )
            }
            (Some(prev), DataType::Array(new_arr)) => {
                // If element is struct, merge nested; else keep as-is
                let new_elem = match new_arr.element_type() {
                    DataType::Struct(st) => {
                        let prev_elem_struct = match prev.data_type() {
                            DataType::Array(prev_arr) => match prev_arr.element_type() {
                                DataType::Struct(pst) => Some(pst),
                                _ => None,
                            },
                            _ => None,
                        };
                        let merged = if let Some(pst) = prev_elem_struct {
                            merge_struct(pst.as_ref(), st.as_ref(), counter)
                        } else {
                            annotate_struct(st.as_ref(), counter)
                        };
                        DataType::Struct(Box::new(merged))
                    }
                    other => other.clone(),
                };
                (
                    StructField {
                        name: prev.name().clone(),
                        data_type: ArrayType::new(new_elem, new_arr.contains_null()).into(),
                        nullable: new_field.is_nullable(),
                        metadata: prev.metadata().clone(),
                    },
                    (),
                )
            }
            (Some(prev), DataType::Map(new_map)) => {
                // Merge nested key/value if struct
                let new_key = match new_map.key_type() {
                    DataType::Struct(st) => {
                        let prev_key_struct = match prev.data_type() {
                            DataType::Map(pm) => match pm.key_type() {
                                DataType::Struct(pst) => Some(pst),
                                _ => None,
                            },
                            _ => None,
                        };
                        let merged = if let Some(pst) = prev_key_struct {
                            merge_struct(pst.as_ref(), st.as_ref(), counter)
                        } else {
                            annotate_struct(st.as_ref(), counter)
                        };
                        DataType::Struct(Box::new(merged))
                    }
                    other => other.clone(),
                };
                let new_val = match new_map.value_type() {
                    DataType::Struct(st) => {
                        let prev_val_struct = match prev.data_type() {
                            DataType::Map(pm) => match pm.value_type() {
                                DataType::Struct(pst) => Some(pst),
                                _ => None,
                            },
                            _ => None,
                        };
                        let merged = if let Some(pst) = prev_val_struct {
                            merge_struct(pst.as_ref(), st.as_ref(), counter)
                        } else {
                            annotate_struct(st.as_ref(), counter)
                        };
                        DataType::Struct(Box::new(merged))
                    }
                    other => other.clone(),
                };
                (
                    StructField {
                        name: prev.name().clone(),
                        data_type: MapType::new(new_key, new_val, new_map.value_contains_null())
                            .into(),
                        nullable: new_field.is_nullable(),
                        metadata: prev.metadata().clone(),
                    },
                    (),
                )
            }
            (Some(prev), _) => {
                // Primitive or non-struct element: keep existing metadata, adopt new nullability and data_type
                (
                    StructField {
                        name: prev.name().clone(),
                        data_type: new_field.data_type().clone(),
                        nullable: new_field.is_nullable(),
                        metadata: prev.metadata().clone(),
                    },
                    (),
                )
            }
            (None, _) => {
                // Brand new field: annotate fully
                (annotate_field(new_field, counter), ())
            }
        }
    }

    fn merge_struct(
        existing: &StructType,
        candidate: &StructType,
        counter: &AtomicI64,
    ) -> StructType {
        let merged_fields = candidate.fields().map(|nf| {
            let prev = find_child(existing, nf.name());
            let (f, _) = merge_datatype(prev, nf, counter);
            Ok::<StructField, delta_kernel::Error>(f)
        });
        #[allow(clippy::expect_used)]
        StructType::try_new(merged_fields).expect("failed to build merged annotated struct")
    }

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
