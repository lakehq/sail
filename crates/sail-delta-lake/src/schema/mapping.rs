use std::sync::atomic::{AtomicI64, Ordering};

use delta_kernel::schema::{ArrayType, DataType, MapType, MetadataValue, StructField, StructType};

/// Annotate a logical kernel schema with column mapping metadata (id + physicalName)
/// using a sequential id assignment. Intended only for new table creation (name mode).
pub fn annotate_schema_for_column_mapping(schema: &StructType) -> StructType {
    let counter = AtomicI64::new(1);
    let annotated_fields: Vec<StructField> = schema
        .fields()
        .map(|f| annotate_field(f, &counter))
        .collect();
    #[allow(clippy::expect_used)]
    StructType::try_new(annotated_fields).expect("failed to build annotated schema")
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
    let last_used = if last > start_id {
        last - 1
    } else {
        start_id - 1
    };
    (merged, last_used)
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

fn annotate_field(field: &StructField, counter: &AtomicI64) -> StructField {
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

fn annotate_nested_type(data_type: &DataType, counter: &AtomicI64) -> DataType {
    match data_type {
        DataType::Struct(st) => {
            let fields: Vec<StructField> =
                st.fields().map(|f| annotate_field(f, counter)).collect();
            #[allow(clippy::expect_used)]
            let result =
                StructType::try_new(fields).expect("failed to build nested annotated struct");
            DataType::Struct(Box::new(result))
        }
        DataType::Array(at) => {
            let new_elem = annotate_nested_type(at.element_type(), counter);
            DataType::Array(Box::new(ArrayType::new(new_elem, at.contains_null())))
        }
        DataType::Map(mt) => {
            let new_key = annotate_nested_type(mt.key_type(), counter);
            let new_value = annotate_nested_type(mt.value_type(), counter);
            DataType::Map(Box::new(MapType::new(
                new_key,
                new_value,
                mt.value_contains_null(),
            )))
        }
        other => other.clone(),
    }
}

fn merge_types(
    existing_type: Option<&DataType>,
    new_type: &DataType,
    counter: &AtomicI64,
) -> DataType {
    match (existing_type, new_type) {
        (Some(DataType::Struct(prev_st)), DataType::Struct(new_st)) => DataType::Struct(Box::new(
            merge_struct(prev_st.as_ref(), new_st.as_ref(), counter),
        )),
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
        (Some(DataType::Map(prev_map)), DataType::Map(new_map)) => {
            let new_key = merge_types(Some(prev_map.key_type()), new_map.key_type(), counter);
            let new_value = merge_types(Some(prev_map.value_type()), new_map.value_type(), counter);
            DataType::Map(Box::new(MapType::new(
                new_key,
                new_value,
                new_map.value_contains_null(),
            )))
        }
        (None, new_type) => annotate_nested_type(new_type, counter),
        (_, _) => new_type.clone(),
    }
}

fn merge_struct(existing: &StructType, candidate: &StructType, counter: &AtomicI64) -> StructType {
    let merged_fields: Vec<StructField> = candidate
        .fields()
        .map(|nf| {
            let prev = existing.fields().find(|f| f.name() == nf.name());
            if let Some(prev_field) = prev {
                let merged_dtype =
                    merge_types(Some(prev_field.data_type()), nf.data_type(), counter);
                StructField {
                    name: prev_field.name().clone(),
                    data_type: merged_dtype,
                    nullable: nf.is_nullable(),
                    metadata: prev_field.metadata().clone(),
                }
            } else {
                annotate_field(nf, counter)
            }
        })
        .collect();
    #[allow(clippy::expect_used)]
    StructType::try_new(merged_fields).expect("failed to build merged annotated struct")
}
