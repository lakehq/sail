// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use delta_kernel::schema::{
    ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField, StructType,
};

/// Annotate a logical kernel schema with column mapping metadata (id + physicalName)
/// using a sequential id assignment. Intended only for new table creation (name mode).
pub fn annotate_schema_for_column_mapping(schema: &StructType) -> StructType {
    let counter = AtomicI64::new(1);
    let annotated_fields: Vec<StructField> = schema
        .fields()
        .map(|f| annotate_field(f, &counter))
        .collect();
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

/// Recursively annotate all nested struct types within a data type.
/// Used when there is no existing type to merge with.
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
        // Use helper function for all None cases
        (None, new_type) => annotate_nested_type(new_type, counter),
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

/// Enrich a physical Arrow schema with PARQUET:field_id metadata for each field whose
/// logical field in the kernel schema carries a `delta.columnMapping.id`.
///
/// This is needed so Parquet readers can resolve columns by field id when names are
/// physical (e.g., `col-<uuid>`) under column mapping Name/Id modes.
pub fn enrich_arrow_with_parquet_field_ids(
    physical_arrow: &ArrowSchema,
    logical_kernel: &StructType,
) -> ArrowSchema {
    // Build recursive mapping: physical path -> (Option<id>, logical_name)
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

    let mut path_to_info: HashMap<Vec<String>, (Option<i64>, String)> = HashMap::new();
    build_path_map(logical_kernel, &mut Vec::new(), &mut path_to_info);

    // Recursively enrich Arrow fields with PARQUET:field_id
    fn enrich_field(
        af: &ArrowField,
        path: &mut Vec<String>,
        map: &HashMap<Vec<String>, (Option<i64>, String)>,
        rename_current: bool,
    ) -> ArrowField {
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
                let new_children: Vec<ArrowField> = children
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
                // Do not include wrapper (element/item) in path; preserve wrapper name
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
                // Descend into value field only; key is never a struct in Spark scenarios
                let kv_struct = match kv.data_type() {
                    datafusion::arrow::datatypes::DataType::Struct(children) => children,
                    _ => {
                        return ArrowField::new(af.name(), af.data_type().clone(), af.is_nullable())
                            .with_metadata(meta)
                    }
                };
                let mut new_kv_children: Vec<ArrowField> = Vec::with_capacity(2);
                for child in kv_struct.iter() {
                    if child.name() == "value" {
                        // Do not include "value" in path; preserve wrapper name and enrich its children
                        let value_dt = match child.data_type() {
                            datafusion::arrow::datatypes::DataType::Struct(value_children) => {
                                let new_value_children: Vec<ArrowField> = value_children
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
                        let updated = ArrowField::new("value", value_dt, child.is_nullable())
                            .with_metadata(child.metadata().clone());
                        new_kv_children.push(updated);
                    } else {
                        new_kv_children.push(child.as_ref().clone());
                    }
                }
                let new_kv = ArrowField::new(
                    kv.name(),
                    datafusion::arrow::datatypes::DataType::Struct(new_kv_children.into()),
                    kv.is_nullable(),
                );
                datafusion::arrow::datatypes::DataType::Map(Arc::new(new_kv), *is_sorted)
            }
            _ => af.data_type().clone(),
        };

        ArrowField::new(&new_name, new_dt, af.is_nullable()).with_metadata(meta)
    }

    let new_fields: Vec<ArrowField> = physical_arrow
        .fields()
        .iter()
        .map(|af| {
            let mut path = vec![af.name().clone()];
            enrich_field(af, &mut path, &path_to_info, false)
        })
        .collect();

    ArrowSchema::new(new_fields)
}
