// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use datafusion::arrow::error::ArrowError;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::schema::{normalize_delta_schema, strip_column_mapping_metadata};
use crate::snapshot::DeltaSnapshot;
use crate::spec::{
    ArrayType, ColumnMappingMode, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField,
    StructType,
};

/// Annotate a logical kernel schema with column mapping metadata (id + physicalName)
/// using a sequential id assignment. Intended only for new table creation (name mode).
pub fn annotate_schema_for_column_mapping(schema: &StructType) -> StructType {
    let counter = AtomicI64::new(1);
    let annotated_fields: Vec<StructField> = schema
        .fields()
        .map(|f| annotate_field(f, &counter))
        .collect();
    StructType::try_new(annotated_fields).unwrap_or_else(|_| empty_struct_type())
}

/// Annotate the Arrow schema of a new table with the column mapping metadata that
/// `annotate_schema_for_column_mapping` assigns, keeping the Arrow types. Any column
/// mapping metadata of the input is replaced.
///
/// A table that is created before its data is written gets the column mapping of this
/// schema, and the data is written with the same schema, so the physical names of the
/// table and of the data files are the same.
pub fn annotate_arrow_schema_for_column_mapping(
    schema: &ArrowSchema,
) -> Result<ArrowSchema, ArrowError> {
    let schema = strip_column_mapping_metadata(&Arc::new(schema.clone()));
    let annotated =
        annotate_schema_for_column_mapping(&StructType::try_from(normalize_delta_schema(&schema))?);
    let fields = schema
        .fields()
        .iter()
        .zip(annotated.fields())
        .map(|(field, annotated)| with_column_mapping_of(field, annotated))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ArrowSchema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    ))
}

fn with_column_mapping_of(
    field: &ArrowField,
    annotated: &StructField,
) -> Result<ArrowField, ArrowError> {
    let mut metadata = field.metadata().clone();
    for key in [
        ColumnMetadataKey::ColumnMappingId,
        ColumnMetadataKey::ColumnMappingPhysicalName,
    ] {
        let value = match annotated.get_config_value(&key) {
            Some(MetadataValue::String(value)) => value.clone(),
            Some(value) => {
                serde_json::to_string(value).map_err(|e| ArrowError::JsonError(e.to_string()))?
            }
            None => continue,
        };
        metadata.insert(key.as_ref().to_string(), value);
    }
    let data_type = with_nested_column_mapping_of(field.data_type(), annotated.data_type())?;
    Ok(field
        .clone()
        .with_data_type(data_type)
        .with_metadata(metadata))
}

fn with_nested_column_mapping_of(
    data_type: &ArrowDataType,
    annotated: &DataType,
) -> Result<ArrowDataType, ArrowError> {
    let element = |item: &Arc<ArrowField>, annotated: &DataType| {
        with_nested_column_mapping_of(item.data_type(), annotated)
            .map(|data_type| Arc::new(item.as_ref().clone().with_data_type(data_type)))
    };
    Ok(match (data_type, annotated) {
        (ArrowDataType::Struct(fields), DataType::Struct(annotated)) => ArrowDataType::Struct(
            fields
                .iter()
                .zip(annotated.fields())
                .map(|(field, annotated)| with_column_mapping_of(field, annotated).map(Arc::new))
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        ),
        (ArrowDataType::List(item), DataType::Array(annotated)) => {
            ArrowDataType::List(element(item, annotated.element_type())?)
        }
        (ArrowDataType::LargeList(item), DataType::Array(annotated)) => {
            ArrowDataType::LargeList(element(item, annotated.element_type())?)
        }
        (ArrowDataType::ListView(item), DataType::Array(annotated)) => {
            ArrowDataType::ListView(element(item, annotated.element_type())?)
        }
        (ArrowDataType::LargeListView(item), DataType::Array(annotated)) => {
            ArrowDataType::LargeListView(element(item, annotated.element_type())?)
        }
        (ArrowDataType::FixedSizeList(item, size), DataType::Array(annotated)) => {
            ArrowDataType::FixedSizeList(element(item, annotated.element_type())?, *size)
        }
        (ArrowDataType::Map(entries, sorted), DataType::Map(annotated)) => {
            let ArrowDataType::Struct(key_value) = entries.data_type() else {
                return Ok(data_type.clone());
            };
            let [key, value] = &key_value[..] else {
                return Ok(data_type.clone());
            };
            let key_value = vec![
                element(key, annotated.key_type())?,
                element(value, annotated.value_type())?,
            ];
            ArrowDataType::Map(
                Arc::new(
                    entries
                        .as_ref()
                        .clone()
                        .with_data_type(ArrowDataType::Struct(key_value.into())),
                ),
                *sorted,
            )
        }
        _ => data_type.clone(),
    })
}

/// Returns the schema with only the column mapping metadata (id + physicalName) of the
/// fields, so that merging it with `annotate_new_fields_for_column_mapping` adds the
/// column mapping and nothing else.
pub fn column_mapping_metadata_only(schema: &StructType) -> StructType {
    retain_field_metadata(schema, &|key| {
        key == ColumnMetadataKey::ColumnMappingId.as_ref()
            || key == ColumnMetadataKey::ColumnMappingPhysicalName.as_ref()
    })
}

/// Returns the schema without the column mapping metadata of the fields, including the
/// Parquet field IDs.
pub fn without_column_mapping_metadata(schema: &StructType) -> StructType {
    retain_field_metadata(schema, &|key| {
        !key.starts_with("delta.columnMapping.")
            && key != PARQUET_FIELD_ID_META_KEY
            && key != ColumnMetadataKey::ParquetFieldId.as_ref()
            && key != "parquet.field.nested.ids"
    })
}

fn retain_field_metadata(schema: &StructType, keep: &dyn Fn(&str) -> bool) -> StructType {
    fn map_field(field: &StructField, keep: &dyn Fn(&str) -> bool) -> StructField {
        StructField {
            name: field.name().clone(),
            data_type: map_type(field.data_type(), keep),
            nullable: field.is_nullable(),
            metadata: field
                .metadata()
                .iter()
                .filter(|(key, _)| keep(key))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        }
    }
    fn map_type(data_type: &DataType, keep: &dyn Fn(&str) -> bool) -> DataType {
        match data_type {
            DataType::Struct(st) => DataType::Struct(Box::new(
                StructType::try_new(st.fields().map(|field| map_field(field, keep)))
                    .unwrap_or_else(|_| empty_struct_type()),
            )),
            DataType::Array(at) => DataType::Array(Box::new(ArrayType::new(
                map_type(at.element_type(), keep),
                at.contains_null(),
            ))),
            DataType::Map(mt) => DataType::Map(Box::new(MapType::new(
                map_type(mt.key_type(), keep),
                map_type(mt.value_type(), keep),
                mt.value_contains_null(),
            ))),
            other => other.clone(),
        }
    }
    StructType::try_new(schema.fields().map(|field| map_field(field, keep)))
        .unwrap_or_else(|_| empty_struct_type())
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

/// Where the column mapping of a table definition starts from.
pub enum ColumnMappingBase<'a> {
    /// The columns keep the column mapping that this schema has for them, and the other
    /// columns get the IDs after the IDs of this schema.
    Assigned(&'a StructType),
    /// Every column gets a new physical name, and an ID after this one. A new table
    /// starts after zero.
    After(i64),
}

impl ColumnMappingBase<'_> {
    /// Returns the base for a table that replaces the table of the snapshot, which is
    /// after the column IDs that the table has used, or `None` if the table has no
    /// column mapping.
    pub fn replacing(snapshot: &DeltaSnapshot) -> Result<Option<Self>, ArrowError> {
        if snapshot.effective_column_mapping_mode() == ColumnMappingMode::None {
            return Ok(None);
        }
        let current = StructType::try_from(snapshot.schema())?;
        let used = snapshot
            .metadata()
            .configuration()
            .get(MAX_COLUMN_ID_KEY)
            .and_then(|id| id.parse::<i64>().ok())
            .unwrap_or_default()
            .max(compute_max_column_id(&current));
        Ok(Some(Self::After(used)))
    }
}

const COLUMN_MAPPING_MODE_KEY: &str = "delta.columnMapping.mode";
const MAX_COLUMN_ID_KEY: &str = "delta.columnMapping.maxColumnId";

/// Returns the column mapping mode of a table configuration, which is no column mapping
/// if the configuration has no valid mode.
pub fn column_mapping_mode(configuration: &HashMap<String, String>) -> ColumnMappingMode {
    configuration
        .get(COLUMN_MAPPING_MODE_KEY)
        .and_then(|mode| ColumnMappingMode::try_from(mode.as_str()).ok())
        .unwrap_or_default()
}

/// Assigns the column mapping of a table definition: annotates the schema from the base,
/// and sets the mode and the maximum column ID in the configuration.
pub fn assign_column_mapping(
    schema: &StructType,
    mode: ColumnMappingMode,
    base: ColumnMappingBase<'_>,
    configuration: &mut HashMap<String, String>,
) -> StructType {
    let (annotated, max_column_id) = match base {
        ColumnMappingBase::Assigned(assigned) => annotate_new_fields_for_column_mapping(
            assigned,
            schema,
            compute_max_column_id(assigned) + 1,
        ),
        ColumnMappingBase::After(used) => {
            annotate_new_fields_for_column_mapping(&empty_struct_type(), schema, used + 1)
        }
    };
    configuration.insert(
        COLUMN_MAPPING_MODE_KEY.to_string(),
        mode.as_ref().to_string(),
    );
    configuration.insert(MAX_COLUMN_ID_KEY.to_string(), max_column_id.to_string());
    annotated
}

/// Compute the maximum `delta.columnMapping.id` present in a logical kernel schema.
pub fn compute_max_column_id(schema: &StructType) -> i64 {
    fn max_in_data_type(data_type: &DataType) -> i64 {
        match data_type {
            DataType::Struct(struct_type) => struct_type
                .fields()
                .map(max_in_field)
                .max()
                .unwrap_or_default(),
            DataType::Array(array_type) => max_in_data_type(array_type.element_type()),
            DataType::Map(map_type) => {
                max_in_data_type(map_type.key_type()).max(max_in_data_type(map_type.value_type()))
            }
            _ => 0,
        }
    }

    fn max_in_field(field: &StructField) -> i64 {
        let field_id = column_mapping_id(field).unwrap_or_default();

        field_id.max(max_in_data_type(field.data_type()))
    }

    schema.fields().map(max_in_field).max().unwrap_or_default()
}

fn column_mapping_id(field: &StructField) -> Option<i64> {
    field
        .get_config_value(&ColumnMetadataKey::ColumnMappingId)
        .and_then(|v| match v {
            MetadataValue::Number(n) => Some(*n),
            _ => None,
        })
}

fn column_mapping_physical_name(field: &StructField) -> Option<&str> {
    field
        .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
        .and_then(|v| match v {
            MetadataValue::String(s) => Some(s.as_str()),
            _ => None,
        })
}

fn strip_parquet_field_id_metadata(metadata: &mut HashMap<String, MetadataValue>) {
    metadata.remove(PARQUET_FIELD_ID_META_KEY);
    metadata.remove(ColumnMetadataKey::ParquetFieldId.as_ref());
}

fn merge_metadata(prev: &StructField, new: &StructField) -> HashMap<String, MetadataValue> {
    let mut merged = prev.metadata().clone();
    strip_parquet_field_id_metadata(&mut merged);

    for (key, value) in new.metadata() {
        let is_column_mapping_key = key == ColumnMetadataKey::ColumnMappingId.as_ref()
            || key == ColumnMetadataKey::ColumnMappingPhysicalName.as_ref();

        if is_column_mapping_key {
            // Preserve existing column mapping metadata if present; otherwise add it.
            merged.entry(key.clone()).or_insert_with(|| value.clone());
        } else if key != PARQUET_FIELD_ID_META_KEY
            && key != ColumnMetadataKey::ParquetFieldId.as_ref()
        {
            // For non-column-mapping keys, prefer the value from the new field (to keep user-added metadata).
            merged.insert(key.clone(), value.clone());
        }
    }

    merged
}

fn annotate_field(field: &StructField, counter: &AtomicI64) -> StructField {
    let next_id = counter.fetch_add(1, Ordering::Relaxed);
    let physical_name = format!("col-{}", uuid::Uuid::new_v4());
    let mut annotated = field.clone();
    strip_parquet_field_id_metadata(&mut annotated.metadata);
    let annotated = annotated.add_metadata([
        (
            ColumnMetadataKey::ColumnMappingId.as_ref(),
            MetadataValue::Number(next_id),
        ),
        (
            ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
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
            let result = StructType::try_new(fields).unwrap_or_else(|_| empty_struct_type());
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

fn find_matching_field<'a>(
    candidate: &StructField,
    existing_fields: &'a [&StructField],
) -> Option<&'a StructField> {
    if let Some(cid) = column_mapping_id(candidate)
        && let Some(field) = existing_fields
            .iter()
            .copied()
            .find(|f| column_mapping_id(f) == Some(cid))
    {
        return Some(field);
    }

    if let Some(phys) = column_mapping_physical_name(candidate)
        && let Some(field) = existing_fields
            .iter()
            .copied()
            .find(|f| column_mapping_physical_name(f) == Some(phys))
    {
        return Some(field);
    }

    existing_fields
        .iter()
        .copied()
        .find(|f| f.name() == candidate.name())
}

fn merge_struct(existing: &StructType, candidate: &StructType, counter: &AtomicI64) -> StructType {
    let existing_fields: Vec<&StructField> = existing.fields().collect();
    let merged_fields: Vec<StructField> = candidate
        .fields()
        .map(|nf| {
            let prev = find_matching_field(nf, &existing_fields);
            if let Some(prev_field) = prev {
                let merged_dtype =
                    merge_types(Some(prev_field.data_type()), nf.data_type(), counter);
                let merged_metadata = merge_metadata(prev_field, nf);
                StructField {
                    name: nf.name().clone(),
                    data_type: merged_dtype,
                    nullable: nf.is_nullable(),
                    metadata: merged_metadata,
                }
            } else {
                annotate_field(nf, counter)
            }
        })
        .collect();
    StructType::try_new(merged_fields).unwrap_or_else(|_| empty_struct_type())
}

fn empty_struct_type() -> StructType {
    StructType::try_new(Vec::<StructField>::new())
        .unwrap_or_else(|_| unreachable!("empty struct type is always valid"))
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn annotation_strips_transient_parquet_field_ids() {
        let rate = StructField::new("rate", DataType::LONG, true).with_metadata([
            (PARQUET_FIELD_ID_META_KEY, MetadataValue::Number(30)),
            (
                ColumnMetadataKey::ParquetFieldId.as_ref(),
                MetadataValue::Number(30),
            ),
        ]);
        let nested = StructType::try_new(vec![rate]).expect("nested schema");
        let details = StructField::new("details", DataType::Struct(Box::new(nested)), true)
            .with_metadata([(PARQUET_FIELD_ID_META_KEY, MetadataValue::Number(20))]);
        let schema = StructType::try_new(vec![details]).expect("schema");

        let annotated = annotate_schema_for_column_mapping(&schema);
        let details = annotated.field("details").expect("details field");
        let DataType::Struct(nested) = details.data_type() else {
            panic!("details must be a struct");
        };
        let rate = nested.field("rate").expect("rate field");

        for field in [details, rate] {
            assert!(!field.metadata().contains_key(PARQUET_FIELD_ID_META_KEY));
            assert!(
                !field
                    .metadata()
                    .contains_key(ColumnMetadataKey::ParquetFieldId.as_ref())
            );
            assert!(field.metadata().contains_key("delta.columnMapping.id"));
        }
    }

    /// The Arrow schema that a write creates a table with has the column mapping of the
    /// table for each field, and keeps the Arrow types of the data.
    #[test]
    fn arrow_annotation_follows_the_table_annotation() {
        let column_mapping = |field: &ArrowField| {
            (
                field.metadata().get("delta.columnMapping.id").cloned(),
                field
                    .metadata()
                    .get("delta.columnMapping.physicalName")
                    .cloned(),
            )
        };
        let point = ArrowDataType::Struct(
            vec![
                ArrowField::new("x", ArrowDataType::Int64, true),
                ArrowField::new("y", ArrowDataType::Utf8View, true),
            ]
            .into(),
        );
        let schema = ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false).with_metadata(HashMap::from([
                ("delta.columnMapping.id".to_string(), "7".to_string()),
                (
                    "delta.columnMapping.physicalName".to_string(),
                    "col-source".to_string(),
                ),
                ("comment".to_string(), "kept".to_string()),
            ])),
            ArrowField::new(
                "points",
                ArrowDataType::LargeList(Arc::new(ArrowField::new("item", point.clone(), true))),
                true,
            ),
            ArrowField::new_map(
                "by_name",
                "entries",
                ArrowField::new("key", ArrowDataType::Utf8, false),
                ArrowField::new("value", point, true),
                false,
                true,
            ),
        ]);

        let annotated = annotate_arrow_schema_for_column_mapping(&schema).expect("annotation");

        // The Arrow types are kept, and the other field metadata too.
        let strip = |schema: &ArrowSchema| {
            crate::schema::strip_column_mapping_metadata(&Arc::new(schema.clone()))
        };
        assert_eq!(strip(&annotated).fields(), strip(&schema).fields());
        assert_eq!(
            annotated
                .field(0)
                .metadata()
                .get("comment")
                .map(String::as_str),
            Some("kept")
        );
        // The column IDs are assigned in the order of the table annotation, and the
        // column mapping of the input is replaced.
        let (id, name) = column_mapping(annotated.field(0));
        assert_eq!(id.as_deref(), Some("1"));
        assert_ne!(name.as_deref(), Some("col-source"));
        let ArrowDataType::LargeList(item) = annotated.field(1).data_type() else {
            panic!("points must be a large list");
        };
        let ArrowDataType::Struct(point) = item.data_type() else {
            panic!("the element of points must be a struct");
        };
        assert_eq!(column_mapping(annotated.field(1)).0.as_deref(), Some("2"));
        assert_eq!(column_mapping(&point[0]).0.as_deref(), Some("3"));
        assert_eq!(column_mapping(&point[1]).0.as_deref(), Some("4"));
        let ArrowDataType::Map(entries, _) = annotated.field(2).data_type() else {
            panic!("by_name must be a map");
        };
        let ArrowDataType::Struct(entries) = entries.data_type() else {
            panic!("the entries of by_name must be a struct");
        };
        let ArrowDataType::Struct(value) = entries[1].data_type() else {
            panic!("the value of by_name must be a struct");
        };
        assert_eq!(column_mapping(annotated.field(2)).0.as_deref(), Some("5"));
        assert_eq!(column_mapping(&value[0]).0.as_deref(), Some("6"));
        assert_eq!(column_mapping(&value[1]).0.as_deref(), Some("7"));

        // The table that is created from the schema has the same column mapping.
        let table = StructType::try_from(&annotated).expect("table schema");
        assert_eq!(compute_max_column_id(&table), 7);
        let (merged, last_id) = annotate_new_fields_for_column_mapping(
            &table,
            &StructType::try_from(&strip(&annotated).as_ref().clone()).expect("data schema"),
            8,
        );
        assert_eq!(last_id, 7);
        assert_eq!(merged, table);
    }

    /// Returns the column IDs of the fields of a schema, in the order of the fields.
    fn column_ids(schema: &StructType) -> Vec<(String, Option<i64>)> {
        fn visit(data_type: &DataType, ids: &mut Vec<(String, Option<i64>)>) {
            match data_type {
                DataType::Struct(fields) => {
                    for field in fields.fields() {
                        ids.push((field.name().clone(), column_mapping_id(field)));
                        visit(field.data_type(), ids);
                    }
                }
                DataType::Array(array) => visit(array.element_type(), ids),
                DataType::Map(map) => {
                    visit(map.key_type(), ids);
                    visit(map.value_type(), ids);
                }
                _ => {}
            }
        }
        let mut ids = Vec::new();
        visit(&DataType::Struct(Box::new(schema.clone())), &mut ids);
        ids
    }

    fn mapped(name: &str, data_type: DataType, id: i64, physical_name: &str) -> StructField {
        StructField::new(name, data_type, true).with_metadata([
            (
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(id),
            ),
            (
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String(physical_name.to_string()),
            ),
        ])
    }

    #[test]
    fn column_mapping_is_assigned_from_each_base() {
        let point = |fields: Vec<StructField>| {
            DataType::Struct(Box::new(StructType::try_new(fields).expect("point")))
        };
        let schema = StructType::try_new(vec![
            mapped("id", DataType::LONG, 9, "col-old"),
            StructField::new(
                "point",
                point(vec![
                    StructField::new("x", DataType::LONG, true),
                    StructField::new("y", DataType::LONG, true),
                ]),
                true,
            ),
            StructField::new(
                "tags",
                DataType::Array(Box::new(ArrayType::new(
                    point(vec![StructField::new("tag", DataType::STRING, true)]),
                    true,
                ))),
                true,
            ),
        ])
        .expect("schema");
        let ids = |start: i64| {
            ["id", "point", "x", "y", "tags", "tag"]
                .into_iter()
                .zip(start..)
                .map(|(name, id)| (name.to_string(), Some(id)))
                .collect::<Vec<_>>()
        };
        let configuration = |max_column_id: &str| {
            HashMap::from([
                ("delta.columnMapping.mode".to_string(), "name".to_string()),
                (
                    "delta.columnMapping.maxColumnId".to_string(),
                    max_column_id.to_string(),
                ),
                ("kept".to_string(), "true".to_string()),
            ])
        };
        let assign = |schema: &StructType, base| {
            let mut configuration = HashMap::from([
                ("delta.columnMapping.mode".to_string(), "NAME".to_string()),
                ("kept".to_string(), "true".to_string()),
            ]);
            let mode = column_mapping_mode(&configuration);
            let annotated = assign_column_mapping(schema, mode, base, &mut configuration);
            (annotated, configuration)
        };

        // A new table has the column mapping of the annotation of a new table.
        let (annotated, assigned) = assign(&schema, ColumnMappingBase::After(0));
        assert_eq!(column_ids(&annotated), ids(1));
        assert_eq!(
            column_ids(&annotated),
            column_ids(&annotate_schema_for_column_mapping(&schema))
        );
        assert_eq!(assigned, configuration("6"));
        assert_ne!(
            annotated.field("id").and_then(column_mapping_physical_name),
            Some("col-old")
        );
        let (_, assigned) = assign(
            &StructType::try_new(Vec::<StructField>::new()).expect("no columns"),
            ColumnMappingBase::After(0),
        );
        assert_eq!(assigned, configuration("0"));

        // Every column gets an ID after the IDs that a replaced table has used.
        let (annotated, assigned) = assign(&schema, ColumnMappingBase::After(41));
        assert_eq!(column_ids(&annotated), ids(42));
        assert_eq!(assigned, configuration("47"));

        // The columns keep the column mapping of the schema that the table is created
        // with. The maximum column ID is not less than an ID of that schema, also for a
        // column that the written schema does not have.
        let created = StructType::try_new(vec![
            mapped("id", DataType::LONG, 1, "col-id"),
            mapped(
                "point",
                point(vec![
                    mapped("x", DataType::LONG, 3, "col-x"),
                    mapped("y", DataType::LONG, 4, "col-y"),
                ]),
                2,
                "col-point",
            ),
            mapped("gone", DataType::LONG, 12, "col-gone"),
        ])
        .expect("created schema");
        let written = without_column_mapping_metadata(&schema);
        let (annotated, assigned) = assign(&written, ColumnMappingBase::Assigned(&created));
        assert_eq!(
            column_ids(&annotated),
            [
                ("id", 1),
                ("point", 2),
                ("x", 3),
                ("y", 4),
                ("tags", 13),
                ("tag", 14)
            ]
            .map(|(name, id)| (name.to_string(), Some(id)))
        );
        assert_eq!(
            annotated.field("id").and_then(column_mapping_physical_name),
            Some("col-id")
        );
        assert_eq!(assigned, configuration("14"));
        let written = StructType::try_new(vec![StructField::new("id", DataType::LONG, true)])
            .expect("written schema");
        let (annotated, assigned) = assign(&written, ColumnMappingBase::Assigned(&created));
        assert_eq!(column_ids(&annotated), [("id".to_string(), Some(1))]);
        assert_eq!(assigned, configuration("12"));
    }
}
