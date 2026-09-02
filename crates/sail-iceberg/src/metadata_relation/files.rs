use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, new_empty_array};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};

use crate::datasource::type_converter::iceberg_type_to_arrow;
use crate::io::{load_manifest, load_manifest_list_with_version};
use crate::spec::schema::visit_fields_bfs;
use crate::spec::types::values::{Literal, PrimitiveLiteral};
use crate::spec::types::{ListType, MapType, NestedField, PrimitiveType, StructType, Type};
use crate::spec::{DataContentType, DataFile, DataFileFormat, ManifestStatus, TableMetadata};
use crate::table::Table;
use crate::utils::conversions::to_scalar;

#[derive(Clone)]
struct ReadableField {
    id: i32,
    name: String,
    primitive: PrimitiveType,
}

pub(super) fn schema(metadata: &TableMetadata) -> Result<SchemaRef> {
    let partition_type = unified_partition_type(metadata)?;
    let readable_fields = readable_fields(metadata)?;
    let mut fields = vec![
        Field::new("content", DataType::Int32, true),
        Field::new("file_path", DataType::Utf8, false),
        Field::new("file_format", DataType::Utf8, false),
        Field::new("spec_id", DataType::Int32, true),
    ];
    if !partition_type.fields().is_empty() {
        fields.push(Field::new(
            "partition",
            iceberg_type_to_arrow(&Type::Struct(partition_type))?,
            false,
        ));
    }
    fields.extend([
        Field::new("record_count", DataType::Int64, false),
        Field::new("file_size_in_bytes", DataType::Int64, false),
        Field::new(
            "column_sizes",
            iceberg_type_to_arrow(&count_map_type())?,
            true,
        ),
        Field::new(
            "value_counts",
            iceberg_type_to_arrow(&count_map_type())?,
            true,
        ),
        Field::new(
            "null_value_counts",
            iceberg_type_to_arrow(&count_map_type())?,
            true,
        ),
        Field::new(
            "nan_value_counts",
            iceberg_type_to_arrow(&count_map_type())?,
            true,
        ),
        Field::new(
            "lower_bounds",
            iceberg_type_to_arrow(&bound_map_type())?,
            true,
        ),
        Field::new(
            "upper_bounds",
            iceberg_type_to_arrow(&bound_map_type())?,
            true,
        ),
        Field::new("key_metadata", DataType::Binary, true),
        Field::new(
            "split_offsets",
            iceberg_type_to_arrow(&i64_list_type())?,
            true,
        ),
        Field::new(
            "equality_ids",
            iceberg_type_to_arrow(&i32_list_type())?,
            true,
        ),
        Field::new("sort_order_id", DataType::Int32, true),
        Field::new("first_row_id", DataType::Int64, true),
        Field::new("referenced_data_file", DataType::Utf8, true),
        Field::new("content_offset", DataType::Int64, true),
        Field::new("content_size_in_bytes", DataType::Int64, true),
        Field::new(
            "readable_metrics",
            readable_metrics_type(&readable_fields)?,
            true,
        ),
    ]);
    Ok(Arc::new(ArrowSchema::new(fields)))
}

pub(super) async fn batch(table: &Table) -> Result<RecordBatch> {
    let metadata = table.metadata();
    let output_schema = schema(metadata)?;
    let partition_type = unified_partition_type(metadata)?;
    let readable_fields = readable_fields(metadata)?;
    let files = current_live_files(table).await?;
    if files.is_empty() {
        let columns = output_schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect();
        return RecordBatch::try_new(output_schema, columns).map_err(Into::into);
    }

    let mut columns = vec![Vec::with_capacity(files.len()); output_schema.fields().len()];
    for file in &files {
        let values = file_values(metadata, file, &partition_type, &readable_fields)?;
        if values.len() != columns.len() {
            return Err(DataFusionError::Internal(format!(
                "Iceberg files row has {} values for a {}-column schema",
                values.len(),
                columns.len()
            )));
        }
        for (column, value) in columns.iter_mut().zip(values) {
            column.push(value);
        }
    }
    let columns = columns
        .into_iter()
        .map(ScalarValue::iter_to_array)
        .collect::<Result<Vec<ArrayRef>>>()?;
    RecordBatch::try_new(output_schema, columns).map_err(Into::into)
}

fn file_values(
    metadata: &TableMetadata,
    file: &DataFile,
    partition_type: &StructType,
    readable_fields: &[ReadableField],
) -> Result<Vec<ScalarValue>> {
    let mut values = vec![
        ScalarValue::Int32(Some(match file.content {
            DataContentType::Data => 0,
            DataContentType::PositionDeletes => 1,
            DataContentType::EqualityDeletes => 2,
        })),
        ScalarValue::Utf8(Some(file.file_path.clone())),
        ScalarValue::Utf8(Some(file_format_name(file.file_format).to_string())),
        ScalarValue::Int32(Some(file.partition_spec_id)),
    ];
    if !partition_type.fields().is_empty() {
        values.push(partition_value(metadata, file, partition_type)?);
    }
    values.extend([
        ScalarValue::Int64(Some(u64_to_i64(file.record_count, "record_count")?)),
        ScalarValue::Int64(Some(u64_to_i64(
            file.file_size_in_bytes,
            "file_size_in_bytes",
        )?)),
        count_map_scalar(&file.column_sizes)?,
        count_map_scalar(&file.value_counts)?,
        count_map_scalar(&file.null_value_counts)?,
        count_map_scalar(&file.nan_value_counts)?,
        bound_map_scalar(&file.lower_bounds)?,
        bound_map_scalar(&file.upper_bounds)?,
        ScalarValue::Binary(file.key_metadata.clone()),
        i64_list_scalar(&file.split_offsets)?,
        i32_list_scalar(&file.equality_ids)?,
        ScalarValue::Int32(file.sort_order_id),
        ScalarValue::Int64(file.first_row_id),
        ScalarValue::Utf8(file.referenced_data_file.clone()),
        ScalarValue::Int64(file.content_offset),
        ScalarValue::Int64(file.content_size_in_bytes),
        readable_metrics_scalar(file, readable_fields)?,
    ]);
    Ok(values)
}

fn count_map_type() -> Type {
    Type::Map(MapType::new(
        Arc::new(NestedField::map_key_element(
            1,
            Type::Primitive(PrimitiveType::Int),
        )),
        Arc::new(NestedField::map_value_element(
            2,
            Type::Primitive(PrimitiveType::Long),
            true,
        )),
    ))
}

fn bound_map_type() -> Type {
    Type::Map(MapType::new(
        Arc::new(NestedField::map_key_element(
            1,
            Type::Primitive(PrimitiveType::Int),
        )),
        Arc::new(NestedField::map_value_element(
            2,
            Type::Primitive(PrimitiveType::Binary),
            true,
        )),
    ))
}

fn count_map_scalar(values: &std::collections::HashMap<i32, u64>) -> Result<ScalarValue> {
    let map_type = count_map_type();
    if values.is_empty() {
        return null_scalar(&iceberg_type_to_arrow(&map_type)?);
    }
    let mut entries = values.iter().collect::<Vec<_>>();
    entries.sort_by_key(|(key, _)| **key);
    let literal = Literal::Map(
        entries
            .into_iter()
            .map(|(key, value)| {
                Ok((
                    Literal::Primitive(PrimitiveLiteral::Int(*key)),
                    Some(Literal::Primitive(PrimitiveLiteral::Long(u64_to_i64(
                        *value,
                        "file metric",
                    )?))),
                ))
            })
            .collect::<Result<Vec<_>>>()?,
    );
    to_scalar(&literal, &map_type)
}

fn bound_map_scalar(
    values: &std::collections::HashMap<i32, crate::spec::Datum>,
) -> Result<ScalarValue> {
    let map_type = bound_map_type();
    if values.is_empty() {
        return null_scalar(&iceberg_type_to_arrow(&map_type)?);
    }
    let mut entries = values.iter().collect::<Vec<_>>();
    entries.sort_by_key(|(key, _)| **key);
    let literal = Literal::Map(
        entries
            .into_iter()
            .map(|(key, datum)| {
                let bytes = datum
                    .r#type
                    .literal_to_bytes(&datum.literal)
                    .map_err(DataFusionError::Plan)?;
                Ok((
                    Literal::Primitive(PrimitiveLiteral::Int(*key)),
                    Some(Literal::Primitive(PrimitiveLiteral::Binary(bytes))),
                ))
            })
            .collect::<Result<Vec<_>>>()?,
    );
    to_scalar(&literal, &map_type)
}

fn i64_list_type() -> Type {
    Type::List(ListType::new(Arc::new(NestedField::list_element(
        1,
        Type::Primitive(PrimitiveType::Long),
        true,
    ))))
}

fn i32_list_type() -> Type {
    Type::List(ListType::new(Arc::new(NestedField::list_element(
        1,
        Type::Primitive(PrimitiveType::Int),
        true,
    ))))
}

fn i64_list_scalar(values: &[i64]) -> Result<ScalarValue> {
    let list_type = i64_list_type();
    if values.is_empty() {
        return null_scalar(&iceberg_type_to_arrow(&list_type)?);
    }
    to_scalar(
        &Literal::List(
            values
                .iter()
                .map(|value| Some(Literal::Primitive(PrimitiveLiteral::Long(*value))))
                .collect(),
        ),
        &list_type,
    )
}

fn i32_list_scalar(values: &[i32]) -> Result<ScalarValue> {
    let list_type = i32_list_type();
    if values.is_empty() {
        return null_scalar(&iceberg_type_to_arrow(&list_type)?);
    }
    to_scalar(
        &Literal::List(
            values
                .iter()
                .map(|value| Some(Literal::Primitive(PrimitiveLiteral::Int(*value))))
                .collect(),
        ),
        &list_type,
    )
}

fn partition_value(
    metadata: &TableMetadata,
    file: &DataFile,
    partition_type: &StructType,
) -> Result<ScalarValue> {
    let spec = metadata
        .partition_specs
        .iter()
        .find(|spec| spec.spec_id() == file.partition_spec_id)
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Unknown Iceberg partition spec {} for file '{}'",
                file.partition_spec_id, file.file_path
            ))
        })?;
    let values = partition_type
        .fields()
        .iter()
        .map(|field| {
            let value = spec
                .fields()
                .iter()
                .position(|partition_field| partition_field.field_id == field.id)
                .and_then(|index| file.partition.get(index))
                .cloned()
                .flatten();
            (field.name.clone(), value)
        })
        .collect();
    to_scalar(
        &Literal::Struct(values),
        &Type::Struct(partition_type.clone()),
    )
}

fn readable_metrics_type(fields: &[ReadableField]) -> Result<DataType> {
    iceberg_type_to_arrow(&readable_metrics_iceberg_type(fields))
}

fn readable_metrics_scalar(file: &DataFile, fields: &[ReadableField]) -> Result<ScalarValue> {
    let mut metric_fields = Vec::with_capacity(fields.len());
    let mut metric_values = Vec::with_capacity(fields.len());
    for field in fields {
        let metric_type = readable_metric_iceberg_type(&field.primitive);
        metric_fields.push(Arc::new(NestedField::optional(
            field.id,
            &field.name,
            metric_type.clone(),
        )));
        let count = |values: &std::collections::HashMap<i32, u64>| {
            values
                .get(&field.id)
                .map(|value| {
                    u64_to_i64(*value, "readable metric")
                        .map(|value| Literal::Primitive(PrimitiveLiteral::Long(value)))
                })
                .transpose()
        };
        let bound = |values: &std::collections::HashMap<i32, crate::spec::Datum>| {
            values
                .get(&field.id)
                .map(|datum| Literal::Primitive(datum.literal.clone()))
        };
        metric_values.push((
            field.name.clone(),
            Some(Literal::Struct(vec![
                ("column_size".to_string(), count(&file.column_sizes)?),
                ("value_count".to_string(), count(&file.value_counts)?),
                (
                    "null_value_count".to_string(),
                    count(&file.null_value_counts)?,
                ),
                (
                    "nan_value_count".to_string(),
                    count(&file.nan_value_counts)?,
                ),
                ("lower_bound".to_string(), bound(&file.lower_bounds)),
                ("upper_bound".to_string(), bound(&file.upper_bounds)),
            ])),
        ));
    }
    to_scalar(
        &Literal::Struct(metric_values),
        &Type::Struct(StructType::new(metric_fields)),
    )
}

fn readable_metrics_iceberg_type(fields: &[ReadableField]) -> Type {
    Type::Struct(StructType::new(
        fields
            .iter()
            .map(|field| {
                Arc::new(NestedField::optional(
                    field.id,
                    &field.name,
                    readable_metric_iceberg_type(&field.primitive),
                ))
            })
            .collect(),
    ))
}

fn readable_metric_iceberg_type(primitive: &PrimitiveType) -> Type {
    Type::Struct(StructType::new(vec![
        Arc::new(NestedField::optional(
            1,
            "column_size",
            Type::Primitive(PrimitiveType::Long),
        )),
        Arc::new(NestedField::optional(
            2,
            "value_count",
            Type::Primitive(PrimitiveType::Long),
        )),
        Arc::new(NestedField::optional(
            3,
            "null_value_count",
            Type::Primitive(PrimitiveType::Long),
        )),
        Arc::new(NestedField::optional(
            4,
            "nan_value_count",
            Type::Primitive(PrimitiveType::Long),
        )),
        Arc::new(NestedField::optional(
            5,
            "lower_bound",
            Type::Primitive(primitive.clone()),
        )),
        Arc::new(NestedField::optional(
            6,
            "upper_bound",
            Type::Primitive(primitive.clone()),
        )),
    ]))
}

fn readable_fields(metadata: &TableMetadata) -> Result<Vec<ReadableField>> {
    let schema = metadata.current_schema().ok_or_else(|| {
        DataFusionError::Plan("Iceberg table metadata is missing the current schema".to_string())
    })?;
    let mut fields = Vec::new();
    visit_fields_bfs(schema, |id, field| {
        if let Type::Primitive(primitive) = field.field_type.as_ref()
            && let Some(name) = schema.name_by_field_id(id)
        {
            fields.push(ReadableField {
                id,
                name: name.to_string(),
                primitive: primitive.clone(),
            });
        }
    });
    fields.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(fields)
}

fn unified_partition_type(metadata: &TableMetadata) -> Result<StructType> {
    let schema = metadata.current_schema().ok_or_else(|| {
        DataFusionError::Plan("Iceberg table metadata is missing the current schema".to_string())
    })?;
    let mut fields = BTreeMap::new();
    let mut specs = metadata.partition_specs.iter().collect::<Vec<_>>();
    specs.sort_by_key(|spec| std::cmp::Reverse(spec.spec_id()));
    for spec in specs {
        for partition_field in spec.fields() {
            if fields.contains_key(&partition_field.field_id) {
                continue;
            }
            let Ok(source_id) = partition_field.source_id() else {
                continue;
            };
            let Some(source) = schema.field_by_id(source_id) else {
                continue;
            };
            let field_type = if matches!(partition_field.transform, crate::spec::Transform::Day) {
                Type::Primitive(PrimitiveType::Date)
            } else {
                partition_field
                    .transform
                    .result_type(source.field_type.as_ref())
                    .map_err(DataFusionError::Plan)?
            };
            fields.insert(
                partition_field.field_id,
                Arc::new(NestedField::optional(
                    partition_field.field_id,
                    &partition_field.name,
                    field_type,
                )),
            );
        }
    }
    Ok(StructType::new(fields.into_values().collect()))
}

fn null_scalar(data_type: &DataType) -> Result<ScalarValue> {
    ScalarValue::try_new_null(data_type)
}

fn u64_to_i64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).map_err(|error| {
        DataFusionError::Plan(format!("Iceberg {field} value does not fit int64: {error}"))
    })
}

fn file_format_name(format: DataFileFormat) -> &'static str {
    match format {
        DataFileFormat::Avro => "AVRO",
        DataFileFormat::Orc => "ORC",
        DataFileFormat::Parquet => "PARQUET",
        DataFileFormat::Puffin => "PUFFIN",
    }
}

/// Loads the live content-file entries referenced by the current snapshot.
///
/// The manifest entry carries the file's own spec ID only in newer encodings, so the authoritative
/// manifest-list value is copied onto every returned file.
pub(crate) async fn current_live_files(table: &Table) -> Result<Vec<DataFile>> {
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return Ok(Vec::new());
    };
    if snapshot.manifest_list().is_empty() {
        return Err(DataFusionError::NotImplemented(
            "Iceberg files metadata table does not yet support V1 snapshots without a manifest list"
                .to_string(),
        ));
    }
    let manifest_list = load_manifest_list_with_version(
        table.store_context(),
        snapshot.manifest_list(),
        metadata.format_version,
    )
    .await?;
    let mut files = Vec::new();
    for manifest_file in manifest_list.entries() {
        let manifest = load_manifest(table.store_context(), &manifest_file.manifest_path).await?;
        for entry in manifest.entries().iter().filter(|entry| {
            matches!(
                entry.status,
                ManifestStatus::Added | ManifestStatus::Existing
            )
        }) {
            let mut file = entry.data_file.clone();
            file.partition_spec_id = manifest_file.partition_spec_id;
            files.push(file);
        }
    }
    Ok(files)
}
