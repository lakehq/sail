// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
//
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
use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::array::{
    new_empty_array, Array, ArrayRef, MapArray, StringArray, StructArray,
};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, Fields, Schema as ArrowSchema,
};
use datafusion::arrow::json::ReaderBuilder as JsonReaderBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::scalar::ScalarValue;
use serde::{Deserialize, Serialize};

use super::DeltaSnapshot;
use crate::conversion::parse_optional_partition_value;
use crate::schema::{logical_arrow_to_kernel, make_physical_arrow_schema};
use crate::spec::fields::{
    FIELD_NAME_PARTITION_VALUES_PARSED, FIELD_NAME_STATS, FIELD_NAME_STATS_PARSED,
};
use crate::spec::{
    stats_schema, Add, ColumnMappingMode, DataType, DeltaError as DeltaTableError, DeltaResult,
    StructType,
};

impl DeltaSnapshot {
    pub(super) fn build_files_batch_from_adds(&self, adds: &[Add]) -> DeltaResult<RecordBatch> {
        let rows = adds
            .iter()
            .cloned()
            .map(SnapshotAddRow::from)
            .collect::<Vec<_>>();
        let raw = encode_snapshot_add_rows(&rows)?;
        parse_scan_row_columns(raw, self)
    }

    pub(super) fn build_active_files_batch(&self) -> DeltaResult<RecordBatch> {
        self.build_files_batch_from_adds(self.adds())
    }

    pub(super) fn build_empty_files_batch(&self) -> DeltaResult<RecordBatch> {
        self.build_files_batch_from_adds(&[])
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotDeletionVectorRow {
    storage_type: String,
    path_or_inline_dv: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<i32>,
    size_in_bytes: i32,
    cardinality: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotAddRow {
    #[serde(with = "crate::spec::utils::serde_path")]
    path: String,
    partition_values: HashMap<String, Option<String>>,
    size: i64,
    modification_time: i64,
    data_change: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags: Option<HashMap<String, Option<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deletion_vector: Option<SnapshotDeletionVectorRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_row_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    default_row_commit_version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    clustering_provider: Option<String>,
}

impl From<Add> for SnapshotAddRow {
    fn from(value: Add) -> Self {
        Self {
            path: value.path,
            partition_values: value.partition_values,
            size: value.size,
            modification_time: value.modification_time,
            data_change: value.data_change,
            stats: value.stats,
            tags: value.tags,
            deletion_vector: value.deletion_vector.map(|dv| SnapshotDeletionVectorRow {
                storage_type: dv.storage_type.as_ref().to_string(),
                path_or_inline_dv: dv.path_or_inline_dv,
                offset: dv.offset,
                size_in_bytes: dv.size_in_bytes,
                cardinality: dv.cardinality,
            }),
            base_row_id: value.base_row_id,
            default_row_commit_version: value.default_row_commit_version,
            clustering_provider: value.clustering_provider,
        }
    }
}

fn map_utf8_utf8_field(field_name: &str, nullable: bool, value_nullable: bool) -> Arc<Field> {
    let entry_struct = ArrowDataType::Struct(
        vec![
            Arc::new(Field::new("key", ArrowDataType::Utf8, false)),
            Arc::new(Field::new("value", ArrowDataType::Utf8, value_nullable)),
        ]
        .into(),
    );

    Arc::new(Field::new(
        field_name,
        ArrowDataType::Map(
            Arc::new(Field::new("key_value", entry_struct, false)),
            false,
        ),
        nullable,
    ))
}

fn snapshot_add_fields() -> Vec<Arc<Field>> {
    let deletion_vector = ArrowDataType::Struct(
        vec![
            Arc::new(Field::new("storageType", ArrowDataType::Utf8, false)),
            Arc::new(Field::new("pathOrInlineDv", ArrowDataType::Utf8, false)),
            Arc::new(Field::new("offset", ArrowDataType::Int32, true)),
            Arc::new(Field::new("sizeInBytes", ArrowDataType::Int32, false)),
            Arc::new(Field::new("cardinality", ArrowDataType::Int64, false)),
        ]
        .into(),
    );

    vec![
        Arc::new(Field::new("path", ArrowDataType::Utf8, false)),
        map_utf8_utf8_field("partitionValues", false, true),
        Arc::new(Field::new("size", ArrowDataType::Int64, false)),
        Arc::new(Field::new("modificationTime", ArrowDataType::Int64, false)),
        Arc::new(Field::new("dataChange", ArrowDataType::Boolean, false)),
        Arc::new(Field::new("stats", ArrowDataType::Utf8, true)),
        map_utf8_utf8_field("tags", true, true),
        Arc::new(Field::new("deletionVector", deletion_vector, true)),
        Arc::new(Field::new("baseRowId", ArrowDataType::Int64, true)),
        Arc::new(Field::new(
            "defaultRowCommitVersion",
            ArrowDataType::Int64,
            true,
        )),
        Arc::new(Field::new("clusteringProvider", ArrowDataType::Utf8, true)),
    ]
}

fn encode_snapshot_add_rows(rows: &[SnapshotAddRow]) -> DeltaResult<RecordBatch> {
    let owned_rows = rows.to_vec();
    let fields = snapshot_add_fields();
    serde_arrow::to_record_batch(&fields, &owned_rows).map_err(DeltaTableError::generic_err)
}

fn build_partition_schema(
    schema: &ArrowSchema,
    partition_columns: &[String],
) -> DeltaResult<Option<ArrowSchema>> {
    if partition_columns.is_empty() {
        return Ok(None);
    }
    let fields = partition_columns
        .iter()
        .map(|col| {
            schema
                .field_with_name(col)
                .cloned()
                .map_err(|_| DeltaTableError::missing_column(col))
        })
        .collect::<DeltaResult<Vec<_>>>()?;
    Ok(Some(ArrowSchema::new(fields)))
}

fn build_stats_source_schema(snapshot: &DeltaSnapshot) -> DeltaResult<ArrowSchema> {
    let partition_columns = snapshot.metadata().partition_columns();
    let mode = snapshot.column_mapping_mode();
    let non_partition_fields: Vec<Field> = snapshot
        .schema()
        .fields()
        .iter()
        .filter(|field| !partition_columns.contains(field.name()))
        .map(|field| field.as_ref().clone())
        .collect();
    let logical_non_partition = ArrowSchema::new(non_partition_fields);
    Ok(make_physical_arrow_schema(&logical_non_partition, mode))
}

fn parse_scan_row_columns(raw: RecordBatch, snapshot: &DeltaSnapshot) -> DeltaResult<RecordBatch> {
    let mut fields = raw.schema().fields().to_vec();
    let mut columns = raw.columns().to_vec();
    let mode = snapshot.column_mapping_mode();

    if let Some((stats_idx, _)) = raw.schema_ref().column_with_name(FIELD_NAME_STATS) {
        let stats_source_arrow = build_stats_source_schema(snapshot)?;
        let stats_source_kernel = logical_arrow_to_kernel(&stats_source_arrow)?;
        let stats_schema = Arc::new(stats_schema(
            &stats_source_kernel,
            snapshot.table_properties(),
        )?);
        let arrow_stats_schema = Arc::new(ArrowSchema::try_from(stats_schema.as_ref())?);
        let stats_batch = raw.project(&[stats_idx])?;
        let stats_json = stats_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DeltaTableError::schema("expected Utf8 stats column when parsing stats".to_string())
            })?;
        let mut json_lines = String::new();
        for value in stats_json.iter() {
            if let Some(value) = value {
                json_lines.push_str(value);
            } else {
                json_lines.push_str("{}");
            }
            json_lines.push('\n');
        }
        let mut reader = JsonReaderBuilder::new(Arc::clone(&arrow_stats_schema))
            .with_batch_size(stats_batch.num_rows().max(1))
            .build(Cursor::new(json_lines))
            .map_err(DeltaTableError::generic_err)?;
        let parsed_batch = match reader.next() {
            Some(batch) => batch.map_err(DeltaTableError::generic_err)?,
            None => RecordBatch::new_empty(arrow_stats_schema),
        };
        let stats_array: Arc<StructArray> = Arc::new(parsed_batch.into());
        fields.push(Arc::new(Field::new(
            FIELD_NAME_STATS_PARSED,
            stats_array.data_type().to_owned(),
            true,
        )));
        columns.push(stats_array);
    }

    if let Some(partition_schema_arrow) =
        build_partition_schema(snapshot.schema(), snapshot.metadata().partition_columns())?
    {
        let partition_schema = logical_arrow_to_kernel(&partition_schema_arrow)?;
        let partition_array =
            parse_partition_values_array(&raw, &partition_schema, "partitionValues", mode)?;
        fields.push(Arc::new(Field::new(
            FIELD_NAME_PARTITION_VALUES_PARSED,
            partition_array.data_type().to_owned(),
            false,
        )));
        columns.push(Arc::new(partition_array));
    }

    Ok(RecordBatch::try_new(
        Arc::new(ArrowSchema::new(fields)),
        columns,
    )?)
}

fn parse_partition_values_array(
    batch: &RecordBatch,
    partition_schema: &StructType,
    path: &str,
    column_mapping_mode: ColumnMappingMode,
) -> DeltaResult<StructArray> {
    let partitions = map_array_from_path(batch, path)?;
    let num_rows = partitions.len();

    let mut raw_collected: HashMap<String, Vec<Option<String>>> = partition_schema
        .fields()
        .map(|field| {
            (
                field.physical_name(column_mapping_mode).to_string(),
                Vec::with_capacity(num_rows),
            )
        })
        .collect();

    for row in 0..num_rows {
        if partitions.is_null(row) {
            return Err(DeltaTableError::generic(
                "Expected partition values map, found null entry.",
            ));
        }
        let raw_values = collect_partition_row(&partitions.value(row))?;

        for field in partition_schema.fields() {
            if !matches!(field.data_type(), DataType::Primitive(_)) {
                return Err(DeltaTableError::generic(
                    "nested partitioning values are not supported",
                ));
            }
            let physical_name = field.physical_name(column_mapping_mode);
            let value = raw_values
                .get(physical_name)
                .or_else(|| raw_values.get(field.name()))
                .and_then(Clone::clone);
            raw_collected
                .get_mut(physical_name)
                .ok_or_else(|| DeltaTableError::schema("partition field missing".to_string()))?
                .push(value);
        }
    }

    let arrow_fields: Fields = Fields::from(
        partition_schema
            .fields()
            .map(Field::try_from)
            .collect::<Result<Vec<Field>, _>>()?,
    );

    let columns: Vec<ArrayRef> = partition_schema
        .fields()
        .zip(arrow_fields.iter())
        .map(|(field, arrow_field)| {
            let physical_name = field.physical_name(column_mapping_mode);
            let raw_values = raw_collected
                .get(physical_name)
                .ok_or_else(|| DeltaTableError::schema("partition field missing".to_string()))?;
            let arrow_dt = arrow_field.data_type();
            let scalar_values: Vec<ScalarValue> = raw_values
                .iter()
                .map(|value| {
                    parse_optional_partition_value(value.as_deref(), arrow_dt).map_err(|e| {
                        DeltaTableError::generic(format!("partition value parse error: {e}"))
                    })
                })
                .collect::<DeltaResult<Vec<_>>>()?;
            let array = if scalar_values.is_empty() {
                new_empty_array(arrow_dt)
            } else {
                ScalarValue::iter_to_array(scalar_values)
                    .map_err(|e| DeltaTableError::generic(format!("scalar to array error: {e}")))?
            };
            let array = if array.data_type() != arrow_dt {
                datafusion::arrow::compute::cast(&array, arrow_dt)
                    .map_err(|e| DeltaTableError::generic(format!("cast error: {e}")))?
            } else {
                array
            };
            Ok(Arc::new(array) as ArrayRef)
        })
        .collect::<DeltaResult<Vec<_>>>()?;

    Ok(StructArray::try_new(arrow_fields, columns, None)?)
}

fn map_array_from_path<'a>(batch: &'a RecordBatch, path: &str) -> DeltaResult<&'a MapArray> {
    let mut segments = path.split('.');
    let first = segments
        .next()
        .ok_or_else(|| DeltaTableError::generic("partition column path must not be empty"))?;

    let mut current: &dyn Array = batch
        .column_by_name(first)
        .map(|column| column.as_ref())
        .ok_or_else(|| {
            DeltaTableError::schema(format!("{first} column not found when parsing partitions"))
        })?;

    for segment in segments {
        let struct_array = current
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DeltaTableError::schema(format!("Expected struct column while traversing {path}"))
            })?;
        current = struct_array
            .column_by_name(segment)
            .map(|column| column.as_ref())
            .ok_or_else(|| {
                DeltaTableError::schema(format!(
                    "{segment} column not found while traversing {path}"
                ))
            })?;
    }

    current
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| DeltaTableError::schema(format!("Column {path} is not a map")))
}

fn collect_partition_row(value: &StructArray) -> DeltaResult<HashMap<String, Option<String>>> {
    let keys = value
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DeltaTableError::schema("map key column is not Utf8".to_string()))?;
    let values = value
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DeltaTableError::schema("map value column is not Utf8".to_string()))?;

    let mut result = HashMap::with_capacity(keys.len());
    for (key, value) in keys.iter().zip(values.iter()) {
        if let Some(key) = key {
            result.insert(key.to_string(), value.map(|entry| entry.to_string()));
        }
    }
    Ok(result)
}
