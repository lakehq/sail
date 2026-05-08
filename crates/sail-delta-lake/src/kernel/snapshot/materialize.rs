use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    new_empty_array, Array, ArrayRef, MapArray, StringArray, StructArray,
};
use datafusion::arrow::datatypes::{Field, Fields, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::scalar::ScalarValue;

use super::DeltaSnapshot;
use crate::conversion::parse_optional_partition_value;
use crate::schema::make_physical_arrow_schema;
use crate::spec::fields::{
    FIELD_NAME_PARTITION_VALUES_PARSED, FIELD_NAME_STATS, FIELD_NAME_STATS_PARSED,
};
use crate::spec::{
    parse_stats_json_array, stats_schema, Add, ColumnMappingMode, DataType,
    DeltaError as DeltaTableError, DeltaResult, StructType,
};

impl DeltaSnapshot {
    pub(super) fn build_files_batch_from_adds(&self, adds: &[Add]) -> DeltaResult<RecordBatch> {
        let raw = encode_snapshot_add_rows(adds)?;
        parse_scan_row_columns(raw, self)
    }

    pub(super) fn build_active_files_batch(&self) -> DeltaResult<RecordBatch> {
        self.build_files_batch_from_adds(self.adds())
    }

    pub(super) fn build_empty_files_batch(&self) -> DeltaResult<RecordBatch> {
        self.build_files_batch_from_adds(&[])
    }
}

fn snapshot_add_fields() -> DeltaResult<Vec<Arc<Field>>> {
    crate::spec::add_struct_type()
        .fields()
        .map(|field| {
            Field::try_from(field).map(Arc::new).map_err(|e| {
                DeltaTableError::generic(format!(
                    "snapshot add schema should convert to Arrow: {e}"
                ))
            })
        })
        .collect()
}

fn encode_snapshot_add_rows(rows: &[Add]) -> DeltaResult<RecordBatch> {
    let owned_rows = rows.to_vec();
    let fields = snapshot_add_fields()?;
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
    let mode = snapshot.effective_column_mapping_mode();
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
    let mode = snapshot.effective_column_mapping_mode();

    if let Some((stats_idx, _)) = raw.schema_ref().column_with_name(FIELD_NAME_STATS) {
        let stats_source_arrow = build_stats_source_schema(snapshot)?;
        let stats_source_kernel = StructType::try_from(&stats_source_arrow)?;
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
        let stats_array = Arc::new(parse_stats_json_array(stats_json, &arrow_stats_schema)?);
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
        let partition_schema = StructType::try_from(&partition_schema_arrow)?;
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

pub(crate) fn parse_partition_values_array(
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
            for field in partition_schema.fields() {
                let physical_name = field.physical_name(column_mapping_mode);
                raw_collected
                    .get_mut(physical_name)
                    .ok_or_else(|| DeltaTableError::schema("partition field missing".to_string()))?
                    .push(None);
            }
            continue;
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
            .map(|field| Field::try_from(&field.make_physical(column_mapping_mode)))
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
