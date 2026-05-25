use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{new_null_array, Array, ArrayRef, StringArray, StructArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, FieldRef, Fields, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef,
};
use datafusion::arrow::json::LineDelimitedWriter;
use datafusion::arrow::record_batch::RecordBatch;

use crate::kernel::snapshot::materialize::parse_partition_values_array;
use crate::schema::make_physical_arrow_schema;
use crate::spec::fields::{FIELD_NAME_PARTITION_VALUES_PARSED, FIELD_NAME_STATS_PARSED};
use crate::spec::{
    add_struct_type, parse_stats_json_array, remove_struct_type, stats_schema, ColumnMappingMode,
    ColumnMetadataKey, DeltaError as DeltaTableError, DeltaResult, Metadata, StructField,
    StructType, TableProperties,
};

pub(crate) struct AddAugmentationConfig {
    write_stats_as_struct: bool,
    write_stats_as_json: bool,
    stats_parsed_schema: Option<ArrowSchemaRef>,
    partition_values_parsed_schema: Option<StructType>,
    column_mapping_mode: ColumnMappingMode,
}

impl AddAugmentationConfig {
    pub(crate) fn from_metadata(metadata: &Metadata) -> DeltaResult<Self> {
        let properties = TableProperties::from(metadata.configuration().iter());
        let write_stats_as_struct = properties.checkpoint_write_stats_as_struct.unwrap_or(true);
        let write_stats_as_json = properties.checkpoint_write_stats_as_json.unwrap_or(true);
        let explicit_column_mapping_mode = properties
            .column_mapping_mode
            .unwrap_or(ColumnMappingMode::None);

        let (stats_parsed_schema, partition_values_parsed_schema, column_mapping_mode) =
            if write_stats_as_struct {
                let table_schema = metadata.parse_schema()?;
                let column_mapping_mode =
                    effective_column_mapping_mode(&table_schema, explicit_column_mapping_mode);
                let partition_cols = metadata.partition_columns();

                let non_partition_fields: Vec<StructField> = table_schema
                    .fields()
                    .filter(|field| !partition_cols.contains(&field.name().to_string()))
                    .cloned()
                    .collect();
                let non_partition_schema = StructType::try_new(non_partition_fields)?;
                let non_partition_arrow = ArrowSchema::try_from(&non_partition_schema)?;
                let physical_non_partition_arrow =
                    make_physical_arrow_schema(&non_partition_arrow, column_mapping_mode);
                let physical_non_partition_schema =
                    StructType::try_from(&physical_non_partition_arrow)?;
                let stats_struct = stats_schema(&physical_non_partition_schema, &properties)?;
                let stats_arrow = Arc::new(ArrowSchema::try_from(&stats_struct)?);

                let partition_schema = if partition_cols.is_empty() {
                    None
                } else {
                    let partition_fields: Vec<StructField> = partition_cols
                        .iter()
                        .map(|col| {
                            table_schema
                                .fields()
                                .find(|field| field.name() == col)
                                .cloned()
                                .ok_or_else(|| DeltaTableError::missing_column(col))
                        })
                        .collect::<DeltaResult<Vec<_>>>()?;
                    Some(StructType::try_new(partition_fields)?)
                };

                (Some(stats_arrow), partition_schema, column_mapping_mode)
            } else {
                (None, None, explicit_column_mapping_mode)
            };

        Ok(Self {
            write_stats_as_struct,
            write_stats_as_json,
            stats_parsed_schema,
            partition_values_parsed_schema,
            column_mapping_mode,
        })
    }

    pub(crate) fn is_noop(&self) -> bool {
        !self.write_stats_as_struct && self.write_stats_as_json
    }

    pub(crate) fn augment_add(&self, batch: RecordBatch) -> DeltaResult<RecordBatch> {
        if self.is_noop() {
            return Ok(batch);
        }
        let schema = batch.schema();
        let (add_idx, add_field) = match schema.column_with_name("add") {
            Some(value) => value,
            None => return Ok(batch),
        };
        let add_struct = batch
            .column(add_idx)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DeltaTableError::schema("expected add to be a struct column"))?;

        let mut add_fields: Vec<FieldRef> = add_struct.fields().iter().cloned().collect();
        let mut add_cols: Vec<ArrayRef> = add_struct.columns().to_vec();
        let nulls = add_struct.nulls().cloned();

        let stats_idx = add_fields.iter().position(|field| field.name() == "stats");
        if self.write_stats_as_struct {
            let stats_idx =
                stats_idx.ok_or_else(|| DeltaTableError::schema("add.stats field missing"))?;
            let stats_json = add_cols[stats_idx]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DeltaTableError::schema("add.stats must be Utf8"))?;
            let stats_parsed_schema = self.stats_parsed_schema.as_ref().ok_or_else(|| {
                DeltaTableError::schema(
                    "stats_parsed_schema must be present when writeStatsAsStruct is true",
                )
            })?;
            let stats_parsed = parse_stats_json_array(stats_json, stats_parsed_schema)?;
            add_fields.push(Arc::new(Field::new(
                FIELD_NAME_STATS_PARSED,
                stats_parsed.data_type().clone(),
                true,
            )));
            add_cols.push(Arc::new(stats_parsed));
        }

        if let Some(partition_schema) = &self.partition_values_parsed_schema {
            let pv_idx = add_fields
                .iter()
                .position(|field| field.name() == "partitionValues")
                .ok_or_else(|| DeltaTableError::schema("add.partitionValues field missing"))?;
            let pv_field = add_fields[pv_idx].clone();
            let pv_col = add_cols[pv_idx].clone();
            let inner_batch = RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![Arc::new(Field::new(
                    "partitionValues",
                    pv_field.data_type().clone(),
                    pv_field.is_nullable(),
                ))])),
                vec![pv_col],
            )?;
            let partition_values_parsed = parse_partition_values_array(
                &inner_batch,
                partition_schema,
                "partitionValues",
                self.column_mapping_mode,
            )?;
            add_fields.push(Arc::new(Field::new(
                FIELD_NAME_PARTITION_VALUES_PARSED,
                partition_values_parsed.data_type().clone(),
                false,
            )));
            add_cols.push(Arc::new(partition_values_parsed));
        }

        if !self.write_stats_as_json {
            if let Some(stats_idx) = stats_idx {
                add_fields.remove(stats_idx);
                add_cols.remove(stats_idx);
            }
        }

        let new_add = StructArray::try_new(Fields::from(add_fields), add_cols, nulls)?;
        replace_struct_column(batch, add_idx, add_field, "add", new_add)
    }
}

pub(crate) fn normalize_checkpoint_batch_for_decode(
    batch: &RecordBatch,
) -> DeltaResult<RecordBatch> {
    let batch = normalize_checkpoint_action_for_decode(batch.clone(), "add", add_field_refs()?)?;
    normalize_checkpoint_action_for_decode(batch, "remove", remove_field_refs()?)
}

fn normalize_checkpoint_action_for_decode(
    batch: RecordBatch,
    action_name: &str,
    expected_fields: Vec<FieldRef>,
) -> DeltaResult<RecordBatch> {
    let schema = batch.schema();
    let (action_idx, action_field) = match schema.column_with_name(action_name) {
        Some(value) => value,
        None => return Ok(batch),
    };
    let action_struct = batch
        .column(action_idx)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DeltaTableError::schema(format!("expected {action_name} to be a struct column"))
        })?;

    if !checkpoint_action_needs_normalization(action_struct, &expected_fields) {
        return Ok(batch);
    }

    let existing: HashMap<&str, ArrayRef> = action_struct
        .fields()
        .iter()
        .zip(action_struct.columns())
        .map(|(field, column)| (field.name().as_str(), Arc::clone(column)))
        .collect();
    let stats_from_parsed = if action_name == "add" {
        existing
            .get(FIELD_NAME_STATS_PARSED)
            .map(|column| {
                column
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| DeltaTableError::schema("add.stats_parsed must be a struct"))
                    .and_then(stats_parsed_to_json_array)
            })
            .transpose()?
    } else {
        None
    };
    let mut action_cols = Vec::with_capacity(expected_fields.len());
    for field in &expected_fields {
        if let Some(column) = existing.get(field.name().as_str()) {
            action_cols.push(normalize_checkpoint_field_for_decode(
                field,
                column,
                action_struct.len(),
            )?);
        } else if field.name() == "stats" {
            action_cols.push(
                stats_from_parsed
                    .clone()
                    .unwrap_or_else(|| new_null_array(field.data_type(), action_struct.len())),
            );
        } else {
            action_cols.push(new_null_array(field.data_type(), action_struct.len()));
        }
    }

    let new_action = StructArray::try_new(
        Fields::from(expected_fields),
        action_cols,
        action_struct.nulls().cloned(),
    )?;
    replace_struct_column(batch, action_idx, action_field, action_name, new_action)
}

fn checkpoint_action_needs_normalization(
    action_struct: &StructArray,
    expected_fields: &[FieldRef],
) -> bool {
    let fields = action_struct.fields();
    if fields.len() != expected_fields.len() {
        return true;
    }
    fields
        .iter()
        .zip(expected_fields)
        .any(|(actual, expected)| {
            actual.name() != expected.name()
                || actual.data_type() != expected.data_type()
                || actual.is_nullable() != expected.is_nullable()
        })
}

fn normalize_checkpoint_field_for_decode(
    expected_field: &Field,
    column: &ArrayRef,
    len: usize,
) -> DeltaResult<ArrayRef> {
    let column = if expected_field.name() == "deletionVector" {
        normalize_deletion_vector_for_decode(expected_field, column, len)
    } else {
        Ok(Arc::clone(column))
    }?;
    if column.data_type() == expected_field.data_type() {
        Ok(column)
    } else {
        Ok(cast(column.as_ref(), expected_field.data_type())?)
    }
}

fn normalize_deletion_vector_for_decode(
    expected_field: &Field,
    column: &ArrayRef,
    len: usize,
) -> DeltaResult<ArrayRef> {
    let ArrowDataType::Struct(expected_fields) = expected_field.data_type() else {
        return Err(DeltaTableError::schema(
            "expected deletionVector schema to be a struct",
        ));
    };
    if matches!(column.data_type(), ArrowDataType::Null) {
        return Ok(new_null_array(expected_field.data_type(), len));
    }
    let deletion_vector = column
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| DeltaTableError::schema("deletionVector must be a struct"))?;

    let existing: HashMap<&str, ArrayRef> = deletion_vector
        .fields()
        .iter()
        .zip(deletion_vector.columns())
        .map(|(field, column)| (field.name().as_str(), Arc::clone(column)))
        .collect();

    let mut columns = Vec::with_capacity(expected_fields.len());
    for field in expected_fields {
        let column = existing
            .get(field.name().as_str())
            .cloned()
            .unwrap_or_else(|| new_null_array(field.data_type(), len));
        if column.data_type() == field.data_type() {
            columns.push(column);
        } else {
            columns.push(cast(column.as_ref(), field.data_type())?);
        }
    }

    Ok(Arc::new(StructArray::try_new(
        expected_fields.clone(),
        columns,
        deletion_vector.nulls().cloned(),
    )?))
}

fn stats_parsed_to_json_array(stats_parsed: &StructArray) -> DeltaResult<ArrayRef> {
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(stats_parsed.fields().clone())),
        stats_parsed.columns().to_vec(),
    )?;
    let mut buffer = Vec::new();
    {
        let mut writer = LineDelimitedWriter::new(&mut buffer);
        writer.write(&batch).map_err(DeltaTableError::generic_err)?;
        writer.finish().map_err(DeltaTableError::generic_err)?;
    }
    let text = String::from_utf8(buffer).map_err(DeltaTableError::generic_err)?;
    let mut lines = text.lines();
    let mut values = Vec::with_capacity(stats_parsed.len());
    for row in 0..stats_parsed.len() {
        let line = lines.next().ok_or_else(|| {
            DeltaTableError::generic("stats JSON writer produced fewer rows than expected")
        })?;
        if stats_parsed.is_null(row) {
            values.push(None);
        } else {
            values.push(Some(line.to_string()));
        }
    }
    if lines.next().is_some() {
        return Err(DeltaTableError::generic(
            "stats JSON writer produced more rows than expected",
        ));
    }
    Ok(Arc::new(StringArray::from(values)))
}

fn effective_column_mapping_mode(
    table_schema: &StructType,
    explicit: ColumnMappingMode,
) -> ColumnMappingMode {
    if matches!(explicit, ColumnMappingMode::None)
        && table_schema.fields().any(|field| {
            field
                .metadata()
                .contains_key(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
                && field
                    .metadata()
                    .contains_key(ColumnMetadataKey::ColumnMappingId.as_ref())
        })
    {
        ColumnMappingMode::Name
    } else {
        explicit
    }
}

fn add_field_refs() -> DeltaResult<Vec<FieldRef>> {
    struct_field_refs(add_struct_type(), "add")
}

fn remove_field_refs() -> DeltaResult<Vec<FieldRef>> {
    struct_field_refs(remove_struct_type(), "remove")
}

fn struct_field_refs(schema: StructType, action_name: &str) -> DeltaResult<Vec<FieldRef>> {
    schema
        .fields()
        .map(|field| {
            Field::try_from(field)
                .map(|field| Arc::new(field) as FieldRef)
                .map_err(|err| {
                    DeltaTableError::schema(format!(
                        "{action_name} schema should convert to Arrow for checkpoint decode: {err}"
                    ))
                })
        })
        .collect()
}

fn replace_struct_column(
    batch: RecordBatch,
    column_idx: usize,
    original_field: &Field,
    column_name: &str,
    new_struct: StructArray,
) -> DeltaResult<RecordBatch> {
    let schema = batch.schema();
    let mut out_fields: Vec<FieldRef> = schema.fields().iter().cloned().collect();
    out_fields[column_idx] = Arc::new(Field::new(
        column_name,
        new_struct.data_type().clone(),
        original_field.is_nullable(),
    ));
    let mut out_cols = batch.columns().to_vec();
    out_cols[column_idx] = Arc::new(new_struct);
    Ok(RecordBatch::try_new(
        Arc::new(ArrowSchema::new(out_fields)),
        out_cols,
    )?)
}
