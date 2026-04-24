// Post-processing for checkpoint Arrow batches that augments the `add`
// struct column with optional `stats_parsed` and `partitionValues_parsed`
// substructs when the table property `delta.checkpoint.writeStatsAsStruct`
// is enabled, and/or removes the `stats` JSON column when
// `delta.checkpoint.writeStatsAsJson` is disabled.
//
// The Delta protocol requires `partitionValues_parsed` to be present when
// the table is partitioned and `writeStatsAsStruct=true`; for unpartitioned
// tables the column is omitted.

use std::sync::Arc;

use datafusion::arrow::array::{new_null_array, Array, ArrayRef, StringArray, StructArray};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, FieldRef, Fields, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef,
};
use datafusion::arrow::record_batch::RecordBatch;

use crate::kernel::snapshot::materialize::parse_partition_values_array;
use crate::spec::fields::{FIELD_NAME_PARTITION_VALUES_PARSED, FIELD_NAME_STATS_PARSED};
use crate::spec::{
    parse_stats_json_array, stats_schema, ColumnMappingMode, DeltaError as DeltaTableError,
    DeltaResult, Metadata, StructField, StructType, TableProperties,
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
        let write_stats_as_struct = properties.checkpoint_write_stats_as_struct.unwrap_or(false);
        let write_stats_as_json = properties.checkpoint_write_stats_as_json.unwrap_or(true);
        let column_mapping_mode = properties
            .column_mapping_mode
            .unwrap_or(ColumnMappingMode::None);

        let (stats_parsed_schema, partition_values_parsed_schema) = if write_stats_as_struct {
            let table_schema = metadata.parse_schema()?;
            let partition_cols = metadata.partition_columns();

            let non_partition_fields: Vec<StructField> = table_schema
                .fields()
                .filter(|f| !partition_cols.contains(&f.name().to_string()))
                .cloned()
                .collect();
            let non_partition_schema = StructType::try_new(non_partition_fields)?;
            let stats_struct = stats_schema(&non_partition_schema, &properties)?;
            let stats_arrow = Arc::new(ArrowSchema::try_from(&stats_struct)?);

            let partition_schema = if partition_cols.is_empty() {
                None
            } else {
                let partition_fields: Vec<StructField> = partition_cols
                    .iter()
                    .map(|col| {
                        table_schema
                            .fields()
                            .find(|f| f.name() == col)
                            .cloned()
                            .ok_or_else(|| DeltaTableError::missing_column(col))
                    })
                    .collect::<DeltaResult<Vec<_>>>()?;
                Some(StructType::try_new(partition_fields)?)
            };
            (Some(stats_arrow), partition_schema)
        } else {
            (None, None)
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
            Some(v) => v,
            None => return Ok(batch),
        };
        let add_col = batch.column(add_idx);
        let add_struct = add_col
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DeltaTableError::schema("expected add to be a struct column"))?;

        let mut add_fields: Vec<FieldRef> = add_struct.fields().iter().cloned().collect();
        let mut add_cols: Vec<ArrayRef> = add_struct.columns().to_vec();
        let nulls = add_struct.nulls().cloned();
        let num_rows = add_struct.len();

        let stats_idx = add_fields
            .iter()
            .position(|f| f.name() == "stats")
            .ok_or_else(|| DeltaTableError::schema("add.stats field missing"))?;

        if self.write_stats_as_struct {
            let stats_json = add_cols[stats_idx]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DeltaTableError::schema("add.stats must be Utf8"))?;
            let stats_parsed_schema = self.stats_parsed_schema.as_ref().ok_or_else(|| {
                DeltaTableError::schema(
                    "stats_parsed_schema must be present when write_stats_as_struct is true",
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
                .position(|f| f.name() == "partitionValues")
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
            let pv_parsed = parse_partition_values_array(
                &inner_batch,
                partition_schema,
                "partitionValues",
                self.column_mapping_mode,
            )?;
            add_fields.push(Arc::new(Field::new(
                FIELD_NAME_PARTITION_VALUES_PARSED,
                pv_parsed.data_type().clone(),
                false,
            )));
            add_cols.push(Arc::new(pv_parsed));
        }

        if !self.write_stats_as_json {
            add_cols[stats_idx] = new_null_array(&ArrowDataType::Utf8, num_rows);
        }

        let new_add = StructArray::try_new(Fields::from(add_fields), add_cols, nulls)?;

        let mut out_fields: Vec<FieldRef> = schema.fields().iter().cloned().collect();
        out_fields[add_idx] = Arc::new(Field::new(
            "add",
            new_add.data_type().clone(),
            add_field.is_nullable(),
        ));
        let mut out_cols: Vec<ArrayRef> = batch.columns().to_vec();
        out_cols[add_idx] = Arc::new(new_add);
        Ok(RecordBatch::try_new(
            Arc::new(ArrowSchema::new(out_fields)),
            out_cols,
        )?)
    }
}
