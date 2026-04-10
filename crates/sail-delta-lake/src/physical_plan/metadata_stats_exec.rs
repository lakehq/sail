use std::any::Any;
use std::fmt;
use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{new_null_array, ArrayRef, StringArray, StructArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::json::ReaderBuilder as JsonReaderBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use futures::TryStreamExt;

use crate::spec::fields::{FIELD_NAME_STATS_PARSED, STATS_FIELD_MIN_VALUES};

/// The column name used by the replay pipeline for the raw JSON stats string.
const REPLAY_STATS_JSON_COLUMN: &str = "stats_json";

#[derive(Debug)]
pub struct DeltaMetadataStatsExec {
    input: Arc<dyn ExecutionPlan>,
    stats_schema: SchemaRef,
    output_schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl DeltaMetadataStatsExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, stats_schema: SchemaRef) -> Self {
        let mut fields = input.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(
            FIELD_NAME_STATS_PARSED,
            DataType::Struct(stats_schema.fields().clone()),
            true,
        )));
        let output_schema = Arc::new(Schema::new(fields));
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            input.output_partitioning().clone(),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            input,
            stats_schema,
            output_schema,
            cache,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn stats_schema(&self) -> &SchemaRef {
        &self.stats_schema
    }

    /// Returns the data column names tracked in the stats schema.
    /// These are extracted from the `minValues` sub-struct (if present),
    /// which lists all non-partition columns for which stats are collected.
    fn tracked_column_names(&self) -> Vec<&str> {
        self.stats_schema
            .field_with_name(STATS_FIELD_MIN_VALUES)
            .ok()
            .and_then(|f| {
                if let DataType::Struct(fields) = f.data_type() {
                    Some(fields.iter().map(|f| f.name().as_str()).collect())
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }

    fn parse_stats_array(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        // Priority 1: if the batch already has a typed `stats_parsed` struct column
        // (e.g. read from a checkpoint that persists stats in struct form), use it directly.
        if let Some(existing) = batch.column_by_name(FIELD_NAME_STATS_PARSED) {
            if matches!(existing.data_type(), DataType::Struct(_)) {
                return Ok(Arc::clone(existing));
            }
        }

        // Priority 2: parse from the replay pipeline's `stats_json` column.
        let Some(stats_json_col) = batch.column_by_name(REPLAY_STATS_JSON_COLUMN) else {
            return Ok(new_null_array(
                &DataType::Struct(self.stats_schema.fields().clone()),
                batch.num_rows(),
            ));
        };

        let stats_json_col = cast(stats_json_col, &DataType::Utf8)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let stats_json = stats_json_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "metadata stats exec expects Utf8-compatible stats_json column".to_string(),
                )
            })?;

        let estimated_json_bytes = stats_json
            .iter()
            .map(|value| value.map_or(2, str::len) + 1)
            .sum();
        let mut json_lines = String::with_capacity(estimated_json_bytes);
        for value in stats_json.iter() {
            if let Some(value) = value {
                json_lines.push_str(value);
            } else {
                json_lines.push_str("{}");
            }
            json_lines.push('\n');
        }

        let mut reader = JsonReaderBuilder::new(Arc::clone(&self.stats_schema))
            .with_batch_size(batch.num_rows().max(1))
            .build(Cursor::new(json_lines))
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let parsed_batch = match reader.next() {
            Some(batch) => batch.map_err(|e| DataFusionError::External(Box::new(e)))?,
            None => RecordBatch::new_empty(Arc::clone(&self.stats_schema)),
        };
        let stats_array: Arc<StructArray> = Arc::new(parsed_batch.into());
        Ok(stats_array)
    }
}

#[async_trait]
impl ExecutionPlan for DeltaMetadataStatsExec {
    fn name(&self) -> &'static str {
        "DeltaMetadataStatsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!(
                "DeltaMetadataStatsExec requires exactly one child when used as a unary node"
            );
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.stats_schema),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = Arc::clone(&self.output_schema);
        let input_stream = self.input.execute(partition, context)?;
        let exec = self.clone();
        let stream_schema = Arc::clone(&schema);

        let stream = input_stream.try_filter_map(move |batch| {
            let exec = exec.clone();
            let stream_schema = Arc::clone(&stream_schema);
            async move {
                if batch.num_rows() == 0 {
                    return Ok(None);
                }

                let stats_array = exec.parse_stats_array(&batch)?;
                let mut columns = batch.columns().to_vec();
                columns.push(stats_array);
                let output = RecordBatch::try_new(Arc::clone(&stream_schema), columns)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                Ok(Some(output))
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

impl DisplayAs for DeltaMetadataStatsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let columns = self.tracked_column_names().join(", ");
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "DeltaMetadataStatsExec(output={FIELD_NAME_STATS_PARSED}, columns=[{columns}])"
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "output={FIELD_NAME_STATS_PARSED}, columns=[{columns}]")
            }
        }
    }
}

impl Clone for DeltaMetadataStatsExec {
    fn clone(&self) -> Self {
        Self {
            input: Arc::clone(&self.input),
            stats_schema: Arc::clone(&self.stats_schema),
            output_schema: Arc::clone(&self.output_schema),
            cache: self.cache.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, Int32Array, Int64Array};
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;
    use crate::datasource::PATH_COLUMN;
    use crate::spec::fields::{
        STATS_FIELD_MAX_VALUES, STATS_FIELD_MIN_VALUES, STATS_FIELD_NUM_RECORDS,
    };

    fn stats_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(STATS_FIELD_NUM_RECORDS, DataType::Int64, true),
            Field::new(
                STATS_FIELD_MIN_VALUES,
                DataType::Struct(vec![Arc::new(Field::new("value", DataType::Int32, true))].into()),
                true,
            ),
            Field::new(
                STATS_FIELD_MAX_VALUES,
                DataType::Struct(vec![Arc::new(Field::new("value", DataType::Int32, true))].into()),
                true,
            ),
        ]))
    }

    #[test]
    fn parses_stats_json_into_typed_struct_column() -> Result<()> {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new(PATH_COLUMN, DataType::Utf8, false),
            Field::new(REPLAY_STATS_JSON_COLUMN, DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![
                Arc::new(StringArray::from(vec![Some("file.parquet")])),
                Arc::new(StringArray::from(vec![Some(
                    r#"{"numRecords":3,"minValues":{"value":1},"maxValues":{"value":7}}"#,
                )])),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let exec = DeltaMetadataStatsExec::new(
            Arc::new(EmptyExec::new(Arc::clone(&input_schema))),
            stats_schema(),
        );
        let parsed = exec.parse_stats_array(&batch)?;
        let stats = parsed
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("expected parsed stats struct column".to_string())
            })?;
        let num_records = stats
            .column_by_name(STATS_FIELD_NUM_RECORDS)
            .and_then(|col| col.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| {
                DataFusionError::Internal("expected numRecords Int64 array".to_string())
            })?;
        let min_values = stats
            .column_by_name(STATS_FIELD_MIN_VALUES)
            .and_then(|col| col.as_any().downcast_ref::<StructArray>())
            .ok_or_else(|| {
                DataFusionError::Internal("expected minValues struct array".to_string())
            })?;
        let min_value = min_values
            .column_by_name("value")
            .and_then(|col| col.as_any().downcast_ref::<Int32Array>())
            .ok_or_else(|| {
                DataFusionError::Internal("expected minValues.value Int32 array".to_string())
            })?;

        assert_eq!(num_records.value(0), 3);
        assert_eq!(min_value.value(0), 1);
        Ok(())
    }

    #[test]
    fn reuses_existing_stats_parsed_struct_without_json_parse() -> Result<()> {
        let typed_stats = StructArray::from(vec![(
            Arc::new(Field::new(STATS_FIELD_NUM_RECORDS, DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![Some(42)])) as Arc<_>,
        )]);

        let input_schema = Arc::new(Schema::new(vec![
            Field::new(PATH_COLUMN, DataType::Utf8, false),
            Field::new(
                FIELD_NAME_STATS_PARSED,
                typed_stats.data_type().clone(),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![
                Arc::new(StringArray::from(vec![Some("file.parquet")])),
                Arc::new(typed_stats),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        // The exec's stats_schema only has numRecords; no stats_json column exists.
        let exec = DeltaMetadataStatsExec::new(
            Arc::new(EmptyExec::new(Arc::clone(&input_schema))),
            stats_schema(),
        );
        let parsed = exec.parse_stats_array(&batch)?;
        let stats = parsed
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Internal("expected struct array".to_string()))?;
        let num_records = stats
            .column_by_name(STATS_FIELD_NUM_RECORDS)
            .and_then(|col| col.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| {
                DataFusionError::Internal("expected numRecords Int64 array".to_string())
            })?;
        assert_eq!(num_records.value(0), 42);
        Ok(())
    }
}
