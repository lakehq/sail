use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, StringArray, StructArray, new_null_array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use futures::TryStreamExt;

use crate::datasource::pruning::widen_timestamp_max_stat;
use crate::schema::type_widening::is_supported_type_change;
use crate::spec::fields::{
    FIELD_NAME_STATS_PARSED, STATS_FIELD_MAX_VALUES, STATS_FIELD_MIN_VALUES,
};
use crate::spec::{DataType as DeltaDataType, parse_stats_json_array};

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
        let input_schema = input.schema();
        let normalized_stats_field = Arc::new(Field::new(
            FIELD_NAME_STATS_PARSED,
            DataType::Struct(stats_schema.fields().clone()),
            true,
        ));
        let mut fields = Vec::with_capacity(input_schema.fields().len() + 1);
        let mut stats_field_inserted = false;
        for field in input_schema.fields() {
            if field.name() == FIELD_NAME_STATS_PARSED {
                if !stats_field_inserted {
                    fields.push(Arc::clone(&normalized_stats_field));
                    stats_field_inserted = true;
                }
            } else {
                fields.push(Arc::clone(field));
            }
        }
        if !stats_field_inserted {
            fields.push(normalized_stats_field);
        }
        let output_schema = Arc::new(Schema::new_with_metadata(
            fields,
            input_schema.metadata().clone(),
        ));
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
        // (e.g. read from a checkpoint that persists stats in struct form), normalize it.
        if let Some(existing) = batch.column_by_name(FIELD_NAME_STATS_PARSED) {
            let stats_struct = existing
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    metadata_stats_schema_error(format!(
                        "metadata stats column {FIELD_NAME_STATS_PARSED} has incompatible type {}; expected struct",
                        existing.data_type()
                    ))
                })?;
            let normalized = normalize_metadata_stats_struct(
                stats_struct,
                self.stats_schema.fields(),
                FIELD_NAME_STATS_PARSED,
            )?;
            return Ok(widen_timestamp_max_values(normalized));
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

        let stats_array: ArrayRef =
            Arc::new(parse_stats_json_array(stats_json, &self.stats_schema)?);
        Ok(widen_timestamp_max_values(stats_array))
    }
}

fn metadata_stats_schema_error(message: impl Into<String>) -> DataFusionError {
    ArrowError::SchemaError(message.into()).into()
}

fn normalize_metadata_stats_struct(
    source_stats: &StructArray,
    target_fields: &Fields,
    stats_path: &str,
) -> Result<ArrayRef> {
    let mut normalized_columns = Vec::with_capacity(target_fields.len());
    for target_field in target_fields {
        let field_path = format!("{stats_path}.{}", target_field.name());
        let Some(source_column) = source_stats.column_by_name(target_field.name()) else {
            if !target_field.is_nullable() {
                return Err(metadata_stats_schema_error(format!(
                    "metadata stats field {field_path} is missing but is not nullable"
                )));
            }
            normalized_columns.push(new_null_array(target_field.data_type(), source_stats.len()));
            continue;
        };

        let normalized_column = match (source_column.data_type(), target_field.data_type()) {
            (DataType::Struct(_), DataType::Struct(nested_target_fields)) => {
                let source_struct = source_column
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        metadata_stats_schema_error(format!(
                            "metadata stats field {field_path} has a struct type but is not a StructArray"
                        ))
                    })?;
                normalize_metadata_stats_struct(source_struct, nested_target_fields, &field_path)?
            }
            (source_type, target_type) if source_type == target_type => Arc::clone(source_column),
            (source_type, target_type)
                if DeltaDataType::try_from(source_type)
                    .ok()
                    .zip(DeltaDataType::try_from(target_type).ok())
                    .is_some_and(|(source_delta_type, target_delta_type)| {
                        source_delta_type != target_delta_type
                            && is_supported_type_change(&source_delta_type, &target_delta_type)
                    }) =>
            {
                cast(source_column, target_type).map_err(|error| {
                    metadata_stats_schema_error(format!(
                        "metadata stats field {field_path} cannot be widened from {source_type} to {target_type}: {error}"
                    ))
                })?
            }
            (source_type, target_type) => {
                return Err(metadata_stats_schema_error(format!(
                    "metadata stats field {field_path} has incompatible type {source_type}; expected {target_type}"
                )));
            }
        };
        normalized_columns.push(normalized_column);
    }

    let normalized = StructArray::try_new_with_length(
        target_fields.clone(),
        normalized_columns,
        source_stats.nulls().cloned(),
        source_stats.len(),
    )?;
    Ok(Arc::new(normalized))
}

fn widen_timestamp_max_values(stats: ArrayRef) -> ArrayRef {
    let Some(stats_struct) = stats.as_any().downcast_ref::<StructArray>() else {
        return stats;
    };
    let columns = stats_struct
        .fields()
        .iter()
        .zip(stats_struct.columns())
        .map(|(field, column)| {
            if field.name() == STATS_FIELD_MAX_VALUES {
                widen_timestamp_max_stat(Arc::clone(column))
            } else {
                Arc::clone(column)
            }
        })
        .collect();
    Arc::new(StructArray::new(
        stats_struct.fields().clone(),
        columns,
        stats_struct.nulls().cloned(),
    ))
}

#[async_trait]
impl ExecutionPlan for DeltaMetadataStatsExec {
    fn name(&self) -> &'static str {
        "DeltaMetadataStatsExec"
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
                let batch_schema = batch.schema();
                let mut columns = Vec::with_capacity(stream_schema.fields().len());
                let mut stats_column_inserted = false;
                for (field, column) in batch_schema.fields().iter().zip(batch.columns()) {
                    if field.name() == FIELD_NAME_STATS_PARSED {
                        if !stats_column_inserted {
                            columns.push(Arc::clone(&stats_array));
                            stats_column_inserted = true;
                        }
                    } else {
                        columns.push(Arc::clone(column));
                    }
                }
                if !stats_column_inserted {
                    columns.push(stats_array);
                }
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
    use datafusion::arrow::array::{Array, Int16Array, Int32Array, Int64Array};
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::collect;

    use super::*;
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

    struct ExecutionCase {
        name: &'static str,
        batch: RecordBatch,
        expected_stats_validity: Vec<bool>,
        expected_num_records: Vec<Option<i64>>,
        expected_min_values: Vec<Option<i32>>,
    }

    async fn collect_stats_output(
        batch: RecordBatch,
        stats_schema: SchemaRef,
    ) -> Result<RecordBatch> {
        let input_schema = batch.schema();
        let input: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[vec![batch]], input_schema, None)?;
        let exec: Arc<dyn ExecutionPlan> =
            Arc::new(DeltaMetadataStatsExec::new(input, stats_schema));
        collect(exec, Arc::new(TaskContext::default()))
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Internal("expected output batch".to_string()))
    }

    #[tokio::test]
    async fn execution_materializes_json_and_typed_stats_without_duplication() -> Result<()> {
        let target_stats_schema = stats_schema();
        let json_input_schema = Arc::new(Schema::new(vec![Field::new(
            REPLAY_STATS_JSON_COLUMN,
            DataType::Utf8,
            true,
        )]));
        let json_batch = RecordBatch::try_new(
            json_input_schema,
            vec![Arc::new(StringArray::from(vec![
                Some(r#"{"numRecords":3,"minValues":{"value":1},"maxValues":{"value":7}}"#),
                None,
                Some(""),
            ])) as ArrayRef],
        )
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;

        let typed_stats: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new(STATS_FIELD_NUM_RECORDS, DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![Some(42)])) as Arc<_>,
        )]));
        let typed_input_schema = Arc::new(Schema::new(vec![Field::new(
            FIELD_NAME_STATS_PARSED,
            typed_stats.data_type().clone(),
            true,
        )]));
        let typed_batch = RecordBatch::try_new(typed_input_schema, vec![typed_stats])
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;

        let cases = vec![
            ExecutionCase {
                name: "JSON statistics",
                batch: json_batch,
                expected_stats_validity: vec![true, false, false],
                expected_num_records: vec![Some(3), None, None],
                expected_min_values: vec![Some(1), None, None],
            },
            ExecutionCase {
                name: "sparse typed statistics",
                batch: typed_batch,
                expected_stats_validity: vec![true],
                expected_num_records: vec![Some(42)],
                expected_min_values: vec![None],
            },
        ];

        for case in cases {
            let output = collect_stats_output(case.batch, Arc::clone(&target_stats_schema)).await?;
            let output_schema = output.schema();
            assert_eq!(
                output_schema
                    .fields()
                    .iter()
                    .filter(|field| field.name() == FIELD_NAME_STATS_PARSED)
                    .count(),
                1,
                "{}",
                case.name
            );
            let stats_field = output_schema
                .fields()
                .iter()
                .find(|field| field.name() == FIELD_NAME_STATS_PARSED)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "{} output is missing stats_parsed",
                        case.name
                    ))
                })?;
            assert_eq!(
                stats_field.data_type(),
                &DataType::Struct(target_stats_schema.fields().clone()),
                "{}",
                case.name
            );

            let stats = output
                .column_by_name(FIELD_NAME_STATS_PARSED)
                .and_then(|column| column.as_any().downcast_ref::<StructArray>())
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "{} output stats should be a struct",
                        case.name
                    ))
                })?;
            assert_eq!(
                (0..stats.len())
                    .map(|index| stats.is_valid(index))
                    .collect::<Vec<_>>(),
                case.expected_stats_validity,
                "{}",
                case.name
            );

            let num_records = stats
                .column_by_name(STATS_FIELD_NUM_RECORDS)
                .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| {
                    DataFusionError::Internal(format!("{} numRecords should be Int64", case.name))
                })?;
            assert_eq!(
                num_records.iter().collect::<Vec<_>>(),
                case.expected_num_records,
                "{}",
                case.name
            );

            let min_values = stats
                .column_by_name(STATS_FIELD_MIN_VALUES)
                .and_then(|column| column.as_any().downcast_ref::<StructArray>())
                .and_then(|values| values.column_by_name("value"))
                .and_then(|column| column.as_any().downcast_ref::<Int32Array>())
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "{} minValues.value should be Int32",
                        case.name
                    ))
                })?;
            assert_eq!(
                min_values.iter().collect::<Vec<_>>(),
                case.expected_min_values,
                "{}",
                case.name
            );
        }
        Ok(())
    }

    enum NormalizationExpectation {
        WidenedNestedInteger,
        Rejected(&'static str),
    }

    struct NormalizationCase {
        name: &'static str,
        source: StructArray,
        target_fields: Fields,
        expectation: NormalizationExpectation,
    }

    #[test]
    fn normalizes_metadata_stats_to_target_schema() -> Result<()> {
        let source_leaf_fields: Fields =
            vec![Arc::new(Field::new("a", DataType::Int16, true))].into();
        let source_min_values: ArrayRef = Arc::new(StructArray::new(
            source_leaf_fields,
            vec![Arc::new(Int16Array::from(vec![Some(1), Some(2)]))],
            Some(NullBuffer::from(vec![true, false])),
        ));
        let source_stats = StructArray::new(
            vec![Arc::new(Field::new(
                STATS_FIELD_MIN_VALUES,
                source_min_values.data_type().clone(),
                true,
            ))]
            .into(),
            vec![source_min_values],
            Some(NullBuffer::from(vec![true, false])),
        );
        let target_leaf_fields: Fields =
            vec![Arc::new(Field::new("a", DataType::Int32, true))].into();
        let widened_target_fields: Fields = vec![
            Arc::new(Field::new(STATS_FIELD_NUM_RECORDS, DataType::Int64, true)),
            Arc::new(Field::new(
                STATS_FIELD_MIN_VALUES,
                DataType::Struct(target_leaf_fields.clone()),
                true,
            )),
            Arc::new(Field::new(
                STATS_FIELD_MAX_VALUES,
                DataType::Struct(target_leaf_fields),
                true,
            )),
        ]
        .into();

        let narrowing_source = StructArray::from(vec![(
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(32_768)])) as Arc<_>,
        )]);
        let narrowing_target_fields: Fields =
            vec![Arc::new(Field::new("a", DataType::Int16, true))].into();

        let cases = vec![
            NormalizationCase {
                name: "nested widening and missing nullable fields",
                source: source_stats,
                target_fields: widened_target_fields,
                expectation: NormalizationExpectation::WidenedNestedInteger,
            },
            NormalizationCase {
                name: "integer narrowing",
                source: narrowing_source,
                target_fields: narrowing_target_fields,
                expectation: NormalizationExpectation::Rejected("incompatible type Int32"),
            },
        ];

        for case in cases {
            let normalized = normalize_metadata_stats_struct(
                &case.source,
                &case.target_fields,
                FIELD_NAME_STATS_PARSED,
            );
            match case.expectation {
                NormalizationExpectation::WidenedNestedInteger => {
                    let normalized = normalized?;
                    let stats = normalized
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "{} should produce a struct",
                                case.name
                            ))
                        })?;
                    assert_eq!(stats.fields(), &case.target_fields, "{}", case.name);
                    assert!(stats.is_valid(0), "{}", case.name);
                    assert!(stats.is_null(1), "{}", case.name);

                    let num_records = stats
                        .column_by_name(STATS_FIELD_NUM_RECORDS)
                        .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "{} numRecords should be Int64",
                                case.name
                            ))
                        })?;
                    assert!(num_records.is_null(0), "{}", case.name);
                    assert!(num_records.is_null(1), "{}", case.name);

                    let min_values = stats
                        .column_by_name(STATS_FIELD_MIN_VALUES)
                        .and_then(|column| column.as_any().downcast_ref::<StructArray>())
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "{} minValues should be a struct",
                                case.name
                            ))
                        })?;
                    let min_a = min_values
                        .column_by_name("a")
                        .and_then(|column| column.as_any().downcast_ref::<Int32Array>())
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "{} minValues.a should be widened to Int32",
                                case.name
                            ))
                        })?;
                    assert_eq!(min_a.value(0), 1, "{}", case.name);
                    assert!(min_values.is_valid(0), "{}", case.name);
                    assert!(min_values.is_null(1), "{}", case.name);

                    let max_values = stats
                        .column_by_name(STATS_FIELD_MAX_VALUES)
                        .and_then(|column| column.as_any().downcast_ref::<StructArray>())
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "{} maxValues should be a struct",
                                case.name
                            ))
                        })?;
                    assert!(max_values.is_null(0), "{}", case.name);
                    assert!(max_values.is_null(1), "{}", case.name);
                }
                NormalizationExpectation::Rejected(message) => {
                    let Err(error) = normalized else {
                        return Err(DataFusionError::Internal(format!(
                            "{} should be rejected",
                            case.name
                        )));
                    };
                    assert!(
                        error.to_string().contains(message),
                        "{} returned {error}",
                        case.name
                    );
                }
            }
        }
        Ok(())
    }
}
