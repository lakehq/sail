use std::collections::HashSet;
use std::sync::Arc;

use arrow_schema::DataType as ArrowDataType;
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, StringArray, UInt64Array};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::scalar::ScalarValue;
use datafusion::common::Column;
use datafusion::physical_optimizer::pruning::PruningStatistics;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::Expression;
use delta_kernel::schema::{DataType, PrimitiveType};
use delta_kernel::{EngineData, EvaluationHandler, ExpressionEvaluator};
use deltalake::kernel::{EagerSnapshot, Metadata, StructType};
use deltalake::{DeltaResult, DeltaTableError};

use crate::kernel::ARROW_HANDLER;

/// Helper function to convert Box<dyn EngineData> back to RecordBatch
fn engine_data_to_record_batch(engine_data: Box<dyn EngineData>) -> DeltaResult<RecordBatch> {
    let arrow_data = ArrowEngineData::try_from_engine_data(engine_data)
        .map_err(|e| DeltaTableError::Generic(e.to_string()))?;
    Ok(arrow_data.into())
}

/// Extension trait to simplify evaluating expressions with RecordBatch
pub(crate) trait ExpressionEvaluatorExt {
    fn evaluate_arrow(&self, batch: RecordBatch) -> DeltaResult<RecordBatch>;
}

impl<T: ExpressionEvaluator + ?Sized> ExpressionEvaluatorExt for T {
    fn evaluate_arrow(&self, batch: RecordBatch) -> DeltaResult<RecordBatch> {
        let engine_data = ArrowEngineData::new(batch);
        let result_data = self
            .evaluate(&engine_data)
            .map_err(|e| DeltaTableError::Generic(e.to_string()))?;
        engine_data_to_record_batch(result_data)
    }
}

/// A PruningStatistics implementation that holds the data directly to work around
/// private field access limitations of LogDataHandler
#[derive(Debug)]
pub struct SailLogDataHandler {
    data: Vec<RecordBatch>,
    metadata: Metadata,
    schema: StructType,
}
use deltalake::kernel::Snapshot;
use deltalake::logstore::LogStoreRef;
use deltalake::DeltaTableConfig;
use futures::TryStreamExt;

impl SailLogDataHandler {
    pub async fn new(
        log_store: LogStoreRef,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        let snapshot = Snapshot::try_new(log_store.as_ref(), config.clone(), version).await?;

        let files: Vec<RecordBatch> = snapshot
            .files(log_store.as_ref(), &mut vec![])?
            .try_collect()
            .await?;

        let metadata = snapshot.metadata().clone();
        let schema = snapshot.schema().clone();

        Ok(Self {
            data: files,
            metadata,
            schema,
        })
    }

    /// Extract statistics from the pre-parsed log data following delta-rs pattern
    fn pick_stats(&self, column: &Column, stats_field: &'static str) -> Option<ArrayRef> {
        let field = self.schema.field(&column.name)?;

        // Binary type does not support natural order which is required for DataFusion to prune
        if field.data_type() == &DataType::Primitive(PrimitiveType::Binary) {
            return None;
        }

        let expression = if self.metadata.partition_columns().contains(&column.name) {
            Expression::column(["add", "partitionValues_parsed", &column.name])
        } else {
            Expression::column(["add", "stats_parsed", stats_field, &column.name])
        };

        let evaluator = ARROW_HANDLER.new_expression_evaluator(
            crate::kernel::models::fields::log_schema_ref().clone(),
            expression,
            field.data_type().clone(),
        );

        let mut results = Vec::new();

        for batch in self.data.iter() {
            let result = evaluator.evaluate_arrow(batch.clone()).ok()?;
            results.push(result);
        }

        if results.is_empty() {
            return None;
        }

        let schema = results[0].schema();
        let batch = concat_batches(&schema, &results).ok()?;
        batch.column_by_name("output").cloned()
    }

    /// Get the number of containers (files) being pruned
    pub fn num_containers(&self) -> usize {
        self.data.iter().map(|batch| batch.num_rows()).sum()
    }
}

impl PruningStatistics for SailLogDataHandler {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.pick_stats(column, "minValues")
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.pick_stats(column, "maxValues")
    }

    fn num_containers(&self) -> usize {
        self.num_containers()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        if !self.metadata.partition_columns().contains(&column.name) {
            let counts = self.pick_stats(column, "nullCount")?;
            return datafusion::arrow::compute::cast(counts.as_ref(), &ArrowDataType::UInt64).ok();
        }

        // For partition columns, calculate null counts based on partition values
        let partition_values_array = self.pick_stats(column, "__dummy__")?;
        let row_counts = self.row_counts(column)?;
        let row_counts = row_counts.as_any().downcast_ref::<UInt64Array>()?;
        let mut null_counts = Vec::with_capacity(partition_values_array.len());

        for i in 0..partition_values_array.len() {
            let null_count = if partition_values_array.is_null(i) {
                row_counts.value(i)
            } else {
                0
            };
            null_counts.push(null_count);
        }

        Some(Arc::new(UInt64Array::from(null_counts)))
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        use std::sync::LazyLock;

        static ROW_COUNTS_EVAL: LazyLock<Arc<dyn ExpressionEvaluator>> = LazyLock::new(|| {
            ARROW_HANDLER.new_expression_evaluator(
                crate::kernel::models::fields::log_schema_ref().clone(),
                Expression::column(["add", "stats_parsed", "numRecords"]),
                DataType::Primitive(PrimitiveType::Long),
            )
        });

        let mut results = Vec::new();

        for batch in self.data.iter() {
            let result = ROW_COUNTS_EVAL.evaluate_arrow(batch.clone()).ok()?;
            results.push(result);
        }

        if results.is_empty() {
            return None;
        }

        let schema = results[0].schema();
        let batch = concat_batches(&schema, &results).ok()?;
        datafusion::arrow::compute::cast(batch.column_by_name("output")?, &ArrowDataType::UInt64)
            .ok()
    }

    // This function is optional but will optimize partition column pruning
    fn contained(&self, column: &Column, values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        if values.is_empty() || !self.metadata.partition_columns().contains(&column.name) {
            return None;
        }

        // Retrieve the partition values for the column
        let partition_values_array = self.pick_stats(column, "__dummy__")?;

        let partition_values_str = partition_values_array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(DeltaTableError::generic(
                "failed to downcast string result to StringArray.",
            ))
            .ok()?;

        let mut contains = Vec::with_capacity(partition_values_str.len());

        // Helper function to check if a partition value matches any of the target values
        fn check_scalar(pv: &str, value: &ScalarValue) -> bool {
            match value {
                ScalarValue::Utf8(Some(v))
                | ScalarValue::Utf8View(Some(v))
                | ScalarValue::LargeUtf8(Some(v)) => pv == v,
                ScalarValue::Dictionary(_, inner) => check_scalar(pv, inner),
                _ => value.to_string() == pv,
            }
        }

        for i in 0..partition_values_str.len() {
            if partition_values_str.is_null(i) {
                contains.push(false);
            } else {
                contains.push(
                    values
                        .iter()
                        .any(|scalar| check_scalar(partition_values_str.value(i), scalar)),
                );
            }
        }

        Some(BooleanArray::from(contains))
    }
}
