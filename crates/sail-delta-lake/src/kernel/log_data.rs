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

#[derive(Debug)]
pub struct EagerSnapshotPruningStatistics<'a> {
    snapshot: &'a EagerSnapshot,
    schema: arrow_schema::SchemaRef,
}

impl<'a> EagerSnapshotPruningStatistics<'a> {
    /// Create a new instance wrapping an existing EagerSnapshot
    pub fn new(snapshot: &'a EagerSnapshot, schema: arrow_schema::SchemaRef) -> Self {
        Self { snapshot, schema }
    }

        /// Get the number of containers (Add actions) in the snapshot
    fn get_num_containers(&self) -> usize {
        // In PruningStatistics, num_containers() represents the number of containers (files)
        // being pruned. This should match the total number of Add actions in the log data.
        // Each Add action represents one file, so we count the total number of files.
        match self.snapshot.file_actions() {
            Ok(file_iter) => file_iter.count(),
            Err(_) => 0,
        }
    }

    /// Extract statistics arrays by iterating through LogicalFile instances directly
    fn extract_stats_array(&self, column: &Column, extract_fn: impl Fn(&deltalake::kernel::LogicalFile) -> Option<ScalarValue>) -> Option<ArrayRef> {
        let log_data = self.snapshot.log_data();
        let values: Vec<ScalarValue> = log_data
            .into_iter()
            .map(|logical_file| {
                extract_fn(&logical_file).unwrap_or_else(|| {
                    // Get null value for the column type
                    let (_, field) = self.schema.column_with_name(&column.name).unwrap();
                    get_null_scalar_value(field.data_type())
                })
            })
            .collect();

        // Ensure the array length matches num_containers
        if values.len() != self.get_num_containers() {
            return None;
        }

        ScalarValue::iter_to_array(values).ok()
    }

    fn extract_partition_values(&self, column: &Column) -> Option<ArrayRef> {
        self.extract_stats_array(column, |logical_file| {
            if let Ok(pvs) = logical_file.partition_values() {
                if let Some(val) = pvs.get(column.name.as_str()) {
                    return self.kernel_scalar_to_datafusion_scalar(val);
                }
            }
            None
        })
    }

    /// Extract min values for a column
    fn extract_min_values(&self, column: &Column) -> Option<ArrayRef> {
        if self.snapshot.metadata().partition_columns().contains(&column.name) {
            return self.extract_partition_values(column);
        }
        self.extract_stats_array(column, |logical_file| {
            self.extract_scalar_from_stats(logical_file.min_values(), &column.name)
        })
    }

    /// Extract max values for a column
    fn extract_max_values(&self, column: &Column) -> Option<ArrayRef> {
        if self.snapshot.metadata().partition_columns().contains(&column.name) {
            return self.extract_partition_values(column);
        }
        self.extract_stats_array(column, |logical_file| {
            self.extract_scalar_from_stats(logical_file.max_values(), &column.name)
        })
    }

    /// Extract null counts for a column
    fn extract_null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let log_data = self.snapshot.log_data();
        let values: Vec<u64> = log_data
            .into_iter()
            .map(|logical_file| {
                // Check if it's a partition column
                if self.snapshot.metadata().partition_columns().contains(&column.name) {
                    // For partition columns, if the partition value is null, return the row count
                    // Otherwise return 0
                    if let Ok(partition_values) = logical_file.partition_values() {
                        if let Some(partition_value) = partition_values.get(column.name.as_str()) {
                            if partition_value.is_null() {
                                logical_file.num_records().unwrap_or(0) as u64
                            } else {
                                0
                            }
                        } else {
                            0
                        }
                    } else {
                        0
                    }
                } else {
                    // For regular columns, extract null count from statistics
                    self.extract_scalar_from_stats(logical_file.null_counts(), &column.name)
                        .and_then(|scalar| match scalar {
                            ScalarValue::Int64(Some(count)) => Some(count as u64),
                            ScalarValue::UInt64(Some(count)) => Some(count),
                            _ => None,
                        })
                        .unwrap_or(0)
                }
            })
            .collect();

        // Ensure the array length matches num_containers
        if values.len() != self.get_num_containers() {
            return None;
        }

        Some(Arc::new(UInt64Array::from(values)))
    }

    /// Extract row counts for a column
    fn extract_row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        let log_data = self.snapshot.log_data();
        let values: Vec<u64> = log_data
            .into_iter()
            .map(|logical_file| logical_file.num_records().unwrap_or(0) as u64)
            .collect();

        // Ensure the array length matches num_containers
        if values.len() != self.get_num_containers() {
            return None;
        }

        Some(Arc::new(UInt64Array::from(values)))
    }

    /// Helper function to extract a scalar value from nested statistics structure
    fn extract_scalar_from_stats(&self, stats_scalar: Option<delta_kernel::expressions::Scalar>, field_name: &str) -> Option<ScalarValue> {


        stats_scalar.and_then(|scalar| {
            self.find_nested_scalar(&scalar, field_name)
                .and_then(|nested_scalar| self.kernel_scalar_to_datafusion_scalar(nested_scalar))
        })
    }

        /// Recursively find a scalar value in nested structures by field name
    fn find_nested_scalar<'b>(
        &self,
        scalar: &'b delta_kernel::expressions::Scalar,
        field_name: &str,
    ) -> Option<&'b delta_kernel::expressions::Scalar> {
        use delta_kernel::expressions::Scalar;

        let mut parts = field_name.split('.');
        let mut current_scalar = scalar;

        while let Some(part) = parts.next() {
            match current_scalar {
                Scalar::Struct(struct_data) => {
                    if let Some((_, value)) = struct_data
                        .fields()
                        .iter()
                        .zip(struct_data.values().iter())
                        .find(|(field, _)| field.name() == part)
                    {
                        current_scalar = value;
                    } else {
                        return None;
                    }
                }
                _ => return None,
            }
        }
        Some(current_scalar)
    }

    /// Convert delta_kernel::Scalar to datafusion::ScalarValue
    fn kernel_scalar_to_datafusion_scalar(&self, kernel_scalar: &delta_kernel::expressions::Scalar) -> Option<ScalarValue> {
        use delta_kernel::expressions::Scalar;

        match kernel_scalar {
            Scalar::Null(_) => None,
            Scalar::Boolean(b) => Some(ScalarValue::Boolean(Some(*b))),
            Scalar::Byte(b) => Some(ScalarValue::Int8(Some(*b as i8))),
            Scalar::Short(s) => Some(ScalarValue::Int16(Some(*s))),
            Scalar::Integer(i) => Some(ScalarValue::Int32(Some(*i))),
            Scalar::Long(l) => Some(ScalarValue::Int64(Some(*l))),
            Scalar::Float(f) => Some(ScalarValue::Float32(Some(*f))),
            Scalar::Double(d) => Some(ScalarValue::Float64(Some(*d))),
            Scalar::String(s) => Some(ScalarValue::Utf8(Some(s.clone()))),
            Scalar::Binary(b) => Some(ScalarValue::Binary(Some(b.clone()))),
            Scalar::Date(d) => Some(ScalarValue::Date32(Some(*d))),
            Scalar::Timestamp(ts) => Some(ScalarValue::TimestampMicrosecond(Some(*ts), None)),
            Scalar::TimestampNtz(ts) => Some(ScalarValue::TimestampMicrosecond(Some(*ts), None)),
            // For complex types, return None for now
            _ => None,
        }
    }

        /// Check if any partition values match the given set for contained() implementation
    fn check_partition_contained(&self, column: &Column, values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        if !self.snapshot.metadata().partition_columns().contains(&column.name) || values.is_empty() {
            return None;
        }

        let log_data = self.snapshot.log_data();
        let contains: Vec<bool> = log_data
            .into_iter()
            .map(|logical_file| {
                if let Ok(partition_values) = logical_file.partition_values() {
                    if let Some(partition_value) = partition_values.get(column.name.as_str()) {
                        if let Some(df_scalar) = self.kernel_scalar_to_datafusion_scalar(partition_value) {
                            return values.contains(&df_scalar);
                        }
                    }
                }
                false
            })
            .collect();

        // Ensure the array length matches num_containers
        if contains.len() != self.get_num_containers() {
            return None;
        }

        Some(BooleanArray::from(contains))
    }
}

/// Helper function to get a null ScalarValue for a given Arrow DataType
fn get_null_scalar_value(data_type: &ArrowDataType) -> ScalarValue {
    match data_type {
        ArrowDataType::Boolean => ScalarValue::Boolean(None),
        ArrowDataType::Int8 => ScalarValue::Int8(None),
        ArrowDataType::Int16 => ScalarValue::Int16(None),
        ArrowDataType::Int32 => ScalarValue::Int32(None),
        ArrowDataType::Int64 => ScalarValue::Int64(None),
        ArrowDataType::UInt8 => ScalarValue::UInt8(None),
        ArrowDataType::UInt16 => ScalarValue::UInt16(None),
        ArrowDataType::UInt32 => ScalarValue::UInt32(None),
        ArrowDataType::UInt64 => ScalarValue::UInt64(None),
        ArrowDataType::Float32 => ScalarValue::Float32(None),
        ArrowDataType::Float64 => ScalarValue::Float64(None),
        ArrowDataType::Utf8 => ScalarValue::Utf8(None),
        ArrowDataType::LargeUtf8 => ScalarValue::LargeUtf8(None),
        ArrowDataType::Binary => ScalarValue::Binary(None),
        ArrowDataType::Date32 => ScalarValue::Date32(None),
        ArrowDataType::Date64 => ScalarValue::Date64(None),
        ArrowDataType::Timestamp(unit, tz) => match unit {
            arrow_schema::TimeUnit::Second => ScalarValue::TimestampSecond(None, tz.clone()),
            arrow_schema::TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(None, tz.clone()),
            arrow_schema::TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(None, tz.clone()),
            arrow_schema::TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(None, tz.clone()),
        },
        _ => ScalarValue::Null,
    }
}

impl<'a> PruningStatistics for EagerSnapshotPruningStatistics<'a> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.extract_min_values(column)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.extract_max_values(column)
    }

    fn num_containers(&self) -> usize {
        self.get_num_containers()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.extract_null_counts(column)
    }

    fn row_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.extract_row_counts(column)
    }

    fn contained(&self, column: &Column, values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        self.check_partition_contained(column, values)
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
