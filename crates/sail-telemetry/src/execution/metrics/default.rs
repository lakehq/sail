use datafusion::physical_plan::Metric;
use datafusion::physical_plan::metrics::{Label, MetricValue};
use sail_common::telemetry::KeyValue;

use crate::execution::metrics::{MetricEmitter, MetricHandled};
use crate::metrics::{MetricAttribute, MetricRegistry};

/// A default metric emitter that handles common execution metrics.
/// This emitter must be used at last since it increments the unknown metric counter
/// for all unknown metrics in debug builds.
pub struct DefaultMetricEmitter;

impl MetricEmitter for DefaultMetricEmitter {
    fn try_emit(
        &self,
        metric: &Metric,
        attributes: &[KeyValue],
        registry: &MetricRegistry,
    ) -> MetricHandled {
        match metric.value() {
            MetricValue::OutputRows(count) => {
                registry
                    .execution_output_row_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
            }
            MetricValue::ElapsedCompute(time) => {
                registry
                    .execution_elapsed_compute_time
                    .recorder(time)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
            }
            MetricValue::SpillCount(count) => {
                registry
                    .execution_spill_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
            }
            MetricValue::SpilledBytes(count) => {
                registry
                    .execution_spill_size
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
            }
            MetricValue::OutputBytes(count) => {
                registry
                    .execution_output_size
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
            }
            MetricValue::SpilledRows(count) => {
                registry
                    .execution_spill_row_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
            }
            MetricValue::CurrentMemoryUsage(gauge) => {
                registry
                    .execution_memory_used
                    .recorder(gauge)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
            }
            MetricValue::OutputBatches(count) => {
                registry
                    .execution_output_batch_count
                    .recorder(count)
                    .with_attributes(attributes)
                    .with_optional_attribute(
                        MetricAttribute::EXECUTION_PARTITION,
                        metric.partition(),
                    )
                    .emit();
            }
            // Peak-memory metrics are handled only by emitters that recognize both the physical
            // operator and the DataFusion metric name.
            MetricValue::PeakMemoryUsage { .. }
            | MetricValue::Count { .. }
            | MetricValue::Gauge { .. }
            | MetricValue::Time { .. }
            | MetricValue::Ratio { .. }
            | MetricValue::PruningMetrics { .. }
            | MetricValue::Custom { .. } => {
                // These metric types are not yet handled by any emitter.
                #[cfg(debug_assertions)]
                registry.execution_unknown_metric_count.adder(1u64).emit();
            }
            MetricValue::StartTimestamp(_) | MetricValue::EndTimestamp(_) => {
                // The timestamp is ignored since it is not a metric.
                // We record timestamps via our tracing spans instead.
            }
        }
        // The default emitter always claims to have handled the metric.
        MetricHandled::Yes
    }
}

#[derive(Default)]
pub struct LabelExtractor {
    extractors: Vec<(&'static str, &'static str)>,
}

impl LabelExtractor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_extractor(mut self, from: &'static str, to: &'static str) -> Self {
        self.extractors.push((from, to));
        self
    }

    pub fn extract(&self, labels: &[Label], registry: &MetricRegistry) -> Vec<KeyValue> {
        #[cfg(not(debug_assertions))]
        let _ = registry;
        let mut attributes = vec![];
        'outer: for label in labels {
            for (from, to) in &self.extractors {
                if label.name() == *from {
                    attributes.push((*to, label.value().to_string().into()));
                    continue 'outer;
                }
            }
            #[cfg(debug_assertions)]
            registry
                .execution_unknown_metric_label_count
                .adder(1u64)
                .emit();
        }
        attributes
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::Result;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::parquet::arrow::ArrowWriter;

    use crate::execution::metrics::testing::MetricEmitterTester;

    fn write_parquet_file(
        dir: &Path,
        name: &str,
        schema: SchemaRef,
        rows: i64,
    ) -> Result<PartitionedFile> {
        let path = dir.join(name);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(0..rows))],
        )?;
        let mut writer = ArrowWriter::try_new(std::fs::File::create(&path)?, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        let size = std::fs::metadata(&path)?.len();
        Ok(PartitionedFile::new(
            path.to_string_lossy().trim_start_matches('/'),
            size,
        ))
    }

    /// Returns a tester for a scan of two files, with 100 and 50 rows, in one partition.
    fn two_file_scan_tester(dir: &Path) -> Result<MetricEmitterTester> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let files = vec![
            write_parquet_file(dir, "a.parquet", schema.clone(), 100)?,
            write_parquet_file(dir, "b.parquet", schema.clone(), 50)?,
        ];
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(ParquetSource::new(schema)),
        )
        .with_file_group(files.into())
        .build();
        Ok(MetricEmitterTester::new()
            .with_plan(DataSourceExec::from_data_source(config))
            .with_baseline_metrics()
            .with_unknown_metrics())
    }

    #[tokio::test]
    async fn test_scan_output_rows_include_all_files() -> Result<()> {
        let dir = tempfile::tempdir()?;
        two_file_scan_tester(dir.path())?
            .with_expected_value(|r| r.execution_output_row_count.name(), |v| v == 150)
            .run()
            .await
    }

    #[tokio::test]
    async fn test_scan_output_batch_count_includes_all_files() -> Result<()> {
        let dir = tempfile::tempdir()?;
        two_file_scan_tester(dir.path())?
            .with_expected_value(|r| r.execution_output_batch_count.name(), |v| v == 2)
            .run()
            .await
    }

    #[tokio::test]
    async fn test_scan_output_size_includes_all_files() -> Result<()> {
        let dir = tempfile::tempdir()?;
        two_file_scan_tester(dir.path())?
            .with_expected_value(|r| r.execution_output_size.name(), |v| v > 0)
            .run()
            .await
    }
}
