/// Execution plan for Python DataSource batch reads.
///
/// This execution plan reads data from a Python datasource in parallel,
/// with one partition per InputPartition returned by the reader.
///
/// # Architecture
///
/// Uses `InProcessExecutor` for direct PyO3 calls. The executor is created
/// lazily in `execute()` rather than at construction time, which:
/// - Keeps the codec/serialization layer lightweight
/// - Ensures workers create their own executor at runtime
/// - Decouples construction from execution context
///
/// # Phase 6 Enhancements (Performance & Polish)
///
/// TODO: Expose partitioning metadata from Python datasources.
/// Currently uses `UnknownPartitioning` which is correct but prevents query optimizations.
/// If Python datasource provides hash/range partitioning info via an optional `partitioning()`
/// method, this could enable partition pruning and join optimization in DataFusion.
/// See: <https://docs.rs/datafusion/latest/datafusion/physical_expr/enum.Partitioning.html>
///
/// TODO: Integrate GIL metrics with DataFusion's MetricsSet.
/// `PythonExecutionMetrics` (gil_wait_ns, gil_hold_ns, etc.) are currently only logged.
/// Exposing via `fn metrics(&self) -> Option<MetricsSet>` would enable:
/// - EXPLAIN ANALYZE visibility
/// - Programmatic access via `ctx.collect_metrics()`
/// - UI dashboards for execution bottleneck visualization
///
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_err, internal_err, Result};

use super::executor::InputPartition;
use super::filter::PythonFilter;

/// Execution plan for reading from a Python datasource.
///
/// This is a source node (no children) that reads data in parallel
/// across multiple partitions. The executor is created lazily in
/// `execute()` to keep construction lightweight and enable proper
/// worker-side initialization in distributed mode.
#[derive(Debug)]
pub struct PythonDataSourceExec {
    /// Pickled Python DataSource instance
    command: Vec<u8>,
    /// Schema of the output data
    schema: SchemaRef,
    /// Partitions for parallel reading
    partitions: Vec<InputPartition>,
    /// Filters to push down
    filters: Vec<PythonFilter>,
    /// Execution plan properties
    properties: PlanProperties,
}

impl PythonDataSourceExec {
    /// Create a new execution plan.
    ///
    /// # Arguments
    ///
    /// * `command` - Pickled Python DataSource instance
    /// * `schema` - Schema of the output data
    /// * `partitions` - Partitions for parallel reading
    /// * `filters` - Filters to push down
    pub fn new(
        command: Vec<u8>,
        schema: SchemaRef,
        partitions: Vec<InputPartition>,
        filters: Vec<PythonFilter>,
    ) -> Self {
        let num_partitions = partitions.len().max(1);
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            command,
            schema,
            partitions,
            filters,
            properties,
        }
    }

    /// Get the number of partitions.
    pub fn num_partitions(&self) -> usize {
        self.partitions.len()
    }

    /// Get the pickled command.
    pub fn command(&self) -> &[u8] {
        &self.command
    }

    /// Get the partitions.
    pub fn partitions(&self) -> &[InputPartition] {
        &self.partitions
    }

    /// Get the filters.
    pub fn filters(&self) -> &[PythonFilter] {
        &self.filters
    }
}

impl DisplayAs for PythonDataSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PythonDataSourceExec: partitions={}",
            self.partitions.len()
        )
    }
}

impl ExecutionPlan for PythonDataSourceExec {
    fn name(&self) -> &'static str {
        "PythonDataSourceExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // Source node - no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("PythonDataSourceExec should have no children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Handle empty partitions case: return empty stream for partition 0
        if self.partitions.is_empty() {
            if partition == 0 {
                return Ok(Box::pin(
                    datafusion::physical_plan::stream::EmptyRecordBatchStream::new(
                        self.schema.clone(),
                    ),
                ));
            }
            return exec_err!(
                "partition index {} out of range for empty datasource",
                partition
            );
        }

        if partition >= self.partitions.len() {
            return exec_err!(
                "partition index {} out of range (0..{})",
                partition,
                self.partitions.len()
            );
        }

        // Get batch size from TaskContext session config
        // Get batch size from TaskContext session config
        let batch_size = context.session_config().batch_size();

        // Create executor lazily at execution time
        // This keeps codec/construction lightweight and ensures proper worker-side initialization
        let executor: Arc<dyn super::executor::PythonExecutor> =
            Arc::new(super::executor::InProcessExecutor::new());

        let command = self.command.clone();
        let part = self.partitions[partition].clone();
        let schema = self.schema.clone();

        // Create async stream that uses the executor
        // We need to handle the Result<BoxStream> properly
        use futures::TryStreamExt;
        let filters = self.filters.clone();
        let stream = futures::stream::once(async move {
            executor
                .execute_read(&command, &part, schema, filters, batch_size)
                .await
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

/// Adapter to convert a Stream<Item=Result<RecordBatch>> to SendableRecordBatchStream
struct RecordBatchStreamAdapter {
    schema: SchemaRef,
    inner: std::pin::Pin<Box<dyn futures::Stream<Item = Result<arrow::array::RecordBatch>> + Send>>,
}

impl RecordBatchStreamAdapter {
    fn new(
        schema: SchemaRef,
        stream: impl futures::Stream<Item = Result<arrow::array::RecordBatch>> + Send + 'static,
    ) -> Self {
        Self {
            schema,
            inner: Box::pin(stream),
        }
    }
}

impl futures::Stream for RecordBatchStreamAdapter {
    type Item = Result<arrow::array::RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl datafusion::physical_plan::RecordBatchStream for RecordBatchStreamAdapter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn test_python_datasource_exec_properties() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let partitions = vec![
            InputPartition {
                partition_id: 0,
                data: vec![1, 2, 3],
            },
            InputPartition {
                partition_id: 1,
                data: vec![4, 5, 6],
            },
        ];

        let exec = PythonDataSourceExec::new(vec![0, 0], schema.clone(), partitions, vec![]);

        assert_eq!(exec.num_partitions(), 2);
        assert_eq!(exec.children().len(), 0);
        assert_eq!(exec.name(), "PythonDataSourceExec");

        // Check properties
        let props = exec.properties();
        assert!(matches!(
            props.partitioning,
            Partitioning::UnknownPartitioning(2)
        ));
    }

    #[test]
    fn test_display_as() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let partitions = vec![
            InputPartition {
                partition_id: 0,
                data: vec![],
            },
            InputPartition {
                partition_id: 1,
                data: vec![],
            },
            InputPartition {
                partition_id: 2,
                data: vec![],
            },
        ];

        let exec = PythonDataSourceExec::new(vec![], schema, partitions, vec![]);

        // Verify the struct was created correctly
        assert_eq!(exec.num_partitions(), 3);
        assert_eq!(exec.name(), "PythonDataSourceExec");
    }

    #[test]
    fn test_empty_partitions() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let partitions: Vec<InputPartition> = vec![];

        let exec = PythonDataSourceExec::new(vec![], schema, partitions, vec![]);

        // Empty partitions reports 0 partitions, but properties use max(1) for DataFusion
        assert_eq!(exec.num_partitions(), 0);
        let props = exec.properties();
        assert!(matches!(
            props.partitioning,
            Partitioning::UnknownPartitioning(1)
        ));
    }
}
