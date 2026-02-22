//! Execution plan for Python data source writes.
//!
//! This execution plan writes data to a Python datasource using the two-phase
//! commit protocol: write() → commit() or abort().
//!
//! # Architecture
//!
//! The write exec is a sink node with one child (the input data plan). It:
//! 1. Executes the input plan to get RecordBatch streams from all partitions
//! 2. Feeds batches to Python writer.write() per partition
//! 3. Collects WriterCommitMessages from all partitions
//! 4. Calls writer.commit() or writer.abort() based on success/failure
//!
//! Only partition 0 orchestrates the entire write operation. Other partitions
//! return empty streams immediately.
//!
//! # Stateless Writer Boundary
//!
//! Each phase (write, commit, abort) deserializes a **fresh** Python writer
//! instance from the pickled bytes. Writers must not rely on in-memory state
//! across the write → commit/abort boundary. Commit messages are the only
//! channel for passing state from write() to commit()/abort(). This matches
//! PySpark's distributed execution semantics.
//!
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_err, internal_err, Result};
use futures::stream::StreamExt;

use super::executor::{InProcessExecutor, PythonExecutor};

/// Execution plan for writing to a Python datasource.
///
/// This is a sink node (one child: the input data plan) that orchestrates
/// the two-phase commit protocol for writing data.
#[derive(Debug)]
pub struct PythonDataSourceWriteExec {
    /// Input execution plan (data to write)
    input: Arc<dyn ExecutionPlan>,
    /// Pickled Python DataSourceWriter instance
    pickled_writer: Vec<u8>,
    /// Schema of the data being written
    schema: SchemaRef,
    /// Whether writer is DataSourceArrowWriter (true) or DataSourceWriter (false)
    is_arrow: bool,
    /// Execution plan properties
    properties: PlanProperties,
}

impl PythonDataSourceWriteExec {
    /// Create a new write execution plan.
    ///
    /// # Arguments
    ///
    /// * `input` - Input execution plan providing data to write
    /// * `pickled_writer` - Pickled Python DataSourceWriter instance
    /// * `schema` - Schema of the data being written
    /// * `is_arrow` - Whether writer is Arrow-based (DataSourceArrowWriter)
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        pickled_writer: Vec<u8>,
        schema: SchemaRef,
        is_arrow: bool,
    ) -> Self {
        // Write exec has single partition, empty output schema, final emission, bounded
        let empty_schema = Arc::new(Schema::empty());
        let properties = PlanProperties::new(
            EquivalenceProperties::new(empty_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        // Sanity check: write schema should match input plan's output schema
        debug_assert_eq!(
            schema.fields().len(),
            input.schema().fields().len(),
            "PythonDataSourceWriteExec: write schema field count ({}) != input schema field count ({})",
            schema.fields().len(),
            input.schema().fields().len(),
        );

        Self {
            input,
            pickled_writer,
            schema,
            is_arrow,
            properties,
        }
    }

    /// Get the pickled writer.
    pub fn pickled_writer(&self) -> &[u8] {
        &self.pickled_writer
    }

    /// Get whether this is an Arrow-based writer.
    pub fn is_arrow(&self) -> bool {
        self.is_arrow
    }

    /// Get the input plan.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Get the schema of the data being written.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl DisplayAs for PythonDataSourceWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PythonDataSourceWriteExec: input_partitions={}, writer_type={}",
            self.input.properties().partitioning.partition_count(),
            if self.is_arrow { "Arrow" } else { "Row" }
        )
    }
}

impl ExecutionPlan for PythonDataSourceWriteExec {
    fn name(&self) -> &'static str {
        "PythonDataSourceWriteExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("PythonDataSourceWriteExec should have exactly one child");
        }
        Ok(Arc::new(PythonDataSourceWriteExec::new(
            children[0].clone(),
            self.pickled_writer.clone(),
            self.schema.clone(),
            self.is_arrow,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Only partition 0 orchestrates the write
        if partition != 0 {
            return Ok(Box::pin(
                datafusion::physical_plan::stream::EmptyRecordBatchStream::new(Arc::new(
                    Schema::empty(),
                )),
            ));
        }

        let input = self.input.clone();
        let pickled_writer = self.pickled_writer.clone();
        let is_arrow = self.is_arrow;
        let write_schema = self.schema.clone();

        // Create async stream that orchestrates the write/commit/abort
        let stream = futures::stream::once(async move {
            // Load executor with config from application.yaml (python.data_source_write_channel_capacity,
            // python.data_source_slow_write_warn_ms). Falls back to defaults if config loading fails.
            let executor = Arc::new(InProcessExecutor::from_app_config());
            let num_partitions = input.properties().partitioning.partition_count();

            // Phase 1: Execute writes in parallel for all partitions
            let mut write_futures = Vec::new();

            for p in 0..num_partitions {
                let input_stream = input.execute(p, context.clone())?;
                let executor_clone = executor.clone();
                let pickled_writer_clone = pickled_writer.clone();
                let schema_clone = write_schema.clone();

                // Create a future for this partition's write
                let write_future = async move {
                    // Create a stream that validates schema for each batch
                    let schema_for_validation = schema_clone.clone();
                    let stream = input_stream.map(move |batch_result| {
                        let batch = batch_result?;
                        super::arrow_utils::validate_schema(
                            &schema_for_validation,
                            batch.schema_ref(),
                        )?;
                        Ok(batch)
                    });

                    // Execute write for this partition (streaming)
                    executor_clone
                        .execute_write(
                            &pickled_writer_clone,
                            schema_clone,
                            is_arrow,
                            Box::pin(stream),
                        )
                        .await
                };

                write_futures.push(write_future);
            }

            // Phase 2: Collect results from all partitions
            let results = futures::future::join_all(write_futures).await;
            let mut commit_messages = Vec::new();
            let mut had_failure = false;
            let mut first_error = None;

            for result in results {
                match result {
                    Ok(write_result) => {
                        commit_messages.push(write_result.commit_message);
                    }
                    Err(e) => {
                        had_failure = true;
                        if first_error.is_none() {
                            first_error = Some(e);
                        }
                        // Push None for failed partition
                        commit_messages.push(None);
                    }
                }
            }

            // Phase 3: Commit or abort based on results
            if had_failure {
                // Attempt abort (best effort, errors are logged)
                let _ = executor.abort_write(&pickled_writer, commit_messages).await;

                // Return the first error we encountered
                if let Some(err) = first_error {
                    return Err(err);
                } else {
                    return exec_err!("Write operation failed but no error was captured");
                }
            } else {
                // All writes succeeded, attempt commit.
                // Clone messages before commit so abort can use them on failure.
                let messages_for_abort = commit_messages.clone();
                match executor
                    .commit_write(&pickled_writer, commit_messages)
                    .await
                {
                    Ok(()) => { /* commit succeeded */ }
                    Err(commit_err) => {
                        // Commit failed — must abort to avoid leaving writer in limbo.
                        log::error!("Commit failed, attempting abort: {}", commit_err);
                        let _ = executor
                            .abort_write(&pickled_writer, messages_for_abort)
                            .await;
                        return Err(commit_err);
                    }
                }
            }

            // Return empty batch to signal completion
            Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};

    use super::*;

    #[test]
    fn test_python_datasource_write_exec_properties() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Float64, true),
        ]));

        // Create a mock input plan (empty exec)
        let input = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
            schema.clone(),
        ));

        let exec = PythonDataSourceWriteExec::new(
            input,
            vec![1, 2, 3], // mock pickled writer
            schema.clone(),
            true, // is_arrow
        );

        assert!(exec.is_arrow());
        assert_eq!(exec.pickled_writer(), &[1, 2, 3]);
        assert_eq!(exec.children().len(), 1);
        assert_eq!(exec.name(), "PythonDataSourceWriteExec");

        // Check properties
        let props = exec.properties();
        assert!(matches!(
            props.partitioning,
            Partitioning::UnknownPartitioning(1)
        ));
    }

    #[test]
    fn test_display_as() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let input = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
            schema.clone(),
        ));

        let exec = PythonDataSourceWriteExec::new(input, vec![], schema.clone(), false);

        assert!(!exec.is_arrow());
        assert_eq!(exec.name(), "PythonDataSourceWriteExec");
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_with_new_children() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let input1 = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
            schema.clone(),
        ));
        let input2 = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
            schema.clone(),
        ));

        let exec = Arc::new(PythonDataSourceWriteExec::new(
            input1,
            vec![],
            schema.clone(),
            true,
        ));

        // Replace with new child
        let new_exec = exec.clone().with_new_children(vec![input2]).unwrap();

        assert!(new_exec.as_any().is::<PythonDataSourceWriteExec>());
    }

    #[test]
    fn test_with_new_children_wrong_count() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let input = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
            schema.clone(),
        ));

        let exec = Arc::new(PythonDataSourceWriteExec::new(
            input,
            vec![],
            schema.clone(),
            true,
        ));

        // Should fail with wrong number of children
        assert!(exec.clone().with_new_children(vec![]).is_err());
        assert!(exec
            .clone()
            .with_new_children(vec![
                Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                    schema.clone()
                )),
                Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                    schema.clone()
                ))
            ])
            .is_err());
    }
}
