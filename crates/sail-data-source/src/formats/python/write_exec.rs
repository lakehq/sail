//! Distributed write execution plan for Python data sources.
//!
//! This execution plan runs `writer.write(iterator)` per input partition and
//! emits one result row per partition. A separate
//! `PythonDataSourceWriteCommitExec` node consumes these rows and performs
//! `writer.commit(messages)` or `writer.abort(messages)`.
//!
//! # Stateless Writer Boundary
//!
//! Each phase (write, commit, abort) deserializes a fresh Python writer from
//! pickled bytes. Writers must not rely on in-memory state across the
//! write -> commit/abort boundary.
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{BinaryArray, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{internal_err, Result};
use futures::StreamExt;

use super::executor::{InProcessExecutor, PythonExecutor};

pub(crate) const COL_PARTITION_ID: &str = "partition_id";
pub(crate) const COL_COMMIT_MESSAGE: &str = "commit_message";
pub(crate) const COL_ERROR: &str = "error";

pub(crate) fn write_result_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COL_PARTITION_ID, DataType::UInt64, false),
        Field::new(COL_COMMIT_MESSAGE, DataType::Binary, true),
        Field::new(COL_ERROR, DataType::Utf8, true),
    ]))
}

pub(crate) fn build_write_result_batch(
    partition_id: usize,
    commit_message: Option<Vec<u8>>,
    error: Option<String>,
) -> Result<RecordBatch> {
    let partition_ids = UInt64Array::from(vec![partition_id as u64]);
    let commit_messages = BinaryArray::from(vec![commit_message.as_deref()]);
    let errors = StringArray::from(vec![error.as_deref()]);
    Ok(RecordBatch::try_new(
        write_result_schema(),
        vec![
            Arc::new(partition_ids),
            Arc::new(commit_messages),
            Arc::new(errors),
        ],
    )?)
}

/// Execution plan for distributed partition-local writes to a Python datasource.
#[derive(Debug)]
pub struct PythonDataSourceWriteExec {
    /// Input execution plan (data to write)
    input: Arc<dyn ExecutionPlan>,
    /// Pickled Python DataSourceWriter instance
    pickled_writer: Vec<u8>,
    /// Whether writer is DataSourceArrowWriter (true) or DataSourceWriter (false)
    is_arrow: bool,
    /// Execution plan properties
    properties: Arc<PlanProperties>,
}

impl PythonDataSourceWriteExec {
    /// Create a new distributed write execution plan.
    pub fn new(input: Arc<dyn ExecutionPlan>, pickled_writer: Vec<u8>, is_arrow: bool) -> Self {
        let output_partition_count = input.properties().partitioning.partition_count();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(write_result_schema()),
            Partitioning::UnknownPartitioning(output_partition_count),
            EmissionType::Final,
            Boundedness::Bounded,
        ));

        Self {
            input,
            pickled_writer,
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

    fn properties(&self) -> &Arc<PlanProperties> {
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
            self.is_arrow,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.clone();
        let pickled_writer = self.pickled_writer.clone();
        let is_arrow = self.is_arrow;
        let input_schema = input.schema();
        let output_schema = write_result_schema();

        let stream = futures::stream::once(async move {
            let executor = Arc::new(InProcessExecutor::from_app_config());

            let input_stream = match input.execute(partition, context) {
                Ok(stream) => stream,
                Err(e) => {
                    return build_write_result_batch(partition, None, Some(e.to_string()));
                }
            };

            let schema_for_validation = input_schema.clone();
            let stream = input_stream.map(move |batch_result| {
                let batch = batch_result?;
                super::arrow_utils::validate_schema(&schema_for_validation, batch.schema_ref())?;
                Ok(batch)
            });

            match executor
                .execute_write(&pickled_writer, input_schema, is_arrow, Box::pin(stream))
                .await
            {
                Ok(write_result) => {
                    build_write_result_batch(partition, write_result.commit_message, None)
                }
                Err(e) => build_write_result_batch(partition, None, Some(e.to_string())),
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Array;
    use arrow::datatypes::{DataType, Field};

    use super::*;

    #[test]
    fn test_python_datasource_write_exec_properties() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Float64, true),
        ]));

        let input = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
            schema.clone(),
        ));

        let exec = PythonDataSourceWriteExec::new(
            input,
            vec![1, 2, 3], // mock pickled writer
            true,          // is_arrow
        );

        assert!(exec.is_arrow());
        assert_eq!(exec.pickled_writer(), &[1, 2, 3]);
        assert_eq!(exec.children().len(), 1);
        assert_eq!(exec.name(), "PythonDataSourceWriteExec");

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

        let exec = PythonDataSourceWriteExec::new(input, vec![], false);

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

        let exec = Arc::new(PythonDataSourceWriteExec::new(input1, vec![], true));

        let new_exec = exec.clone().with_new_children(vec![input2]).unwrap();

        assert!(new_exec.as_any().is::<PythonDataSourceWriteExec>());
    }

    #[test]
    fn test_with_new_children_wrong_count() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let input = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
            schema.clone(),
        ));

        let exec = Arc::new(PythonDataSourceWriteExec::new(input, vec![], true));

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

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_build_write_result_batch() {
        let batch =
            build_write_result_batch(2, Some(vec![1, 2, 3]), Some("failed".to_string())).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema().field(0).name(), COL_PARTITION_ID);
        assert_eq!(batch.schema().field(1).name(), COL_COMMIT_MESSAGE);
        assert_eq!(batch.schema().field(2).name(), COL_ERROR);

        let partition_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(partition_ids.value(0), 2);

        let commit_messages = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert!(!commit_messages.is_null(0));
        assert_eq!(commit_messages.value(0), [1, 2, 3]);

        let errors = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(!errors.is_null(0));
        assert_eq!(errors.value(0), "failed");
    }
}
