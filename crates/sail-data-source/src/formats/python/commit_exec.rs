//! Commit execution plan for Python data source writes.
//!
//! This node consumes per-partition write results produced by
//! `PythonDataSourceWriteExec`, coalesces them into a single partition, then
//! invokes `writer.commit(messages)` or `writer.abort(messages)`.
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, BinaryArray, RecordBatch, StringArray, UInt64Array};
use arrow_schema::Schema;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{exec_err, internal_err, DataFusionError, Result};
use futures::StreamExt;

use super::executor::{InProcessExecutor, PythonExecutor};
use super::write_exec::{COL_COMMIT_MESSAGE, COL_ERROR, COL_PARTITION_ID};

/// Execution plan for commit/abort in Python datasource write flow.
#[derive(Debug)]
pub struct PythonDataSourceWriteCommitExec {
    /// Input execution plan that yields per-partition write results.
    input: Arc<dyn ExecutionPlan>,
    /// Pickled Python DataSourceWriter instance.
    pickled_writer: Vec<u8>,
    /// Number of partition results expected from the write stage.
    expected_partitions: usize,
    /// Execution plan properties.
    properties: Arc<PlanProperties>,
}

impl PythonDataSourceWriteCommitExec {
    /// Create a new commit execution plan.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        pickled_writer: Vec<u8>,
        expected_partitions: usize,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::empty())),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));

        Self {
            input,
            pickled_writer,
            expected_partitions,
            properties,
        }
    }

    /// Get the input plan.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Get the pickled writer.
    pub fn pickled_writer(&self) -> &[u8] {
        &self.pickled_writer
    }

    /// Get expected partition count.
    pub fn expected_partitions(&self) -> usize {
        self.expected_partitions
    }
}

impl DisplayAs for PythonDataSourceWriteCommitExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PythonDataSourceWriteCommitExec: expected_partitions={}",
            self.expected_partitions
        )
    }
}

impl ExecutionPlan for PythonDataSourceWriteCommitExec {
    fn name(&self) -> &'static str {
        "PythonDataSourceWriteCommitExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("PythonDataSourceWriteCommitExec should have exactly one child");
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.pickled_writer.clone(),
            self.expected_partitions,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("{} can only run on partition 0", self.name());
        }

        let input_partitions = self.input.output_partitioning().partition_count();
        if input_partitions != 1 {
            return internal_err!(
                "{} requires a single input partition, got {}",
                self.name(),
                input_partitions
            );
        }

        let input_stream = self.input.execute(0, context)?;
        let pickled_writer = self.pickled_writer.clone();
        let expected_partitions = self.expected_partitions;

        let stream = futures::stream::once(async move {
            let executor = Arc::new(InProcessExecutor::from_app_config());
            let mut commit_messages: Vec<Option<Vec<u8>>> = vec![None; expected_partitions];
            let mut seen_partitions = vec![false; expected_partitions];
            let mut first_error: Option<String> = None;
            let mut data = input_stream;

            while let Some(batch_result) = data.next().await {
                let batch = match batch_result {
                    Ok(batch) => batch,
                    Err(e) => {
                        if first_error.is_none() {
                            first_error = Some(e.to_string());
                        }
                        break;
                    }
                };

                if let Err(e) = decode_write_result_batch(
                    &batch,
                    &mut commit_messages,
                    &mut seen_partitions,
                    &mut first_error,
                ) {
                    if first_error.is_none() {
                        first_error = Some(e.to_string());
                    }
                }
            }

            for (partition_id, seen) in seen_partitions.iter().enumerate() {
                if !seen && first_error.is_none() {
                    first_error = Some(format!(
                        "Missing write result for partition {}",
                        partition_id
                    ));
                }
            }

            if let Some(err) = first_error {
                let _ = executor.abort_write(&pickled_writer, commit_messages).await;
                return exec_err!("{err}");
            }

            let messages_for_abort = commit_messages.clone();
            match executor
                .commit_write(&pickled_writer, commit_messages)
                .await
            {
                Ok(()) => {}
                Err(commit_err) => {
                    log::error!("Commit failed, attempting abort: {}", commit_err);
                    let _ = executor
                        .abort_write(&pickled_writer, messages_for_abort)
                        .await;
                    return Err(commit_err);
                }
            }

            Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            stream,
        )))
    }
}

fn decode_write_result_batch(
    batch: &RecordBatch,
    commit_messages: &mut [Option<Vec<u8>>],
    seen_partitions: &mut [bool],
    first_error: &mut Option<String>,
) -> Result<()> {
    let partition_ids = batch
        .column_by_name(COL_PARTITION_ID)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Missing '{}' column in write result batch",
                COL_PARTITION_ID
            ))
        })?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!("Column '{}' has unexpected type", COL_PARTITION_ID))
        })?;
    let commit_col = batch
        .column_by_name(COL_COMMIT_MESSAGE)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Missing '{}' column in write result batch",
                COL_COMMIT_MESSAGE
            ))
        })?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Column '{}' has unexpected type",
                COL_COMMIT_MESSAGE
            ))
        })?;
    let error_col = batch
        .column_by_name(COL_ERROR)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Missing '{}' column in write result batch",
                COL_ERROR
            ))
        })?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!("Column '{}' has unexpected type", COL_ERROR))
        })?;

    for row_idx in 0..batch.num_rows() {
        let partition_id = partition_ids.value(row_idx) as usize;
        if partition_id >= commit_messages.len() {
            if first_error.is_none() {
                *first_error = Some(format!(
                    "Invalid partition id {} in write result (expected < {})",
                    partition_id,
                    commit_messages.len()
                ));
            }
            continue;
        }

        if seen_partitions[partition_id] {
            if first_error.is_none() {
                *first_error = Some(format!(
                    "Duplicate write result for partition {}",
                    partition_id
                ));
            }
            continue;
        }
        seen_partitions[partition_id] = true;

        if !error_col.is_null(row_idx) {
            if first_error.is_none() {
                *first_error = Some(format!(
                    "Write failed for partition {}: {}",
                    partition_id,
                    error_col.value(row_idx)
                ));
            }
            commit_messages[partition_id] = None;
            continue;
        }

        commit_messages[partition_id] = if commit_col.is_null(row_idx) {
            None
        } else {
            Some(commit_col.value(row_idx).to_vec())
        };
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};

    use super::*;
    use crate::formats::python::write_exec::build_write_result_batch;

    #[test]
    fn test_commit_exec_properties() {
        let write_result_schema = Arc::new(Schema::new(vec![
            Field::new(COL_PARTITION_ID, DataType::UInt64, false),
            Field::new(COL_COMMIT_MESSAGE, DataType::Binary, true),
            Field::new(COL_ERROR, DataType::Utf8, true),
        ]));
        let input = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
            write_result_schema,
        ));
        let exec = PythonDataSourceWriteCommitExec::new(input, vec![1, 2], 1);

        assert_eq!(exec.name(), "PythonDataSourceWriteCommitExec");
        assert!(matches!(
            exec.properties().partitioning,
            Partitioning::UnknownPartitioning(1)
        ));
        let required_distribution = exec.required_input_distribution();
        assert_eq!(required_distribution.len(), 1);
        assert!(matches!(
            required_distribution[0],
            Distribution::SinglePartition
        ));
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_with_new_children() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(COL_PARTITION_ID, DataType::UInt64, false),
            Field::new(COL_COMMIT_MESSAGE, DataType::Binary, true),
            Field::new(COL_ERROR, DataType::Utf8, true),
        ]));
        let input1 = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
            schema.clone(),
        ));
        let input2 = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
            schema.clone(),
        ));
        let exec = Arc::new(PythonDataSourceWriteCommitExec::new(input1, vec![], 2));

        let new_exec = exec.clone().with_new_children(vec![input2]).unwrap();
        assert!(new_exec.as_any().is::<PythonDataSourceWriteCommitExec>());
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_decode_write_result_batch() {
        let mut commit_messages = vec![None, None];
        let mut seen = vec![false, false];
        let mut first_error = None;

        let batch0 = build_write_result_batch(0, Some(vec![1, 2, 3]), None).unwrap();
        decode_write_result_batch(&batch0, &mut commit_messages, &mut seen, &mut first_error)
            .unwrap();

        let batch1 = build_write_result_batch(1, None, Some("boom".to_string())).unwrap();
        decode_write_result_batch(&batch1, &mut commit_messages, &mut seen, &mut first_error)
            .unwrap();

        assert_eq!(commit_messages[0], Some(vec![1, 2, 3]));
        assert_eq!(commit_messages[1], None);
        assert!(seen[0]);
        assert!(seen[1]);
        assert!(first_error.is_some());
    }
}
