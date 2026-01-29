/// Execution plan for Python DataSource batch reads.
///
/// This execution plan reads data from a Python datasource in parallel,
/// with one partition per InputPartition returned by the reader.
///
/// Key patterns (from sail_engineering skill):
/// - Implements `ExecutionPlan` trait following DataFusion conventions
/// - Implements `DisplayAs` for clear EXPLAIN output
/// - Source node with no children
/// - Uses `PythonDataSourceStream` for actual data reading
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
use super::stream::PythonDataSourceStream;

/// Execution plan for reading from a Python datasource.
///
/// This is a source node (no children) that reads data in parallel
/// across multiple partitions.
#[derive(Debug)]
pub struct PythonDataSourceExec {
    /// Pickled Python DataSource instance
    command: Vec<u8>,
    /// Schema of the output data
    schema: SchemaRef,
    /// Partitions for parallel reading
    partitions: Vec<InputPartition>,
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
    pub fn new(command: Vec<u8>, schema: SchemaRef, partitions: Vec<InputPartition>) -> Self {
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
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.partitions.len() {
            return exec_err!(
                "partition index {} out of range (0..{})",
                partition,
                self.partitions.len()
            );
        }

        // Create stream for reading from Python
        let stream = PythonDataSourceStream::new(
            self.command.clone(),
            self.partitions[partition].clone(),
            self.schema.clone(),
        )?;

        Ok(Box::pin(stream))
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

        let exec = PythonDataSourceExec::new(vec![0, 0], schema.clone(), partitions);

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

        let exec = PythonDataSourceExec::new(vec![], schema, partitions);

        // Verify the struct was created correctly
        assert_eq!(exec.num_partitions(), 3);
        assert_eq!(exec.name(), "PythonDataSourceExec");
    }
}
