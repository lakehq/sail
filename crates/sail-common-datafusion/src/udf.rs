use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion_common::Result;
use sail_common::impl_dyn_object_traits;
use sail_common::utils::object::DynObject;

/// Trait for implementing UDF to map input stream to output stream.
pub trait StreamUDF: DynObject + Debug + Send + Sync {
    /// The name of the UDF.
    fn name(&self) -> &str;

    /// The schema of the output stream.
    fn output_schema(&self) -> SchemaRef;

    /// Invoke the UDF to map input stream to output stream.
    fn invoke(
        &self,
        input: SendableRecordBatchStream,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream>;

    /// Input columns required for a subset of the output, in original input order.
    fn required_input_columns(
        &self,
        _output_columns: &[usize],
        _input_columns: usize,
    ) -> Option<Vec<usize>> {
        None
    }

    /// Adapt to input pruning and identify the surviving original output columns.
    fn project_input(&self, _input_columns: &[usize]) -> Result<Option<StreamUDFProjection>> {
        Ok(None)
    }
}

pub struct StreamUDFProjection {
    pub udf: Arc<dyn StreamUDF>,
    pub output_columns: Vec<usize>,
}

impl_dyn_object_traits!(StreamUDF);
