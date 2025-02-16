use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::Result;
use sail_common::impl_dyn_object_traits;
use sail_common::object::DynObject;

/// Trait for implementing UDF to map input stream to output stream.
pub trait StreamUDF: DynObject + Debug + Send + Sync {
    /// The name of the UDF.
    fn name(&self) -> &str;

    /// The schema of the output stream.
    fn output_schema(&self) -> SchemaRef;

    /// Invoke the UDF to map input stream to output stream.
    fn invoke(&self, input: SendableRecordBatchStream) -> Result<SendableRecordBatchStream>;
}

impl_dyn_object_traits!(StreamUDF);
