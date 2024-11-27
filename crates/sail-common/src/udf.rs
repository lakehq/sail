use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use arrow_schema::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::Result;

use crate::impl_dyn_object_traits;
use crate::object::DynObject;

/// Trait for implementing UDF to map input stream to output stream.
pub trait MapIterUDF: DynObject + Debug + Send + Sync {
    /// The schema of the output stream.
    fn output_schema(&self) -> SchemaRef;

    /// Invoke the UDF to map input stream to output stream.
    fn invoke(&self, input: SendableRecordBatchStream) -> Result<SendableRecordBatchStream>;
}

impl_dyn_object_traits!(MapIterUDF);
