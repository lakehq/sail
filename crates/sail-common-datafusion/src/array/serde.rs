use std::ops::Deref;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion_common::{internal_datafusion_err, Result};
use serde::{Deserialize, Serialize};
use serde_arrow::schema::{SchemaLike, TracingOptions};
use serde_arrow::to_arrow;

#[derive(Default)]
pub struct ArrowSerializer {
    options: TracingOptions,
}

impl ArrowSerializer {
    pub fn new(options: TracingOptions) -> Self {
        Self { options }
    }

    pub fn schema<T>(&self) -> Result<SchemaRef>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let fields = Vec::<FieldRef>::from_type::<T>(self.options.clone())
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        Ok(Arc::new(Schema::new(fields)))
    }

    pub fn build_record_batch<T>(&self, items: &[T]) -> Result<RecordBatch>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let schema = self.schema::<T>()?;
        Self::build_record_batch_with_schema(items, schema)
    }

    pub fn build_record_batch_with_schema<T>(items: &[T], schema: SchemaRef) -> Result<RecordBatch>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let arrays = to_arrow(schema.fields().deref(), items)
            .map_err(|e| internal_datafusion_err!("failed to create record batch: {e}"))?;
        // We must specify the row count if the schema has no fields.
        let options = RecordBatchOptions::new().with_row_count(Some(items.len()));
        let batch = RecordBatch::try_new_with_options(schema, arrays, &options)
            .map_err(|e| internal_datafusion_err!("failed to create record batch: {e}"))?;
        Ok(batch)
    }
}
