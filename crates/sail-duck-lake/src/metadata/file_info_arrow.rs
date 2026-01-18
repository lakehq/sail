use std::sync::Arc;

use datafusion::arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use serde_arrow::schema::{SchemaLike, TracingOptions};

use crate::spec::FileInfo;

pub fn file_info_fields() -> DataFusionResult<Vec<FieldRef>> {
    Vec::<FieldRef>::from_type::<FileInfo>(TracingOptions::default())
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub fn file_info_schema() -> DataFusionResult<SchemaRef> {
    Ok(Arc::new(Schema::new(file_info_fields()?)))
}
