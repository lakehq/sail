use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringArray, StructArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};

pub fn get_struct_column(
    batch: &RecordBatch,
    col: &str,
    exec: &'static str,
) -> Result<Option<Arc<StructArray>>> {
    let Some(arr) = batch.column_by_name(col) else {
        return Ok(None);
    };
    let s = arr.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
        DataFusionError::Plan(format!("{exec} input column '{col}' must be a Struct"))
    })?;
    Ok(Some(Arc::new(s.clone())))
}

pub fn struct_field<'a>(s: &'a StructArray, name: &str) -> Option<&'a ArrayRef> {
    s.column_by_name(name)
}

pub fn as_string_array(
    a: &ArrayRef,
    context: &str,
    exec: &'static str,
) -> Result<Arc<StringArray>> {
    let casted = cast(a.as_ref(), &DataType::Utf8)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let s = casted
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Plan(format!("{exec} '{context}' must be Utf8")))?;
    Ok(Arc::new(s.clone()))
}
