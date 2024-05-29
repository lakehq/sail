use datafusion::arrow::array::{
    as_boolean_array, as_struct_array, types, Array, ArrayRef, PrimitiveArray, StructArray,
};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, TimeUnit};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

// TODO: Move this to a separate module/crate.
pub fn downcast_array_ref<T: ArrowPrimitiveType>(
    arr: &ArrayRef,
) -> Result<&PrimitiveArray<T>, DataFusionError> {
    let native_values = arr
        .as_ref()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| DataFusionError::Internal("Failed to downcast input array".to_string()))?;

    Ok(native_values)
}
