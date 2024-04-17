use datafusion::common::{DataFusionError};
use datafusion::arrow::array::{Array, ArrayRef, PrimitiveArray};
use datafusion::arrow::datatypes::{ArrowPrimitiveType};


// If you need more flexibility use impl AsRef<dyn Array>
pub fn get_native_values_from_array<T: ArrowPrimitiveType>(arr: &ArrayRef) -> Result<Vec<T::Native>, DataFusionError> {
    let native_values = arr
        .as_ref()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| DataFusionError::Internal(format!("Failed to downcast array")))?
        .values()
        .to_vec();

    Ok(native_values)
}
