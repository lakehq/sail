// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::CastOptions;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;

/// This is part of the shared contract for bitmap aggregate functions that
/// interoperate on the same binary bitmap representation.
pub(crate) const BITMAP_NUM_BYTES: usize = 4 * 1024;

pub fn get_scalar_value(expr: &Arc<dyn PhysicalExpr>) -> Result<ScalarValue> {
    let empty_schema = Arc::new(Schema::empty());
    let batch = RecordBatch::new_empty(Arc::clone(&empty_schema));
    match expr.evaluate(&batch)? {
        ColumnarValue::Scalar(s) => Ok(s),
        _ => Err(DataFusionError::Internal(
            "Didn't expect ColumnarValue::Array".to_string(),
        )),
    }
}

/// Casts an array to the target type if it doesn't already match.
///
/// Uses a safe cast (invalid values become NULL).
pub fn cast_to_type(array: &ArrayRef, target_type: &DataType) -> Result<ArrayRef> {
    cast_to_type_with_safe(array, target_type, true)
}

/// Casts an array to the target type if it doesn't already match, choosing
/// between a safe cast (`safe = true`, invalid values become NULL) and a strict
/// cast (`safe = false`, invalid values raise an error).
///
/// This drives the ANSI-mode behavior of percentile aggregates: under
/// `spark.sql.ansi.enabled = false` a non-numeric string is coerced to NULL and
/// ignored, while under ANSI mode an invalid cast surfaces an error.
pub fn cast_to_type_with_safe(
    array: &ArrayRef,
    target_type: &DataType,
    safe: bool,
) -> Result<ArrayRef> {
    if array.data_type() != target_type {
        let options = CastOptions {
            safe,
            ..Default::default()
        };
        arrow::compute::cast_with_options(array, target_type, &options)
            .map_err(DataFusionError::from)
    } else {
        Ok(Arc::clone(array))
    }
}

pub fn filtered_null_mask(
    opt_filter: Option<&BooleanArray>,
    input: &dyn Array,
) -> Option<NullBuffer> {
    let opt_filter = opt_filter.and_then(|f| {
        let (filter_bools, filter_nulls) = f.clone().into_parts();
        let filter_bools = NullBuffer::from(filter_bools);
        NullBuffer::union(Some(&filter_bools), filter_nulls.as_ref())
    });
    NullBuffer::union(opt_filter.as_ref(), input.nulls())
}
