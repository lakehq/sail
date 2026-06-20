// [CREDIT]: https://raw.githubusercontent.com/apache/datafusion-comet/c41e4645a92dc3121d62004515f3fb03f1348631/native/spark-expr/src/array_funcs/array_position.rs

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, GenericListArray, Int64Array, OffsetSizeTrait,
};
use arrow::buffer::{NullBuffer, ScalarBuffer};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Decimal128Type, Float32Type, Float64Type, Int16Type,
    Int32Type, Int64Type, Int8Type, TimestampMicrosecondType,
};
use datafusion::common::{exec_err, DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use num::Float;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SparkArrayPosition {
    signature: Signature,
}

impl Default for SparkArrayPosition {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayPosition {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkArrayPosition {
    fn name(&self) -> &str {
        "spark_array_position"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        spark_array_position(&args.args)
    }
}

/// Spark array_position() function that returns the 1-based position of an element in an array.
/// Returns 0 if the element is not found (Spark behavior differs from DataFusion which returns null).
fn spark_array_position(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 2 {
        return exec_err!("array_position function takes exactly two arguments");
    }

    let len = args
        .iter()
        .fold(Option::<usize>::None, |acc, arg| match arg {
            ColumnarValue::Scalar(_) => acc,
            ColumnarValue::Array(a) => Some(a.len()),
        });

    let is_scalar = len.is_none();
    let arrays = ColumnarValue::values_to_arrays(args)?;

    let result = array_position_inner(&arrays)?;

    if is_scalar {
        let scalar = ScalarValue::try_from_array(&result, 0)?;
        Ok(ColumnarValue::Scalar(scalar))
    } else {
        Ok(ColumnarValue::Array(result))
    }
}

fn array_position_inner(args: &[ArrayRef]) -> Result<ArrayRef, DataFusionError> {
    let array = &args[0];
    let element = &args[1];

    match array.data_type() {
        DataType::List(_) => generic_array_position::<i32>(array, element),
        DataType::LargeList(_) => generic_array_position::<i64>(array, element),
        other => exec_err!("array_position does not support type '{other:?}'"),
    }
}

/// Searches for an element in a list array using the flat values buffer and offsets directly,
/// avoiding per-row subarray allocation. Dispatches to typed fast paths by element data type.
fn generic_array_position<O: OffsetSizeTrait>(
    array: &ArrayRef,
    element: &ArrayRef,
) -> Result<ArrayRef, DataFusionError> {
    let list_array = array
        .as_any()
        .downcast_ref::<GenericListArray<O>>()
        .ok_or_else(|| DataFusionError::Internal("expected list array".into()))?;

    let values = list_array.values();
    let offsets = list_array.offsets();
    let elem_type = values.data_type().clone();

    match &elem_type {
        DataType::Boolean => position_boolean::<O>(list_array, offsets, values, element),
        DataType::Int8 => position_primitive::<O, Int8Type>(list_array, offsets, values, element),
        DataType::Int16 => position_primitive::<O, Int16Type>(list_array, offsets, values, element),
        DataType::Int32 => position_primitive::<O, Int32Type>(list_array, offsets, values, element),
        DataType::Int64 => position_primitive::<O, Int64Type>(list_array, offsets, values, element),
        DataType::Float32 => position_float::<O, Float32Type>(list_array, offsets, values, element),
        DataType::Float64 => position_float::<O, Float64Type>(list_array, offsets, values, element),
        DataType::Decimal128(_, _) => {
            position_primitive::<O, Decimal128Type>(list_array, offsets, values, element)
        }
        DataType::Date32 => {
            position_primitive::<O, Date32Type>(list_array, offsets, values, element)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
            position_primitive::<O, TimestampMicrosecondType>(list_array, offsets, values, element)
        }
        DataType::Utf8 => position_string::<O, i32>(list_array, offsets, values, element),
        DataType::LargeUtf8 => position_string::<O, i64>(list_array, offsets, values, element),
        // Fallback to ScalarValue for complex types (nested arrays, etc.)
        _ => position_fallback::<O>(list_array, offsets, values, element),
    }
}

/// Compute the combined null buffer from list array and element nulls.
fn combined_nulls(
    list_array_nulls: Option<&NullBuffer>,
    element_nulls: Option<&NullBuffer>,
) -> Option<NullBuffer> {
    match (list_array_nulls, element_nulls) {
        (Some(a), Some(b)) => NullBuffer::union(Some(a), Some(b)),
        (Some(a), None) => Some(a.clone()),
        (None, Some(b)) => Some(b.clone()),
        (None, None) => None,
    }
}

/// Fast path for primitive types: downcast once, iterate using offsets into the flat buffer.
fn position_primitive<O: OffsetSizeTrait, T: ArrowPrimitiveType>(
    list_array: &GenericListArray<O>,
    offsets: &arrow::buffer::OffsetBuffer<O>,
    values: &ArrayRef,
    element: &ArrayRef,
) -> Result<ArrayRef, DataFusionError>
where
    T::Native: PartialEq,
{
    let values_typed = values.as_primitive::<T>();
    let element_typed = element.as_primitive::<T>();
    let num_rows = list_array.len();
    let nulls = combined_nulls(list_array.nulls(), element.nulls());
    let mut result = vec![0i64; num_rows];

    for (row_index, w) in offsets.windows(2).enumerate() {
        if nulls.as_ref().is_some_and(|n| n.is_null(row_index)) {
            continue;
        }
        let start = w[0].as_usize();
        let end = w[1].as_usize();
        let search_val = element_typed.value(row_index);
        for i in start..end {
            if !values_typed.is_null(i) && values_typed.value(i) == search_val {
                result[row_index] = (i - start + 1) as i64;
                break;
            }
        }
    }

    Ok(Arc::new(Int64Array::new(ScalarBuffer::from(result), nulls)))
}

/// Float path: same as primitive but treats NaN == NaN (Spark's ordering.equiv() semantics).
fn position_float<O: OffsetSizeTrait, T: ArrowPrimitiveType>(
    list_array: &GenericListArray<O>,
    offsets: &arrow::buffer::OffsetBuffer<O>,
    values: &ArrayRef,
    element: &ArrayRef,
) -> Result<ArrayRef, DataFusionError>
where
    T::Native: PartialEq + Float,
{
    let values_typed = values.as_primitive::<T>();
    let element_typed = element.as_primitive::<T>();
    let num_rows = list_array.len();
    let nulls = combined_nulls(list_array.nulls(), element.nulls());
    let mut result = vec![0i64; num_rows];

    for (row_index, w) in offsets.windows(2).enumerate() {
        if nulls.as_ref().is_some_and(|n| n.is_null(row_index)) {
            continue;
        }
        let start = w[0].as_usize();
        let end = w[1].as_usize();
        let search_val = element_typed.value(row_index);
        let search_is_nan = search_val.is_nan();
        for i in start..end {
            if !values_typed.is_null(i) {
                let v = values_typed.value(i);
                if (search_is_nan && v.is_nan()) || v == search_val {
                    result[row_index] = (i - start + 1) as i64;
                    break;
                }
            }
        }
    }

    Ok(Arc::new(Int64Array::new(ScalarBuffer::from(result), nulls)))
}

/// Boolean path.
fn position_boolean<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    offsets: &arrow::buffer::OffsetBuffer<O>,
    values: &ArrayRef,
    element: &ArrayRef,
) -> Result<ArrayRef, DataFusionError> {
    let values_typed = values
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| DataFusionError::Internal("expected boolean array".into()))?;
    let element_typed = element
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| DataFusionError::Internal("expected boolean array".into()))?;
    let num_rows = list_array.len();
    let nulls = combined_nulls(list_array.nulls(), element.nulls());
    let mut result = vec![0i64; num_rows];

    for (row_index, w) in offsets.windows(2).enumerate() {
        if nulls.as_ref().is_some_and(|n| n.is_null(row_index)) {
            continue;
        }
        let start = w[0].as_usize();
        let end = w[1].as_usize();
        let search_val = element_typed.value(row_index);
        for i in start..end {
            if !values_typed.is_null(i) && values_typed.value(i) == search_val {
                result[row_index] = (i - start + 1) as i64;
                break;
            }
        }
    }

    Ok(Arc::new(Int64Array::new(ScalarBuffer::from(result), nulls)))
}

/// String path: downcast once, iterate using offsets into the flat string buffer.
fn position_string<O: OffsetSizeTrait, S: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    offsets: &arrow::buffer::OffsetBuffer<O>,
    values: &ArrayRef,
    element: &ArrayRef,
) -> Result<ArrayRef, DataFusionError> {
    let values_typed = values.as_string::<S>();
    let element_typed = element.as_string::<S>();
    let num_rows = list_array.len();
    let nulls = combined_nulls(list_array.nulls(), element.nulls());
    let mut result = vec![0i64; num_rows];

    for (row_index, w) in offsets.windows(2).enumerate() {
        if nulls.as_ref().is_some_and(|n| n.is_null(row_index)) {
            continue;
        }
        let start = w[0].as_usize();
        let end = w[1].as_usize();
        let search_val = element_typed.value(row_index);
        for i in start..end {
            if !values_typed.is_null(i) && values_typed.value(i) == search_val {
                result[row_index] = (i - start + 1) as i64;
                break;
            }
        }
    }

    Ok(Arc::new(Int64Array::new(ScalarBuffer::from(result), nulls)))
}

/// Fallback for complex types (nested arrays, structs, etc.) using ScalarValue comparison.
fn position_fallback<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    offsets: &arrow::buffer::OffsetBuffer<O>,
    values: &ArrayRef,
    element: &ArrayRef,
) -> Result<ArrayRef, DataFusionError> {
    let num_rows = list_array.len();
    let nulls = combined_nulls(list_array.nulls(), element.nulls());
    let mut result = vec![0i64; num_rows];

    for (row_index, w) in offsets.windows(2).enumerate() {
        if nulls.as_ref().is_some_and(|n| n.is_null(row_index)) {
            continue;
        }
        let start = w[0].as_usize();
        let end = w[1].as_usize();
        let search_scalar = ScalarValue::try_from_array(element, row_index)?;
        for i in start..end {
            if !values.is_null(i) {
                let item_scalar = ScalarValue::try_from_array(values, i)?;
                if search_scalar == item_scalar {
                    result[row_index] = (i - start + 1) as i64;
                    break;
                }
            }
        }
    }

    Ok(Arc::new(Int64Array::new(ScalarBuffer::from(result), nulls)))
}
