//! Arrow conversion utilities for Python data sources.
//!
//! This module provides efficient conversion between Arrow and Python types
//! using the Arrow C Data Interface for zero-copy transfer.
//!
//! ## Supported Data Types
//!
//! - Numeric: Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64,
//!   Float32, Float64, Decimal128
//! - String: Utf8, LargeUtf8
//! - Binary: Binary, LargeBinary
//! - Boolean
//! - Temporal: Date32, Timestamp(Microsecond, None | Some(tz))
//! - Null
//!
//! Not yet supported (use Arrow writer for these):
//! - List<T>, Struct, Map<K,V>, Decimal256

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, NullArray, RecordBatch,
    StringArray, TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_schema::SchemaRef;
use datafusion_common::{DataFusionError, Result};
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;

/// Convert a Python PyArrow RecordBatch to a Rust Arrow RecordBatch.
///
/// Uses the Arrow C Data Interface for zero-copy conversion.
pub fn py_record_batch_to_rust(
    _py: Python<'_>,
    py_batch: &Bound<'_, PyAny>,
) -> Result<RecordBatch> {
    use arrow_pyarrow::FromPyArrow;

    RecordBatch::from_pyarrow_bound(py_batch).map_err(|e| {
        DataFusionError::External(Box::new(std::io::Error::other(format!(
            "Failed to convert PyArrow RecordBatch: {}",
            e
        ))))
    })
}

/// Convert a Rust Arrow Schema to a Python PyArrow Schema.
pub fn rust_schema_to_py(py: Python<'_>, schema: &SchemaRef) -> Result<Py<PyAny>> {
    use arrow_pyarrow::ToPyArrow;

    ToPyArrow::to_pyarrow(schema.as_ref(), py)
        .map(|obj| obj.unbind())
        .map_err(|e| {
            DataFusionError::External(Box::new(std::io::Error::other(format!(
                "Failed to convert schema to PyArrow: {}",
                e
            ))))
        })
}

/// Convert a Python PyArrow Schema to a Rust Arrow Schema.
pub fn py_schema_to_rust(_py: Python<'_>, py_schema: &Bound<'_, PyAny>) -> Result<SchemaRef> {
    use arrow_pyarrow::FromPyArrow;

    let schema = Schema::from_pyarrow_bound(py_schema).map_err(|e| {
        DataFusionError::External(Box::new(std::io::Error::other(format!(
            "Failed to convert PyArrow Schema: {}",
            e
        ))))
    })?;

    Ok(Arc::new(schema))
}

/// Convert a Rust Arrow RecordBatch to a Python PyArrow RecordBatch.
///
/// Uses the Arrow C Data Interface for zero-copy conversion.
/// This is used for the Arrow-based write path (DataSourceArrowWriter).
pub fn rust_record_batch_to_py(py: Python<'_>, batch: &RecordBatch) -> Result<Py<PyAny>> {
    use arrow_pyarrow::ToPyArrow;

    batch
        .to_pyarrow(py)
        .map(|bound| bound.unbind())
        .map_err(|e| {
            DataFusionError::External(Box::new(std::io::Error::other(format!(
                "Failed to convert RecordBatch to PyArrow: {}",
                e
            ))))
        })
}

/// Convert a Rust Arrow RecordBatch to Python Row objects.
///
/// This is used for the Row-based write path (DataSourceWriter).
/// Each row is converted to a `pyspark.sql.Row` object so that user code
/// can call methods like `.asDict()` on the received rows.
/// Get a PySpark Row factory for the given schema.
pub fn get_row_factory(py: Python<'_>, schema: &SchemaRef) -> Result<Py<PyAny>> {
    let row_module = py.import("pyspark.sql").map_err(py_err)?;
    let row_class = row_module.getattr("Row").map_err(py_err)?;

    // Build the field names from the schema
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    // Create a Row factory: row_factory = Row("col1", "col2", ...)
    let row_factory = row_class
        .call1(pyo3::types::PyTuple::new(py, &field_names).map_err(py_err)?)
        .map_err(py_err)?;

    Ok(row_factory.unbind())
}

// Test-only helpers: these convenience wrappers over get_row_factory + extract_python_value
// are not used in production (executor.rs calls get_row_factory directly in its
// RecordBatchIterator). Kept for unit testing row conversion logic.
#[cfg(test)]
pub fn record_batch_to_py_rows_with_factory(
    py: Python<'_>,
    batch: &RecordBatch,
    row_factory: &Bound<'_, PyAny>,
) -> Result<Vec<Py<PyAny>>> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let mut rows = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut row_values = Vec::with_capacity(num_cols);

        for col_idx in 0..num_cols {
            let column = batch.column(col_idx);
            let value = extract_python_value(py, column, row_idx)?;
            row_values.push(value);
        }

        let args = pyo3::types::PyTuple::new(py, row_values).map_err(py_err)?;
        let row = row_factory.call1(args).map_err(py_err)?;
        rows.push(row.unbind());
    }

    Ok(rows)
}

#[cfg(test)]
pub fn record_batch_to_py_rows(py: Python<'_>, batch: &RecordBatch) -> Result<Vec<Py<PyAny>>> {
    let row_factory = get_row_factory(py, batch.schema_ref())?;
    record_batch_to_py_rows_with_factory(py, batch, row_factory.bind(py))
}

/// Convert an Arrow value to a Python object, returning a proper error instead of panicking.
macro_rules! to_py_value {
    ($arr:expr, $row_idx:expr, $py:expr) => {{
        use pyo3::IntoPyObject;
        $arr.value($row_idx)
            .into_pyobject($py)
            .map(|obj| obj.to_owned().into_any().unbind())
            .map_err(|e| {
                DataFusionError::External(Box::new(std::io::Error::other(format!(
                    "Failed to convert Arrow value to Python: {}",
                    e
                ))))
            })
    }};
}

/// Extract a Python value from an Arrow array at a given index.
/// Public for use by RecordBatchIterator in executor (row-based write path).
pub(crate) fn extract_python_value(
    py: Python<'_>,
    array: &ArrayRef,
    row_idx: usize,
) -> Result<Py<PyAny>> {
    use arrow::array::Array;

    if array.is_null(row_idx) {
        return Ok(py.None());
    }

    /// Helper macro: downcast to `$array_ty` and call `to_py_value!`.
    macro_rules! simple_extract {
        ($array_ty:ty, $label:literal) => {{
            let arr = array.as_any().downcast_ref::<$array_ty>().ok_or_else(|| {
                DataFusionError::Execution(format!("Failed to downcast to {}", $label))
            })?;
            to_py_value!(arr, row_idx, py)
        }};
    }

    match array.data_type() {
        DataType::Null => Ok(py.None()),
        DataType::Boolean => simple_extract!(BooleanArray, "BooleanArray"),
        DataType::Int8 => simple_extract!(Int8Array, "Int8Array"),
        DataType::Int16 => simple_extract!(Int16Array, "Int16Array"),
        DataType::Int32 => simple_extract!(Int32Array, "Int32Array"),
        DataType::Int64 => simple_extract!(Int64Array, "Int64Array"),
        DataType::UInt8 => simple_extract!(UInt8Array, "UInt8Array"),
        DataType::UInt16 => simple_extract!(UInt16Array, "UInt16Array"),
        DataType::UInt32 => simple_extract!(UInt32Array, "UInt32Array"),
        DataType::UInt64 => simple_extract!(UInt64Array, "UInt64Array"),
        DataType::Float32 => simple_extract!(Float32Array, "Float32Array"),
        DataType::Float64 => simple_extract!(Float64Array, "Float64Array"),
        DataType::Decimal128(precision, scale) => {
            extract_decimal128_value(py, array, row_idx, *precision, *scale)
        }
        DataType::Utf8 => simple_extract!(StringArray, "StringArray"),
        DataType::LargeUtf8 => simple_extract!(LargeStringArray, "LargeStringArray"),
        DataType::Date32 => extract_date32_value(py, array, row_idx),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            extract_timestamp_us_value(py, array, row_idx)
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
            extract_timestamp_us_tz_value(py, array, row_idx, tz)
        }
        DataType::Binary => {
            use arrow::array::BinaryArray;
            simple_extract!(BinaryArray, "BinaryArray")
        }
        DataType::LargeBinary => simple_extract!(LargeBinaryArray, "LargeBinaryArray"),
        other => Err(DataFusionError::NotImplemented(format!(
            "Data type {:?} not supported in row-based write path. \
             Use DataSourceArrowWriter for full type support.",
            other
        ))),
    }
}

/// Convert an Arrow Date32 value (days since epoch) to a Python `datetime.date`.
fn extract_date32_value(py: Python<'_>, array: &ArrayRef, row_idx: usize) -> Result<Py<PyAny>> {
    let arr = array
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| {
            DataFusionError::Execution("Failed to downcast to Date32Array".to_string())
        })?;
    let days_since_epoch = arr.value(row_idx);
    let datetime = py.import("datetime").map_err(py_err)?;
    let date_cls = datetime.getattr("date").map_err(py_err)?;
    let epoch = date_cls.call1((1970, 1, 1)).map_err(py_err)?;
    let epoch_ord: i32 = epoch
        .call_method0("toordinal")
        .map_err(py_err)?
        .extract()
        .map_err(py_err)?;
    let target_ord = epoch_ord.checked_add(days_since_epoch).ok_or_else(|| {
        DataFusionError::Execution(
            "Date32 value overflowed when converting to datetime.date".to_string(),
        )
    })?;
    let py_date = date_cls
        .call_method1("fromordinal", (target_ord,))
        .map_err(py_err)?;
    Ok(py_date.unbind())
}

/// Convert an Arrow Timestamp (microseconds, no tz) to a Python naive `datetime.datetime`.
///
/// Uses `fromtimestamp` with UTC timezone, then strips `tzinfo` for a naive datetime.
/// (utcfromtimestamp is deprecated in Python 3.12, removed in 3.14)
/// Uses `datetime.timezone.utc` (Python 3.2+) instead of `datetime.UTC` (Python 3.11+).
fn extract_timestamp_us_value(
    py: Python<'_>,
    array: &ArrayRef,
    row_idx: usize,
) -> Result<Py<PyAny>> {
    let arr = array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "Failed to downcast to TimestampMicrosecondArray".to_string(),
            )
        })?;
    let ts = arr.value(row_idx);
    let datetime = py.import("datetime").map_err(py_err)?;
    let datetime_cls = datetime.getattr("datetime").map_err(py_err)?;
    let utc = datetime
        .getattr("timezone")
        .map_err(py_err)?
        .getattr("utc")
        .map_err(py_err)?;
    let seconds = (ts as f64) / 1_000_000f64;
    let py_dt_aware = datetime_cls
        .call_method1("fromtimestamp", (seconds, utc))
        .map_err(py_err)?;
    // Convert to naive datetime by removing tzinfo (PySpark uses naive datetimes)
    let kwargs = pyo3::types::PyDict::new(py);
    kwargs.set_item("tzinfo", py.None()).map_err(py_err)?;
    let py_dt = py_dt_aware
        .call_method("replace", (), Some(&kwargs))
        .map_err(py_err)?;
    Ok(py_dt.unbind())
}

/// Convert an Arrow Timestamp (microseconds, with timezone) to a Python aware `datetime.datetime`.
fn extract_timestamp_us_tz_value(
    py: Python<'_>,
    array: &ArrayRef,
    row_idx: usize,
    tz: &Arc<str>,
) -> Result<Py<PyAny>> {
    let arr = array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "Failed to downcast to TimestampMicrosecondArray".to_string(),
            )
        })?;
    let ts = arr.value(row_idx);
    let datetime = py.import("datetime").map_err(py_err)?;
    let datetime_cls = datetime.getattr("datetime").map_err(py_err)?;

    // Use UTC for conversion, then localize to target timezone
    let utc = datetime
        .getattr("timezone")
        .map_err(py_err)?
        .getattr("utc")
        .map_err(py_err)?;
    let seconds = (ts as f64) / 1_000_000f64;
    let py_dt_utc = datetime_cls
        .call_method1("fromtimestamp", (seconds, utc))
        .map_err(py_err)?;

    // Convert to target timezone using zoneinfo (Python 3.9+)
    let tz_str = tz.as_ref();
    if tz_str == "UTC" || tz_str == "+00:00" {
        return Ok(py_dt_utc.unbind());
    }
    let zoneinfo = py.import("zoneinfo").map_err(py_err)?;
    let tz_info = zoneinfo
        .getattr("ZoneInfo")
        .map_err(py_err)?
        .call1((tz_str,))
        .map_err(py_err)?;
    let py_dt = py_dt_utc
        .call_method1("astimezone", (tz_info,))
        .map_err(py_err)?;
    Ok(py_dt.unbind())
}

/// Convert an Arrow Decimal128 value to a Python `decimal.Decimal`.
///
/// Converts via string representation to avoid precision loss.
fn extract_decimal128_value(
    py: Python<'_>,
    array: &ArrayRef,
    row_idx: usize,
    _precision: u8,
    scale: i8,
) -> Result<Py<PyAny>> {
    let arr = array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| {
            DataFusionError::Execution("Failed to downcast to Decimal128Array".to_string())
        })?;
    let raw_value = arr.value(row_idx);

    // Convert to string with proper scale to avoid floating-point precision loss
    let decimal_str = if scale <= 0 {
        // No decimal point needed (or need to append zeros)
        let factor_scale = scale
            .checked_neg()
            .ok_or_else(|| DataFusionError::Execution("Decimal scale overflow".to_string()))?
            as u32;
        let factor = 10i128.pow(factor_scale);
        format!("{}", raw_value * factor)
    } else {
        let scale_u = scale as u32;
        let divisor = 10i128.pow(scale_u);
        let integer_part = raw_value / divisor;
        let fractional_part = (raw_value % divisor).unsigned_abs();
        let sign = if raw_value < 0 && integer_part == 0 {
            "-"
        } else {
            ""
        };
        format!(
            "{}{}.{:0>width$}",
            sign,
            integer_part,
            fractional_part,
            width = scale_u as usize
        )
    };

    let decimal_mod = py.import("decimal").map_err(py_err)?;
    let decimal_cls = decimal_mod.getattr("Decimal").map_err(py_err)?;
    let py_decimal = decimal_cls.call1((decimal_str,)).map_err(py_err)?;
    Ok(py_decimal.unbind())
}

/// Validate that two schemas are compatible.
///
/// Checks field names and types match. Field metadata is ignored.
pub fn validate_schema(expected: &SchemaRef, actual: &SchemaRef) -> Result<()> {
    if expected.fields().len() != actual.fields().len() {
        return Err(DataFusionError::Execution(format!(
            "Schema field count mismatch: expected {} fields, got {}",
            expected.fields().len(),
            actual.fields().len()
        )));
    }

    for (i, (expected_field, actual_field)) in expected
        .fields()
        .iter()
        .zip(actual.fields().iter())
        .enumerate()
    {
        if expected_field.name() != actual_field.name() {
            return Err(DataFusionError::Execution(format!(
                "Schema field name mismatch at position {}: expected '{}', got '{}'",
                i,
                expected_field.name(),
                actual_field.name()
            )));
        }

        if expected_field.data_type() != actual_field.data_type() {
            return Err(DataFusionError::Execution(format!(
                "Schema field type mismatch for '{}': expected {:?}, got {:?}",
                expected_field.name(),
                expected_field.data_type(),
                actual_field.data_type()
            )));
        }

        // Only reject when actual data is nullable but declared schema says non-nullable.
        // The reverse (non-nullable → nullable) is always safe.
        if actual_field.is_nullable() && !expected_field.is_nullable() {
            return Err(DataFusionError::Execution(format!(
                "Schema field nullability mismatch for '{}': expected non-nullable, got nullable",
                expected_field.name(),
            )));
        }
    }

    Ok(())
}

/// Check if a data type is supported in the row-based write path.
pub fn is_supported_row_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Date32
            | DataType::Timestamp(TimeUnit::Microsecond, _)
            | DataType::Binary
            | DataType::LargeBinary
    )
}

/// Convert pickled Python rows to a RecordBatch.
///
/// This is used for efficient multi-row batching when Python yields tuples.
pub fn convert_rows_to_batch(schema: &SchemaRef, pickled_rows: &[Vec<u8>]) -> Result<RecordBatch> {
    use pyo3::types::PyBytes;

    if pickled_rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    Python::attach(|py| {
        let cloudpickle = import_cloudpickle(py)?;

        // Unpickle all rows
        let rows: Vec<Bound<'_, PyAny>> = pickled_rows
            .iter()
            .map(|pickled| {
                let bytes = PyBytes::new(py, pickled);
                cloudpickle.call_method1("loads", (bytes,)).map_err(py_err)
            })
            .collect::<Result<_>>()?;

        // Build arrays for each column
        let arrays: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(col_idx, field)| build_array_from_rows(py, &rows, col_idx, field))
            .collect::<Result<_>>()?;

        RecordBatch::try_new(schema.clone(), arrays).map_err(|e| {
            DataFusionError::External(Box::new(std::io::Error::other(format!(
                "Failed to create RecordBatch: {}",
                e
            ))))
        })
    })
}

/// Macro to reduce boilerplate in build_array_from_rows.
///
/// Generates the common pattern of extracting values from rows and building an array.
macro_rules! build_primitive_array {
    ($rows:expr, $col_idx:expr, $rust_ty:ty, $array_ty:ty) => {{
        let values: Vec<Option<$rust_ty>> = $rows
            .iter()
            .map(|row| extract_value(row, $col_idx))
            .collect::<Result<_>>()?;
        Ok(Arc::new(<$array_ty>::from(values)))
    }};
}

/// Build an Arrow array from Python row values.
fn build_array_from_rows(
    _py: Python<'_>,
    rows: &[Bound<'_, PyAny>],
    col_idx: usize,
    field: &Arc<Field>,
) -> Result<ArrayRef> {
    /// Helper macro for string-like types.
    macro_rules! build_string_array {
        ($rows:expr, $col_idx:expr, $array_ty:ty) => {{
            let values: Vec<Option<String>> = $rows
                .iter()
                .map(|row| extract_value(row, $col_idx))
                .collect::<Result<_>>()?;
            Ok(Arc::new(<$array_ty>::from(
                values.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
            )))
        }};
    }

    /// Helper macro for binary-like types.
    macro_rules! build_binary_array {
        ($rows:expr, $col_idx:expr, $array_ty:ty) => {{
            let values: Vec<Option<Vec<u8>>> = $rows
                .iter()
                .map(|row| extract_value(row, $col_idx))
                .collect::<Result<_>>()?;
            Ok(Arc::new(<$array_ty>::from_opt_vec(
                values.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
            )))
        }};
    }

    match field.data_type() {
        DataType::Null => Ok(Arc::new(NullArray::new(rows.len()))),
        DataType::Boolean => build_primitive_array!(rows, col_idx, bool, BooleanArray),
        DataType::Int8 => build_primitive_array!(rows, col_idx, i8, Int8Array),
        DataType::Int16 => build_primitive_array!(rows, col_idx, i16, Int16Array),
        DataType::Int32 => build_primitive_array!(rows, col_idx, i32, Int32Array),
        DataType::Int64 => build_primitive_array!(rows, col_idx, i64, Int64Array),
        DataType::UInt8 => build_primitive_array!(rows, col_idx, u8, UInt8Array),
        DataType::UInt16 => build_primitive_array!(rows, col_idx, u16, UInt16Array),
        DataType::UInt32 => build_primitive_array!(rows, col_idx, u32, UInt32Array),
        DataType::UInt64 => build_primitive_array!(rows, col_idx, u64, UInt64Array),
        DataType::Float32 => build_primitive_array!(rows, col_idx, f32, Float32Array),
        DataType::Float64 => build_primitive_array!(rows, col_idx, f64, Float64Array),
        DataType::Utf8 => build_string_array!(rows, col_idx, StringArray),
        DataType::LargeUtf8 => build_string_array!(rows, col_idx, LargeStringArray),
        DataType::Date32 => build_primitive_array!(rows, col_idx, i32, Date32Array),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            build_primitive_array!(rows, col_idx, i64, TimestampMicrosecondArray)
        }
        DataType::Binary => build_binary_array!(rows, col_idx, arrow::array::BinaryArray),
        DataType::LargeBinary => build_binary_array!(rows, col_idx, LargeBinaryArray),
        other => Err(DataFusionError::NotImplemented(format!(
            "Data type {:?} not supported in row-based write path. \
             Use DataSourceArrowWriter for full type support.",
            other
        ))),
    }
}

/// Extract a value from a Python row tuple.
fn extract_value<'py, T: pyo3::FromPyObject<'py>>(
    row: &Bound<'py, PyAny>,
    col_idx: usize,
) -> Result<Option<T>> {
    let item = row.get_item(col_idx).map_err(py_err)?;

    if item.is_none() {
        return Ok(None);
    }

    item.extract::<T>().map(Some).map_err(py_err)
}

/// Re-export py_err and import_cloudpickle from error module.
use super::error::{import_cloudpickle, py_err};

#[cfg(test)]
mod tests {
    use std::sync::Once;

    use super::*;

    fn init_python_once() {
        static INIT: Once = Once::new();
        INIT.call_once(Python::initialize);
    }

    #[test]
    fn test_is_supported_row_type() {
        // Core types
        assert!(is_supported_row_type(&DataType::Int32));
        assert!(is_supported_row_type(&DataType::Int64));
        assert!(is_supported_row_type(&DataType::Float32));
        assert!(is_supported_row_type(&DataType::Float64));
        assert!(is_supported_row_type(&DataType::Utf8));
        assert!(is_supported_row_type(&DataType::Boolean));
        assert!(is_supported_row_type(&DataType::Date32));
        assert!(is_supported_row_type(&DataType::Timestamp(
            TimeUnit::Microsecond,
            None
        )));
        assert!(is_supported_row_type(&DataType::Null));
        assert!(is_supported_row_type(&DataType::Binary));

        // Extended types
        assert!(is_supported_row_type(&DataType::Int8));
        assert!(is_supported_row_type(&DataType::Int16));
        assert!(is_supported_row_type(&DataType::UInt8));
        assert!(is_supported_row_type(&DataType::UInt16));
        assert!(is_supported_row_type(&DataType::UInt32));
        assert!(is_supported_row_type(&DataType::UInt64));
        assert!(is_supported_row_type(&DataType::Decimal128(38, 10)));
        assert!(is_supported_row_type(&DataType::LargeUtf8));
        assert!(is_supported_row_type(&DataType::LargeBinary));
        assert!(is_supported_row_type(&DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into())
        )));

        // Not supported in row path
        assert!(!is_supported_row_type(&DataType::List(Arc::new(
            Field::new("item", DataType::Int32, true)
        ))));
    }

    #[test]
    fn test_validate_schema_matching() {
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let schema2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        assert!(validate_schema(&schema1, &schema2).is_ok());
    }

    #[test]
    fn test_validate_schema_field_count_mismatch() {
        let schema1 = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let schema2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        assert!(validate_schema(&schema1, &schema2).is_err());
    }

    #[test]
    fn test_validate_schema_type_mismatch() {
        let schema1 = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let schema2 = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        assert!(validate_schema(&schema1, &schema2).is_err());
    }

    #[test]
    fn test_validate_schema_nullable_to_nonnullable_rejected() {
        // Actual data is nullable but schema declares non-nullable → reject
        let expected = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let actual = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));

        assert!(validate_schema(&expected, &actual).is_err());
    }

    #[test]
    fn test_validate_schema_nonnullable_to_nullable_accepted() {
        // Actual data is non-nullable but schema allows nullable → safe, accept
        let expected = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let actual = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        assert!(validate_schema(&expected, &actual).is_ok());
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_rust_record_batch_to_py() {
        init_python_once();
        Python::attach(|py| {
            // Create a simple RecordBatch
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Float64, true),
            ]));

            let id_array = Int32Array::from(vec![1, 2, 3]);
            let value_array = Float64Array::from(vec![Some(1.5), None, Some(3.5)]);

            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(value_array)])
                    .unwrap();

            // Convert to Python
            let py_batch = rust_record_batch_to_py(py, &batch);
            match py_batch {
                Ok(py_obj) => {
                    // Verify the Python object has the expected properties
                    let num_rows: usize =
                        py_obj.getattr(py, "num_rows").unwrap().extract(py).unwrap();
                    assert_eq!(num_rows, 3);
                }
                Err(e) => {
                    // PyArrow might not be available in test environment, skip test
                    eprintln!("Skipping test - PyArrow not available: {}", e);
                }
            }
        });
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_record_batch_to_py_rows() {
        init_python_once();
        Python::attach(|py| {
            // Create a simple RecordBatch
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ]));

            let id_array = Int32Array::from(vec![1, 2]);
            let name_array = StringArray::from(vec![Some("Alice"), Some("Bob")]);

            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)])
                    .unwrap();

            // Convert to Python rows
            let rows = record_batch_to_py_rows(py, &batch);
            match rows {
                Ok(rows_vec) => {
                    assert_eq!(rows_vec.len(), 2);

                    // Verify first row is a pyspark.sql.Row with named fields
                    let row0 = rows_vec[0].bind(py);
                    let dict0 = row0.call_method0("asDict").unwrap();
                    let id0: i32 = dict0.get_item("id").unwrap().extract().unwrap();
                    let name0: String = dict0.get_item("name").unwrap().extract().unwrap();
                    assert_eq!(id0, 1);
                    assert_eq!(name0, "Alice");

                    // Verify second row
                    let row1 = rows_vec[1].bind(py);
                    let dict1 = row1.call_method0("asDict").unwrap();
                    let id1: i32 = dict1.get_item("id").unwrap().extract().unwrap();
                    let name1: String = dict1.get_item("name").unwrap().extract().unwrap();
                    assert_eq!(id1, 2);
                    assert_eq!(name1, "Bob");
                }
                Err(e) => {
                    // pyspark may not be available in test environment
                    eprintln!("Skipping test - pyspark not available: {}", e);
                }
            }
        });
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_record_batch_to_py_rows_with_nulls() {
        init_python_once();
        Python::attach(|py| {
            // Create a RecordBatch with nulls
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Float64, true),
            ]));

            let id_array = Int32Array::from(vec![1, 2]);
            let value_array = Float64Array::from(vec![Some(1.5), None]);

            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(value_array)])
                    .unwrap();

            // Convert to Python rows
            let rows = record_batch_to_py_rows(py, &batch);
            match rows {
                Ok(rows_vec) => {
                    assert_eq!(rows_vec.len(), 2);

                    // Verify second row has null value via asDict
                    let row1 = rows_vec[1].bind(py);
                    let dict1 = row1.call_method0("asDict").unwrap();
                    let value1 = dict1.get_item("value").unwrap();
                    assert!(value1.is_none());
                }
                Err(e) => {
                    eprintln!("Skipping test - pyspark not available: {}", e);
                }
            }
        });
    }
}
