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
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, NullArray, RecordBatch,
    StringArray, TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_schema::SchemaRef;
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, Timelike, Utc};
use datafusion_common::{DataFusionError, Result};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDate, PyDateTime, PyTzInfoAccess};
use sail_common_datafusion::array::record_batch::{
    retag_record_batch_timestamp_timezone, retag_schema_timestamp_timezone,
};
use sail_common_datafusion::utils::datetime::{SparkTimeZone, parse_spark_timezone};

/// Convert a Python PyArrow RecordBatch to a Rust Arrow RecordBatch.
///
/// Uses the Arrow C Data Interface for zero-copy conversion.
pub fn py_record_batch_to_rust(
    _py: Python<'_>,
    py_batch: &Bound<'_, PyAny>,
) -> Result<RecordBatch> {
    use sail_pyarrow::FromPyArrow;

    let batch = RecordBatch::from_pyarrow_bound(py_batch).map_err(|e| {
        DataFusionError::External(Box::new(std::io::Error::other(format!(
            "Failed to convert PyArrow RecordBatch: {}",
            e
        ))))
    })?;
    retag_record_batch_timestamp_timezone(&batch, "UTC")
}

/// Convert a Rust Arrow Schema to a Python PyArrow Schema.
pub fn rust_schema_to_py(py: Python<'_>, schema: &SchemaRef) -> Result<Py<PyAny>> {
    use sail_pyarrow::ToPyArrow;

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
    use sail_pyarrow::FromPyArrow;

    let schema = Schema::from_pyarrow_bound(py_schema).map_err(|e| {
        DataFusionError::External(Box::new(std::io::Error::other(format!(
            "Failed to convert PyArrow Schema: {}",
            e
        ))))
    })?;

    Ok(Arc::new(retag_schema_timestamp_timezone(&schema, "UTC")?))
}

/// Convert a Rust Arrow RecordBatch to a Python PyArrow RecordBatch.
///
/// Uses the Arrow C Data Interface for zero-copy conversion.
/// This is used for the Arrow-based write path (DataSourceArrowWriter).
pub fn rust_record_batch_to_py(py: Python<'_>, batch: &RecordBatch) -> Result<Py<PyAny>> {
    use sail_pyarrow::ToPyArrow;

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

/// Converts rows for a `DataSourceWriter` using converters built once from its schema.
///
/// LTZ timestamps are exposed as session-local naive `datetime` values, matching
/// `ArrowTableToRowsConversion`. Arrow writers bypass this converter entirely.
pub(crate) struct RowWriterConverter {
    row_factory: Py<PyAny>,
    fields: Vec<RowWriterField>,
    decimal_class: Option<Py<PyAny>>,
}

enum RowWriterField {
    Direct,
    Decimal { scale: i8 },
    Date,
    TimestampNtz,
    TimestampLtz(SparkTimeZone),
}

impl RowWriterConverter {
    pub(crate) fn try_new(py: Python<'_>, schema: &SchemaRef) -> Result<Self> {
        let fields = schema
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                DataType::Decimal128(_, scale) => Ok(RowWriterField::Decimal { scale: *scale }),
                DataType::Date32 => Ok(RowWriterField::Date),
                DataType::Timestamp(TimeUnit::Microsecond, None) => {
                    Ok(RowWriterField::TimestampNtz)
                }
                DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)) => {
                    Ok(RowWriterField::TimestampLtz(parse_spark_timezone(
                        timezone,
                    )?))
                }
                data_type if is_supported_row_type(data_type) => Ok(RowWriterField::Direct),
                data_type => Err(DataFusionError::NotImplemented(format!(
                    "Data type {data_type:?} not supported in row-based write path. Use DataSourceArrowWriter for full type support."
                ))),
            })
            .collect::<Result<Vec<_>>>()?;
        let decimal_class = fields
            .iter()
            .any(|field| matches!(field, RowWriterField::Decimal { .. }))
            .then(|| {
                py.import("decimal")
                    .and_then(|module| module.getattr("Decimal"))
                    .map(Bound::unbind)
                    .map_err(py_err)
            })
            .transpose()?;

        Ok(Self {
            row_factory: get_row_factory(py, schema)?,
            fields,
            decimal_class,
        })
    }

    pub(crate) fn convert_row(
        &self,
        py: Python<'_>,
        batch: &RecordBatch,
        row: usize,
    ) -> Result<Py<PyAny>> {
        if batch.num_columns() != self.fields.len() {
            return Err(DataFusionError::Execution(format!(
                "Row converter expected {} columns, got {}",
                self.fields.len(),
                batch.num_columns()
            )));
        }
        let values = batch
            .columns()
            .iter()
            .zip(&self.fields)
            .map(|(array, field)| field.convert(py, array, row, self.decimal_class.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        let args = pyo3::types::PyTuple::new(py, values).map_err(py_err)?;
        self.row_factory
            .bind(py)
            .call1(args)
            .map(Bound::unbind)
            .map_err(py_err)
    }
}

impl RowWriterField {
    fn convert(
        &self,
        py: Python<'_>,
        array: &ArrayRef,
        row: usize,
        decimal_class: Option<&Py<PyAny>>,
    ) -> Result<Py<PyAny>> {
        use arrow::array::Array;

        if array.is_null(row) {
            return Ok(py.None());
        }
        match self {
            Self::Direct => extract_direct_python_value(py, array, row),
            Self::Decimal { scale } => extract_decimal128_value(
                py,
                array,
                row,
                *scale,
                decimal_class.ok_or_else(|| {
                    DataFusionError::Internal("decimal converter was not initialized".to_string())
                })?,
            ),
            Self::Date => extract_date32_value(py, array, row),
            Self::TimestampNtz => extract_timestamp_value(py, array, row, None),
            Self::TimestampLtz(timezone) => extract_timestamp_value(py, array, row, Some(timezone)),
        }
    }
}

fn get_row_factory(py: Python<'_>, schema: &SchemaRef) -> Result<Py<PyAny>> {
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

#[cfg(test)]
fn record_batch_to_py_rows(py: Python<'_>, batch: &RecordBatch) -> Result<Vec<Py<PyAny>>> {
    let converter = RowWriterConverter::try_new(py, batch.schema_ref())?;
    (0..batch.num_rows())
        .map(|row| converter.convert_row(py, batch, row))
        .collect()
}

/// Convert an Arrow value to a Python object, returning a proper error instead of panicking.
macro_rules! to_py_value {
    ($arr:expr_2021, $row_idx:expr_2021, $py:expr_2021) => {{
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

fn extract_direct_python_value(
    py: Python<'_>,
    array: &ArrayRef,
    row_idx: usize,
) -> Result<Py<PyAny>> {
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
        DataType::Utf8 => simple_extract!(StringArray, "StringArray"),
        DataType::LargeUtf8 => simple_extract!(LargeStringArray, "LargeStringArray"),
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
    let date = NaiveDate::from_ymd_opt(1970, 1, 1)
        .and_then(|epoch| epoch.checked_add_signed(Duration::days(i64::from(arr.value(row_idx)))))
        .ok_or_else(|| DataFusionError::Execution("Date32 value is out of range".to_string()))?;
    PyDate::new(py, date.year(), date.month() as u8, date.day() as u8)
        .map(|value| value.into_any().unbind())
        .map_err(py_err)
}

/// Converts physical timestamp micros to a naive Python datetime. LTZ values are
/// first rendered in the Spark session zone; NTZ values retain their wall clock.
fn extract_timestamp_value(
    py: Python<'_>,
    array: &ArrayRef,
    row_idx: usize,
    timezone: Option<&SparkTimeZone>,
) -> Result<Py<PyAny>> {
    let arr = array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "Failed to downcast to TimestampMicrosecondArray".to_string(),
            )
        })?;
    let instant = DateTime::<Utc>::from_timestamp_micros(arr.value(row_idx)).ok_or_else(|| {
        DataFusionError::Execution("Timestamp value is out of Python datetime range".to_string())
    })?;
    let datetime = match timezone {
        Some(timezone) => instant.with_timezone(timezone).naive_local(),
        None => instant.naive_utc(),
    };
    naive_datetime_to_py(py, &datetime)
}

fn naive_datetime_to_py(py: Python<'_>, value: &NaiveDateTime) -> Result<Py<PyAny>> {
    PyDateTime::new(
        py,
        value.year(),
        value.month() as u8,
        value.day() as u8,
        value.hour() as u8,
        value.minute() as u8,
        value.second() as u8,
        value.and_utc().timestamp_subsec_micros(),
        None,
    )
    .map(|value| value.into_any().unbind())
    .map_err(py_err)
}

/// Convert an Arrow Decimal128 value to a Python `decimal.Decimal`.
///
/// Converts via string representation to avoid precision loss.
fn extract_decimal128_value(
    py: Python<'_>,
    array: &ArrayRef,
    row_idx: usize,
    scale: i8,
    decimal_class: &Py<PyAny>,
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

    let py_decimal = decimal_class
        .bind(py)
        .call1((decimal_str,))
        .map_err(py_err)?;
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

/// Converts Python rows to Arrow using converters built once from the reader schema.
pub(crate) struct RowReaderConverter {
    schema: SchemaRef,
    utc: Py<PyAny>,
}

impl RowReaderConverter {
    pub(crate) fn try_new(py: Python<'_>, schema: SchemaRef) -> Result<Self> {
        if let Some(field) = schema
            .fields()
            .iter()
            .find(|field| !is_supported_row_type(field.data_type()))
        {
            return Err(DataFusionError::NotImplemented(format!(
                "Data type {:?} not supported in row-based read path. Use PyArrow RecordBatch output for full type support.",
                field.data_type()
            )));
        }
        let utc = py
            .import("datetime")
            .and_then(|module| module.getattr("timezone"))
            .and_then(|timezone| timezone.getattr("utc"))
            .map(Bound::unbind)
            .map_err(py_err)?;
        Ok(Self { schema, utc })
    }

    pub(crate) fn convert_rows(&self, py: Python<'_>, rows: &[Py<PyAny>]) -> Result<RecordBatch> {
        if rows.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }
        let arrays = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(col_idx, field)| {
                build_array_from_rows(py, rows, col_idx, field, self.utc.bind(py))
            })
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(Arc::clone(&self.schema), arrays).map_err(|error| {
            DataFusionError::External(Box::new(std::io::Error::other(format!(
                "Failed to create RecordBatch: {error}"
            ))))
        })
    }
}

/// Macro to reduce boilerplate in build_array_from_rows.
///
/// Generates the common pattern of extracting values from rows and building an array.
macro_rules! build_primitive_array {
    ($py:expr_2021, $rows:expr_2021, $col_idx:expr_2021, $rust_ty:ty, $array_ty:ty) => {{
        let values: Vec<Option<$rust_ty>> = $rows
            .iter()
            .map(|row| extract_value($py, row, $col_idx))
            .collect::<Result<_>>()?;
        Ok(Arc::new(<$array_ty>::from(values)))
    }};
}

/// Build an Arrow array from Python row values.
fn build_array_from_rows(
    py: Python<'_>,
    rows: &[Py<PyAny>],
    col_idx: usize,
    field: &Arc<Field>,
    utc: &Bound<'_, PyAny>,
) -> Result<ArrayRef> {
    /// Helper macro for string-like types.
    macro_rules! build_string_array {
        ($rows:expr_2021, $col_idx:expr_2021, $array_ty:ty) => {{
            let values: Vec<Option<String>> = $rows
                .iter()
                .map(|row| extract_value(py, row, $col_idx))
                .collect::<Result<_>>()?;
            Ok(Arc::new(<$array_ty>::from(
                values.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
            )))
        }};
    }

    /// Helper macro for binary-like types.
    macro_rules! build_binary_array {
        ($rows:expr_2021, $col_idx:expr_2021, $array_ty:ty) => {{
            let values: Vec<Option<Vec<u8>>> = $rows
                .iter()
                .map(|row| extract_value(py, row, $col_idx))
                .collect::<Result<_>>()?;
            Ok(Arc::new(<$array_ty>::from_opt_vec(
                values.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
            )))
        }};
    }

    match field.data_type() {
        DataType::Null => Ok(Arc::new(NullArray::new(rows.len()))),
        DataType::Boolean => build_primitive_array!(py, rows, col_idx, bool, BooleanArray),
        DataType::Int8 => build_primitive_array!(py, rows, col_idx, i8, Int8Array),
        DataType::Int16 => build_primitive_array!(py, rows, col_idx, i16, Int16Array),
        DataType::Int32 => build_primitive_array!(py, rows, col_idx, i32, Int32Array),
        DataType::Int64 => build_primitive_array!(py, rows, col_idx, i64, Int64Array),
        DataType::UInt8 => build_primitive_array!(py, rows, col_idx, u8, UInt8Array),
        DataType::UInt16 => build_primitive_array!(py, rows, col_idx, u16, UInt16Array),
        DataType::UInt32 => build_primitive_array!(py, rows, col_idx, u32, UInt32Array),
        DataType::UInt64 => build_primitive_array!(py, rows, col_idx, u64, UInt64Array),
        DataType::Float32 => build_primitive_array!(py, rows, col_idx, f32, Float32Array),
        DataType::Float64 => build_primitive_array!(py, rows, col_idx, f64, Float64Array),
        DataType::Utf8 => build_string_array!(rows, col_idx, StringArray),
        DataType::LargeUtf8 => build_string_array!(rows, col_idx, LargeStringArray),
        DataType::Date32 => {
            let values = rows
                .iter()
                .map(|row| extract_date32_from_row(py, row, col_idx))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(Date32Array::from(values)))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let values = rows
                .iter()
                .map(|row| extract_timestamp_from_row(py, row, col_idx, None, utc))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(TimestampMicrosecondArray::from(values)))
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)) => {
            let values = rows
                .iter()
                .map(|row| extract_timestamp_from_row(py, row, col_idx, Some(timezone), utc))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(
                TimestampMicrosecondArray::from(values).with_timezone(Arc::clone(timezone)),
            ))
        }
        DataType::Binary => build_binary_array!(rows, col_idx, arrow::array::BinaryArray),
        DataType::LargeBinary => build_binary_array!(rows, col_idx, LargeBinaryArray),
        other => Err(DataFusionError::NotImplemented(format!(
            "Data type {:?} not supported in row-based read path. \
             Use PyArrow RecordBatch output for full type support.",
            other
        ))),
    }
}

/// Extract a value from a Python row tuple.
fn extract_value<'py, T: for<'a> pyo3::FromPyObject<'a, 'py>>(
    py: Python<'py>,
    row: &'py Py<PyAny>,
    col_idx: usize,
) -> Result<Option<T>> {
    let item = row.bind(py).get_item(col_idx).map_err(py_err)?;

    if item.is_none() {
        return Ok(None);
    }

    item.extract::<T>().map(Some).map_err(|e| py_err(e.into()))
}

fn extract_date32_from_row(py: Python<'_>, row: &Py<PyAny>, col_idx: usize) -> Result<Option<i32>> {
    let item = row.bind(py).get_item(col_idx).map_err(py_err)?;
    if item.is_none() {
        return Ok(None);
    }
    let date = item
        .cast::<PyDate>()
        .map_err(|error| py_err(error.into()))?;
    let value = NaiveDate::from_ymd_opt(
        py_datetime_component(date.as_any(), "year")?,
        py_datetime_component(date.as_any(), "month")?,
        py_datetime_component(date.as_any(), "day")?,
    )
    .ok_or_else(|| DataFusionError::Execution("Python date is out of range".to_string()))?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
        .ok_or_else(|| DataFusionError::Internal("invalid Unix epoch".to_string()))?;
    i32::try_from((value - epoch).num_days())
        .map(Some)
        .map_err(|_| {
            DataFusionError::Execution("Python date is out of Arrow Date32 range".to_string())
        })
}

fn extract_timestamp_from_row(
    py: Python<'_>,
    row: &Py<PyAny>,
    col_idx: usize,
    timezone: Option<&Arc<str>>,
    utc: &Bound<'_, PyAny>,
) -> Result<Option<i64>> {
    let item = row.bind(py).get_item(col_idx).map_err(py_err)?;
    if item.is_none() {
        return Ok(None);
    }
    let datetime = item
        .cast::<PyDateTime>()
        .map_err(|error| py_err(error.into()))?;
    if timezone.is_none() {
        if datetime.get_tzinfo().is_some() {
            return Err(DataFusionError::Execution(
                "TimestampNTZType cannot accept a timezone-aware datetime".to_string(),
            ));
        }
        return datetime_to_micros(datetime).map(Some);
    }

    // This is deliberately Python's `astimezone(UTC)`: like PySpark it honors an
    // aware value's own offset and treats a naive value in the process local zone.
    let datetime = datetime
        .call_method1("astimezone", (utc,))
        .map_err(py_err)?;
    datetime_to_micros(
        datetime
            .cast::<PyDateTime>()
            .map_err(|error| py_err(error.into()))?,
    )
    .map(Some)
}

fn datetime_to_micros(value: &Bound<'_, PyDateTime>) -> Result<i64> {
    let year = py_datetime_component(value.as_any(), "year")?;
    let month = py_datetime_component(value.as_any(), "month")?;
    let day = py_datetime_component(value.as_any(), "day")?;
    let hour = py_datetime_component(value.as_any(), "hour")?;
    let minute = py_datetime_component(value.as_any(), "minute")?;
    let second = py_datetime_component(value.as_any(), "second")?;
    let microsecond = py_datetime_component(value.as_any(), "microsecond")?;
    let datetime = NaiveDate::from_ymd_opt(year, month, day)
        .and_then(|date| date.and_hms_micro_opt(hour, minute, second, microsecond))
        .ok_or_else(|| DataFusionError::Execution("Python datetime is out of range".to_string()))?;
    Ok(datetime.and_utc().timestamp_micros())
}

fn py_datetime_component<T>(value: &Bound<'_, PyAny>, name: &str) -> Result<T>
where
    for<'py> T: FromPyObject<'py, 'py>,
{
    value
        .getattr(name)
        .map_err(py_err)?
        .extract()
        .map_err(|error: T::Error| py_err(error.into()))
}

use super::error::py_err;

#[cfg(test)]
mod tests {
    use arrow::array::Array;
    use pyo3::types::PyTuple;

    use super::*;

    fn init_python() {
        Python::initialize();
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
        init_python();
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
        init_python();
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
        init_python();
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

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_row_writer_timestamp_semantics() {
        init_python();
        Python::attach(|py| {
            let ltz: ArrayRef = Arc::new(
                TimestampMicrosecondArray::from(vec![-3_723_000_000]).with_timezone("+01:02:03"),
            );
            let timezone = parse_spark_timezone("+01:02:03").unwrap();
            let value = extract_timestamp_value(py, &ltz, 0, Some(&timezone)).unwrap();
            assert_eq!(
                value
                    .bind(py)
                    .call_method0("isoformat")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "1970-01-01T00:00:00"
            );
            assert!(value.bind(py).getattr("tzinfo").unwrap().is_none());

            let ntz: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![0]));
            let value = extract_timestamp_value(py, &ntz, 0, None).unwrap();
            assert_eq!(
                value
                    .bind(py)
                    .call_method0("isoformat")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "1970-01-01T00:00:00"
            );
            assert!(value.bind(py).getattr("tzinfo").unwrap().is_none());
        });
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_row_reader_timestamp_semantics() {
        init_python();
        Python::attach(|py| {
            let datetime = py.import("datetime").unwrap();
            let offset = datetime
                .getattr("timezone")
                .unwrap()
                .call1((datetime
                    .getattr("timedelta")
                    .unwrap()
                    .call1((0, 3_723))
                    .unwrap(),))
                .unwrap();
            let aware = datetime
                .getattr("datetime")
                .unwrap()
                .call1((1970, 1, 1, 0, 0, 0, 0, offset))
                .unwrap()
                .unbind();
            let naive = datetime
                .getattr("datetime")
                .unwrap()
                .call1((1970, 1, 1))
                .unwrap()
                .unbind();
            let row = PyTuple::new(py, [aware, naive])
                .unwrap()
                .into_any()
                .unbind();
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "ltz",
                    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                    false,
                ),
                Field::new(
                    "ntz",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
            ]));
            let converter = RowReaderConverter::try_new(py, schema).unwrap();
            let batch = converter.convert_rows(py, &[row]).unwrap();
            let ltz = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let ntz = batch
                .column(1)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            assert_eq!(ltz.value(0), -3_723_000_000);
            assert_eq!(
                ltz.data_type(),
                &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
            );
            assert_eq!(ntz.value(0), 0);
        });
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_row_reader_rejects_aware_timestamp_ntz() {
        init_python();
        Python::attach(|py| {
            let datetime = py.import("datetime").unwrap();
            let aware = datetime
                .getattr("datetime")
                .unwrap()
                .call1((
                    1970,
                    1,
                    1,
                    0,
                    0,
                    0,
                    0,
                    datetime
                        .getattr("timezone")
                        .unwrap()
                        .getattr("utc")
                        .unwrap(),
                ))
                .unwrap()
                .unbind();
            let row = PyTuple::new(py, [aware]).unwrap().into_any().unbind();
            let schema = Arc::new(Schema::new(vec![Field::new(
                "ntz",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )]));
            let converter = RowReaderConverter::try_new(py, schema).unwrap();
            let error = converter.convert_rows(py, &[row]).unwrap_err();
            assert!(error.to_string().contains("TimestampNTZType"));
        });
    }
}
