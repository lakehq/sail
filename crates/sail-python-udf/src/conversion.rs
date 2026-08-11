use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, make_array};
use datafusion::arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use datafusion_common::arrow::array::ArrayData;
use pyo3::exceptions::PyRuntimeError;
use pyo3::{Bound, BoundObject, IntoPyObject, Py, PyAny, PyErr, PyResult, Python};
use sail_common_datafusion::array::record_batch::{
    cast_array_recursively, cast_record_batch_positionally, retag_record_batch_timestamp_timezone,
    retag_timestamp_array,
};
use sail_pyarrow::{FromPyArrow, ToPyArrow};

fn normalize_field_with_options(
    field: &FieldRef,
    large_var_types: bool,
    timestamp_timezone: Option<&str>,
) -> FieldRef {
    Arc::new(field.as_ref().clone().with_data_type(normalize_data_type(
        field.data_type(),
        large_var_types,
        timestamp_timezone,
    )))
}

fn normalize_data_type(
    data_type: &DataType,
    large_var_types: bool,
    timestamp_timezone: Option<&str>,
) -> DataType {
    match data_type {
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView if large_var_types => {
            DataType::LargeBinary
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => DataType::Binary,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View if large_var_types => {
            DataType::LargeUtf8
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::Timestamp(unit, Some(timezone)) => DataType::Timestamp(
            *unit,
            Some(timestamp_timezone.map_or_else(|| Arc::clone(timezone), Arc::from)),
        ),
        DataType::List(field) => DataType::List(normalize_field_with_options(
            field,
            large_var_types,
            timestamp_timezone,
        )),
        DataType::ListView(field) => DataType::List(normalize_field_with_options(
            field,
            large_var_types,
            timestamp_timezone,
        )),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(
            normalize_field_with_options(field, large_var_types, timestamp_timezone),
            *size,
        ),
        DataType::LargeList(field) => DataType::LargeList(normalize_field_with_options(
            field,
            large_var_types,
            timestamp_timezone,
        )),
        DataType::LargeListView(field) => DataType::LargeList(normalize_field_with_options(
            field,
            large_var_types,
            timestamp_timezone,
        )),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| {
                    normalize_field_with_options(field, large_var_types, timestamp_timezone)
                })
                .collect(),
        ),
        DataType::Dictionary(_, value) => {
            normalize_data_type(value, large_var_types, timestamp_timezone)
        }
        DataType::Map(field, sorted) => DataType::Map(
            normalize_field_with_options(field, large_var_types, timestamp_timezone),
            *sorted,
        ),
        DataType::RunEndEncoded(_, values) => {
            normalize_data_type(values.data_type(), large_var_types, timestamp_timezone)
        }
        _ => data_type.clone(),
    }
}

fn normalize_schema(
    schema: &Schema,
    large_var_types: bool,
    timestamp_timezone: Option<&str>,
) -> Schema {
    Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| normalize_field_with_options(field, large_var_types, timestamp_timezone))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    )
}

fn normalize_array(
    array: &ArrayRef,
    large_var_types: bool,
    timestamp_timezone: Option<&str>,
) -> PyResult<ArrayRef> {
    let array = match timestamp_timezone {
        Some(timezone) => retag_timestamp_array(array, &Arc::from(timezone))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
        None => Arc::clone(array),
    };
    cast_array_recursively(
        &array,
        &normalize_data_type(array.data_type(), large_var_types, None),
    )
    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

fn normalize_record_batch(
    batch: &RecordBatch,
    large_var_types: bool,
    timestamp_timezone: Option<&str>,
) -> PyResult<RecordBatch> {
    let batch = match timestamp_timezone {
        Some(timezone) => retag_record_batch_timestamp_timezone(batch, timezone)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
        None => batch.clone(),
    };
    let schema = Arc::new(normalize_schema(
        batch.schema().as_ref(),
        large_var_types,
        None,
    ));
    if schema.as_ref() == batch.schema().as_ref() {
        Ok(batch)
    } else {
        cast_record_batch_positionally(batch, schema)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

fn canonicalize_array_data(data: ArrayData) -> PyResult<ArrayData> {
    retag_timestamp_array(&make_array(data), &Arc::from("UTC"))
        .map(Array::into_data)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

fn canonicalize_record_batch(batch: &RecordBatch) -> PyResult<RecordBatch> {
    retag_record_batch_timestamp_timezone(batch, "UTC")
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

/// A trait that defines the custom behavior of converting Rust data to a Python object.
pub trait TryToPy<'py> {
    type Target;
    type Output: BoundObject<'py, Self::Target>;
    type Error: Into<PyErr>;

    fn try_to_py(
        &self,
        py: Python<'py>,
        large_var_types: bool,
        timestamp_timezone: Option<&str>,
    ) -> Result<Self::Output, Self::Error>;
}

impl<'py> TryToPy<'py> for &DataType {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(
        &self,
        py: Python<'py>,
        large_var_types: bool,
        timestamp_timezone: Option<&str>,
    ) -> Result<Self::Output, Self::Error> {
        normalize_data_type(self, large_var_types, timestamp_timezone)
            .to_pyarrow(py)
            .map(|obj| obj.into_bound())
    }
}

impl<'py> TryToPy<'py> for &[DataType] {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(
        &self,
        py: Python<'py>,
        large_var_types: bool,
        timestamp_timezone: Option<&str>,
    ) -> Result<Self::Output, Self::Error> {
        self.iter()
            .map(|x| normalize_data_type(x, large_var_types, timestamp_timezone).to_pyarrow(py))
            .collect::<PyResult<Vec<_>>>()
            .map(|x| x.into_pyobject(py))?
    }
}

impl<'py> TryToPy<'py> for ArrayRef {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(
        &self,
        py: Python<'py>,
        large_var_types: bool,
        timestamp_timezone: Option<&str>,
    ) -> Result<Self::Output, Self::Error> {
        normalize_array(self, large_var_types, timestamp_timezone)?
            .into_data()
            .to_pyarrow(py)
            .map(|obj| obj.into_bound())
    }
}

impl<'py> TryToPy<'py> for &[ArrayRef] {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(
        &self,
        py: Python<'py>,
        large_var_types: bool,
        timestamp_timezone: Option<&str>,
    ) -> Result<Self::Output, Self::Error> {
        self.iter()
            .map(|x| x.try_to_py(py, large_var_types, timestamp_timezone))
            .collect::<PyResult<Vec<_>>>()
            .map(|x| x.into_pyobject(py))?
    }
}

impl<'py> TryToPy<'py> for Vec<ArrayRef> {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(
        &self,
        py: Python<'py>,
        large_var_types: bool,
        timestamp_timezone: Option<&str>,
    ) -> Result<Self::Output, Self::Error> {
        self.as_slice()
            .try_to_py(py, large_var_types, timestamp_timezone)
    }
}

impl<'py> TryToPy<'py> for &Schema {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(
        &self,
        py: Python<'py>,
        large_var_types: bool,
        timestamp_timezone: Option<&str>,
    ) -> Result<Self::Output, Self::Error> {
        normalize_schema(self, large_var_types, timestamp_timezone)
            .to_pyarrow(py)
            .map(|obj| obj.into_bound())
    }
}

impl<'py> TryToPy<'py> for SchemaRef {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(
        &self,
        py: Python<'py>,
        large_var_types: bool,
        timestamp_timezone: Option<&str>,
    ) -> Result<Self::Output, Self::Error> {
        normalize_schema(self.as_ref(), large_var_types, timestamp_timezone)
            .to_pyarrow(py)
            .map(|obj| obj.into_bound())
    }
}

impl<'py> TryToPy<'py> for RecordBatch {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn try_to_py(
        &self,
        py: Python<'py>,
        large_var_types: bool,
        timestamp_timezone: Option<&str>,
    ) -> Result<Self::Output, Self::Error> {
        normalize_record_batch(self, large_var_types, timestamp_timezone)?
            .to_pyarrow(py)
            .map(|obj| obj.into_bound())
    }
}

/// A trait that defines the custom behavior of converting a Python object to Rust data.
pub trait TryFromPy: Sized {
    fn try_from_py(py: Python, obj: &Py<PyAny>) -> PyResult<Self>;
}

impl TryFromPy for ArrayData {
    fn try_from_py(py: Python, obj: &Py<PyAny>) -> PyResult<Self> {
        canonicalize_array_data(Self::from_pyarrow_bound(obj.bind(py))?)
    }
}

impl TryFromPy for RecordBatch {
    fn try_from_py(py: Python, obj: &Py<PyAny>) -> PyResult<Self> {
        canonicalize_record_batch(&Self::from_pyarrow_bound(obj.bind(py))?)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{
        ArrayRef, FixedSizeListArray, StructArray, TimestampMicrosecondArray,
    };
    use datafusion::arrow::datatypes::{Field, Fields, TimeUnit};

    use super::*;

    #[test]
    fn python_input_uses_session_timezone_recursively() {
        let timestamp = DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")));
        let input = DataType::Struct(
            vec![
                Field::new("timestamp", timestamp.clone(), true),
                Field::new(
                    "timestamps",
                    DataType::List(Arc::new(Field::new_list_field(timestamp.clone(), true))),
                    true,
                ),
            ]
            .into(),
        );
        let session_timestamp = DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::from("America/Los_Angeles")),
        );
        let expected = DataType::Struct(
            vec![
                Field::new("timestamp", session_timestamp.clone(), true),
                Field::new(
                    "timestamps",
                    DataType::List(Arc::new(Field::new_list_field(session_timestamp, true))),
                    true,
                ),
            ]
            .into(),
        );

        assert_eq!(
            normalize_data_type(&input, false, Some("America/Los_Angeles")),
            expected
        );
    }

    #[test]
    fn python_input_retags_fixed_size_list_without_parsing_timezone() -> PyResult<()> {
        let values: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![Some(0), Some(1)]).with_timezone("UTC"));
        let input: ArrayRef = Arc::new(
            FixedSizeListArray::try_new(
                Arc::new(Field::new_list_field(values.data_type().clone(), true)),
                2,
                values,
                None,
            )
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
        );

        let output = normalize_array(&input, false, Some("+01:02:03"))?;
        let expected = DataType::FixedSizeList(
            Arc::new(Field::new_list_field(
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+01:02:03"))),
                true,
            )),
            2,
        );

        assert_eq!(output.data_type(), &expected);
        assert_eq!(
            output.to_data().child_data()[0].buffers(),
            input.to_data().child_data()[0].buffers()
        );
        Ok(())
    }

    #[test]
    fn python_output_is_canonicalized_to_utc_recursively() -> PyResult<()> {
        let input_type = DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::from("America/Los_Angeles")),
        );
        let fields = Fields::from(vec![Field::new("timestamp", input_type, true)]);
        let timestamp: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![Some(1_555_113_001_000_000)])
                .with_timezone("America/Los_Angeles"),
        );
        let input: ArrayRef = Arc::new(StructArray::new(fields, vec![timestamp], None));
        let output = canonicalize_array_data(input.into_data())?;
        let expected = DataType::Struct(
            vec![Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                true,
            )]
            .into(),
        );

        assert_eq!(output.data_type(), &expected);
        Ok(())
    }
}
