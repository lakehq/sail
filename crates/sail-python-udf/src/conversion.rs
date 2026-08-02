use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use datafusion_common::arrow::array::ArrayData;
use pyo3::exceptions::PyRuntimeError;
use pyo3::{Bound, BoundObject, IntoPyObject, Py, PyAny, PyErr, PyResult, Python};
use sail_common_datafusion::array::record_batch::{
    cast_array_recursively, cast_record_batch_positionally,
};
use sail_pyarrow::{FromPyArrow, ToPyArrow};

fn normalize_field_with_options(field: &FieldRef, large_var_types: bool) -> FieldRef {
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(normalize_data_type(field.data_type(), large_var_types)),
    )
}

fn normalize_data_type(data_type: &DataType, large_var_types: bool) -> DataType {
    match data_type {
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView if large_var_types => {
            DataType::LargeBinary
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => DataType::Binary,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View if large_var_types => {
            DataType::LargeUtf8
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::List(field) => {
            DataType::List(normalize_field_with_options(field, large_var_types))
        }
        DataType::ListView(field) => {
            DataType::List(normalize_field_with_options(field, large_var_types))
        }
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(normalize_field_with_options(field, large_var_types), *size)
        }
        DataType::LargeList(field) => {
            DataType::LargeList(normalize_field_with_options(field, large_var_types))
        }
        DataType::LargeListView(field) => {
            DataType::LargeList(normalize_field_with_options(field, large_var_types))
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| normalize_field_with_options(field, large_var_types))
                .collect(),
        ),
        DataType::Dictionary(_, value) => normalize_data_type(value, large_var_types),
        DataType::Map(field, sorted) => DataType::Map(
            normalize_field_with_options(field, large_var_types),
            *sorted,
        ),
        DataType::RunEndEncoded(_, values) => {
            normalize_data_type(values.data_type(), large_var_types)
        }
        _ => data_type.clone(),
    }
}

fn normalize_schema(schema: &Schema, large_var_types: bool) -> Schema {
    Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| normalize_field_with_options(field, large_var_types))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    )
}

fn normalize_array(array: &ArrayRef, large_var_types: bool) -> PyResult<ArrayRef> {
    cast_array_recursively(
        array,
        &normalize_data_type(array.data_type(), large_var_types),
    )
    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

fn normalize_record_batch(batch: &RecordBatch, large_var_types: bool) -> PyResult<RecordBatch> {
    let schema = Arc::new(normalize_schema(batch.schema().as_ref(), large_var_types));
    if schema.as_ref() == batch.schema().as_ref() {
        Ok(batch.clone())
    } else {
        cast_record_batch_positionally(batch.clone(), schema)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
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
    ) -> Result<Self::Output, Self::Error> {
        normalize_data_type(self, large_var_types)
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
    ) -> Result<Self::Output, Self::Error> {
        self.iter()
            .map(|x| normalize_data_type(x, large_var_types).to_pyarrow(py))
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
    ) -> Result<Self::Output, Self::Error> {
        normalize_array(self, large_var_types)?
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
    ) -> Result<Self::Output, Self::Error> {
        self.iter()
            .map(|x| x.try_to_py(py, large_var_types))
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
    ) -> Result<Self::Output, Self::Error> {
        self.as_slice().try_to_py(py, large_var_types)
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
    ) -> Result<Self::Output, Self::Error> {
        normalize_schema(self, large_var_types)
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
    ) -> Result<Self::Output, Self::Error> {
        normalize_schema(self.as_ref(), large_var_types)
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
    ) -> Result<Self::Output, Self::Error> {
        normalize_record_batch(self, large_var_types)?
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
        Self::from_pyarrow_bound(obj.bind(py))
    }
}

impl TryFromPy for RecordBatch {
    fn try_from_py(py: Python, obj: &Py<PyAny>) -> PyResult<Self> {
        Self::from_pyarrow_bound(obj.bind(py))
    }
}
