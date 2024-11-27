use pyo3::prelude::{PyAnyMethods, PyModule, PyObject};
use pyo3::sync::Interned;
use pyo3::types::PyDict;
use pyo3::{intern, Bound, PyAny, PyResult, Python};

use crate::utils::std::PyFunctools;

/// Methods for working with the `pyarrow` module.
pub struct PyArrow;

impl PyArrow {
    fn module(py: Python) -> PyResult<Bound<PyModule>> {
        PyModule::import_bound(py, intern!(py, "pyarrow"))
    }

    /// Creates a partial function for `pyarrow.array()`.
    pub fn array(py: Python, options: PyArrowArrayOptions) -> PyResult<Bound<PyAny>> {
        let func = Self::module(py)?.getattr(intern!(py, "array"))?;
        let kwargs = PyDict::new_bound(py);
        if let Some(r#type) = options.r#type {
            kwargs.set_item(intern!(py, "type"), r#type)?;
        }
        if let Some(from_pandas) = options.from_pandas {
            kwargs.set_item(intern!(py, "from_pandas"), from_pandas)?;
        }
        PyFunctools::partial(py)?.call((func,), Some(&kwargs))
    }
}

#[derive(Default)]
pub struct PyArrowArrayOptions {
    pub r#type: Option<PyObject>,
    pub from_pandas: Option<bool>,
}

/// Methods for working with the `pyarrow.Array` class.
pub struct PyArrowArray;

impl PyArrowArray {
    fn class(py: Python) -> PyResult<Bound<PyAny>> {
        PyArrow::module(py)?.getattr(intern!(py, "Array"))
    }

    /// Creates a partial function for `pyarrow.Array.to_pandas()`.
    pub fn to_pandas(py: Python, options: PyArrowToPandasOptions) -> PyResult<Bound<PyAny>> {
        let func = Self::class(py)?.getattr(intern!(py, "to_pandas"))?;
        let kwargs = get_pyarrow_to_pandas_kwargs(py, options)?;
        PyFunctools::partial(py)?.call((func,), Some(&kwargs))
    }

    /// Creates a partial function for `pyarrow.Array.to_pylist()`.
    pub fn to_pylist(py: Python) -> PyResult<Bound<PyAny>> {
        Self::class(py)?.getattr(intern!(py, "to_pylist"))
    }
}

/// Methods for working with the `pyarrow.RecordBatch` class.
pub struct PyArrowRecordBatch;

impl PyArrowRecordBatch {
    fn class(py: Python) -> PyResult<Bound<PyAny>> {
        PyArrow::module(py)?.getattr(intern!(py, "RecordBatch"))
    }

    /// Creates a partial function for `pyarrow.RecordBatch.from_pandas()`.
    pub fn from_pandas(py: Python, schema: Option<PyObject>) -> PyResult<Bound<PyAny>> {
        let func = Self::class(py)?.getattr(intern!(py, "from_pandas"))?;
        let kwargs = PyDict::new_bound(py);
        if let Some(schema) = schema {
            kwargs.set_item(intern!(py, "schema"), schema)?;
        }
        PyFunctools::partial(py)?.call((func,), Some(&kwargs))
    }

    /// Creates a partial function for `pyarrow.RecordBatch.from_pylist()`.
    pub fn from_pylist(py: Python, schema: Option<PyObject>) -> PyResult<Bound<PyAny>> {
        let func = Self::class(py)?.getattr(intern!(py, "from_pylist"))?;
        let kwargs = PyDict::new_bound(py);
        if let Some(schema) = schema {
            kwargs.set_item(intern!(py, "schema"), schema)?;
        }
        PyFunctools::partial(py)?.call((func,), Some(&kwargs))
    }

    /// Creates a partial function for `pyarrow.RecordBatch.to_pandas()`.
    pub fn to_pandas(py: Python, options: PyArrowToPandasOptions) -> PyResult<Bound<PyAny>> {
        let func = Self::class(py)?.getattr(intern!(py, "to_pandas"))?;
        let kwargs = get_pyarrow_to_pandas_kwargs(py, options)?;
        PyFunctools::partial(py)?.call((func,), Some(&kwargs))
    }
}

pub struct PyArrowToPandasOptions {
    pub use_pandas_nullable_types: bool,
}

fn get_pyarrow_to_pandas_kwargs(
    py: Python,
    options: PyArrowToPandasOptions,
) -> PyResult<Bound<PyDict>> {
    let kwargs = PyDict::new_bound(py);
    if options.use_pandas_nullable_types {
        kwargs.set_item(
            intern!(py, "types_mapper"),
            get_pyarrow_to_pandas_nullable_type_mapper(py)?,
        )?;
    }
    // https://arrow.apache.org/docs/python/pandas.html#reducing-memory-use-in-table-to-pandas
    kwargs.set_item(intern!(py, "split_blocks"), true)?;
    Ok(kwargs)
}

/// Returns a mapper function for all currently supported nullable types in Pandas.
/// See also: https://arrow.apache.org/docs/python/pandas.html#nullable-types
fn get_pyarrow_to_pandas_nullable_type_mapper(py: Python) -> PyResult<Bound<PyAny>> {
    static ARROW_TO_PANDAS_NULLABLE_TYPES: [(Interned, Interned); 12] = [
        (Interned::new("int8"), Interned::new("Int8Dtype")),
        (Interned::new("int16"), Interned::new("Int16Dtype")),
        (Interned::new("int32"), Interned::new("Int32Dtype")),
        (Interned::new("int64"), Interned::new("Int64Dtype")),
        (Interned::new("uint8"), Interned::new("UInt8Dtype")),
        (Interned::new("uint16"), Interned::new("UInt16Dtype")),
        (Interned::new("uint32"), Interned::new("UInt32Dtype")),
        (Interned::new("uint64"), Interned::new("UInt64Dtype")),
        (Interned::new("bool_"), Interned::new("BooleanDtype")),
        (Interned::new("float32"), Interned::new("Float32Dtype")),
        (Interned::new("float64"), Interned::new("Float64Dtype")),
        (Interned::new("string"), Interned::new("StringDtype")),
    ];

    let pa = PyModule::import_bound(py, intern!(py, "pyarrow"))?;
    let pd = PyModule::import_bound(py, intern!(py, "pandas"))?;
    let mapping = PyDict::new_bound(py);
    for (arrow_type, pandas_type) in ARROW_TO_PANDAS_NULLABLE_TYPES.iter() {
        mapping.set_item(
            pa.getattr(arrow_type.get(py))?.call0()?,
            pd.getattr(pandas_type.get(py))?.call0()?,
        )?;
    }
    mapping.getattr(intern!(py, "get"))
}
