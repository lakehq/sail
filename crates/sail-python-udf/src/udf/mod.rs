pub mod pyspark_udaf;
pub mod pyspark_udf;
pub mod pyspark_udtf;
pub mod unresolved_pyspark_udf;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::pyarrow::ToPyArrow;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::error::PyUdfResult;

/// Generates a unique function name with the memory address of the Python function.
/// Without this, lambda functions with the name `<lambda>` will be treated as the same function
/// by logical plan optimization rules (e.g. common sub-expression elimination), resulting in
/// incorrect logical plans.
pub fn get_udf_name(function_name: &str, function: &PyObject) -> String {
    format!("{}@0x{:x}", function_name, function.as_ptr() as usize)
}

pub fn get_python_builtins(py: Python) -> PyUdfResult<Bound<PyModule>> {
    Ok(PyModule::import_bound(py, intern!(py, "builtins"))?)
}

pub fn get_python_builtins_list_function(py: Python) -> PyUdfResult<Bound<PyAny>> {
    Ok(get_python_builtins(py)?.getattr(intern!(py, "list"))?)
}

pub fn get_python_builtins_str_function(py: Python) -> PyUdfResult<Bound<PyAny>> {
    Ok(get_python_builtins(py)?.getattr(intern!(py, "str"))?)
}

pub fn get_pyarrow_array_function(py: Python) -> PyUdfResult<Bound<PyAny>> {
    Ok(PyModule::import_bound(py, intern!(py, "pyarrow"))?.getattr(intern!(py, "array"))?)
}

pub fn build_pyarrow_array_kwargs<'py>(
    py: Python<'py>,
    pyarrow_data_type: Bound<'py, PyAny>,
    from_pandas: bool,
) -> PyUdfResult<Bound<'py, PyDict>> {
    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("type", pyarrow_data_type)?;
    if from_pandas {
        kwargs.set_item("from_pandas", from_pandas)?;
    }
    Ok(kwargs)
}

pub fn get_pyarrow_output_data_type<'py>(
    output_type: &DataType,
    py: Python<'py>,
) -> PyUdfResult<Bound<'py, PyAny>> {
    Ok(output_type.to_pyarrow(py)?.clone_ref(py).into_bound(py))
}

pub fn get_pyarrow_record_batch_from_pandas_function(py: Python) -> PyUdfResult<Bound<PyAny>> {
    Ok(PyModule::import_bound(py, intern!(py, "pyarrow"))?
        .getattr(intern!(py, "RecordBatch"))?
        .getattr(intern!(py, "from_pandas"))?)
}

pub fn get_pyarrow_record_batch_from_pylist_function(py: Python) -> PyUdfResult<Bound<PyAny>> {
    Ok(PyModule::import_bound(py, intern!(py, "pyarrow"))?
        .getattr(intern!(py, "RecordBatch"))?
        .getattr(intern!(py, "from_pylist"))?)
}

pub fn build_pyarrow_record_batch_kwargs<'py>(
    py: Python<'py>,
    pyarrow_schema: Bound<'py, PyAny>,
) -> PyUdfResult<Bound<'py, PyDict>> {
    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("schema", pyarrow_schema)?;
    Ok(kwargs)
}

pub fn get_pyarrow_schema<'py>(
    schema: &SchemaRef,
    py: Python<'py>,
) -> PyUdfResult<Bound<'py, PyAny>> {
    Ok(schema.to_pyarrow(py)?.clone_ref(py).into_bound(py))
}

pub fn get_pyarrow_table_function(py: Python) -> PyUdfResult<Bound<PyAny>> {
    Ok(PyModule::import_bound(py, intern!(py, "pyarrow"))?.getattr(intern!(py, "table"))?)
}
