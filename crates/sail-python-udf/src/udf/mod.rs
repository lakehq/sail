pub mod pyspark_udaf;
pub mod pyspark_udf;
pub mod pyspark_udtf;
pub mod unresolved_pyspark_udf;

use std::hash::{DefaultHasher, Hash, Hasher};

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::pyarrow::ToPyArrow;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::error::PyUdfResult;

/// Generates a unique function name by combining the base name with a hash of the Python function's bytes.
/// Without this, lambda functions with the name `<lambda>` will be treated as the same function
/// by logical plan optimization rules (e.g. common sub-expression elimination), resulting in
/// incorrect logical plans.
pub fn get_udf_name(function_name: &str, function_bytes: &[u8]) -> String {
    // FIXME: Hash collision is possible
    let mut hasher = DefaultHasher::new();
    function_bytes.hash(&mut hasher);
    let hash = hasher.finish();
    format!("{function_name}@0x{hash:x}")
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

/// In Arrow all data types are nullable, meaning they support storing missing values. In pandas,
/// however, not all data types have support for missing data. Most notably, the default integer
/// data types do not, and will get casted to float when missing values are introduced. Therefore,
/// when an Arrow array or table gets converted to pandas, integer columns will become float when
/// missing values are present.
/// This function returns use all currently supported nullable dtypes by pandas.
/// See: https://arrow.apache.org/docs/python/pandas.html#nullable-types
pub fn build_pyarrow_to_pandas_kwargs(py: Python<'_>) -> PyUdfResult<Bound<'_, PyDict>> {
    let pa = PyModule::import_bound(py, intern!(py, "pyarrow"))?;
    let pd = PyModule::import_bound(py, intern!(py, "pandas"))?;

    let kwargs = PyDict::new_bound(py);
    let dtype_mapping = PyDict::new_bound(py);
    let mappings = [
        (
            pa.getattr(intern!(py, "int8"))?.call0()?,
            pd.getattr(intern!(py, "Int8Dtype"))?.call0()?,
        ),
        (
            pa.getattr(intern!(py, "int16"))?.call0()?,
            pd.getattr(intern!(py, "Int16Dtype"))?.call0()?,
        ),
        (
            pa.getattr(intern!(py, "int32"))?.call0()?,
            pd.getattr(intern!(py, "Int32Dtype"))?.call0()?,
        ),
        (
            pa.getattr(intern!(py, "int64"))?.call0()?,
            pd.getattr(intern!(py, "Int64Dtype"))?.call0()?,
        ),
        (
            pa.getattr(intern!(py, "uint8"))?.call0()?,
            pd.getattr(intern!(py, "UInt8Dtype"))?.call0()?,
        ),
        (
            pa.getattr(intern!(py, "uint16"))?.call0()?,
            pd.getattr(intern!(py, "UInt16Dtype"))?.call0()?,
        ),
        (
            pa.getattr(intern!(py, "uint32"))?.call0()?,
            pd.getattr(intern!(py, "UInt32Dtype"))?.call0()?,
        ),
        (
            pa.getattr(intern!(py, "uint64"))?.call0()?,
            pd.getattr(intern!(py, "UInt64Dtype"))?.call0()?,
        ),
        (
            pa.getattr(intern!(py, "bool_"))?.call0()?,
            pd.getattr(intern!(py, "BooleanDtype"))?.call0()?,
        ),
        (
            pa.getattr(intern!(py, "float32"))?.call0()?,
            pd.getattr(intern!(py, "Float32Dtype"))?.call0()?,
        ),
        (
            pa.getattr(intern!(py, "float64"))?.call0()?,
            pd.getattr(intern!(py, "Float64Dtype"))?.call0()?,
        ),
        (
            pa.getattr(intern!(py, "string"))?.call0()?,
            pd.getattr(intern!(py, "StringDtype"))?.call0()?,
        ),
    ];
    for (key, value) in mappings {
        dtype_mapping.set_item(key, value)?;
    }
    kwargs.set_item("types_mapper", dtype_mapping.getattr(intern!(py, "get"))?)?;
    // https://arrow.apache.org/docs/python/pandas.html#reducing-memory-use-in-table-to-pandas
    kwargs.set_item("split_blocks", true)?;
    Ok(kwargs)
}
