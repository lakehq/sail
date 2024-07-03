pub mod pyspark_udf;
pub mod pyspark_udtf;
pub mod python_udf;
pub mod unresolved_pyspark_udf;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::pyarrow::ToPyArrow;
use datafusion::common::Result;
use datafusion_common::DataFusionError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::cereal::partial_pyspark_udf::PartialPySparkUDF;
use crate::cereal::partial_python_udf::PartialPythonUDF;
use crate::cereal::pyspark_udtf::PySparkUDTF;

#[derive(Debug, Clone)]
pub enum PythonFunctionType {
    PartialPythonUDF(PartialPythonUDF),
    PartialPySparkUDF(PartialPySparkUDF),
    PySparkUDTF(PySparkUDTF),
}

impl PythonFunctionType {
    fn get_inner<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        match self {
            PythonFunctionType::PartialPythonUDF(udf) => udf.0.clone_ref(py).into_bound(py),
            PythonFunctionType::PartialPySparkUDF(udf) => udf.0.clone_ref(py).into_bound(py),
            PythonFunctionType::PySparkUDTF(udf) => udf.0.clone_ref(py).into_bound(py),
        }
    }

    pub fn get_python_function<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyAny>> {
        let python_function: Bound<PyAny> = self
            .get_inner(py)
            .get_item(0)
            .map_err(|err| DataFusionError::External(err.into()))?;
        Ok(python_function)
    }
}

/// Generates a unique function name with the memory address of the Python function.
/// Without this, lambda functions with the name `<lambda>` will be treated as the same function
/// by logical plan optimization rules (e.g. common sub-expression elimination), resulting in
/// incorrect logical plans.
pub fn get_udf_name(function_name: &str, function: &PyObject) -> String {
    format!("{}@0x{:x}", function_name, function.as_ptr() as usize)
}

pub fn get_python_builtins(py: Python) -> Result<Bound<PyModule>> {
    let builtins: Bound<PyModule> = PyModule::import_bound(py, pyo3::intern!(py, "builtins"))
        .map_err(|err| DataFusionError::External(err.into()))?;
    Ok(builtins)
}

pub fn get_python_builtins_list_function(py: Python) -> Result<Bound<PyAny>> {
    let builtins_list: Bound<PyAny> = get_python_builtins(py)?
        .getattr(pyo3::intern!(py, "list"))
        .map_err(|err| DataFusionError::External(err.into()))?;
    Ok(builtins_list)
}

pub fn get_python_builtins_str_function(py: Python) -> Result<Bound<PyAny>> {
    let builtins_str: Bound<PyAny> = get_python_builtins(py)?
        .getattr(pyo3::intern!(py, "str"))
        .map_err(|err| DataFusionError::External(err.into()))?;
    Ok(builtins_str)
}

pub fn get_pyarrow_array_function(py: Python) -> Result<Bound<PyAny>> {
    let pyarrow_module_array: Bound<PyAny> =
        PyModule::import_bound(py, pyo3::intern!(py, "pyarrow"))
            .map_err(|err| DataFusionError::External(err.into()))?
            .getattr(pyo3::intern!(py, "array"))
            .map_err(|err| DataFusionError::External(err.into()))?;
    Ok(pyarrow_module_array)
}

pub fn build_pyarrow_array_kwargs<'py>(
    py: Python<'py>,
    pyarrow_data_type: Bound<'py, PyAny>,
    from_pandas: bool,
) -> Result<Bound<'py, PyDict>> {
    let array_kwargs: Bound<PyDict> = PyDict::new_bound(py);
    array_kwargs
        .set_item("type", pyarrow_data_type)
        .map_err(|err| DataFusionError::External(err.into()))?;
    if from_pandas {
        array_kwargs
            .set_item("from_pandas", from_pandas)
            .map_err(|err| DataFusionError::External(err.into()))?;
    }
    Ok(array_kwargs)
}

pub fn get_pyarrow_output_data_type<'py>(
    output_type: &DataType,
    py: Python<'py>,
) -> Result<Bound<'py, PyAny>> {
    let pyarrow_output_data_type: Bound<PyAny> = output_type
        .to_pyarrow(py)
        .map_err(|err| DataFusionError::External(err.into()))?
        .clone_ref(py)
        .into_bound(py);
    Ok(pyarrow_output_data_type)
}

pub fn get_pyarrow_record_batch_from_pandas_function(py: Python) -> Result<Bound<PyAny>> {
    let record_batch_from_pandas: Bound<PyAny> =
        PyModule::import_bound(py, pyo3::intern!(py, "pyarrow"))
            .map_err(|err| DataFusionError::External(err.into()))?
            .getattr(pyo3::intern!(py, "RecordBatch"))
            .map_err(|err| DataFusionError::External(err.into()))?
            .getattr(pyo3::intern!(py, "from_pandas"))
            .map_err(|err| DataFusionError::External(err.into()))?;
    Ok(record_batch_from_pandas)
}

pub fn get_pyarrow_record_batch_from_pylist_function(py: Python) -> Result<Bound<PyAny>> {
    let record_batch_from_pylist: Bound<PyAny> =
        PyModule::import_bound(py, pyo3::intern!(py, "pyarrow"))
            .map_err(|err| DataFusionError::External(err.into()))?
            .getattr(pyo3::intern!(py, "RecordBatch"))
            .map_err(|err| DataFusionError::External(err.into()))?
            .getattr(pyo3::intern!(py, "from_pylist"))
            .map_err(|err| DataFusionError::External(err.into()))?;
    Ok(record_batch_from_pylist)
}

pub fn build_pyarrow_record_batch_kwargs<'py>(
    py: Python<'py>,
    pyarrow_schema: Bound<'py, PyAny>,
) -> Result<Bound<'py, PyDict>> {
    let record_batch_kwargs: Bound<PyDict> = PyDict::new_bound(py);
    record_batch_kwargs
        .set_item("schema", pyarrow_schema)
        .map_err(|err| DataFusionError::External(err.into()))?;
    Ok(record_batch_kwargs)
}

pub fn get_pyarrow_schema<'py>(schema: &SchemaRef, py: Python<'py>) -> Result<Bound<'py, PyAny>> {
    let pyarrow_schema: Bound<PyAny> = schema
        .to_pyarrow(py)
        .map_err(|err| DataFusionError::External(err.into()))?
        .clone_ref(py)
        .into_bound(py);
    Ok(pyarrow_schema)
}

pub fn get_pyarrow_table_function(py: Python) -> Result<Bound<PyAny>> {
    let pyarrow_table: Bound<PyAny> = PyModule::import_bound(py, pyo3::intern!(py, "pyarrow"))
        .map_err(|err| DataFusionError::External(err.into()))?
        .getattr(pyo3::intern!(py, "table"))
        .map_err(|err| DataFusionError::External(err.into()))?;
    Ok(pyarrow_table)
}
