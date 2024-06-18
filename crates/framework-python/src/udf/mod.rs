pub mod pyspark_udf;
pub mod pyspark_udtf;
pub mod python_udf;
pub mod unresolved_pyspark_udf;

use crate::cereal::partial_pyspark_udf::PartialPySparkUDF;
use crate::cereal::partial_python_udf::PartialPythonUDF;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion_common::DataFusionError;
use pyo3::{prelude::*, types::PyDict};

use crate::pyarrow::ToPyArrow;

pub trait PythonFunction {
    fn get_inner<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny>;
}

impl PythonFunction for PartialPythonUDF {
    fn get_inner<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.0.clone_ref(py).into_bound(py)
    }
}

impl PythonFunction for PartialPySparkUDF {
    fn get_inner<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.0.clone_ref(py).into_bound(py)
    }
}

// TODO: return DataFusionError::External for PyErr
pub trait CommonPythonUDF {
    type PythonFunctionType: PythonFunction;

    fn python_function(&self) -> &Self::PythonFunctionType;
    fn output_type(&self) -> &DataType;

    fn get_python_function<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyAny>> {
        let python_function: Bound<PyAny> = self
            .python_function()
            .get_inner(py)
            .get_item(0)
            .map_err(|err| DataFusionError::Internal(format!("python_function {}", err)))?;
        Ok(python_function)
    }

    fn get_python_builtins<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyModule>> {
        let builtins: Bound<PyModule> = PyModule::import_bound(py, pyo3::intern!(py, "builtins"))
            .map_err(|err| {
            DataFusionError::Internal(format!(
                "get_python_builtins_list_function Error importing builtins: {}",
                err
            ))
        })?;
        Ok(builtins)
    }

    fn get_python_builtins_list_function<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyAny>> {
        let builtins_list: Bound<PyAny> = self
            .get_python_builtins(py)?
            .getattr(pyo3::intern!(py, "list"))
            .map_err(|err| {
                DataFusionError::Internal(format!("Error getting builtins list function: {}", err))
            })?;
        Ok(builtins_list)
    }

    fn get_python_builtins_str_function<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyAny>> {
        let builtins_str: Bound<PyAny> = self
            .get_python_builtins(py)?
            .getattr(pyo3::intern!(py, "str"))
            .map_err(|err| {
                DataFusionError::Internal(format!("Error getting builtins str function: {}", err))
            })?;
        Ok(builtins_str)
    }

    fn get_pyarrow_module_array_function<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyAny>> {
        let pyarrow_module_array: Bound<PyAny> =
            PyModule::import_bound(py, pyo3::intern!(py, "pyarrow"))
                .map_err(|err| DataFusionError::Internal(format!("pyarrow import error: {}", err)))?
                .getattr(pyo3::intern!(py, "array"))
                .map_err(|err| {
                    DataFusionError::Internal(format!("pyarrow array error: {}", err))
                })?;
        Ok(pyarrow_module_array)
    }

    fn get_pyarrow_output_data_type<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyAny>> {
        let pyarrow_output_data_type: Bound<PyAny> = self
            .output_type()
            .to_pyarrow(py)
            .map_err(|err| DataFusionError::Internal(format!("output_type to_pyarrow {}", err)))?
            .clone_ref(py)
            .into_bound(py);
        Ok(pyarrow_output_data_type)
    }

    fn build_pyarrow_module_array_kwargs<'py>(
        &self,
        py: Python<'py>,
        output_data_type: Bound<'py, PyAny>,
        from_pandas: bool,
    ) -> Result<Bound<'py, PyDict>> {
        let output_data_type_kwargs: Bound<PyDict> = PyDict::new_bound(py);
        output_data_type_kwargs
            .set_item("type", output_data_type)
            .map_err(|err| DataFusionError::Internal(format!("kwargs {}", err)))?;
        if from_pandas {
            output_data_type_kwargs
                .set_item("from_pandas", from_pandas)
                .map_err(|err| DataFusionError::Internal(format!("kwargs from_pandas {}", err)))?;
        }
        Ok(output_data_type_kwargs)
    }
}
