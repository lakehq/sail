use datafusion::arrow::array::{Array, ArrayData, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::pyarrow::{FromPyArrow, ToPyArrow};
use pyo3::prelude::{PyAnyMethods, PyModule};
use pyo3::sync::GILOnceCell;
use pyo3::{Bound, Py, PyAny, PyResult, Python};

const MODULE_SOURCE_CODE: &str = include_str!("helper.py");

pub struct UdfHelper;

impl UdfHelper {
    fn module(py: Python) -> PyResult<Bound<PyModule>> {
        static MODULE: GILOnceCell<Py<PyModule>> = GILOnceCell::new();

        Ok(MODULE
            .get_or_try_init(py, || -> PyResult<_> {
                Ok(
                    PyModule::from_code_bound(py, MODULE_SOURCE_CODE, "helper.py", "helper")?
                        .unbind(),
                )
            })?
            .clone_ref(py)
            .into_bound(py))
    }

    pub fn arrow_array_to_pyspark<'py>(
        py: Python<'py>,
        array: &ArrayRef,
    ) -> PyResult<Bound<'py, PyAny>> {
        let func = Self::module(py)?.getattr("arrow_array_to_pyspark")?;
        func.call1((array.into_data().to_pyarrow(py)?,))
    }

    pub fn pyspark_to_arrow_array(obj: &Bound<PyAny>, data_type: &DataType) -> PyResult<ArrayData> {
        let py = obj.py();
        let func = Self::module(py)?.getattr("pyspark_to_arrow_array")?;
        let array = func.call1((obj, data_type.to_pyarrow(py)?))?;
        ArrayData::from_pyarrow_bound(&array)
    }
}
