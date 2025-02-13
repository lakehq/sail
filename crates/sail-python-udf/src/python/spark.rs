use std::ffi::CString;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use pyo3::prelude::PyModule;
use pyo3::sync::GILOnceCell;
use pyo3::{intern, Bound, Py, PyAny, PyResult, Python};

use crate::config::PySparkUdfConfig;
use crate::conversion::TryToPy;
use crate::python::py_init_object;

const MODULE_NAME: &str = "utils.spark";
const MODULE_FILE_NAME: &str = "spark.py";
const MODULE_SOURCE_CODE: &str = include_str!("spark.py");

pub struct PySpark;

impl PySpark {
    fn module(py: Python) -> PyResult<Bound<PyModule>> {
        static MODULE: GILOnceCell<Py<PyModule>> = GILOnceCell::new();

        Ok(MODULE
            .get_or_try_init(py, || -> PyResult<_> {
                Ok(PyModule::from_code(
                    py,
                    CString::new(MODULE_SOURCE_CODE)?.as_c_str(),
                    CString::new(MODULE_FILE_NAME)?.as_c_str(),
                    CString::new(MODULE_NAME)?.as_c_str(),
                )?
                .unbind())
            })?
            .clone_ref(py)
            .into_bound(py))
    }

    pub fn batch_udf<'py>(
        py: Python<'py>,
        udf: Bound<'py, PyAny>,
        input_types: &[DataType],
        output_type: &DataType,
        _config: &PySparkUdfConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        py_init_object(
            Self::module(py)?,
            intern!(py, "PySparkBatchUdf"),
            (udf, input_types.try_to_py(py)?, output_type.try_to_py(py)?),
        )
    }

    pub fn arrow_batch_udf<'py>(
        py: Python<'py>,
        udf: Bound<'py, PyAny>,
        config: &PySparkUdfConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        py_init_object(
            Self::module(py)?,
            intern!(py, "PySparkArrowBatchUdf"),
            (udf, config.clone()),
        )
    }

    pub fn scalar_pandas_udf<'py>(
        py: Python<'py>,
        udf: Bound<'py, PyAny>,
        config: &PySparkUdfConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        py_init_object(
            Self::module(py)?,
            intern!(py, "PySparkScalarPandasUdf"),
            (udf, config.clone()),
        )
    }

    pub fn scalar_pandas_iter_udf<'py>(
        py: Python<'py>,
        udf: Bound<'py, PyAny>,
        config: &PySparkUdfConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        py_init_object(
            Self::module(py)?,
            intern!(py, "PySparkScalarPandasIterUdf"),
            (udf, config.clone()),
        )
    }

    pub fn group_agg_udf<'py>(
        py: Python<'py>,
        udf: Bound<'py, PyAny>,
        input_names: Vec<String>,
        config: &PySparkUdfConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        py_init_object(
            Self::module(py)?,
            intern!(py, "PySparkGroupAggUdf"),
            (udf, input_names, config.clone()),
        )
    }

    pub fn group_map_udf<'py>(
        py: Python<'py>,
        udf: Bound<'py, PyAny>,
        input_names: Vec<String>,
        config: &PySparkUdfConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        py_init_object(
            Self::module(py)?,
            intern!(py, "PySparkGroupMapUdf"),
            (udf, input_names, config.clone()),
        )
    }

    pub fn cogroup_map_udf<'py>(
        py: Python<'py>,
        udf: Bound<'py, PyAny>,
        left_names: Vec<String>,
        right_names: Vec<String>,
        config: &PySparkUdfConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        py_init_object(
            Self::module(py)?,
            intern!(py, "PySparkCoGroupMapUdf"),
            (udf, left_names, right_names, config.clone()),
        )
    }

    pub fn map_pandas_iter_udf<'py>(
        py: Python<'py>,
        udf: Bound<'py, PyAny>,
        config: &PySparkUdfConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        py_init_object(
            Self::module(py)?,
            intern!(py, "PySparkMapPandasIterUdf"),
            (udf, config.clone()),
        )
    }

    pub fn map_arrow_iter_udf<'py>(
        py: Python<'py>,
        udf: Bound<'py, PyAny>,
        _config: &PySparkUdfConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        py_init_object(
            Self::module(py)?,
            intern!(py, "PySparkMapArrowIterUdf"),
            (udf,),
        )
    }

    pub fn table_udf<'py>(
        py: Python<'py>,
        udf: Bound<'py, PyAny>,
        input_types: &[DataType],
        passthrough_columns: usize,
        output_schema: &SchemaRef,
        config: &PySparkUdfConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        py_init_object(
            Self::module(py)?,
            intern!(py, "PySparkTableUdf"),
            (
                udf,
                input_types.try_to_py(py)?,
                passthrough_columns,
                output_schema.try_to_py(py)?,
                config.clone(),
            ),
        )
    }

    pub fn arrow_table_udf<'py>(
        py: Python<'py>,
        udf: Bound<'py, PyAny>,
        input_names: &[String],
        passthrough_columns: usize,
        output_schema: &SchemaRef,
        config: &PySparkUdfConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        py_init_object(
            Self::module(py)?,
            intern!(py, "PySparkArrowTableUdf"),
            (
                udf,
                input_names.to_vec(),
                passthrough_columns,
                output_schema.try_to_py(py)?,
                config.clone(),
            ),
        )
    }
}
