use datafusion::arrow::datatypes::{DataType, SchemaRef};
use pyo3::prelude::{PyAnyMethods, PyModule};
use pyo3::sync::GILOnceCell;
use pyo3::{intern, Bound, Py, PyAny, PyObject, PyResult, Python};

use crate::conversion::TryToPy;
use crate::udf::ColumnMatch;

const MODULE_NAME: &str = "utils.spark";
const MODULE_FILE_NAME: &str = "spark.py";
const MODULE_SOURCE_CODE: &str = include_str!("spark.py");

pub struct PySpark;

impl PySpark {
    fn module(py: Python) -> PyResult<Bound<PyModule>> {
        static MODULE: GILOnceCell<Py<PyModule>> = GILOnceCell::new();

        Ok(MODULE
            .get_or_try_init(py, || -> PyResult<_> {
                Ok(PyModule::from_code_bound(
                    py,
                    MODULE_SOURCE_CODE,
                    MODULE_FILE_NAME,
                    MODULE_NAME,
                )?
                .unbind())
            })?
            .clone_ref(py)
            .into_bound(py))
    }

    pub fn batch_udf<'py>(
        py: Python<'py>,
        udf: PyObject,
        input_types: &[DataType],
        output_type: &DataType,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::module(py)?
            .getattr(intern!(py, "PySparkBatchUdf"))?
            .call_method1(
                intern!(py, "init"),
                (udf, input_types.try_to_py(py)?, output_type.try_to_py(py)?),
            )
    }

    pub fn arrow_batch_udf<'py>(
        py: Python<'py>,
        udf: PyObject,
        input_types: &[DataType],
        output_type: &DataType,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::module(py)?
            .getattr(intern!(py, "PySparkArrowBatchUdf"))?
            .call_method1(
                intern!(py, "init"),
                (udf, input_types.try_to_py(py)?, output_type.try_to_py(py)?),
            )
    }

    pub fn scalar_pandas_udf<'py>(
        py: Python<'py>,
        udf: PyObject,
        input_types: &[DataType],
        output_type: &DataType,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::module(py)?
            .getattr(intern!(py, "PySparkScalarPandasUdf"))?
            .call_method1(
                intern!(py, "init"),
                (udf, input_types.try_to_py(py)?, output_type.try_to_py(py)?),
            )
    }

    pub fn scalar_pandas_iter_udf<'py>(
        py: Python<'py>,
        udf: PyObject,
        input_types: &[DataType],
        output_type: &DataType,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::module(py)?
            .getattr(intern!(py, "PySparkScalarPandasIterUdf"))?
            .call_method1(
                intern!(py, "init"),
                (udf, input_types.try_to_py(py)?, output_type.try_to_py(py)?),
            )
    }

    pub fn group_agg_udf<'py>(
        py: Python<'py>,
        udf: PyObject,
        input_names: Vec<String>,
        input_types: &[DataType],
        output_type: &DataType,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::module(py)?
            .getattr(intern!(py, "PySparkGroupAggUdf"))?
            .call_method1(
                intern!(py, "init"),
                (
                    udf,
                    input_names,
                    input_types.try_to_py(py)?,
                    output_type.try_to_py(py)?,
                ),
            )
    }

    pub fn group_map_udf(
        py: Python,
        udf: PyObject,
        input_names: Vec<String>,
        output_schema: SchemaRef,
        column_match: ColumnMatch,
    ) -> PyResult<Bound<PyAny>> {
        Self::module(py)?
            .getattr(intern!(py, "PySparkGroupMapUdf"))?
            .call_method1(
                intern!(py, "init"),
                (
                    udf,
                    input_names,
                    output_schema.try_to_py(py)?,
                    column_match.is_by_name(),
                ),
            )
    }

    pub fn cogroup_map_udf(
        py: Python,
        udf: PyObject,
        output_schema: SchemaRef,
        column_match: ColumnMatch,
    ) -> PyResult<Bound<PyAny>> {
        Self::module(py)?
            .getattr(intern!(py, "PySparkCoGroupMapUdf"))?
            .call_method1(
                intern!(py, "init"),
                (udf, output_schema.try_to_py(py)?, column_match.is_by_name()),
            )
    }

    pub fn map_pandas_iter_udf(
        py: Python,
        udf: PyObject,
        output_schema: SchemaRef,
    ) -> PyResult<Bound<PyAny>> {
        Self::module(py)?
            .getattr(intern!(py, "PySparkMapPandasIterUdf"))?
            .call_method1(intern!(py, "init"), (udf, output_schema.try_to_py(py)?))
    }

    pub fn map_arrow_iter_udf(py: Python, udf: PyObject) -> PyResult<Bound<PyAny>> {
        Self::module(py)?
            .getattr(intern!(py, "PySparkMapArrowIterUdf"))?
            .call_method1(intern!(py, "init"), (udf,))
    }
}
