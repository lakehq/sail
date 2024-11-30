use datafusion::arrow::datatypes::SchemaRef;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::{PyList, PyString};
use pyo3::{intern, Bound, PyAny, PyResult};

/// Methods for working with the `pandas.DataFrame` class.
pub struct PandasDataFrame;

impl PandasDataFrame {
    pub fn has_string_columns(df: &Bound<PyAny>) -> PyResult<bool> {
        let py = df.py();
        Ok(df
            .getattr(intern!(py, "columns"))?
            .iter()?
            .map(|c| c.map(|c| c.is_instance_of::<PyString>()))
            .collect::<PyResult<Vec<bool>>>()?
            .iter()
            .any(|x| *x))
    }

    pub fn rename_columns_by_position<'py>(
        df: &Bound<'py, PyAny>,
        schema: &SchemaRef,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = df.py();
        let columns = schema
            .fields()
            .iter()
            .map(|x| x.name().clone())
            .collect::<Vec<_>>();
        let truncated = df
            .getattr(intern!(py, "columns"))?
            .iter()?
            .take(columns.len())
            .collect::<PyResult<Vec<_>>>()?;
        let df = df.get_item(truncated)?;
        df.setattr(intern!(py, "columns"), columns)?;
        Ok(df)
    }

    /// Converts the columns of a Pandas DataFrame to a Python list of Pandas Series.
    pub fn to_series_list<'py>(df: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let py = df.py();
        let columns = df.getattr(intern!(py, "columns"))?;
        let items = columns
            .iter()?
            .map(|c| df.get_item(c?))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyList::new_bound(py, items).into_any())
    }
}
