use pyo3::pyclass;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
#[pyclass(frozen, from_py_object)]
pub struct PySparkUdfConfig {
    #[pyo3(get)]
    pub session_timezone: String,
    #[pyo3(get, name = "window_bound_types")]
    pub pandas_window_bound_types: Option<String>,
    #[pyo3(get, name = "assign_columns_by_name")]
    pub pandas_grouped_map_assign_columns_by_name: bool,
    #[pyo3(get, name = "arrow_convert_safely")]
    pub pandas_convert_to_arrow_array_safely: bool,
    #[pyo3(get)]
    pub arrow_max_records_per_batch: usize,
    #[pyo3(get)]
    pub python_udf_pandas_conversion_enabled: bool,
    #[pyo3(get)]
    pub python_udtf_pandas_conversion_enabled: bool,
    #[pyo3(get)]
    pub python_udf_pandas_int_to_decimal_coercion_enabled: bool,
    #[pyo3(get)]
    pub binary_as_bytes: bool,
}

impl Default for PySparkUdfConfig {
    fn default() -> Self {
        Self {
            session_timezone: "UTC".to_string(),
            pandas_window_bound_types: None,
            pandas_grouped_map_assign_columns_by_name: true,
            pandas_convert_to_arrow_array_safely: false,
            arrow_max_records_per_batch: 10000,
            python_udf_pandas_conversion_enabled: false,
            python_udtf_pandas_conversion_enabled: false,
            python_udf_pandas_int_to_decimal_coercion_enabled: false,
            binary_as_bytes: true,
        }
    }
}

impl PySparkUdfConfig {
    pub fn with_pandas_window_bound_types(mut self, value: Option<String>) -> Self {
        self.pandas_window_bound_types = value;
        self
    }

    /// Converts the configuration to a list of key-value pairs,
    /// so that it can be read by `worker.py` in PySpark.
    /// Missing values are not included.
    pub fn to_key_value_pairs(&self) -> Vec<(String, String)> {
        let mut out = vec![];
        out.push((
            "spark.sql.session.timeZone".to_string(),
            self.session_timezone.clone(),
        ));
        if let Some(value) = &self.pandas_window_bound_types {
            out.push(("pandas_window_bound_types".to_string(), value.clone()));
        }
        out.push((
            "spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName".to_string(),
            self.pandas_grouped_map_assign_columns_by_name.to_string(),
        ));
        out.push((
            "spark.sql.execution.pandas.convertToArrowArraySafely".to_string(),
            self.pandas_convert_to_arrow_array_safely.to_string(),
        ));
        out.push((
            "spark.sql.execution.arrow.maxRecordsPerBatch".to_string(),
            self.arrow_max_records_per_batch.to_string(),
        ));
        out.push((
            "spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled".to_string(),
            self.python_udf_pandas_conversion_enabled.to_string(),
        ));
        out.push((
            "spark.sql.legacy.execution.pythonUDTF.pandas.conversion.enabled".to_string(),
            self.python_udtf_pandas_conversion_enabled.to_string(),
        ));
        out.push((
            "spark.sql.execution.pythonUDF.pandas.intToDecimalCoercionEnabled".to_string(),
            self.python_udf_pandas_int_to_decimal_coercion_enabled
                .to_string(),
        ));
        out.push((
            "spark.sql.execution.pyspark.binaryAsBytes".to_string(),
            self.binary_as_bytes.to_string(),
        ));
        out
    }
}
