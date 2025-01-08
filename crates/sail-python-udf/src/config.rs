use sail_common::datetime::get_system_timezone;

use crate::error::{PyUdfError, PyUdfResult};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct SparkUdfConfig {
    pub session_timezone: String,
    pub system_timezone: String,
    pub pandas_window_bound_types: Option<String>,
    pub pandas_grouped_map_assign_columns_by_name: bool,
    pub pandas_convert_to_arrow_array_safely: bool,
    pub arrow_max_records_per_batch: usize,
}

impl SparkUdfConfig {
    pub fn new() -> PyUdfResult<Self> {
        Ok(Self {
            system_timezone: get_system_timezone()
                .map_err(|e| PyUdfError::internal(format!("{e}")))?,
            ..Default::default()
        })
    }
}

impl Default for SparkUdfConfig {
    fn default() -> Self {
        Self {
            session_timezone: "UTC".to_string(),
            system_timezone: "UTC".to_string(),
            pandas_window_bound_types: None,
            pandas_grouped_map_assign_columns_by_name: true,
            pandas_convert_to_arrow_array_safely: false,
            arrow_max_records_per_batch: 10000,
        }
    }
}

impl SparkUdfConfig {
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
        out
    }
}
