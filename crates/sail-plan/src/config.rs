use std::collections::BTreeMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use sail_python_udf::config::PySparkUdfConfig;

use crate::error::{PlanError, PlanResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
pub enum DefaultTimestampType {
    TimestampLtz,
    TimestampNtz,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
pub enum StoreAssignmentPolicy {
    Ansi,
    Strict,
    Legacy,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
pub enum MapKeyDedupPolicy {
    #[default]
    Exception,
    LastWin,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct PlanConfig {
    /// The time zone of the session.
    pub session_timezone: Arc<str>,
    /// The locale of the session.
    pub session_locale: Arc<str>,
    /// The default timestamp type.
    pub default_timestamp_type: DefaultTimestampType,
    /// Whether to use large variable types in Arrow.
    pub arrow_use_large_var_types: bool,
    /// The Spark UDF configuration.
    pub pyspark_udf_config: Arc<PySparkUdfConfig>,
    /// The default table file format.
    pub default_table_file_format: String,
    /// The default location for managed databases and tables.
    pub default_warehouse_directory: String,
    pub session_user_id: String,
    pub ansi_mode: bool,
    /// Whether decimal common types retain fractional digits when precision exceeds 38.
    pub legacy_decimal_retain_fraction_digits: bool,
    /// Modified SQL settings eligible for capture in persistent views.
    pub view_sql_configs: BTreeMap<String, String>,
    /// Whether persistent views are resolved with the current configuration
    /// instead of the captured creation-time configuration.
    pub legacy_use_current_configs_for_view: bool,
    /// Whether legacy non-ANSI ordering comparisons cast date/timestamp values to strings.
    pub legacy_type_coercion_datetime_to_string: bool,
    /// Whether size/cardinality return -1 for null input when ANSI mode is disabled.
    pub legacy_size_of_null: bool,
    /// Type coercion policy for values written into table columns.
    pub store_assignment_policy: StoreAssignmentPolicy,
    /// Policy for duplicate keys created by map functions.
    pub map_key_dedup_policy: MapKeyDedupPolicy,
    /// Whether to allow cartesian products (cross joins) without explicit `CROSS JOIN` syntax.
    pub cross_join_enabled: bool,
    /// Whether identifiers (e.g. column names) are matched case-sensitively.
    /// Spark defaults to case-insensitive matching (`spark.sql.caseSensitive=false`).
    pub case_sensitive: bool,
    /// The maximum number of distinct values collected for a pivot without an explicit
    /// value list (`spark.sql.pivotMaxValues`, default 10000). Exceeding it is an error.
    pub pivot_max_values: usize,
    /// Whether a table-valued function may receive more than one `TABLE (...)` argument
    /// (`spark.sql.tvf.allowMultipleTableArguments.enabled`, default false). Multiple table
    /// arguments produce the cartesian product of their rows.
    pub tvf_allow_multiple_table_arguments: bool,
    /// Whether `COUNT()` is accepted with no arguments. Spark's legacy behavior returns zero;
    /// it does not interpret the call as `COUNT(*)`.
    pub legacy_allow_parameterless_count: bool,
}

impl PlanConfig {
    pub fn new() -> PlanResult<Self> {
        Ok(Self {
            pyspark_udf_config: Arc::new(PySparkUdfConfig::default()),
            ..Default::default()
        })
    }
    /// Parse session and captured view settings through the same configuration path.
    pub fn from_sql_config<'a>(get: impl Fn(&str) -> Option<&'a str>) -> PlanResult<Self> {
        let mut output = Self::new()?;

        if let Some(value) = get("spark.sql.session.timeZone").map(|x| x.to_string()) {
            output.session_timezone = Arc::from(value);
        }

        if let Some(value) = get("spark.sql.execution.arrow.useLargeVarTypes")
            .map(|x| x.trim().to_lowercase().parse::<bool>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            output.arrow_use_large_var_types = value;
        }

        if let Some(value) = get("spark.sql.sources.default").map(|x| x.to_string()) {
            output.default_table_file_format = value;
        }

        if let Some(value) = get("spark.sql.warehouse.dir") {
            output.default_warehouse_directory = value.to_string();
        }

        if let Some(value) = get("spark.sql.timestampType") {
            let value = value.to_uppercase().trim().to_string();
            if value == "TIMESTAMP_NTZ" {
                output.default_timestamp_type = DefaultTimestampType::TimestampNtz;
            } else if value.is_empty() || value == "TIMESTAMP_LTZ" {
                output.default_timestamp_type = DefaultTimestampType::TimestampLtz;
            } else {
                return Err(PlanError::invalid(format!(
                    "invalid timestamp type: {value}"
                )));
            }
        }

        if let Some(value) = get("spark.sql.ansi.enabled")
            .map(|x| x.trim().to_lowercase().parse::<bool>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            output.ansi_mode = value;
        }

        if let Some(value) = get("spark.sql.legacy.decimal.retainFractionDigitsOnTruncate")
            .map(|x| x.trim().to_lowercase().parse::<bool>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            output.legacy_decimal_retain_fraction_digits = value;
        }

        if let Some(value) = get("spark.sql.legacy.useCurrentConfigsForView")
            .map(|x| x.trim().to_lowercase().parse::<bool>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            output.legacy_use_current_configs_for_view = value;
        }

        if let Some(value) = get("spark.sql.legacy.typeCoercion.datetimeToString.enabled")
            .map(|x| x.trim().to_lowercase().parse::<bool>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            output.legacy_type_coercion_datetime_to_string = value;
        }

        if let Some(value) = get("spark.sql.storeAssignmentPolicy") {
            output.store_assignment_policy = match value.trim().to_ascii_uppercase().as_str() {
                "ANSI" => StoreAssignmentPolicy::Ansi,
                "STRICT" => StoreAssignmentPolicy::Strict,
                "LEGACY" => StoreAssignmentPolicy::Legacy,
                _ => {
                    return Err(PlanError::invalid(format!(
                        "invalid store assignment policy: {value}"
                    )));
                }
            };
        }

        if let Some(value) = get("spark.sql.mapKeyDedupPolicy") {
            output.map_key_dedup_policy = match value.trim().to_ascii_uppercase().as_str() {
                "EXCEPTION" => MapKeyDedupPolicy::Exception,
                "LAST_WIN" => MapKeyDedupPolicy::LastWin,
                _ => {
                    return Err(PlanError::invalid(format!(
                        "invalid map key dedup policy: {value}"
                    )));
                }
            };
        }

        if let Some(value) = get("spark.sql.crossJoin.enabled")
            .map(|x| x.trim().to_lowercase().parse::<bool>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            output.cross_join_enabled = value;
        }

        if let Some(value) = get("spark.sql.caseSensitive")
            .map(|x| x.trim().to_lowercase().parse::<bool>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            output.case_sensitive = value;
        }

        if let Some(value) = get("spark.sql.pivotMaxValues")
            .map(|x| x.trim().parse::<usize>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            output.pivot_max_values = value;
        }

        if let Some(value) = get("spark.sql.tvf.allowMultipleTableArguments.enabled")
            .map(|x| x.trim().to_lowercase().parse::<bool>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            output.tvf_allow_multiple_table_arguments = value;
        }

        if let Some(value) = get("spark.sql.legacy.allowParameterlessCount")
            .map(|x| x.trim().to_lowercase().parse::<bool>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            output.legacy_allow_parameterless_count = value;
        }

        if let Some(value) = get("spark.sql.legacy.sizeOfNull")
            .map(|x| x.trim().to_lowercase().parse::<bool>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            output.legacy_size_of_null = value;
        }

        let mut udf = PySparkUdfConfig::default();

        if let Some(value) = get("spark.sql.session.timeZone").map(|x| x.to_string()) {
            udf.session_timezone = value;
        }

        if let Some(value) = get("spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName")
            .map(|x| x.trim().to_lowercase().parse::<bool>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            udf.pandas_grouped_map_assign_columns_by_name = value;
        }

        if let Some(value) = get("spark.sql.execution.pandas.convertToArrowArraySafely")
            .map(|x| x.trim().to_lowercase().parse::<bool>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            udf.pandas_convert_to_arrow_array_safely = value;
        }

        if let Some(value) = get("spark.sql.execution.arrow.maxRecordsPerBatch")
            .map(|x| x.trim().parse::<i128>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            udf.arrow_max_records_per_batch = if value <= 0 || value > usize::MAX as i128 {
                usize::MAX
            } else {
                value as usize
            };
        }

        if let Some(value) = get("spark.sql.execution.arrow.useLargeVarTypes")
            .map(|x| x.trim().to_lowercase().parse::<bool>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            udf.arrow_use_large_var_types = value;
        }

        if let Some(value) = get("spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled")
            .map(|x| x.trim().to_lowercase().parse::<bool>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            udf.python_udf_pandas_conversion_enabled = value;
        }

        if let Some(value) = get("spark.sql.legacy.execution.pythonUDTF.pandas.conversion.enabled")
            .map(|x| x.trim().to_lowercase().parse::<bool>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            udf.python_udtf_pandas_conversion_enabled = value;
        }

        if let Some(value) = get("spark.sql.execution.pythonUDF.pandas.intToDecimalCoercionEnabled")
            .map(|x| x.trim().to_lowercase().parse::<bool>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            udf.python_udf_pandas_int_to_decimal_coercion_enabled = value;
        }

        if let Some(value) = get("spark.sql.execution.pythonUDF.pandas.preferIntExtensionDtype")
            .map(|x| x.trim().to_lowercase().parse::<bool>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            udf.python_udf_pandas_prefer_int_extension_dtype = value;
        }

        if let Some(value) = get("spark.sql.execution.pyspark.binaryAsBytes")
            .map(|x| x.trim().to_lowercase().parse::<bool>())
            .transpose()
            .map_err(|e| PlanError::invalid(e.to_string()))?
        {
            udf.binary_as_bytes = value;
        }

        output.pyspark_udf_config = Arc::new(udf);
        Ok(output)
    }

    /// Spark always persists ANSI mode and the effective session time zone.
    pub fn view_sql_configs(&self) -> BTreeMap<String, String> {
        let mut configs = self.view_sql_configs.clone();
        configs
            .entry("spark.sql.ansi.enabled".to_string())
            .or_insert_with(|| self.ansi_mode.to_string());
        configs
            .entry("spark.sql.session.timeZone".to_string())
            .or_insert_with(|| self.session_timezone.to_string());
        configs
    }

    pub fn with_view_sql_configs(&self, configs: BTreeMap<String, String>) -> PlanResult<Self> {
        let mut output = Self::from_sql_config(|key| {
            configs.get(key).map(String::as_str).or_else(|| {
                // Spark assumes non-ANSI behavior for older views without captured ANSI mode.
                (key == "spark.sql.ansi.enabled").then_some("false")
            })
        })?;
        output.view_sql_configs = configs;
        // Execution settings and session identity belong to the reader, as in Spark's
        // retained resolution configuration. They are not captured in view properties.
        output.session_user_id = self.session_user_id.clone();
        output.session_locale = Arc::clone(&self.session_locale);
        output.default_warehouse_directory = self.default_warehouse_directory.clone();
        output.arrow_use_large_var_types = self.arrow_use_large_var_types;
        let udf = Arc::make_mut(&mut output.pyspark_udf_config);
        let current = &self.pyspark_udf_config;
        udf.arrow_max_records_per_batch = current.arrow_max_records_per_batch;
        udf.arrow_use_large_var_types = current.arrow_use_large_var_types;
        udf.pandas_convert_to_arrow_array_safely = current.pandas_convert_to_arrow_array_safely;
        udf.python_udf_pandas_int_to_decimal_coercion_enabled =
            current.python_udf_pandas_int_to_decimal_coercion_enabled;
        udf.python_udf_pandas_prefer_int_extension_dtype =
            current.python_udf_pandas_prefer_int_extension_dtype;
        udf.binary_as_bytes = current.binary_as_bytes;
        Ok(output)
    }
}

impl Default for PlanConfig {
    fn default() -> Self {
        Self {
            session_timezone: Arc::from("UTC"),
            session_locale: Arc::from("en-US"),
            default_timestamp_type: DefaultTimestampType::TimestampLtz,
            arrow_use_large_var_types: false,
            pyspark_udf_config: Arc::new(PySparkUdfConfig::default()),
            default_table_file_format: "PARQUET".to_string(),
            default_warehouse_directory: "spark-warehouse".to_string(),
            session_user_id: "".to_string(),
            ansi_mode: true,
            legacy_decimal_retain_fraction_digits: false,
            view_sql_configs: BTreeMap::new(),
            legacy_use_current_configs_for_view: false,
            legacy_type_coercion_datetime_to_string: false,
            legacy_size_of_null: true,
            store_assignment_policy: StoreAssignmentPolicy::Ansi,
            map_key_dedup_policy: MapKeyDedupPolicy::Exception,
            cross_join_enabled: true,
            case_sensitive: false,
            pivot_max_values: 10000,
            tvf_allow_multiple_table_arguments: false,
            legacy_allow_parameterless_count: false,
        }
    }
}
