use crate::formatter::{DefaultPlanFormatter, PlanFormatter};
use framework_common::config::{ConfigKeyValue, SparkUdfConfig};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TimestampType {
    TimestampLtz,
    TimestampNtz,
}

// The generic type parameter is used to work around the issue deriving `PartialEq` for `dyn` trait.
// See also: https://github.com/rust-lang/rust/issues/78808
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanConfig<F: ?Sized = dyn PlanFormatter> {
    /// The time zone of the session.
    pub time_zone: String,
    /// The default timestamp type.
    pub timestamp_type: TimestampType,
    /// The plan formatter.
    pub plan_formatter: Arc<F>,
    // TODO: Revisit how to handle spark_udf_config
    //  https://github.com/lakehq/framework/pull/53#discussion_r1643683600
    pub spark_udf_config: SparkUdfConfig,
}

impl Default for PlanConfig {
    fn default() -> Self {
        Self {
            time_zone: "UTC".to_string(),
            timestamp_type: TimestampType::TimestampLtz,
            plan_formatter: Arc::new(DefaultPlanFormatter),
            spark_udf_config: SparkUdfConfig {
                timezone: ConfigKeyValue {
                    key: "spark.sql.session.timeZone".to_string(),
                    value: Some("UTC".to_string()),
                },
                pandas_window_bound_types: ConfigKeyValue {
                    key: "pandas_window_bound_types".to_string(),
                    value: None,
                },
                pandas_grouped_map_assign_columns_by_name: ConfigKeyValue {
                    key: "spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName"
                        .to_string(),
                    value: None,
                },
                pandas_convert_to_arrow_array_safely: ConfigKeyValue {
                    key: "spark.sql.execution.pandas.convertToArrowArraySafely".to_string(),
                    value: None,
                },
                arrow_max_records_per_batch: ConfigKeyValue {
                    key: "spark.sql.execution.arrow.maxRecordsPerBatch".to_string(),
                    value: None,
                },
            },
        }
    }
}
