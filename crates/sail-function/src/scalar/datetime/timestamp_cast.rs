use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_expr::{Expr, ScalarUDF, cast, lit};
use sail_common::utils::datetime::is_fixed_zero_offset_timezone;

use crate::scalar::datetime::convert_tz::ConvertTz;

/// Spark's cast of TIMESTAMP_NTZ to TIMESTAMP, which reads the value as a wall-clock time in
/// `timezone` and resolves DST gaps and overlaps like Spark.
pub fn cast_timestamp_ntz_to_ltz(expression: Expr, timezone: &Arc<str>, is_try: bool) -> Expr {
    if is_fixed_zero_offset_timezone(timezone) {
        // The wall-clock time is the instant. Arrow's cast would compute it with Chrono, which
        // does not represent Spark's full range.
        let micros = cast(
            cast(expression, DataType::Timestamp(TimeUnit::Microsecond, None)),
            DataType::Int64,
        );
        return cast(
            micros,
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::clone(timezone))),
        );
    }
    ScalarUDF::from(ConvertTz::cast(is_try).with_output_timezone(Some(Arc::clone(timezone)))).call(
        vec![
            lit(timezone.to_string()),
            lit("UTC"),
            cast(expression, DataType::Timestamp(TimeUnit::Microsecond, None)),
        ],
    )
}

/// Spark's cast of TIMESTAMP to TIMESTAMP_NTZ, which takes the wall-clock time of the value in
/// `timezone`. Arrow would take the wall-clock time in UTC.
pub fn cast_timestamp_ltz_to_ntz(expression: Expr, timezone: &str, is_try: bool) -> Expr {
    let utc = cast(expression, DataType::Timestamp(TimeUnit::Microsecond, None));
    if is_fixed_zero_offset_timezone(timezone) {
        return utc;
    }
    ScalarUDF::from(ConvertTz::cast(is_try)).call(vec![lit("UTC"), lit(timezone), utc])
}

/// Spark's cast of TIMESTAMP to TIMESTAMP_NTZ `data_type`, which takes the wall-clock time of the
/// value in the session time zone. Arrow's cast takes the wall-clock time in UTC.
pub fn session_wall_clock(expr: Expr, data_type: &DataType, timezone: &str) -> Expr {
    let wall = cast_timestamp_ltz_to_ntz(expr, timezone, false);
    if *data_type == DataType::Timestamp(TimeUnit::Microsecond, None) {
        wall
    } else {
        cast(wall, data_type.clone())
    }
}
