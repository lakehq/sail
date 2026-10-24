use chrono::FixedOffset;
use datafusion_common::arrow::array::timezone::Tz;

pub fn is_spark_compatible_arrow_fixed_offset(timezone: &str) -> bool {
    if !timezone.starts_with('+') && !timezone.starts_with('-') {
        return false;
    }
    if timezone.parse::<Tz>().is_err() {
        return false;
    }
    let normalized = (timezone.len() == 3).then(|| format!("{timezone}:00"));
    normalized
        .as_deref()
        .unwrap_or(timezone)
        .parse::<FixedOffset>()
        .is_ok_and(|offset| offset.local_minus_utc().unsigned_abs() <= 18 * 60 * 60)
}

/// Common temporal branch type, before string coercion chooses the ANSI policy.
pub fn common_temporal_type(
    types: &[arrow_schema::DataType],
    timezone: &std::sync::Arc<str>,
) -> Option<arrow_schema::DataType> {
    use arrow_schema::{DataType, TimeUnit};
    if types
        .iter()
        .any(|t| matches!(t, DataType::Timestamp(_, Some(_))))
    {
        Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(timezone.clone()),
        ))
    } else if types
        .iter()
        .any(|t| matches!(t, DataType::Timestamp(_, None)))
    {
        Some(DataType::Timestamp(TimeUnit::Microsecond, None))
    } else {
        types
            .iter()
            .any(|t| matches!(t, DataType::Date32 | DataType::Date64))
            .then_some(DataType::Date32)
    }
}
