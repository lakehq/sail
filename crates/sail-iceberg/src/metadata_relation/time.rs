use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::{DataFusionError, Result};

pub(super) fn timestamp_micros(timestamp_ms: i64) -> Result<i64> {
    timestamp_ms.checked_mul(1_000).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "Iceberg metadata timestamp is outside microsecond range: {timestamp_ms}"
        ))
    })
}

pub(super) fn timestamp_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    )
}
