// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use chrono::{DateTime, Datelike, FixedOffset, Timelike, Utc};
use datafusion::arrow::array::{Array, TimestampMicrosecondArray};
use datafusion::arrow::compute::kernels::cast_utils::string_to_datetime;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::{Result, exec_datafusion_err, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use sail_common_datafusion::utils::datetime::{SparkZone, arrow_timezone_label};
use sail_common_datafusion::utils::items::ItemTaker;

use crate::scalar::datetime::timestamp::parse_unformatted_timestamp;
use crate::scalar::datetime::utils::common::string_array_iter;

/// Parses a Delta TIMESTAMP partition value in the session time zone `session_zone`, into
/// microseconds since the epoch.
///
/// A value is parsed as `CAST(value AS TIMESTAMP)` does, and otherwise with the grammar of Arrow's
/// cast, which earlier Sail versions read partition values with, so that every value they read
/// keeps its instant. A value without a zone is a wall clock in the session time zone either way.
pub fn parse_partition_timestamp(value: &str, session_zone: &SparkZone) -> Result<i64> {
    parse_unformatted_timestamp(value, Some(session_zone)).or_else(|error| {
        let Ok(instant) = string_to_datetime(&Utc, value) else {
            return Err(error);
        };
        // A leap second reads as the next second, as Arrow's cast reads it.
        let micros = instant.timestamp_micros();
        // A value reads as the same instant in two zones only if it names its zone.
        let offset = FixedOffset::east_opt(3600)
            .ok_or_else(|| exec_datafusion_err!("invalid fixed offset"))?;
        if string_to_datetime(&offset, value).is_ok_and(|shifted| shifted == instant) {
            return Ok(micros);
        }
        let wall = DateTime::from_timestamp_micros(micros)
            .ok_or_else(|| exec_datafusion_err!("invalid timestamp: {value}"))?;
        session_zone.to_utc_micros(
            i64::from(wall.year()),
            wall.month(),
            wall.day(),
            wall.hour(),
            wall.minute(),
            wall.second(),
            wall.timestamp_subsec_micros(),
            i128::from(micros),
        )
    })
}

/// Parses the raw values of a Delta TIMESTAMP partition column with [`parse_partition_timestamp`],
/// for partition filters evaluated over the Delta log. A value that does not parse is NULL, or an
/// error in ANSI mode, as Delta-Spark casts partition values in its filters.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DeltaPartitionTimestamp {
    time_zone: Arc<str>,
    label: Arc<str>,
    ansi_mode: bool,
    signature: Signature,
}

impl DeltaPartitionTimestamp {
    pub fn new(time_zone: Arc<str>, ansi_mode: bool) -> Self {
        let label = Arc::from(arrow_timezone_label(&time_zone).as_ref());
        Self {
            time_zone,
            label,
            ansi_mode,
            signature: Signature::string(1, Volatility::Immutable),
        }
    }

    pub fn time_zone(&self) -> &str {
        &self.time_zone
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }
}

impl ScalarUDFImpl for DeltaPartitionTimestamp {
    fn name(&self) -> &str {
        "delta_partition_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::clone(&self.label)),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        let array = args.one()?.into_array(number_rows)?;
        let zone = SparkZone::parse(&self.time_zone)?;
        let values = string_array_iter(array.as_ref())?
            .map(|value| {
                value
                    .map(|value| match parse_partition_timestamp(value, &zone) {
                        Ok(micros) => Ok(Some(micros)),
                        Err(_) if self.ansi_mode => {
                            let sql_value = value.replace('\\', "\\\\").replace('\'', "\\'");
                            exec_err!(
                                "[CAST_INVALID_INPUT] The value '{sql_value}' of the type \"STRING\" cannot be cast to \"TIMESTAMP\" because it is malformed. Correct the value as per the syntax, or set \"spark.sql.ansi.enabled\" to \"false\" to read it as NULL."
                            )
                        }
                        Err(_) => Ok(None),
                    })
                    .transpose()
                    .map(Option::flatten)
            })
            .collect::<Result<TimestampMicrosecondArray>>()?
            .with_timezone(Arc::clone(&self.label));
        Ok(ColumnarValue::Array(Arc::new(values) as Arc<dyn Array>))
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::Result;
    use sail_common_datafusion::utils::datetime::SparkZone;

    use super::parse_partition_timestamp;

    /// Spellings that Spark's cast rejects and Arrow's cast accepts keep the instant that earlier
    /// Sail versions read; without a zone they are wall clocks in the session time zone.
    #[test]
    fn partition_timestamps_fall_back_to_the_arrow_grammar() -> Result<()> {
        let utc = SparkZone::parse("UTC")?;
        let la = SparkZone::parse("America/Los_Angeles")?;
        let nine_utc = 1_704_099_600_000_000;
        let nine_la = 1_704_128_400_000_000;
        for (value, in_utc, in_la) in [
            ("2024-01-01t09:00:00", nine_utc, nine_la),
            ("2024-01-01t09:00:00Z", nine_utc, nine_utc),
            ("2024-01-01t09:00:00z", nine_utc, nine_utc),
            ("2024-01-01T09:00:00z", nine_utc, nine_utc),
            ("2024-01-01 09:00:00z", nine_utc, nine_utc),
            ("2024-01-01 090000", nine_utc, nine_la),
            ("2024-01-01T090000", nine_utc, nine_la),
            ("2024-01-01T090000Z", nine_utc, nine_utc),
            ("2024-01-01T09:00Z", nine_utc, nine_utc),
            (
                "2024-01-01T09:00+01:00",
                nine_utc - 3_600_000_000,
                nine_utc - 3_600_000_000,
            ),
            (
                "2024-12-31T23:59:60Z",
                1_735_689_600_000_000,
                1_735_689_600_000_000,
            ),
            (
                "2024-12-31 23:59:60",
                1_735_689_600_000_000,
                1_735_718_400_000_000,
            ),
        ] {
            assert_eq!(
                parse_partition_timestamp(value, &utc)?,
                in_utc,
                "{value} in UTC"
            );
            assert_eq!(
                parse_partition_timestamp(value, &la)?,
                in_la,
                "{value} in LA"
            );
        }
        // Spark's cast still decides the spellings it accepts, such as the first instant of a
        // repeated hour that the string names.
        assert_eq!(
            parse_partition_timestamp("2024-11-03 01:30:00 America/New_York", &utc)?,
            1_730_611_800_000_000
        );
        assert!(parse_partition_timestamp("garbage", &utc).is_err());
        assert!(parse_partition_timestamp("2024-01-01T09:00:00 Q", &utc).is_err());
        Ok(())
    }
}
