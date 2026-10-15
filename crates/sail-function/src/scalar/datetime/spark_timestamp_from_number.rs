use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Field, FieldRef, Float32Type, Float64Type, Int64Type, TimeUnit, i256,
};
use datafusion_common::{Result, exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::functions_nested_utils::make_scalar_function;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimestampNumberMode {
    Seconds,
    Millis,
    Micros,
    CastAnsi,
    CastLegacy,
    TryCast,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimestampFromNumber {
    signature: Signature,
    mode: TimestampNumberMode,
}

impl SparkTimestampFromNumber {
    pub fn new(mode: TimestampNumberMode) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            mode,
        }
    }

    fn floating_micros(&self, value: f64) -> Result<Option<i64>> {
        let micros = value * 1_000_000.0;
        let checked = matches!(
            self.mode,
            TimestampNumberMode::CastAnsi | TimestampNumberMode::TryCast
        );
        if !value.is_finite() {
            return if self.mode == TimestampNumberMode::CastAnsi {
                exec_err!("{value} cannot be cast to \"TIMESTAMP\" because it is malformed")
            } else {
                Ok(None)
            };
        }
        if checked && !(micros.floor() <= i64::MAX as f64 && micros.ceil() >= i64::MIN as f64) {
            return if self.mode == TimestampNumberMode::CastAnsi {
                exec_err!("{micros} cannot be cast to \"BIGINT\" due to an overflow")
            } else {
                Ok(None)
            };
        }
        // Java's floating-point-to-long conversion truncates and saturates.
        Ok(Some(micros as i64))
    }

    fn decimal_micros(&self, value: i128, scale: i8) -> Result<i64> {
        let overflow = || {
            exec_datafusion_err!("timestamp_seconds decimal overflow or sub-microsecond precision")
        };
        let value = i256::from_i128(value);
        let exponent = 6 - i16::from(scale);
        let factor = i256::from(10)
            .checked_pow(u32::from(exponent.unsigned_abs()))
            .ok_or_else(overflow)?;
        let value = if exponent >= 0 {
            value.checked_mul(factor).ok_or_else(overflow)?
        } else {
            if self.mode == TimestampNumberMode::Seconds && value % factor != i256::ZERO {
                return Err(overflow());
            }
            value / factor
        };
        if self.mode == TimestampNumberMode::Seconds {
            value
                .to_i128()
                .and_then(|value| i64::try_from(value).ok())
                .ok_or_else(overflow)
        } else {
            // Spark CAST uses BigDecimal.longValue, including truncation and wrapping.
            Ok(value.as_i128() as i64)
        }
    }

    fn kernel(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let array = &args[0];
        let values = (0..array.len())
            .map(|row| {
                if array.is_null(row) {
                    return Ok(None);
                }
                match array.data_type() {
                    DataType::Int64 => {
                        let value = array.as_primitive::<Int64Type>().value(row);
                        let factor = match self.mode {
                            TimestampNumberMode::Micros => 1,
                            TimestampNumberMode::Millis => 1_000,
                            _ => 1_000_000,
                        };
                        let value = if matches!(
                            self.mode,
                            TimestampNumberMode::CastAnsi
                                | TimestampNumberMode::CastLegacy
                                | TimestampNumberMode::TryCast
                        ) {
                            // java.util.concurrent.TimeUnit.SECONDS.toMicros saturates.
                            value.saturating_mul(factor)
                        } else {
                            value
                                .checked_mul(factor)
                                .ok_or_else(|| exec_datafusion_err!("timestamp overflow"))?
                        };
                        Ok(Some(value))
                    }
                    DataType::Float32 => self
                        .floating_micros(f64::from(array.as_primitive::<Float32Type>().value(row))),
                    DataType::Float64 => {
                        self.floating_micros(array.as_primitive::<Float64Type>().value(row))
                    }
                    DataType::Decimal128(_, scale) => self
                        .decimal_micros(array.as_primitive::<Decimal128Type>().value(row), *scale)
                        .map(Some),
                    other => exec_err!("invalid numeric timestamp input: {other}"),
                }
            })
            .collect::<Result<TimestampMicrosecondArray>>()?
            .with_timezone("UTC");
        Ok(Arc::new(values))
    }
}

impl ScalarUDFImpl for SparkTimestampFromNumber {
    fn name(&self) -> &str {
        match self.mode {
            TimestampNumberMode::Seconds => "spark_timestamp_seconds",
            TimestampNumberMode::Millis => "spark_timestamp_millis",
            TimestampNumberMode::Micros => "spark_timestamp_micros",
            TimestampNumberMode::CastAnsi => "spark_cast_number_to_timestamp",
            TimestampNumberMode::CastLegacy => "spark_cast_number_to_timestamp_legacy",
            TimestampNumberMode::TryCast => "spark_try_cast_number_to_timestamp",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ))
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| {
            field.is_nullable()
                || (matches!(field.data_type(), DataType::Float32 | DataType::Float64)
                    && self.mode != TimestampNumberMode::CastAnsi)
        });
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&[])?,
            nullable,
        )))
    }
    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        let [data_type] = types else {
            return exec_err!("numeric timestamp conversion requires one argument");
        };
        if data_type.is_integer() || data_type.is_null() {
            return Ok(vec![DataType::Int64]);
        }
        if !matches!(
            self.mode,
            TimestampNumberMode::Millis | TimestampNumberMode::Micros
        ) && matches!(
            data_type,
            DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _)
        ) {
            return Ok(types.to_vec());
        }
        exec_err!("invalid argument type for {}: {data_type}", self.name())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(|arrays| self.kernel(arrays))(&args.args)
    }
}
