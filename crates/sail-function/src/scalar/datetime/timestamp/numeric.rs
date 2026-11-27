// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, AsArray, PrimitiveArray, TimestampMicrosecondArray,
    downcast_integer_array,
};
use arrow::compute::cast;
use arrow::datatypes::{
    DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DecimalType,
    Float64Type,
};
use datafusion_common::{Result, exec_err};
use sail_common::utils::datetime::MICROS_PER_SECOND;

// [CREDIT]: https://github.com/apache/spark/blob/v4.3.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/Cast.scala
pub(super) fn timestamp_from_seconds(
    seconds: &ArrayRef,
    zone: &Arc<str>,
    safe: bool,
) -> Result<ArrayRef> {
    let micros: TimestampMicrosecondArray = match seconds.data_type() {
        dt if dt.is_floating() => {
            let values = cast(seconds, &DataType::Float64)?;
            values
                .as_primitive::<Float64Type>()
                .iter()
                .map(|value| match value {
                    Some(value) => float_micros(value, safe),
                    None => Ok(None),
                })
                .collect::<Result<_>>()?
        }
        DataType::Decimal32(..) => {
            decimal_micros(seconds.as_primitive::<Decimal32Type>(), i64::from)
        }
        DataType::Decimal64(..) => decimal_micros(seconds.as_primitive::<Decimal64Type>(), |x| x),
        DataType::Decimal128(..) => {
            decimal_micros(seconds.as_primitive::<Decimal128Type>(), |x| x as i64)
        }
        DataType::Decimal256(..) => decimal_micros(seconds.as_primitive::<Decimal256Type>(), |x| {
            x.as_i128() as i64
        }),
        _ => downcast_integer_array!(seconds => {
            seconds.unary(|value| {
                let micros = i128::from(value) * i128::from(MICROS_PER_SECOND);
                micros.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
            })
        }
        dt => return exec_err!("expected numeric timestamp seconds, got {dt}")),
    };
    Ok(Arc::new(micros.with_timezone(Arc::clone(zone))))
}

fn float_micros(seconds: f64, safe: bool) -> Result<Option<i64>> {
    if !seconds.is_finite() {
        if safe {
            return Ok(None);
        }
        return exec_err!(
            "[CAST_INVALID_INPUT] The value {seconds} cannot be cast to \"TIMESTAMP\" because it is malformed"
        );
    }
    let micros = seconds * MICROS_PER_SECOND as f64;
    if !safe && !(i64::MIN as f64..=i64::MAX as f64).contains(&micros) {
        return exec_err!(
            "[CAST_OVERFLOW] The value {micros} cannot be cast to \"BIGINT\" due to an overflow"
        );
    }
    Ok(Some(micros as i64))
}

// BigDecimal.longValue truncates towards zero and takes the low 64 bits.
fn decimal_micros<T: DecimalType>(
    values: &PrimitiveArray<T>,
    low_bits: impl Fn(T::Native) -> i64,
) -> TimestampMicrosecondArray
where
    T::Native: From<i32> + ArrowNativeTypeOp,
{
    let shift = 6 - i32::from(values.scale());
    if shift >= 0 {
        let factor = 10_i64.wrapping_pow(shift as u32);
        values.unary(|x| low_bits(x).wrapping_mul(factor))
    } else {
        let divisor = T::Native::from(10).pow_wrapping(shift.unsigned_abs());
        values.unary(|x| low_bits(x.div_wrapping(divisor)))
    }
}
