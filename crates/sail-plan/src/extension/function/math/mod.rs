use std::cmp::{max, min};

use datafusion::arrow::datatypes::{DataType, DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION};

pub mod least_greatest;
pub mod randn;
pub mod random;
pub mod spark_abs;
pub mod spark_bin;
pub mod spark_ceil_floor;
pub mod spark_expm1;
pub mod spark_hex_unhex;
pub mod spark_pmod;
pub mod spark_signum;

#[inline]
pub(crate) fn both_are_decimal(left: &DataType, right: &DataType) -> bool {
    matches!(
        (left, right),
        (DataType::Decimal128(_, _), DataType::Decimal128(_, _))
            | (DataType::Decimal128(_, _), DataType::Decimal256(_, _))
            | (DataType::Decimal256(_, _), DataType::Decimal128(_, _))
            | (DataType::Decimal256(_, _), DataType::Decimal256(_, _))
    )
}

// https://github.com/apache/spark/blob/50a328ba98577ea12bbae50f2cbf406438b01a2f/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L1491-L1508
#[inline]
pub fn round_decimal_base(
    precision: i32,
    scale: i32,
    target_scale: i32,
    decimal_128: bool,
) -> (u8, i8) {
    let integral_least_num_digits = precision - scale + 1;
    if target_scale < 0 {
        let new_precision = max(integral_least_num_digits, -target_scale + 1) as u8;
        if decimal_128 {
            (min(new_precision, DECIMAL128_MAX_PRECISION), 0)
        } else {
            (min(new_precision, DECIMAL256_MAX_PRECISION), 0)
        }
    } else {
        let new_scale = min(scale, target_scale);
        let max_precision = if decimal_128 {
            DECIMAL128_MAX_PRECISION
        } else {
            DECIMAL256_MAX_PRECISION
        } as i32;
        (
            min(integral_least_num_digits + new_scale, max_precision) as u8,
            new_scale as i8,
        )
    }
}
