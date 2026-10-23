//! Shared type utilities.

use std::cmp::{max, min};

use arrow_schema::{DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION};

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
