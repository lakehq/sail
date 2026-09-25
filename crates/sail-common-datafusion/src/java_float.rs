use std::cmp::Ordering;

use num_bigint::BigUint;

const DOUBLE_SIGNIFICAND_MASK: u64 = 0x000f_ffff_ffff_ffff;
const DOUBLE_SIGNIFICAND_HIGH_BIT: u64 = 1 << 52;
const DOUBLE_EXPONENT_MASK: u64 = 0x7ff;
const DOUBLE_EXPONENT_BIAS: i32 = 1023;
const FLOAT_SIGNIFICAND_MASK: u32 = 0x007f_ffff;
const FLOAT_SIGNIFICAND_HIGH_BIT: u32 = 1 << 23;
const FLOAT_EXPONENT_MASK: u32 = 0xff;
const FLOAT_EXPONENT_BIAS: i32 = 127;

const FIVE_POWER_BITS: [i32; 27] = [
    0, 3, 5, 7, 10, 12, 14, 17, 19, 21, 24, 26, 28, 31, 33, 35, 38, 40, 42, 45, 47, 49, 52, 54, 56,
    59, 61,
];

const INSIGNIFICANT_DIGITS_FOR_POWER_OF_TWO: [u32; 64] = [
    0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9,
    9, 9, 10, 10, 10, 11, 11, 11, 12, 12, 12, 12, 13, 13, 13, 14, 14, 14, 15, 15, 15, 15, 16, 16,
    16, 17, 17, 17, 18, 18, 18, 19,
];

#[derive(Clone, Copy)]
enum PrimitiveWidth {
    Bits32,
    Bits64,
}

struct DecimalDigits {
    negative: bool,
    decimal_exponent: i32,
    digits: Vec<u8>,
}

impl DecimalDigits {
    fn round_up(&mut self) {
        let Some(mut index) = self.digits.len().checked_sub(1) else {
            return;
        };
        while index > 0 && self.digits[index] == 9 {
            self.digits[index] = 0;
            index -= 1;
        }
        if self.digits[index] == 9 {
            self.digits[index] = 1;
            self.decimal_exponent += 1;
        } else {
            self.digits[index] += 1;
        }
    }

    fn render(&self) -> String {
        let mut result = String::with_capacity(26);
        if self.negative {
            result.push('-');
        }

        let digit_count = self.digits.len();
        if self.decimal_exponent > 0 && self.decimal_exponent < 8 {
            let integer_count = digit_count.min(self.decimal_exponent as usize);
            push_digits(&mut result, &self.digits[..integer_count]);
            if integer_count < self.decimal_exponent as usize {
                result.extend(std::iter::repeat_n(
                    '0',
                    self.decimal_exponent as usize - integer_count,
                ));
                result.push_str(".0");
            } else {
                result.push('.');
                if integer_count < digit_count {
                    push_digits(&mut result, &self.digits[integer_count..]);
                } else {
                    result.push('0');
                }
            }
        } else if self.decimal_exponent <= 0 && self.decimal_exponent > -3 {
            result.push_str("0.");
            result.extend(std::iter::repeat_n('0', (-self.decimal_exponent) as usize));
            push_digits(&mut result, &self.digits);
        } else {
            if let Some(first) = self.digits.first() {
                result.push(char::from(b'0' + first));
            }
            result.push('.');
            if digit_count > 1 {
                push_digits(&mut result, &self.digits[1..]);
            } else {
                result.push('0');
            }
            result.push('E');
            result.push_str(&(self.decimal_exponent - 1).to_string());
        }
        result
    }
}

fn push_digits(result: &mut String, digits: &[u8]) {
    result.extend(digits.iter().map(|digit| char::from(b'0' + digit)));
}

pub(crate) fn format_f64(value: f64) -> String {
    let bits = value.to_bits();
    let negative = bits >> 63 != 0;
    let mut exponent = ((bits >> 52) & DOUBLE_EXPONENT_MASK) as i32;
    let mut fraction = bits & DOUBLE_SIGNIFICAND_MASK;

    if exponent == DOUBLE_EXPONENT_MASK as i32 {
        return if fraction != 0 {
            "NaN".to_string()
        } else if negative {
            "-Infinity".to_string()
        } else {
            "Infinity".to_string()
        };
    }
    if exponent == 0 && fraction == 0 {
        return if negative { "-0.0" } else { "0.0" }.to_string();
    }

    let significant_bits;
    if exponent == 0 {
        let leading_zeros = fraction.leading_zeros();
        let shift = leading_zeros - 11;
        fraction <<= shift;
        exponent = 1 - shift as i32;
        significant_bits = 64 - leading_zeros as i32;
    } else {
        fraction |= DOUBLE_SIGNIFICAND_HIGH_BIT;
        significant_bits = 53;
    }

    binary_to_decimal(
        negative,
        exponent - DOUBLE_EXPONENT_BIAS,
        fraction,
        significant_bits,
    )
    .render()
}

pub(crate) fn format_f32(value: f32) -> String {
    let bits = value.to_bits();
    let negative = bits >> 31 != 0;
    let mut exponent = ((bits >> 23) & FLOAT_EXPONENT_MASK) as i32;
    let mut fraction = bits & FLOAT_SIGNIFICAND_MASK;

    if exponent == FLOAT_EXPONENT_MASK as i32 {
        return if fraction != 0 {
            "NaN".to_string()
        } else if negative {
            "-Infinity".to_string()
        } else {
            "Infinity".to_string()
        };
    }
    if exponent == 0 && fraction == 0 {
        return if negative { "-0.0" } else { "0.0" }.to_string();
    }

    let significant_bits;
    if exponent == 0 {
        let leading_zeros = fraction.leading_zeros();
        let shift = leading_zeros - 8;
        fraction <<= shift;
        exponent = 1 - shift as i32;
        significant_bits = 32 - leading_zeros as i32;
    } else {
        fraction |= FLOAT_SIGNIFICAND_HIGH_BIT;
        significant_bits = 24;
    }

    binary_to_decimal(
        negative,
        exponent - FLOAT_EXPONENT_BIAS,
        u64::from(fraction) << 29,
        significant_bits,
    )
    .render()
}

fn binary_to_decimal(
    negative: bool,
    binary_exponent: i32,
    mut fraction: u64,
    significant_bits: i32,
) -> DecimalDigits {
    let trailing_zeros = fraction.trailing_zeros() as i32;
    let fraction_bits = 53 - trailing_zeros;
    let tiny_bits = 0.max(fraction_bits - binary_exponent - 1);

    if tiny_bits == 0 && binary_exponent <= 62 {
        return integer_decimal_digits(negative, binary_exponent, fraction, significant_bits);
    }

    let mut decimal_exponent = estimate_decimal_exponent(fraction, binary_exponent);
    let five_numerator = 0.max(-decimal_exponent);
    let mut two_numerator = five_numerator + tiny_bits + binary_exponent;
    let five_denominator = 0.max(decimal_exponent);
    let mut two_denominator = five_denominator + tiny_bits;
    let five_margin = five_numerator;
    let mut two_margin = two_numerator - significant_bits;

    fraction >>= trailing_zeros;
    two_numerator -= fraction_bits - 1;
    let common_power_of_two = two_numerator.min(two_denominator);
    two_numerator -= common_power_of_two;
    two_denominator -= common_power_of_two;
    two_margin -= common_power_of_two;

    if fraction_bits == 1 {
        two_margin -= 1;
    }
    if two_margin < 0 {
        two_numerator -= two_margin;
        two_denominator -= two_margin;
        two_margin = 0;
    }

    let numerator_bits =
        fraction_bits + two_numerator + approximate_five_power_bits(five_numerator);
    let ten_denominator_bits =
        two_denominator + 1 + approximate_five_power_bits(five_denominator + 1);

    let width = if numerator_bits < 32 && ten_denominator_bits < 32 {
        Some(PrimitiveWidth::Bits32)
    } else if numerator_bits < 64 && ten_denominator_bits < 64 {
        Some(PrimitiveWidth::Bits64)
    } else {
        None
    };

    if let Some(width) = width {
        primitive_decimal_digits(
            negative,
            &mut decimal_exponent,
            fraction,
            five_numerator,
            two_numerator,
            five_denominator,
            two_denominator,
            five_margin,
            two_margin,
            width,
        )
    } else {
        big_decimal_digits(
            negative,
            &mut decimal_exponent,
            fraction,
            five_numerator,
            two_numerator,
            five_denominator,
            two_denominator,
            five_margin,
            two_margin,
        )
    }
}

fn integer_decimal_digits(
    negative: bool,
    binary_exponent: i32,
    fraction: u64,
    significant_bits: i32,
) -> DecimalDigits {
    let insignificant_digits = if binary_exponent > significant_bits {
        let power = (binary_exponent - significant_bits - 1) as usize;
        INSIGNIFICANT_DIGITS_FOR_POWER_OF_TWO
            .get(power)
            .copied()
            .unwrap_or(0)
    } else {
        0
    };
    let mut value = if binary_exponent >= 52 {
        fraction << (binary_exponent - 52)
    } else {
        fraction >> (52 - binary_exponent)
    };

    if insignificant_digits != 0 {
        let scale = 10_u64.pow(insignificant_digits);
        let residue = value % scale;
        value /= scale;
        if residue >= scale / 2 {
            value += 1;
        }
    }

    let decimal = value.to_string();
    let digits = decimal
        .trim_end_matches('0')
        .bytes()
        .map(|digit| digit - b'0')
        .collect();
    DecimalDigits {
        negative,
        decimal_exponent: insignificant_digits as i32 + decimal.len() as i32,
        digits,
    }
}

#[expect(clippy::too_many_arguments)]
fn primitive_decimal_digits(
    negative: bool,
    decimal_exponent: &mut i32,
    fraction: u64,
    five_numerator: i32,
    two_numerator: i32,
    five_denominator: i32,
    two_denominator: i32,
    five_margin: i32,
    two_margin: i32,
    width: PrimitiveWidth,
) -> DecimalDigits {
    let mut numerator = wrap_primitive(scaled_u128(fraction, five_numerator, two_numerator), width);
    let denominator = wrap_primitive(scaled_u128(1, five_denominator, two_denominator), width);
    let mut margin = wrap_primitive(scaled_u128(1, five_margin, two_margin), width);
    let ten_denominator = wrap_primitive(i128::from(denominator) * 10, width);

    let mut quotient = numerator / denominator;
    numerator = wrap_primitive(i128::from(numerator % denominator) * 10, width);
    margin = wrap_primitive(i128::from(margin) * 10, width);
    let mut low = numerator < margin;
    let mut high =
        wrap_primitive(i128::from(numerator) + i128::from(margin), width) > ten_denominator;
    let mut digits = Vec::with_capacity(20);

    if quotient == 0 && !high {
        *decimal_exponent -= 1;
    } else {
        digits.push(quotient as u8);
    }
    if *decimal_exponent < -3 || *decimal_exponent >= 8 {
        low = false;
        high = false;
    }

    while !low && !high {
        quotient = numerator / denominator;
        numerator = wrap_primitive(i128::from(numerator % denominator) * 10, width);
        margin = wrap_primitive(i128::from(margin) * 10, width);
        if margin > 0 {
            low = numerator < margin;
            high =
                wrap_primitive(i128::from(numerator) + i128::from(margin), width) > ten_denominator;
        } else {
            low = true;
            high = true;
        }
        digits.push(quotient as u8);
    }

    let low_digit_difference = wrap_primitive(
        i128::from(wrap_primitive(i128::from(numerator) << 1, width)) - i128::from(ten_denominator),
        width,
    );
    let mut result = DecimalDigits {
        negative,
        decimal_exponent: *decimal_exponent + 1,
        digits,
    };
    if should_round_up(&result.digits, low, high, low_digit_difference.cmp(&0)) {
        result.round_up();
    }
    result
}

#[expect(clippy::too_many_arguments)]
fn big_decimal_digits(
    negative: bool,
    decimal_exponent: &mut i32,
    fraction: u64,
    five_numerator: i32,
    two_numerator: i32,
    five_denominator: i32,
    two_denominator: i32,
    five_margin: i32,
    two_margin: i32,
) -> DecimalDigits {
    let mut numerator = scaled_big_uint(fraction, five_numerator, two_numerator);
    let denominator = scaled_big_uint(1, five_denominator, two_denominator);
    let mut margin = scaled_big_uint(1, five_margin + 1, two_margin + 1);
    let ten_denominator = scaled_big_uint(1, five_denominator + 1, two_denominator + 1);

    let mut quotient = big_quotient(&numerator, &denominator);
    numerator = (&numerator % &denominator) * 10_u8;
    let mut low = numerator < margin;
    let mut high = &numerator + &margin >= ten_denominator;
    let mut digits = Vec::with_capacity(20);

    if quotient == 0 && !high {
        *decimal_exponent -= 1;
    } else {
        digits.push(quotient);
    }
    if *decimal_exponent < -3 || *decimal_exponent >= 8 {
        low = false;
        high = false;
    }

    while !low && !high {
        quotient = big_quotient(&numerator, &denominator);
        numerator = (&numerator % &denominator) * 10_u8;
        margin *= 10_u8;
        low = numerator < margin;
        high = &numerator + &margin >= ten_denominator;
        digits.push(quotient);
    }

    let low_digit_difference = if high && low {
        (&numerator << 1_usize).cmp(&ten_denominator)
    } else {
        Ordering::Equal
    };
    let mut result = DecimalDigits {
        negative,
        decimal_exponent: *decimal_exponent + 1,
        digits,
    };
    if should_round_up(&result.digits, low, high, low_digit_difference) {
        result.round_up();
    }
    result
}

fn should_round_up(digits: &[u8], low: bool, high: bool, low_digit_difference: Ordering) -> bool {
    if !high {
        return false;
    }
    if !low {
        return true;
    }
    match low_digit_difference {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => digits.last().is_some_and(|digit| digit & 1 != 0),
    }
}

fn approximate_five_power_bits(power: i32) -> i32 {
    FIVE_POWER_BITS
        .get(power as usize)
        .copied()
        .unwrap_or(power * 3)
}

fn scaled_u128(value: u64, five_power: i32, two_power: i32) -> i128 {
    let value = u128::from(value) * 5_u128.pow(five_power as u32);
    (value << two_power) as i128
}

fn scaled_big_uint(value: u64, five_power: i32, two_power: i32) -> BigUint {
    (BigUint::from(value) * BigUint::from(5_u8).pow(five_power as u32)) << two_power as usize
}

fn big_quotient(numerator: &BigUint, denominator: &BigUint) -> u8 {
    (numerator / denominator)
        .to_u32_digits()
        .first()
        .copied()
        .unwrap_or(0) as u8
}

fn wrap_primitive(value: i128, width: PrimitiveWidth) -> i64 {
    match width {
        PrimitiveWidth::Bits32 => i64::from(value as i32),
        PrimitiveWidth::Bits64 => value as i64,
    }
}

#[expect(
    clippy::approx_constant,
    reason = "the decimal-exponent estimator requires this exact coefficient"
)]
fn estimate_decimal_exponent(fraction: u64, binary_exponent: i32) -> i32 {
    let normalized = f64::from_bits(
        u64::from(DOUBLE_EXPONENT_BIAS as u32) << 52 | (fraction & DOUBLE_SIGNIFICAND_MASK),
    );
    ((normalized - 1.5) * 0.289_529_654
        + 0.176_091_259
        + f64::from(binary_exponent) * 0.301_029_995_663_981)
        .floor() as i32
}
