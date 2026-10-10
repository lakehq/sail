use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Array, Int64Array, PrimitiveArray, StringArray,
    StringBuilder, as_largestring_array, as_string_array, new_null_array,
};
use datafusion::arrow::datatypes::{DataType, Decimal128Type, Float32Type, Float64Type, Int64Type};
use datafusion_common::cast::as_string_view_array;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use sail_common_datafusion::display::{spark_f32_to_string, spark_f64_to_string};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::scalar::math::spark_hex::decimal_array_to_i64;

const BIN_SUPPORTED_TYPES: &str = "numeric or string";

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBin {
    signature: Signature,
    ansi_mode: bool,
}

impl Default for SparkBin {
    fn default() -> Self {
        Self::new(false)
    }
}

impl SparkBin {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            ansi_mode,
        }
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }
}

impl ScalarUDFImpl for SparkBin {
    fn name(&self) -> &str {
        "spark_bin"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Arity is enforced by `coerce_types` at planning time.
        let ansi = self.ansi_mode;
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Int64(value)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(value.map(bin))))
            }
            ColumnarValue::Scalar(ScalarValue::Decimal128(value, precision, scale)) => {
                let array = Decimal128Array::from(vec![*value])
                    .with_precision_and_scale(*precision, *scale)?;
                let longs = decimal_array_to_i64(&array, *precision, *scale, ansi)?;
                let text = bin_int_array_to_string(longs.as_primitive::<Int64Type>());
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &text, 0,
                )?))
            }
            ColumnarValue::Scalar(ScalarValue::Float32(value)) => {
                let out = match value {
                    Some(v) => Some(bin(float_to_i64(*v, ansi)?)),
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(out)))
            }
            ColumnarValue::Scalar(ScalarValue::Float64(value)) => {
                let out = match value {
                    Some(v) => Some(bin(double_to_i64(*v, ansi)?)),
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(out)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(value))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(value))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(value)) => {
                let out = match value.as_deref() {
                    Some(s) => match parse_spark_string_i64_with_mode(s, ansi) {
                        Some(v) => Some(bin(v)),
                        None if ansi => return Err(cast_invalid_input_err(s)),
                        None => None,
                    },
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(out)))
            }
            ColumnarValue::Array(array) => {
                // All-null short-circuit: avoid N format!() allocs when the
                // input column is fully NULL. Safe to place here because
                // coerce_types has already validated the input type; only
                // per-row formatting is being skipped.
                if array.null_count() == array.len() {
                    return Ok(ColumnarValue::Array(new_null_array(
                        args.return_field.data_type(),
                        array.len(),
                    )));
                }
                match array.data_type() {
                    DataType::Int64 => {
                        let array = array.as_primitive::<Int64Type>();
                        Ok(ColumnarValue::Array(bin_int_array_to_string(array)))
                    }
                    DataType::Decimal128(precision, scale) => {
                        let longs = decimal_array_to_i64(
                            array.as_primitive::<Decimal128Type>(),
                            *precision,
                            *scale,
                            ansi,
                        )?;
                        Ok(ColumnarValue::Array(bin_int_array_to_string(
                            longs.as_primitive::<Int64Type>(),
                        )))
                    }
                    DataType::Float32 => {
                        let longs = array
                            .as_primitive::<Float32Type>()
                            .iter()
                            .map(|v| v.map(|v| float_to_i64(v, ansi)).transpose())
                            .collect::<Result<Int64Array>>()?;
                        Ok(ColumnarValue::Array(bin_int_array_to_string(&longs)))
                    }
                    DataType::Float64 => {
                        let array = array.as_primitive::<Float64Type>();
                        let result = bin_float_array(array, ansi)?;
                        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                    }
                    DataType::Utf8 => {
                        let array = as_string_array(array);
                        let result = bin_string_array(array.iter(), ansi)?;
                        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                    }
                    DataType::LargeUtf8 => {
                        let array = as_largestring_array(array);
                        let result = bin_string_array(array.iter(), ansi)?;
                        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                    }
                    DataType::Utf8View => {
                        let array = as_string_view_array(array)?;
                        let result = bin_string_array(array.iter(), ansi)?;
                        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                    }
                    other => Err(unsupported_data_type_exec_err(
                        "bin",
                        BIN_SUPPORTED_TYPES,
                        other,
                    )),
                }
            }
            other => Err(unsupported_data_type_exec_err(
                "bin",
                BIN_SUPPORTED_TYPES,
                &other.data_type(),
            )),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return Err(invalid_arg_count_exec_err("bin", (1, 1), arg_types.len()));
        }
        // Spark's `bin` accepts numeric and string types only. Anything else
        // (BOOLEAN, BINARY, DATE, TIMESTAMP, ARRAY, MAP, STRUCT, …) is rejected
        // at analysis time with DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE.
        match &arg_types[0] {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Ok(vec![arg_types[0].clone()])
            }
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Decimal256(_, _)
            | DataType::Null => Ok(vec![DataType::Int64]),
            // Spark's DECIMAL -> BIGINT cast wraps (ANSI off) or overflows (ANSI on), which
            // DataFusion's cast does not do: `bin` converts the DECIMAL itself.
            DataType::Decimal32(p, s) | DataType::Decimal64(p, s) => {
                Ok(vec![DataType::Decimal128(*p, *s)])
            }
            DataType::Decimal128(_, _) => Ok(vec![arg_types[0].clone()]),
            // Keep floats as DOUBLE: DataFusion's Float->Int64 cast errors on
            // NaN/Infinity, so `bin` applies Spark's saturating cast itself.
            DataType::Float16 | DataType::Float64 => Ok(vec![DataType::Float64]),
            DataType::Float32 => Ok(vec![DataType::Float32]),
            other => Err(unsupported_data_type_exec_err(
                "bin",
                BIN_SUPPORTED_TYPES,
                other,
            )),
        }
    }
}

fn bin_string_array<'a, I>(iter: I, ansi: bool) -> Result<StringArray>
where
    I: Iterator<Item = Option<&'a str>>,
{
    // Reuse a single scratch buffer across rows to avoid the per-row
    // `String` allocation that the old `bin(i64) -> String` path required.
    // The builder still copies the bytes into its internal buffer, but the
    // scratch capacity (64 bytes — max binary repr of i64) is allocated
    // exactly once.
    let (lower, upper) = iter.size_hint();
    let cap = upper.unwrap_or(lower);
    let mut builder = StringBuilder::with_capacity(cap, 0);
    let mut scratch = String::with_capacity(64);
    for v in iter {
        match v {
            None => builder.append_null(),
            Some(s) => match parse_spark_string_i64_with_mode(s, ansi) {
                Some(value) => {
                    scratch.clear();
                    bin_into(value, &mut scratch);
                    builder.append_value(&scratch);
                }
                None if ansi => return Err(cast_invalid_input_err(s)),
                None => builder.append_null(),
            },
        }
    }
    Ok(builder.finish())
}

/// Write the binary representation of `value` to `dst`.
///
/// Equivalent to `dst.push_str(&format!("{value:b}"))` for non-negative and
/// `dst.push_str(&format!("{value:064b}"))` for negative, but bypasses the
/// `format!` machinery (trait dispatch + intermediate `String` allocation
/// inside `format_args!`). Pushing one byte per bit is just an arithmetic
/// shift + `String::push`, which compiles to a tight loop.
fn bin_into(value: i64, dst: &mut String) {
    let v = value as u64;
    if value >= 0 {
        // Trim leading zeros — at least 1 bit (for value == 0).
        let n_bits = v.bit_width().max(1) as usize;
        for i in (0..n_bits).rev() {
            dst.push(if (v >> i) & 1 == 1 { '1' } else { '0' });
        }
    } else {
        // Spark renders negatives as full 64-bit two's complement.
        for i in (0..u64::BITS).rev() {
            dst.push(if (v >> i) & 1 == 1 { '1' } else { '0' });
        }
    }
}

/// Render every row of an Int64 array into a `StringArray`, writing directly
/// into a `StringBuilder` and reusing a single scratch buffer for the formatted
/// bits. Null rows are propagated as nulls.
fn bin_int_array_to_string(
    array: &datafusion::arrow::array::PrimitiveArray<Int64Type>,
) -> ArrayRef {
    let len = array.len();
    let mut builder = StringBuilder::with_capacity(len, 0);
    let mut scratch = String::with_capacity(64);
    if array.null_count() == 0 {
        for value in array.values().iter().copied() {
            scratch.clear();
            bin_into(value, &mut scratch);
            builder.append_value(&scratch);
        }
    } else {
        for v in array.iter() {
            match v {
                None => builder.append_null(),
                Some(value) => {
                    scratch.clear();
                    bin_into(value, &mut scratch);
                    builder.append_value(&scratch);
                }
            }
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

fn cast_invalid_input_err(value: &str) -> datafusion_common::DataFusionError {
    // Mirror Spark's CAST_INVALID_INPUT message shape for ANSI mode.
    exec_datafusion_err!(
        "[CAST_INVALID_INPUT] The value '{value}' of the type \"STRING\" cannot be cast to \"BIGINT\" because it is malformed."
    )
}

/// `QueryExecutionErrors.castingCauseOverflowError`: `shown` is the value as Spark prints it for its
/// source type (`1.0E30D`, `1.0E30`, `9223372036854775808.9BD`) and `source_type` the SQL type name.
pub(crate) fn cast_overflow_err(
    shown: &str,
    source_type: &str,
) -> datafusion_common::DataFusionError {
    exec_datafusion_err!(
        "[CAST_OVERFLOW] The value {shown} of the type \"{source_type}\" cannot be cast to \"BIGINT\" due to an overflow. Use `try_cast` to tolerate overflow and return NULL instead."
    )
}

/// Spark's DOUBLE -> BIGINT cast: Rust's `as` matches the JVM's `(long) double`
/// (NaN -> 0, +/-Infinity -> i64::MAX/MIN, truncate toward zero, finite
/// out-of-range saturates). ANSI mode instead raises CAST_OVERFLOW for any value
/// outside the BIGINT range — NaN, +/-Infinity, and finite overflow alike.
pub(crate) fn double_to_i64(value: f64, ansi: bool) -> Result<i64> {
    if ansi && overflows_i64(value) {
        // `toSQLValue(Double)` prints NaN and the infinities bare; only a finite value gets `D`.
        let shown = match value.is_finite() {
            true => format!("{}D", spark_f64_to_string(value)),
            false => spark_f64_to_string(value),
        };
        return Err(cast_overflow_err(&shown, "DOUBLE"));
    }
    Ok(value as i64)
}

/// Spark's FLOAT -> BIGINT cast. A float widens to a double exactly, so the range check is the
/// same as for a DOUBLE; only the error names the FLOAT and prints it as `Float.toString` does.
pub(crate) fn float_to_i64(value: f32, ansi: bool) -> Result<i64> {
    if ansi && overflows_i64(f64::from(value)) {
        return Err(cast_overflow_err(&spark_f32_to_string(value), "FLOAT"));
    }
    Ok(value as i64)
}

fn overflows_i64(value: f64) -> bool {
    // Spark checks `Math.floor(x) <= Long.MaxValue && Math.ceil(x) >= Long.MinValue` with both
    // bounds promoted to double (`Cast.scala:2031`). `Long.MaxValue` rounds up to `2^63`, so
    // `2^63` itself is accepted and saturates to `i64::MAX`; the next f64 above it overflows.
    // The comparisons also cover +/-Infinity; NaN needs an explicit check since all NaN
    // comparisons are false.
    const I64_UPPER_BOUND: f64 = 9223372036854775808.0;
    value.is_nan() || value.floor() > I64_UPPER_BOUND || value.ceil() < i64::MIN as f64
}

fn bin_float_array(array: &PrimitiveArray<Float64Type>, ansi: bool) -> Result<StringArray> {
    let mut builder = StringBuilder::with_capacity(array.len(), 0);
    let mut scratch = String::with_capacity(64);
    for v in array.iter() {
        match v {
            None => builder.append_null(),
            Some(value) => {
                scratch.clear();
                bin_into(double_to_i64(value, ansi)?, &mut scratch);
                builder.append_value(&scratch);
            }
        }
    }
    Ok(builder.finish())
}

/// Parse a Spark-style string to i64. When `ansi` is true, only strict
/// integer literals (no decimal point) are accepted — matching Spark's
/// CAST_INVALID_INPUT semantics under `spark.sql.ansi.enabled = true`.
/// When `ansi` is false, decimals are tolerated and truncated toward zero
/// to mirror Spark's lenient cast behavior.
fn parse_spark_string_i64_with_mode(value: &str, ansi: bool) -> Option<i64> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if let Ok(value) = value.parse::<i64>() {
        return Some(value);
    }
    if ansi {
        // Under ANSI, anything that didn't parse strictly is a cast failure.
        return None;
    }

    let (negative, value) = match value.as_bytes()[0] {
        b'+' => (false, &value[1..]),
        b'-' => (true, &value[1..]),
        _ => (false, value),
    };
    let (integer, fraction) = value.split_once('.')?;
    if fraction.contains('.') || (integer.is_empty() && fraction.is_empty()) {
        return None;
    }
    if !integer
        .bytes()
        .chain(fraction.bytes())
        .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    if integer.trim_start_matches('0').len() > 19 {
        return None;
    }

    let magnitude = integer.bytes().try_fold(0_u128, |acc, b| {
        acc.checked_mul(10)?.checked_add((b - b'0') as u128)
    })?;
    if negative {
        match magnitude {
            0..=9223372036854775807 => Some(-(magnitude as i64)),
            9223372036854775808 => Some(i64::MIN),
            _ => None,
        }
    } else if magnitude <= i64::MAX as u128 {
        Some(magnitude as i64)
    } else {
        None
    }
}

fn bin(value: i64) -> String {
    let mut s = String::with_capacity(64);
    bin_into(value, &mut s);
    s
}
