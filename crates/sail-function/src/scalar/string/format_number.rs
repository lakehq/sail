use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::as_float64_array;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

/// Formats a number to a string with comma grouping or a DecimalFormat pattern.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FormatNumber {
    signature: Signature,
}

impl Default for FormatNumber {
    fn default() -> Self {
        Self::new()
    }
}

impl FormatNumber {
    /// Creates a new `FormatNumber` instance.
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for FormatNumber {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "format_number"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.len() != 2 {
            return exec_err!("`format_number` requires 2 arguments, got {}", args.len());
        }

        match &args[1] {
            ColumnarValue::Scalar(s) => match s {
                ScalarValue::Int8(Some(d)) => {
                    format_with_scalar_spec(&args[0], |v| format_number_fixed(v, *d as i32))
                }
                ScalarValue::Int16(Some(d)) => {
                    format_with_scalar_spec(&args[0], |v| format_number_fixed(v, *d as i32))
                }
                ScalarValue::Int32(Some(d)) => {
                    format_with_scalar_spec(&args[0], |v| format_number_fixed(v, *d))
                }
                ScalarValue::Int64(Some(d)) => {
                    format_with_scalar_spec(&args[0], |v| format_number_fixed(v, *d as i32))
                }
                ScalarValue::Utf8(Some(pattern)) => {
                    format_with_scalar_spec(&args[0], |v| Some(format_number_pattern(v, pattern)))
                }
                ScalarValue::Int8(None)
                | ScalarValue::Int16(None)
                | ScalarValue::Int32(None)
                | ScalarValue::Int64(None)
                | ScalarValue::Utf8(None) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                other => exec_err!(
                    "`format_number` second argument must be INT or STRING, got {}",
                    other.data_type()
                ),
            },
            ColumnarValue::Array(arr) => {
                let dt = arr.data_type();
                if dt.is_integer() {
                    format_with_per_row_decimal_places(&args[0], arr)
                } else if matches!(
                    dt,
                    DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
                ) {
                    format_with_per_row_pattern(&args[0], arr)
                } else {
                    exec_err!(
                        "`format_number` second argument must be INT or STRING, got {:?}",
                        dt
                    )
                }
            }
        }
    }
}

/// Formats a number with `d` decimal places and comma-separated thousands.
fn format_number_fixed(value: f64, decimal_places: i32) -> Option<String> {
    if decimal_places < 0 {
        return None;
    }
    let d = decimal_places as usize;
    let rounded = format!("{:.prec$}", value, prec = d);
    Some(insert_commas(&rounded))
}

/// Inserts comma grouping into the integer part of a formatted number string.
fn insert_commas(s: &str) -> String {
    let (integer_part, decimal_part) = match s.find('.') {
        Some(pos) => (&s[..pos], Some(&s[pos..])),
        None => (s, None),
    };

    let negative = integer_part.starts_with('-');
    let digits = if negative {
        &integer_part[1..]
    } else {
        integer_part
    };

    let mut result = String::with_capacity(s.len() + digits.len() / 3);
    if negative {
        result.push('-');
    }

    let len = digits.len();
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }

    if let Some(dec) = decimal_part {
        result.push_str(dec);
    }

    result
}

/// Formats a number using a Java DecimalFormat-style pattern string.
fn format_number_pattern(value: f64, pattern: &str) -> String {
    let has_grouping = pattern.contains(',');
    let frac = pattern.rfind('.').map(|pos| &pattern[pos + 1..]);
    let decimal_digits = frac.map_or(0, |f| f.chars().filter(|c| *c == '#' || *c == '0').count());
    let min_decimal_digits = frac.map_or(0, |f| f.chars().filter(|c| *c == '0').count());

    let formatted = format!("{:.prec$}", value, prec = decimal_digits);

    let trimmed = if decimal_digits > min_decimal_digits {
        let (int_part, dec_part) = formatted.split_once('.').unwrap_or((&formatted, ""));
        let mut dec_chars: Vec<char> = dec_part.chars().collect();

        while dec_chars.len() > min_decimal_digits && dec_chars.last() == Some(&'0') {
            dec_chars.pop();
        }

        if dec_chars.is_empty() {
            int_part.to_string()
        } else {
            format!("{}.{}", int_part, dec_chars.iter().collect::<String>())
        }
    } else {
        formatted
    };

    if has_grouping {
        insert_commas(&trimmed)
    } else {
        trimmed
    }
}

/// Formats numbers using a single scalar format spec broadcast across all rows.
fn format_with_scalar_spec(
    number: &ColumnarValue,
    fmt: impl Fn(f64) -> Option<String>,
) -> Result<ColumnarValue> {
    match number {
        ColumnarValue::Scalar(scalar) => {
            let value = scalar_to_f64(scalar)?;
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                value.and_then(&fmt),
            )))
        }
        ColumnarValue::Array(arr) => {
            let f64_arr = cast_arrow_array_to_f64(arr)?;
            let result: StringArray = f64_arr.iter().map(|opt| opt.and_then(&fmt)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

/// Formats numbers where each row has its own decimal places spec.
fn format_with_per_row_decimal_places(
    number: &ColumnarValue,
    decimal_arr: &ArrayRef,
) -> Result<ColumnarValue> {
    let casted = datafusion::arrow::compute::cast(decimal_arr, &DataType::Int32)?;
    let d_arr = casted
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "Failed to cast decimal places to Int32Array".to_string(),
            )
        })?;

    match number {
        ColumnarValue::Array(arr) => {
            let f64_arr = cast_arrow_array_to_f64(arr)?;
            let result: StringArray = f64_arr
                .iter()
                .zip(d_arr.iter())
                .map(|(v_opt, d_opt)| match (v_opt, d_opt) {
                    (Some(v), Some(d)) => format_number_fixed(v, d),
                    _ => None,
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        ColumnarValue::Scalar(scalar) => {
            let value = scalar_to_f64(scalar)?;
            let result: StringArray = d_arr
                .iter()
                .map(|d_opt| match (value, d_opt) {
                    (Some(v), Some(d)) => format_number_fixed(v, d),
                    _ => None,
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

/// Formats numbers where each row has its own pattern string spec.
fn format_with_per_row_pattern(
    number: &ColumnarValue,
    pattern_arr: &ArrayRef,
) -> Result<ColumnarValue> {
    let casted = datafusion::arrow::compute::cast(pattern_arr, &DataType::Utf8)?;
    let p_arr = casted
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "Failed to cast pattern to StringArray".to_string(),
            )
        })?;

    match number {
        ColumnarValue::Array(arr) => {
            let f64_arr = cast_arrow_array_to_f64(arr)?;
            let result: StringArray = f64_arr
                .iter()
                .zip(p_arr.iter())
                .map(|(v_opt, p_opt)| match (v_opt, p_opt) {
                    (Some(v), Some(p)) => Some(format_number_pattern(v, p)),
                    _ => None,
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        ColumnarValue::Scalar(scalar) => {
            let value = scalar_to_f64(scalar)?;
            let result: StringArray = p_arr
                .iter()
                .map(|p_opt| match (value, p_opt) {
                    (Some(v), Some(p)) => Some(format_number_pattern(v, p)),
                    _ => None,
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

/// Extracts an f64 value from a ScalarValue.
fn scalar_to_f64(scalar: &ScalarValue) -> Result<Option<f64>> {
    match scalar {
        ScalarValue::Float64(v) => Ok(*v),
        ScalarValue::Float32(v) => Ok(v.map(|x| x as f64)),
        ScalarValue::Int8(v) => Ok(v.map(|x| x as f64)),
        ScalarValue::Int16(v) => Ok(v.map(|x| x as f64)),
        ScalarValue::Int32(v) => Ok(v.map(|x| x as f64)),
        ScalarValue::Int64(v) => Ok(v.map(|x| x as f64)),
        ScalarValue::UInt8(v) => Ok(v.map(|x| x as f64)),
        ScalarValue::UInt16(v) => Ok(v.map(|x| x as f64)),
        ScalarValue::UInt32(v) => Ok(v.map(|x| x as f64)),
        ScalarValue::UInt64(v) => Ok(v.map(|x| x as f64)),
        ScalarValue::Decimal128(v, _, scale) => Ok(v.map(|x| x as f64 / 10f64.powi(*scale as i32))),
        ScalarValue::Decimal256(v, _, scale) => match v {
            Some(x) => {
                let f = x.to_string().parse::<f64>().map_err(|e| {
                    datafusion_common::DataFusionError::Internal(format!(
                        "failed to parse Decimal256 as f64: {e}"
                    ))
                })?;
                Ok(Some(f / 10f64.powi(*scale as i32)))
            }
            None => Ok(None),
        },
        ScalarValue::Null => Ok(None),
        other => exec_err!(
            "`format_number` first argument must be numeric, got {}",
            other.data_type()
        ),
    }
}

/// Casts an Arrow array to Float64Array using Arrow's cast kernel.
fn cast_arrow_array_to_f64(arr: &ArrayRef) -> Result<Float64Array> {
    let casted = datafusion::arrow::compute::cast(arr, &DataType::Float64)?;
    let f64_arr = as_float64_array(&casted)?;
    Ok(f64_arr.clone())
}
