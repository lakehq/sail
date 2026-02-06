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
            ColumnarValue::Scalar(ScalarValue::Int32(Some(d))) => {
                format_with_decimal_places(&args[0], *d)
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern))) => {
                format_with_pattern(&args[0], pattern)
            }
            ColumnarValue::Scalar(ScalarValue::Int32(None) | ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            ColumnarValue::Array(arr) if *arr.data_type() == DataType::Int32 => {
                format_with_decimal_places_array(&args[0], arr)
            }
            ColumnarValue::Array(arr)
                if matches!(
                    arr.data_type(),
                    DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
                ) =>
            {
                format_with_pattern_array(&args[0], arr)
            }
            other => {
                exec_err!(
                    "`format_number` second argument must be INT or STRING, got {:?}",
                    other.data_type()
                )
            }
        }
    }
}

/// Formats a number with `d` decimal places and comma-separated thousands.
fn format_number_fixed(value: f64, decimal_places: i32) -> String {
    if decimal_places < 0 {
        return "null".to_string();
    }
    let d = decimal_places as usize;
    let rounded = format!("{:.prec$}", value, prec = d);
    insert_commas(&rounded)
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

    let decimal_digits = match pattern.rfind('.') {
        Some(dot_pos) => {
            let frac = &pattern[dot_pos + 1..];
            frac.chars().filter(|c| *c == '#' || *c == '0').count()
        }
        None => 0,
    };

    let min_decimal_digits = match pattern.rfind('.') {
        Some(dot_pos) => {
            let frac = &pattern[dot_pos + 1..];
            frac.chars().filter(|c| *c == '0').count()
        }
        None => 0,
    };

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

/// Handles scalar or array first argument with a scalar decimal places value.
fn format_with_decimal_places(
    number: &ColumnarValue,
    decimal_places: i32,
) -> Result<ColumnarValue> {
    match number {
        ColumnarValue::Scalar(scalar) => {
            let value = scalar_to_f64(scalar)?;
            match value {
                Some(v) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    format_number_fixed(v, decimal_places),
                )))),
                None => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
            }
        }
        ColumnarValue::Array(arr) => {
            let f64_arr = arrow_cast_to_f64(arr)?;
            let result: StringArray = f64_arr
                .iter()
                .map(|opt| opt.map(|v| format_number_fixed(v, decimal_places)))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

/// Handles scalar or array first argument with a scalar pattern string.
fn format_with_pattern(number: &ColumnarValue, pattern: &str) -> Result<ColumnarValue> {
    match number {
        ColumnarValue::Scalar(scalar) => {
            let value = scalar_to_f64(scalar)?;
            match value {
                Some(v) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    format_number_pattern(v, pattern),
                )))),
                None => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
            }
        }
        ColumnarValue::Array(arr) => {
            let f64_arr = arrow_cast_to_f64(arr)?;
            let result: StringArray = f64_arr
                .iter()
                .map(|opt| opt.map(|v| format_number_pattern(v, pattern)))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

/// Handles array first argument with an array of decimal places.
fn format_with_decimal_places_array(
    number: &ColumnarValue,
    decimal_arr: &ArrayRef,
) -> Result<ColumnarValue> {
    let d_arr = decimal_arr
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "Expected Int32Array for decimal places".to_string(),
            )
        })?;

    match number {
        ColumnarValue::Array(arr) => {
            let f64_arr = arrow_cast_to_f64(arr)?;
            let result: StringArray = f64_arr
                .iter()
                .zip(d_arr.iter())
                .map(|(v_opt, d_opt)| match (v_opt, d_opt) {
                    (Some(v), Some(d)) => Some(format_number_fixed(v, d)),
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
                    (Some(v), Some(d)) => Some(format_number_fixed(v, d)),
                    _ => None,
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

/// Handles array first argument with an array of pattern strings.
fn format_with_pattern_array(
    number: &ColumnarValue,
    pattern_arr: &ArrayRef,
) -> Result<ColumnarValue> {
    let p_arr = pattern_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "Expected StringArray for pattern".to_string(),
            )
        })?;

    match number {
        ColumnarValue::Array(arr) => {
            let f64_arr = arrow_cast_to_f64(arr)?;
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
        ScalarValue::Decimal128(v, _, scale) => Ok(v.map(|x| x as f64 / 10f64.powi(*scale as i32))),
        ScalarValue::Null => Ok(None),
        other => exec_err!(
            "`format_number` first argument must be numeric, got {}",
            other.data_type()
        ),
    }
}

/// Casts an Arrow array to Float64Array using Arrow's cast kernel.
fn arrow_cast_to_f64(arr: &ArrayRef) -> Result<Float64Array> {
    let casted = datafusion::arrow::compute::cast(arr, &DataType::Float64)?;
    let f64_arr = as_float64_array(&casted)?;
    Ok(f64_arr.clone())
}
