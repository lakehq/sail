use arrow::datatypes::DataType;
use datafusion_common::{exec_err, not_impl_err, Result, ScalarValue};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};
use std::any::Any;

#[derive(Debug)]
pub struct SparkToChar {
    signature: Signature,
}

/// Represents parsed format string
#[derive(Debug)]
struct FormatSpec {
    tokens: Vec<FormatToken>,
}

/// Supported format tokens (partial set)
#[derive(Debug)]
enum FormatToken {
    DigitZero,       // '0'
    DigitNine,       // '9'
    DecimalPoint,    // '.'
    GroupSeparator,  // ','
    Literal(char),   // other characters
}

impl SparkToChar {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Immutable),
        }
    }
}
impl Default for SparkToChar {
    fn default() -> Self {
        Self::new()
    }
}

/// Parses a format string into tokens
fn parse_format_string(fmt: &str) -> Result<FormatSpec> {
    let mut tokens = Vec::new();
    for c in fmt.chars() {
        match c {
            '0' => tokens.push(FormatToken::DigitZero),
            '9' => tokens.push(FormatToken::DigitNine),
            '.' => tokens.push(FormatToken::DecimalPoint),
            ',' => tokens.push(FormatToken::GroupSeparator),
            other => tokens.push(FormatToken::Literal(other)),
        }
    }

    Ok(FormatSpec { tokens })
}

/// Formats a number using parsed tokens
fn format_number(value: f64, spec: &FormatSpec) -> Result<String> {
    let mut result = String::new();

    for token in &spec.tokens {
        match token {
            FormatToken::DigitZero | FormatToken::DigitNine => {
                // Replace with real logic to extract digits
                result.push('#');
            }
            FormatToken::DecimalPoint => result.push('.'),
            FormatToken::GroupSeparator => result.push(','),
            FormatToken::Literal(c) => result.push(*c),
        }
    }
    // Replace '#' with actual digits later
    // For now return stub output
    Ok(result)
}

fn format_scalar_value(value: &ScalarValue, fmt: &str) -> Result<String> {
    let number = match value {
        ScalarValue::Int64(Some(v)) => *v as f64,
        ScalarValue::Float64(Some(v)) => *v,
        ScalarValue::Decimal128(Some(v), _, _) => *v as f64,
        _ => return not_impl_err!("")
    };
    let spec = parse_format_string(fmt)?;
    let formatted = format_number(number, &spec)?;
    Ok(formatted)
}

impl ScalarUDFImpl for SparkToChar {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_to_char"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = args.args;
        if args.len() != 2 {
            return exec_err!(
                "Spark `to_char` function requires 1 or 2 arguments, got {}",
                args.len()
            );
        }

        match (&args[0], &args[1]) {
            (ColumnarValue::Scalar(v),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(fmt))),
            ) => {
                if v.is_null() {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                } else {
                    let formatted = format_scalar_value(v, &fmt)?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(formatted))))
                }
            }
            _ => not_impl_err!("to_char currently supports only scalar inputs with literal format string")
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::ScalarValue;

    #[test]
    fn test_format_scalar_value_basic() {
        let input = ScalarValue::Int64(Some(45));
        let fmt = "0000.00";

        let result = format_scalar_value(&input, fmt).unwrap();
        assert_eq!(result, "0045.00"); // Expected output
    }

    fn test_format_scalar_value_optional_digits_ok() {
        let input = ScalarValue::Int64(Some(7));
        let fmt = "999.9";
        let result = format_scalar_value(&input, fmt).unwrap();
        assert_eq!(result, "  7.0"); // ✅ This should pass
    }
}