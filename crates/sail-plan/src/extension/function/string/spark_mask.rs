use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{new_null_array, ArrayRef, AsArray, GenericStringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::ScalarUDFImpl;
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};

// Default character to replace upper-case characters
const MASKED_UPPERCASE: char = 'X';

// Default character to replace lower-case characters
const MASKED_LOWERCASE: char = 'x';

// Default character to replace digits
const MASKED_DIGIT: char = 'n';

#[derive(Debug)]
pub struct SparkMask {
    signature: Signature,
}

impl Default for SparkMask {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMask {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic(
                vec![
                    DataType::Utf8,
                    DataType::LargeUtf8,
                    DataType::Utf8View,
                    DataType::Null,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkMask {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_mask"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() || arg_types.len() > 5 {
            return exec_err!(
                "Spark `mask` function requires 1 to 5 arguments, got {}",
                arg_types.len()
            );
        }
        match arg_types[0] {
            DataType::Utf8 | DataType::Utf8View | DataType::Null => Ok(DataType::Utf8),
            DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
            _ => exec_err!(
                "Spark `mask` function: first arg must be string, got {}",
                arg_types[0]
            ),
        }
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
        if args.is_empty() || args.len() > 5 {
            return exec_err!(
                "Spark `mask` function requires 1 to 5 arguments, got {}",
                args.len()
            );
        }

        let masked_upper = if args.len() > 1 {
            match &args[1] {
                ColumnarValue::Scalar(ScalarValue::Utf8(string))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(string))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(string)) => {
                    if let Some(string) = string {
                        if string.chars().count() == 1 {
                            Ok(string.chars().next())
                        } else {
                            exec_err!("Spark `mask` function: second arg must be a single character, got {string}")
                        }
                    } else {
                        Ok(None)
                    }
                }
                ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
                other => exec_err!(
                    "Spark `mask` function: second arg must be literal char or null, got {}",
                    other
                ),
            }
        } else {
            Ok(Some(MASKED_UPPERCASE))
        }?;

        let masked_lower = if args.len() > 2 {
            match &args[2] {
                ColumnarValue::Scalar(ScalarValue::Utf8(string))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(string))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(string)) => {
                    if let Some(string) = string {
                        if string.chars().count() == 1 {
                            Ok(string.chars().next())
                        } else {
                            exec_err!("Spark `mask` function: third arg must be a single character, got {string}")
                        }
                    } else {
                        Ok(None)
                    }
                }
                ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
                other => exec_err!(
                    "Spark `mask` function: third arg must be char string or null, got {}",
                    other
                ),
            }
        } else {
            Ok(Some(MASKED_LOWERCASE))
        }?;

        let masked_digit = if args.len() > 3 {
            match &args[3] {
                ColumnarValue::Scalar(ScalarValue::Utf8(string))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(string))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(string)) => {
                    if let Some(string) = string {
                        if string.chars().count() == 1 {
                            Ok(string.chars().next())
                        } else {
                            exec_err!("Spark `mask` function: fourth arg must be a single character, got {string}")
                        }
                    } else {
                        Ok(None)
                    }
                }
                ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
                other => exec_err!(
                    "Spark `mask` function: fourth arg must be char string or null, got {}",
                    other
                ),
            }
        } else {
            Ok(Some(MASKED_DIGIT))
        }?;

        let masked_other = if args.len() > 4 {
            match &args[4] {
                ColumnarValue::Scalar(ScalarValue::Utf8(string))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(string))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(string)) => {
                    if let Some(string) = string {
                        if string.chars().count() == 1 {
                            Ok(string.chars().next())
                        } else {
                            exec_err!("Spark `mask` function: fifth arg must be a single character, got {string}")
                        }
                    } else {
                        Ok(None)
                    }
                }
                ColumnarValue::Scalar(ScalarValue::Null) => Ok(None),
                other => exec_err!(
                    "Spark `mask` function: fifth arg must be literal string or null, got {}",
                    other
                ),
            }
        } else {
            Ok(None)
        }?;

        match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(string))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(string)) => {
                if let Some(string) = string {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(mask(
                        string.as_str(),
                        masked_upper,
                        masked_lower,
                        masked_digit,
                        masked_other,
                    )))))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                }
            }
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(string)) => {
                if let Some(string) = string {
                    Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(mask(
                        string.as_str(),
                        masked_upper,
                        masked_lower,
                        masked_digit,
                        masked_other,
                    )))))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)))
                }
            }
            ColumnarValue::Scalar(ScalarValue::Null) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            ColumnarValue::Array(array) => {
                let result = match array.data_type() {
                    DataType::Utf8 => {
                        let string_array = array.as_string::<i32>();
                        let mut builder: GenericStringBuilder<i32> = GenericStringBuilder::new();
                        string_array.iter().for_each(|string| {
                            if let Some(string) = string {
                                builder.append_value(mask(
                                    string,
                                    masked_upper,
                                    masked_lower,
                                    masked_digit,
                                    masked_other,
                                ))
                            } else {
                                builder.append_null()
                            }
                        });
                        Ok(Arc::new(builder.finish()) as ArrayRef)
                    }
                    DataType::LargeUtf8 => {
                        let string_array = array.as_string::<i64>();
                        let mut builder: GenericStringBuilder<i64> = GenericStringBuilder::new();
                        string_array.iter().for_each(|string| {
                            if let Some(string) = string {
                                builder.append_value(mask(
                                    string,
                                    masked_upper,
                                    masked_lower,
                                    masked_digit,
                                    masked_other,
                                ))
                            } else {
                                builder.append_null()
                            }
                        });
                        Ok(Arc::new(builder.finish()) as ArrayRef)
                    }
                    DataType::Utf8View => {
                        let string_array = array.as_string_view();
                        let mut builder: GenericStringBuilder<i32> = GenericStringBuilder::new();
                        string_array.iter().for_each(|string| {
                            if let Some(string) = string {
                                builder.append_value(mask(
                                    string,
                                    masked_upper,
                                    masked_lower,
                                    masked_digit,
                                    masked_other,
                                ))
                            } else {
                                builder.append_null()
                            }
                        });
                        Ok(Arc::new(builder.finish()) as ArrayRef)
                    }
                    DataType::Null => Ok(new_null_array(&DataType::Utf8, array.len())),
                    other => exec_err!(
                        "Spark `mask` function: first arg must be string, got {}",
                        other
                    ),
                }?;
                Ok(ColumnarValue::Array(result))
            }
            other => exec_err!(
                "Spark `mask` function: first arg must be string, got {}",
                other
            ),
        }
    }
}

fn mask(
    string: &str,
    masked_upper: Option<char>,
    masked_lower: Option<char>,
    masked_digit: Option<char>,
    masked_other: Option<char>,
) -> String {
    string
        .chars()
        .map(|c| {
            if c.is_uppercase() {
                masked_upper.unwrap_or(c)
            } else if c.is_lowercase() {
                masked_lower.unwrap_or(c)
            } else if c.is_numeric() {
                masked_digit.unwrap_or(c)
            } else {
                masked_other.unwrap_or(c)
            }
        })
        .collect()
}
