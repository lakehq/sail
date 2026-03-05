use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, OffsetSizeTrait, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::as_generic_string_array;
use datafusion_common::{exec_err, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::functions_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Soundex {
    signature: Signature,
}

impl Default for Soundex {
    fn default() -> Self {
        Self::new()
    }
}

impl Soundex {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Soundex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "soundex"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.len() != 1 {
            return exec_err!("`soundex` function requires 1 argument, got {}", args.len());
        }
        match args[0].data_type() {
            DataType::Utf8 | DataType::Utf8View => {
                make_scalar_function(soundex::<i32>, vec![])(&args)
            }
            DataType::LargeUtf8 => make_scalar_function(soundex::<i64>, vec![])(&args),
            other => {
                exec_err!("unsupported data type {other:?} for function `soundex`")
            }
        }
    }
}

/// Represents character categories in Soundex algorithm.
enum SoundexChar {
    Code(char),
    Separator,
    Ignored,
}

/// Classifies a character for Soundex processing.
fn classify_char(c: char) -> SoundexChar {
    match c.to_ascii_uppercase() {
        'B' | 'F' | 'P' | 'V' => SoundexChar::Code('1'),
        'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => SoundexChar::Code('2'),
        'D' | 'T' => SoundexChar::Code('3'),
        'L' => SoundexChar::Code('4'),
        'M' | 'N' => SoundexChar::Code('5'),
        'R' => SoundexChar::Code('6'),
        'H' | 'W' => SoundexChar::Ignored,
        _ => SoundexChar::Separator,
    }
}

/// Computes the 4-character Soundex code for a string.
fn compute_soundex(s: &str) -> String {
    let mut chars = s.chars().filter(|c| c.is_ascii_alphabetic());

    let first_char = match chars.next() {
        Some(c) => c.to_ascii_uppercase(),
        None => return "".to_string(),
    };

    let mut result = String::with_capacity(4);
    result.push(first_char);

    let mut last_code = match classify_char(first_char) {
        SoundexChar::Code(c) => Some(c),
        _ => None,
    };

    for c in chars {
        if result.len() >= 4 {
            break;
        }

        match classify_char(c) {
            SoundexChar::Code(code) => {
                if last_code != Some(code) {
                    result.push(code);
                }
                last_code = Some(code);
            }
            SoundexChar::Separator => {
                last_code = None;
            }
            SoundexChar::Ignored => {}
        }
    }

    while result.len() < 4 {
        result.push('0');
    }

    result
}

/// Applies Soundex to each element in a string array.
fn soundex<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let str_array = as_generic_string_array::<T>(&args[0])?;

    let result = str_array
        .iter()
        .map(|opt_str| opt_str.map(compute_soundex))
        .collect::<StringArray>();

    Ok(Arc::new(result) as ArrayRef)
}
