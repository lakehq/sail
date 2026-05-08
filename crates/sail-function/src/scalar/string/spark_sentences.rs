use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, ListArray, ListBuilder, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::Result;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, TypeSignature, Volatility};

use crate::error::generic_internal_err;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSentences {
    signature: Signature,
}

impl Default for SparkSentences {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSentences {
    pub const NAME: &'static str = "sentences";

    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::String(1),
                    TypeSignature::String(2),
                    TypeSignature::String(3),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkSentences {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new_list_field(
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        // TODO: Support locale-aware sentence splitting using the lang and country parameters.
        let arr = match &args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };
        let result = sentences_inner(&arr)?;
        let is_scalar = matches!(&args[0], ColumnarValue::Scalar(_));
        if is_scalar {
            let scalar = datafusion_common::ScalarValue::try_from_array(&result, 0)?;
            Ok(ColumnarValue::Scalar(scalar))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

fn sentences_inner(arr: &ArrayRef) -> Result<ArrayRef> {
    // Normalize to Utf8 to support Utf8, LargeUtf8, and Utf8View inputs.
    let utf8_arr = cast(arr, &DataType::Utf8)?;
    let string_arr = utf8_arr.as_string_opt::<i32>().ok_or_else(|| {
        generic_internal_err(
            SparkSentences::NAME,
            "Could not downcast argument to Utf8 string array",
        )
    })?;

    let inner_builder = ListBuilder::new(StringBuilder::new());
    let mut builder = ListBuilder::new(inner_builder);

    let len = utf8_arr.len();
    for i in 0..len {
        if utf8_arr.is_null(i) {
            builder.append_null();
        } else {
            let text = string_arr.value(i);
            let sentences = split_into_sentences(text);
            let inner = builder.values();
            for sentence_words in &sentences {
                let word_builder = inner.values();
                for word in sentence_words {
                    word_builder.append_value(word);
                }
                inner.append(true);
            }
            builder.append(true);
        }
    }
    let array: ListArray = builder.finish();
    Ok(Arc::new(array))
}

/// Split text into sentences, then each sentence into words.
/// Approximates Java's BreakIterator behavior:
/// - Sentences are split immediately after `.`, `!`, `?`; any following whitespace is skipped
/// - Words are sequences of Unicode letters or digits
fn split_into_sentences(text: &str) -> Vec<Vec<String>> {
    if text.is_empty() {
        return vec![];
    }

    let mut sentences = Vec::new();
    let mut current_pos = 0;
    let chars: Vec<char> = text.chars().collect();

    while current_pos < chars.len() {
        let mut end = current_pos;
        while end < chars.len() {
            if matches!(chars[end], '.' | '!' | '?') {
                end += 1;
                break;
            }
            end += 1;
        }

        let sentence: String = chars[current_pos..end].iter().collect();
        let words = extract_words(&sentence);
        if !words.is_empty() {
            sentences.push(words);
        }

        while end < chars.len() && chars[end].is_whitespace() {
            end += 1;
        }
        current_pos = end;
    }

    sentences
}

/// Extract words from a sentence.
/// Words are sequences of Unicode letters or digits.
fn extract_words(text: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current_word = String::new();

    for ch in text.chars() {
        if ch.is_alphanumeric() {
            current_word.push(ch);
        } else if !current_word.is_empty() {
            words.push(std::mem::take(&mut current_word));
        }
    }
    if !current_word.is_empty() {
        words.push(current_word);
    }

    words
}
