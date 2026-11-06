// [CREDIT]: https://github.com/apache/datafusion/blob/90b55e38b5de8e74a17e24bf7ea7259cbcdab022/datafusion/functions/src/string/btrim.rs

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::DataType;
use datafusion_common::types::logical_string;
use datafusion_common::{Result, exec_err};
use datafusion_expr::function::Hint;
use datafusion_expr::{
    Coercion, ColumnarValue, EncodingPreservation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, TypeSignatureClass, Volatility,
};

use crate::functions_utils::make_scalar_function;
use crate::scalar::string::common::{TrimBoth, general_trim};

/// Returns the longest string with leading and trailing characters removed. If the characters are not specified, spaces are removed.
/// btrim('xyxtrimyyx', 'xyz') = 'trim'
fn btrim(args: &[ArrayRef]) -> Result<ArrayRef> {
    let args = if args.len() > 1 {
        let arg1 = arrow::compute::kernels::cast::cast(&args[1], args[0].data_type())?;
        vec![Arc::clone(&args[0]), arg1]
    } else {
        args.to_owned()
    };
    match args[0].data_type() {
        DataType::Utf8 => general_trim::<i32, TrimBoth>(&args, false),
        DataType::LargeUtf8 => general_trim::<i64, TrimBoth>(&args, false),
        DataType::Utf8View => general_trim::<i32, TrimBoth>(&args, true),
        DataType::Dictionary(_, _) => {
            let dictionary = args[0].as_any_dictionary();
            let trimmed = btrim(&[Arc::clone(dictionary.values())])?;
            Ok(dictionary.with_values(trimmed))
        }
        other => exec_err!(
            "Unsupported data type {other:?} for function btrim, expected Utf8, LargeUtf8 or Utf8View."
        ),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BTrimFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for BTrimFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl BTrimFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string()))
                            .with_encoding_preservation(EncodingPreservation::dictionary()),
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("trim")],
        }
    }
}

impl ScalarUDFImpl for BTrimFunc {
    fn name(&self) -> &str {
        // IMPORTANT: Do not change the name. DataFusion optimizer rules find these functions by name.
        "btrim"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(btrim, vec![Hint::Pad, Hint::AcceptsSingular])(&args.args)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray, StringViewArray};
    use arrow::datatypes::DataType::{Utf8, Utf8View};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::functions_utils::tests::test_function;
    use crate::scalar::string::btrim::BTrimFunc;

    #[test]
    fn test_functions() {
        // String view cases for checking normal logic
        test_function!(
            BTrimFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                String::from("alphabet  ")
            )))],
            Ok(Some("alphabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            BTrimFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                String::from("  alphabet  ")
            ))),],
            Ok(Some("alphabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            BTrimFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("alphabet")))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("t")))),
            ],
            Ok(Some("alphabe")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            BTrimFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("alphabet")))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("alphabe")))),
            ],
            Ok(Some("t")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            BTrimFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("alphabet")))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(None)),
            ],
            Ok(None),
            &str,
            Utf8View,
            StringViewArray
        );
        // Special string view case for checking unlined output(len > 12)
        test_function!(
            BTrimFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "xxxalphabetalphabetxxx"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("x")))),
            ],
            Ok(Some("alphabetalphabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        // String cases
        test_function!(
            BTrimFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                String::from("alphabet  ")
            ))),],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            BTrimFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                String::from("alphabet  ")
            ))),],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            BTrimFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("alphabet")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("t")))),
            ],
            Ok(Some("alphabe")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            BTrimFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("alphabet")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("alphabe")))),
            ],
            Ok(Some("t")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            BTrimFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("alphabet")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
    }
}
