// [CREDIT]: https://github.com/apache/datafusion/blob/90b55e38b5de8e74a17e24bf7ea7259cbcdab022/datafusion/functions/src/string/rtrim.rs

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::types::logical_string;
use datafusion_common::{Result, exec_err};
use datafusion_expr::function::Hint;
use datafusion_expr::{
    Coercion, ColumnarValue, EncodingPreservation, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignature, TypeSignatureClass, Volatility,
};

use crate::functions_utils::make_scalar_function;
use crate::scalar::string::common::{TrimRight, general_trim};

/// Returns the longest string with trailing characters removed. If the characters are not specified, spaces are removed.
/// rtrim('testxxzx', 'xyz') = 'test'
fn rtrim(args: &[ArrayRef]) -> Result<ArrayRef> {
    let args = if args.len() > 1 {
        let arg1 = arrow::compute::kernels::cast::cast(&args[1], args[0].data_type())?;
        vec![Arc::clone(&args[0]), arg1]
    } else {
        args.to_owned()
    };
    match args[0].data_type() {
        DataType::Utf8 => general_trim::<i32, TrimRight>(&args, false),
        DataType::LargeUtf8 => general_trim::<i64, TrimRight>(&args, false),
        DataType::Utf8View => general_trim::<i32, TrimRight>(&args, true),
        DataType::Dictionary(_, _) => {
            let dictionary = args[0].as_any_dictionary();
            let trimmed = rtrim(&[Arc::clone(dictionary.values())])?;
            Ok(dictionary.with_values(trimmed))
        }
        other => exec_err!(
            "Unsupported data type {other:?} for function rtrim, expected Utf8, LargeUtf8 or Utf8View."
        ),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RtrimFunc {
    signature: Signature,
}

impl Default for RtrimFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RtrimFunc {
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
        }
    }
}

impl ScalarUDFImpl for RtrimFunc {
    fn name(&self) -> &str {
        // IMPORTANT: Do not change the name. DataFusion optimizer rules find these functions by name.
        "rtrim"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    // Like Spark, the result is nullable only if an argument is nullable.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Field::new(self.name(), self.return_type(&arg_types)?, nullable).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(rtrim, vec![Hint::Pad, Hint::AcceptsSingular])(&args.args)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray, StringViewArray};
    use arrow::datatypes::DataType::{Utf8, Utf8View};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::functions_utils::tests::test_function;
    use crate::scalar::string::rtrim::RtrimFunc;

    #[test]
    fn test_functions() {
        // String view cases for checking normal logic
        test_function!(
            RtrimFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                String::from("alphabet  ")
            ))),],
            Ok(Some("alphabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            RtrimFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                String::from("  alphabet  ")
            ))),],
            Ok(Some("  alphabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            RtrimFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("alphabet")))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("t ")))),
            ],
            Ok(Some("alphabe")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            RtrimFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("alphabet")))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("alphabe")))),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            RtrimFunc::new(),
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
            RtrimFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "alphabetalphabetxxx"
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
            RtrimFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                String::from("alphabet  ")
            ))),],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RtrimFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                String::from("  alphabet  ")
            ))),],
            Ok(Some("  alphabet")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RtrimFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("alphabet")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("t ")))),
            ],
            Ok(Some("alphabe")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RtrimFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("alphabet")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("alphabe")))),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RtrimFunc::new(),
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
